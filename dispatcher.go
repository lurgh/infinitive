package main

import (
	"fmt"
	"time"
	"strings"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type EventListener struct {
	ch chan []byte
}

type discoveryTopicSensor struct {
	Topic       string    `json:"state_topic"`
	Name        string    `json:"name"`
	Device_class string   `json:"device_class,omitempty"`
	State_class string   `json:"state_class,omitempty"`
	UoM         string   `json:"unit_of_measurement,omitempty"`
	Unique_id   string    `json:"unique_id"`
}

type discoveryTopicButton struct {
	Topic       string    `json:"command_topic"`
	Name        string    `json:"name"`
	Payload_Press string  `json:"payload_press,omitempty"`
	Qos         int    `json:"qos,omitempty"`
	Retain      bool    `json:"retain,omitempty"`
	Unique_id   string    `json:"unique_id"`
}

type EventDispatcher struct {
	listeners  map[*EventListener]bool
	broadcast  chan []byte
	register   chan *EventListener
	deregister chan *EventListener
}

type MqttEvent struct {
	topic string
	value string
}

type MqttConn struct {
	url string	// "tcp://host.com:1883"
	password string
}

var Dispatcher *EventDispatcher = newEventDispatcher()

var mqttClient mqtt.Client

var mqttZoneFlags [8]bool

func newEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		broadcast:  make(chan []byte, 64),
		register:   make(chan *EventListener),
		deregister: make(chan *EventListener),
		listeners:  make(map[*EventListener]bool),
	}
}

func (d *EventDispatcher) dispatch(msg []byte) {
	d.broadcast <- msg
}

type broadcastEvent struct {
	Source string      `json:"source"`
	Data   interface{} `json:"data"`
}

func serializeEvent(source string, data interface{}) []byte {
	msg, _ := json.Marshal(&broadcastEvent{Source: source, Data: data})
	return msg
}

func (d *EventDispatcher) broadcastEvent(source string, data interface{}) {
	if source[0:5] == "mqtt/" {
		if mqttClient != nil {
			topic := source[5:]
			value := fmt.Sprintf("%v", data)
			log.Infof("MQTT PUB: %s -> %s", topic, value)
			_ = mqttClient.Publish(topic, 0, true, value)
		}
	} else {
		d.broadcast <- serializeEvent(source, data)
	}
}

func (h *EventDispatcher) run() {
	for {
		select {
		case listener := <-h.register:
			h.listeners[listener] = true
		case listener := <-h.deregister:
			if _, ok := h.listeners[listener]; ok {
				delete(h.listeners, listener)
				close(listener.ch)
			}
		case message := <-h.broadcast:
			for listener := range h.listeners {
				select {
				case listener.ch <- message:
				default:
					close(listener.ch)
					delete(h.listeners, listener)
				}
			}
		}
	}
}

// handle messages
// topics: infinitive/SETTING/set (global)
//	infinitive/zone/X/SETTING/set (zone X)
func  mqttMessageHandler(client mqtt.Client, msg mqtt.Message) {
	log.Infof("MQTT: Received message: %s from topic: %s", msg.Payload(), msg.Topic())

	ts := strings.Split(msg.Topic(), "/")
	ps := fmt.Sprintf("%s", msg.Payload())

	if len(ts) < 3 || ts[0] != instanceName || ts[len(ts)-1] != "set" {
		log.Errorf("mqtt received unexpected topic '%s'", msg.Topic())
	} else if len(ts) == 5 && ts[1] == "zone" {
		// zone-based
		if ps[len(ps)-2:len(ps)-1] == "." {
			ps = ps[0:len(ps)-2]
		}
		_ = putConfig(ts[2], ts[3], ps)
	} else if len(ts) == 4 && ts[1] == "vacation" {
		_ = putVacationConfig(ts[2], ps)
	} else if len(ts) == 3 {
		// global
		_ = putConfig("0", ts[1], ps)
	} else {
		log.Errorf("mqtt received malformed topic '%s'", msg.Topic())
	}
}

// set up for async connect/reconnect (for robustness across restarts on eithesride) 
func ConnectMqtt(url string, password string) {

	// enable logging
	// mqtt.ERROR = log.StandardLogger()
	// mqtt.CRITICAL = log.StandardLogger()
	// mqtt.WARN = log.StandardLogger()
	// mqtt.DEBUG = log.StandardLogger()

	// set mqtt client options
	co := mqtt.NewClientOptions()
	co.AddBroker(url)
	co.SetPassword(password)
	co.SetClientID(instanceName + "_mqtt_client")
	co.SetOnConnectHandler(mqttOnConnect)
	co.SetConnectionLostHandler(func(cl mqtt.Client, err error) {log.Info("MQTT: Connection lost: ", err.Error())})
	co.SetReconnectingHandler(func(cl mqtt.Client, _ *mqtt.ClientOptions) {log.Info("MQTT: Trying to reconnect")})
	co.SetConnectRetry(true)
	co.SetConnectRetryInterval(time.Minute)
	co.SetAutoReconnect(true)
	co.SetMaxReconnectInterval(5 * time.Minute)

	// create client
	mqttClient = mqtt.NewClient(co)

	// start trying to connect - resolved in callbacks
	log.Info("MQTT: Start trying to connect as " + co.ClientID)
	mqttClient.Connect()
}

// on connect, subscribe to needed topics
func mqttOnConnect(cl mqtt.Client) {
	log.Info("MQTT: Connected, subscribing...")

	// subscribe for zone settings
	topic := fmt.Sprintf("%s/zone/+/+/set", instanceName)
	t := cl.Subscribe(topic, 0, mqttMessageHandler)
	t.Wait()
	if (t.Error() != nil) {
		log.Errorf("MQTT: failed to subscribe for %s: %v", topic, t.Error())
	} else {
		log.Infof("MQTT: subscribe succeeded for %s", topic)
	}

	// subscribe for vacation settings
	topic = fmt.Sprintf("%s/vacation/+/set", instanceName)
	t = cl.Subscribe(topic, 0, mqttMessageHandler)
	t.Wait()
	if (t.Error() != nil) {
		log.Errorf("MQTT: failed to subscribe for %s: %v", topic, t.Error())
	} else {
		log.Infof("MQTT: subscribe succeeded for %s", topic)
	}

	// subscribe for global settings
	topic = fmt.Sprintf("%s/+/set", instanceName)
	t = cl.Subscribe(topic, 0, mqttMessageHandler)
	t.Wait()
	if (t.Error() != nil) {
		log.Errorf("MQTT: failed to subscribe for %s: %v", topic, t.Error())
	} else {
		log.Infof("MQTT: subscribe succeeded for %s", topic)
	}

	sensors := []discoveryTopicSensor {
		{ "/outdoorTemp", "HVAC Outdoor Temperature", "temperature", "measurement", "°F", "hvac-sensors-odt" },
		{ "/humidity", "HVAC Indoor Humidity", "humidity", "measurement", "%", "hvac-sensors-hum" },
		{ "/rawMode", "HVAC Raw Mode", "", "measurement", "", "hvac-sensors-rawmode" },
		{ "/blowerRPM", "HVAC Blower RPM", "", "measurement", "RPM", "hvac-sensors-blowerrpm" },
		{ "/airflowCFM", "HVAC Airflow CFM", "", "measurement", "CFM", "hvac-sensors-aflo" },
		{ "/staticPressure", "HVAC Static Pressure", "distance", "measurement", "in", "hvac-sensors-ahsp" },
		{ "/coolStage", "HVAC Cool Stage", "", "measurement", "", "hvac-sensors-acstage" },
		{ "/heatStage", "HVAC Heat Stage", "", "measurement", "", "hvac-sensors-heatstage" },
		{ "/action", "HVAC Action", "enum", "", "", "hvac-sensors-actn" },

		{ "/vacation/active", "Vacation Mode Active", "enum", "", "", "hvac-sensors-vacay-active" },  // maybe should be a binary_sensor
		{ "/vacation/days", "Vacation Mode Days Remaining", "duration", "measurement", "d", "hvac-sensors-vacay-days" },
		{ "/vacation/hours", "Vacation Mode Hours Remaining", "duration", "measurement", "h", "hvac-sensors-vacay-hours" },
		{ "/vacation/minTemp", "Vacation Mode Minimum Temperature", "temperature", "measurement", "°F", "hvac-sensors-vacay-mint" },
		{ "/vacation/maxTemp", "Vacation Mode Maximum Temperature", "temperature", "measurement", "°F", "hvac-sensors-vacay-maxt" },
		{ "/vacation/minHumidity", "Vacation Mode Minimum Humidity", "humidity", "measurement", "%", "hvac-sensors-vacay-minh" },
		{ "/vacation/maxHumidity", "Vacation Mode Maximum Humidity", "humidity", "measurement", "%", "hvac-sensors-vacay-maxh" },
		{ "/vacation/fanMode", "Vacation Mode Fan Mode", "enum", "", "", "hvac-sensors-vacay-fm" },

		// per-zone "bonus" sensors (outside of the Climate platform model)
		// TODO: these should be parametrized, maybe do along with the Climate entities)
		{ "/zone/1/damperPos", "HVAC Zone 1 Damper Postion", "", "measurement", "%", "hvac-sensors-z1-dpos" },
		{ "/zone/2/damperPos", "HVAC Zone 2 Damper Postion", "", "measurement", "%", "hvac-sensors-z2-dpos" },
		{ "/zone/1/flowWeight", "HVAC Zone 1 Airflow Weight", "", "measurement", "", "hvac-sensors-z1-fwgt" },
		{ "/zone/2/flowWeight", "HVAC Zone 2 Airflow Weight", "", "measurement", "", "hvac-sensors-z2-fwgt" },
		{ "/zone/1/overrideDurationMins", "HVAC Zone 1 Override Duration", "duration", "measurement", "min", "hvac-sensors-z1-odur" },
		{ "/zone/2/overrideDurationMins", "HVAC Zone 2 Override Duration", "duration", "measurement", "min", "hvac-sensors-z2-odur" },
	}

	buttons := []discoveryTopicButton {
		{ "/vacation/hours/set", "Vacation Cancel", "0", 0, false, "hvac-vac-can" },
		{ "/vacation/hours/set", "Vacation Add 1 Hour", "+1", 0, false, "hvac-vac-plus1" },
		{ "/vacation/hours/set", "Vacation Subtract 1 Hour", "-1", 0, false, "hvac-vac-minus1" },
		{ "/vacation/hours/set", "Vacation 1 Hour", "1", 0, false, "hvac-vac-1hr" },
		{ "/vacation/hours/set", "Vacation 2 Hours", "2", 0, false, "hvac-vac-2hr" },
		{ "/vacation/hours/set", "Vacation 4 Hours", "4", 0, false, "hvac-vac-4hr" },
		{ "/vacation/hours/set", "Vacation 8 Hours", "8", 0, false, "hvac-vac-8hr" },
		{ "/vacation/hours/set", "Vacation 12 Hours", "12", 0, false, "hvac-vac-12hr" },
		{ "/vacation/hours/set", "Vacation 18 Hours", "18", 0, false, "hvac-vac-18hr" },
		{ "/vacation/days/set", "Vacation Add 1 Day", "+1", 0, false, "hvac-vac-plus1d" },
		{ "/vacation/days/set", "Vacation Subtract 1 Day", "-1", 0, false, "hvac-vac-minus1d" },
		{ "/vacation/days/set", "Vacation 1 Day", "1", 0, false, "hvac-vac-1d" },
		{ "/vacation/days/set", "Vacation 2 Days", "2", 0, false, "hvac-vac-2d" },
		{ "/vacation/days/set", "Vacation 3 Days", "3", 0, false, "hvac-vac-3d" },
		{ "/vacation/days/set", "Vacation 4 Days", "4", 0, false, "hvac-vac-4d" },
		{ "/vacation/days/set", "Vacation 5 Days", "5", 0, false, "hvac-vac-5d" },
		{ "/vacation/days/set", "Vacation 6 Days", "6", 0, false, "hvac-vac-6d" },
		{ "/vacation/days/set", "Vacation 7 Days", "7", 0, false, "hvac-vac-7d" },
	}

	// write discovery topics for HA sensors
	/*
	_ = cl.Publish("homeassistant/sensor/infinitive/hs/config", 0, true,
		`{"state_topic": "infinitive/heatStage","state_class": "measurement",
		"name": "Heat Stage",
		"unique_id": "hvac-sensors-heatstage"}`)
		*/
	for _, v := range sensors {
		v.Topic = instanceName + v.Topic
		if instanceName != "infinitive" { v.Unique_id = instanceName + "-" + v.Unique_id }
		j, err := json.Marshal(&v)
		log.Infof("MQTT PUB %v: %s", err, j)
		if err == nil {
			_ = cl.Publish("homeassistant/sensor/infinitive/" + v.Unique_id + "/config", 0, true, j)
		}
	}

	// write discovery topics for HA buttons
	for _, v := range buttons {
		v.Topic = instanceName + v.Topic
		if instanceName != "infinitive" { v.Unique_id = instanceName + "-" + v.Unique_id }
		j, err := json.Marshal(&v)
		log.Infof("MQTT PUB %v: %s", err, j)
		if err == nil {
			_ = cl.Publish("homeassistant/button/infinitive/" + v.Unique_id + "/config", 0, true, j)
		}
	}

	// flush the "zone discovery written" flags
	mqttZoneFlags = [8]bool{}

	// flush the MQTT value cache
	mqttCache.clear()
}

// post discovery message if needed
func mqttDiscoverZone(zi int, zn string, tu uint8) {

	if mqttZoneFlags[zi] || !mqttClient.IsConnected() {
		return
	}

	// write discovery topics for a HA climate entity
	climateTemplate := `{
	"name": "%[1]s",
	"modes": [ "off", "cool", "heat", "auto" ],
	"fan_modes": [ "high", "med", "low", "auto" ],
	"preset_modes": [ "hold", "vacation" ],
	"current_humidity_topic": "%[4]s/zone/%[2]d/humidity",
	"current_temperature_topic": "%[4]s/zone/%[2]d/currentTemp",
	"fan_mode_state_topic": "%[4]s/zone/%[2]d/fanMode",
	"mode_state_topic": "%[4]s/mode",
	"action_topic": "%[4]s/action",
	"temperature_high_state_topic": "%[4]s/zone/%[2]d/coolSetpoint",
	"temperature_low_state_topic": "%[4]s/zone/%[2]d/heatSetpoint",
	"fan_mode_command_topic": "%[4]s/zone/%[2]d/fanMode/set",
	"mode_command_topic": "%[4]s/mode/set",
	"temperature_high_command_topic": "%[4]s/zone/%[2]d/coolSetpoint/set",
	"temperature_low_command_topic": "%[4]s/zone/%[2]d/heatSetpoint/set",
	"preset_mode_state_topic": "%[4]s/zone/%[2]d/preset",
	"preset_mode_command_topic": "%[4]s/zone/%[2]d/preset/set",
	"temp_step": 1,
	"temperature_unit": "%[3]s",
	"unique_id": "%[4]s-hvac-zone-%[2]d-ad"
}`

	tempu := "F"
	if tu > 0 { tempu = "C" }
	dmsg := fmt.Sprintf(climateTemplate, zn, zi+1, tempu, instanceName)
	duid := fmt.Sprintf("climate-zone-%d", zi+1)
	if instanceName != "infinitive" { duid = instanceName + "-" + duid }
	log.Info("MQTT ZONE DISC: ", dmsg)
	_ = mqttClient.Publish("homeassistant/climate/infinitive/" + duid + "/config", 0, true, dmsg)

	mqttZoneFlags[zi] = true
}

func init() {
	go Dispatcher.run()
}
