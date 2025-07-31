package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type TStatZoneConfig struct {
	ZoneNumber      uint8  `json:"zoneNumber,omitempty"`
	CurrentTemp     uint8  `json:"currentTemp"`
	CurrentHumidity uint8  `json:"currentHumidity"`
	TargetHumidity  uint8  `json:"targetHumidity"`
	ZoneName	string `json:"zoneName"`
	FanMode         string `json:"fanMode"`
	Hold            *bool  `json:"hold"`
	Preset          string `json:"preset"`
	HeatSetpoint    uint8  `json:"heatSetpoint"`
	CoolSetpoint    uint8  `json:"coolSetpoint"`
	OvrdDuration	string `json:"overrideDuration"`
	OvrdDurationMins uint16 `json:"overrideDurationMins"`
	// the following are global and should be removed from per-zone but are left in for compatibility for now
	OutdoorTemp     uint8  `json:"outdoorTemp"`
	Mode            string `json:"mode"`
	Stage           uint8  `json:"stage"`
	Action          string `json:"action"`
	RawMode         uint8  `json:"rawMode"`
}

type TStatZonesConfig struct {
	Zones             []TStatZoneConfig  `json:"zones,omitempty"`
	OutdoorTemp       uint8  `json:"outdoorTemp"`
	Mode              string `json:"mode"`
	Stage             uint8  `json:"stage"`
	Action            string `json:"action"`
	RawMode           uint8  `json:"rawMode"`
	DispDOW		  uint8  `json:"dispDOW"`
	DispTime          uint16 `json:"dispTime"`
	DispZone          uint8  `json:"dispZone"`
}

type AirHandler struct {
	BlowerRPM      uint16  `json:"blowerRPM"`
	AirFlowCFM     uint16  `json:"airFlowCFM"`
	StaticPressure float32 `json:"staticPressure"`
	HeatStage      uint8   `json:"heatStage"`
	ElecHeat       bool    `json:"elecHeat"`
	Action         string  `json:"action"`
}

type HeatPump struct {
	CoilTemp    float32 `json:"coilTemp"`
	OutsideTemp float32 `json:"outsideTemp"`
	Stage       uint8   `json:"stage"`
}

type DamperPosition struct {
	DamperPos   [8]uint8 `json:"damperPosition"`
}

var zoneWeight [8]float32

type Logger struct {
	f	*os.File
	basems int64
	tds	string
}

var RLogger Logger;

var infinity *InfinityProtocol

// system instance name (default: "infinitive")
//  used as the root mqtt topic name and to unique-ify entities
var instanceName string

func holdTime(ht uint16) string {
	if ht == 0 {
		return ""
	}
	return fmt.Sprintf("%d:%02d", ht/60, ht % 60)
}

// get vacation config and status
func getVacationConfig() (*APIVacationConfig, bool) {
	vac := TStatVacationParams{}
	ok := infinity.ReadTable(devTSTAT, &vac)
	if !ok {
		return nil, false
	}

	vacAPI := vac.toAPI()
	return &vacAPI, true
}

// get config and status for all zones in one go
// this is more efficient than getting each zone separately since all the zones' data comes in one pair of serial transactions
// we only get the TstatSettings once, since it is assumed not to change without us restarting to pick it up
var _tstat_settings TStatSettings

func getZonesConfig() (*TStatZonesConfig, bool) {
	// only get the TStatSettings once after startup
	if _tstat_settings.DealerName[0] == 0 {
		log.Debugf("getZonesConfig: getting TStatSettings to determine temp units")
		ok := infinity.ReadTable(devTSTAT, &_tstat_settings)
		if !ok {
			return nil, false
		}
		log.Debugf("getZonesConfig: got temp units = %d", _tstat_settings.TempUnits)
	}

	cfg := TStatZoneParams{}
	ok := infinity.ReadTable(devTSTAT, &cfg)
	if !ok {
		return nil, false
	}

	params := TStatCurrentParams{}
	ok = infinity.ReadTable(devTSTAT, &params)
	if !ok {
		return nil, false
	}

	tstat := TStatZonesConfig{
		OutdoorTemp:       params.OutdoorAirTemp,
		Mode:              rawModeToString(params.Mode & 0xf),
		Stage:             params.Mode >> 5,
		Action:            rawActionToString(params.Mode >> 5),
		RawMode:           params.Mode,
		DispDOW:           params.DispDOW,
		DispTime:          params.DispTimeMin,
		DispZone:          params.DispZone,
	}

	zoneArr := [8]TStatZoneConfig{}

	zc := 0
	for zi := range params.ZCurrentTemp {
		if params.ZCurrentTemp[zi] > 0 && params.ZCurrentTemp[zi] < 255 {
			holdz := ((cfg.ZoneHold & (0x01 << zi)) != 0)
			presetz := "none"

			if holdz {
				presetz = "hold"
			}

			zName := string(bytes.Trim(cfg.ZName[zi][:], " \000"))

			zoneArr[zc] = TStatZoneConfig{
					ZoneNumber:       uint8(zi+1),
					CurrentTemp:      params.ZCurrentTemp[zi],
					CurrentHumidity:  params.ZCurrentHumidity[zi],
					FanMode:          rawFanModeToString(cfg.ZFanMode[zi]),
					Hold:             &holdz,
					Preset:           presetz,
					HeatSetpoint:     cfg.ZHeatSetpoint[zi],
					CoolSetpoint:     cfg.ZCoolSetpoint[zi],
					OvrdDuration:     holdTime(cfg.ZOvrdDuration[zi]),
					OvrdDurationMins: cfg.ZOvrdDuration[zi],
					ZoneName:         zName }

			zc++

			// trigger MQTT discovery topic in case needed
			mqttDiscoverZone(zi, zName, _tstat_settings.TempUnits)
		}
	}

	tstat.Zones = zoneArr[0:zc]

	return &tstat, true
}


// write a change to a single parameter of a single zone or global config
// zn == 0 for global params or 1-8 for zone params
// returns ok == true
func putConfig(zone string, param string, value string) bool {
	params := TStatZoneParams{}
	flags := uint16(0)

	zn, err := strconv.Atoi(zone)
	if err != nil {
		log.Errorf("putConfig: invalid zone value '%s'", zone)
		return false
	}
	zi := zn - 1

	// zone parameters
	if (zn >= 1 && zn <= 8) {
		switch param {
		case "fanMode":
			if mode, ok := stringFanModeToRaw(value); !ok {
				log.Errorf("putConfig: invalid fan mode name '%s' for zone %d", value, zn)
				return false
			} else {
				params.ZFanMode[zi] = mode
				flags |= 0x01
			}
		case "coolSetpoint":
			if val, err := strconv.ParseUint(value, 10, 8); err != nil {
				log.Errorf("putConfig: invalid cool setpoint value '%s' for zone %d", value, zn)
				return false
			} else {
				params.ZCoolSetpoint[zi] = uint8(val)
				flags |= 0x08
			}
		case "heatSetpoint":
			if val, err := strconv.ParseUint(value, 10, 8); err != nil {
				log.Errorf("putConfig: invalid heat setpoint value '%s' for zone %d", value, zn)
				return false
			} else {
				params.ZHeatSetpoint[zi] = uint8(val)
				flags |= 0x04
			}
		case "hold":	// dedicated 'hold' semantics
			var val bool
			switch value {
				case "true":
					val = true
				case "false":
					val = false
				default:
					log.Errorf("putConfig: invalid hold value '%s' for zone %d", value, zn)
					return false
				}
			if val {
				params.ZoneHold = 0x01 << zi
			}
			flags |= 0x02
		case "preset":	// 'preset' semantics to control hold - extend this if we add more presets
			var val bool
			switch value {
				case "hold":
					val = true
				case "none":
					val = false
				default:
					log.Errorf("putConfig: invalid preset value '%s' for zone %d", value, zn)
					return false
				}
			if val {
				params.ZoneHold = 0x01 << zi
			}
			flags |= 0x02
		default:
			log.Errorf("putConfig: invalid parameter name '%s' for zone %d", param, zn)
			return false
		}

		if flags != 0 {
			log.Infof("calling WriteTableZ with flags: %d, 0x%x", zi, flags)
			infinity.WriteTableZ(devTSTAT, params, uint8(zi), flags)
		}

		return true
	} else if zn == 0 {
		p := TStatCurrentParams{}

		switch param {
		case "mode":
			if mode, ok := stringModeToRaw(value); !ok {
				log.Errorf("putConfig: invalid mode value '%s'", value)
				return false
			} else {
				p.Mode = mode
				flags = 0x10
			}
		case "dispZone":
			if val, err := strconv.ParseUint(value, 10, 8); err != nil || val < 1 || val > 2 {
				log.Errorf("putConfig: invalid dispZone value '%s'", value)
				return false
			} else {
				p.DispZone = uint8(val)
				flags = 0x200
			}
		default:
			log.Errorf("putConfig: invalid parameter name '%s'", param)
			return false
		}

		// setting these parameters can make our thermostat lose up to 1 min of time keeping
		// so we include a time/day setting with every command
		tnow := time.Now()
		p.DispDOW = uint8(tnow.Weekday())
		p.DispTimeMin = uint16(tnow.Hour() * 60 + tnow.Minute())
		flags = flags | 0x180

		infinity.WriteTable(devTSTAT, p, flags)

		return true
	}

	log.Errorf("putConfig: invalid zone number %d", zn)
	return false
}

func getZNConfig(zi int) (*TStatZoneConfig, bool) {
	if (zi < 0 || zi > 7) {
		return nil, false
	}

	cfg := TStatZoneParams{}
	ok := infinity.ReadTable(devTSTAT, &cfg)
	if !ok {
		return nil, false
	}

	params := TStatCurrentParams{}
	ok = infinity.ReadTable(devTSTAT, &params)
	if !ok {
		return nil, false
	}

	hold := cfg.ZoneHold & (0x01 << zi) != 0
	presetz := "none"

	if hold {
		presetz = "hold"
	}

	return &TStatZoneConfig{
		CurrentTemp:     params.ZCurrentTemp[zi],
		CurrentHumidity: params.ZCurrentHumidity[zi],
		OutdoorTemp:     params.OutdoorAirTemp,
		Mode:            rawModeToString(params.Mode & 0xf),
		Stage:           params.Mode >> 5,
		Action:          rawActionToString(params.Mode >> 5),
		FanMode:         rawFanModeToString(cfg.ZFanMode[zi]),
		Hold:            &hold,
		Preset:          presetz,
		HeatSetpoint:    cfg.ZHeatSetpoint[zi],
		CoolSetpoint:    cfg.ZCoolSetpoint[zi],
		OvrdDuration:    holdTime(cfg.ZOvrdDuration[zi]),
		OvrdDurationMins: cfg.ZOvrdDuration[zi],
		ZoneName:        string(bytes.Trim(cfg.ZName[zi][:], " \000")),
		TargetHumidity:  cfg.ZTargetHumidity[zi],
		RawMode:         params.Mode,
	}, true
}

// write a change to a single parameter of a vacation setting
// param is 'day' or 'hour'
// value is integer possibly with + or - to mean relative
// returns ok == true
func putVacationConfig(param string, value string) bool {
	params := TStatVacationParams{}
	apiConfig := APIVacationConfig{}
	bDays := uint8(0)
	bHours := uint16(0)

	// for relative setting, starting with a + or -, get the current value first
	
	if (value[0] == '+' || value[0] == '-') {
		if apiCfg1, ok1 := getVacationConfig(); ok1 == true {
			bDays = *apiCfg1.Days
			bHours = *apiCfg1.Hours
			log.Infof("putVacationConfig: Relative mode:bDays=%d, bHours=%d", bDays, bHours)
		} else {
			log.Errorf("putVacationConfig: Relative mode: getVacationConfig failed!")
		}
	}

	switch param {
	case "days":
		if val, err := strconv.ParseInt(value, 10, 8); err != nil {
			log.Errorf("putVacationConfig: invalid days value '%s'", value)
			return false
		} else {
			val = max(0, val + int64(bDays))
			v8 := uint8(val)
			apiConfig.Days = &v8
		}
	case "hours":
		if val, err := strconv.ParseInt(value, 10, 16); err != nil {
			log.Errorf("putVacationConfig: invalid days value '%s'", value)
			return false
		} else {
			val = max(0, val + int64(bHours))
			v16 := uint16(val)
			apiConfig.Hours = &v16
		}
	default:
		log.Errorf("putVacationConfig: invalid parameter name '%s'", param)
		return false
	}

	flags := params.fromAPI(&apiConfig)

	if flags != 0 {
		log.Infof("putVacationConfig: calling WriteTable with flags: 0x%x", flags)
		infinity.WriteTable(devTSTAT, params, flags)
	} else {
		log.Warn("putVacationConfig: nothing to write")
	}

	return true
}


func getTstatSettings() (*TStatSettings, bool) {
	tss := TStatSettings{}
	ok := infinity.ReadTable(devTSTAT, &tss)
	if !ok {
		return nil, false
	}

	return &TStatSettings{
		BacklightSetting: tss.BacklightSetting,
		AutoMode:         tss.AutoMode,
		DeadBand:         tss.DeadBand,
		CyclesPerHour:    tss.CyclesPerHour,
		SchedulePeriods:  tss.SchedulePeriods,
		ProgramsEnabled:  tss.ProgramsEnabled,
		TempUnits:        tss.TempUnits,
		DealerName:       tss.DealerName,
		DealerPhone:      tss.DealerPhone,
	}, true
}

func getRawData(dev uint16, tbl []byte) {
	var addr InfinityTableAddr
	copy(addr[:], tbl[0:3])
	raw := InfinityProtocolRawRequest{&[]byte{}}

	success := infinity.Read(dev, addr, raw)

	if success {
		log.Debugf("RAW: %04x/%02x%02x%02x: %s", dev, tbl[0], tbl[1], tbl[2], hex.EncodeToString(*raw.data))
	} else {
		log.Debugf("RAW: %04x/%02x%02x%02x: timeout", dev, tbl[0], tbl[1], tbl[2])
	}
}

func getAirHandler() (AirHandler, bool) {
	b := wsCache.get("blower")
	tb, ok := b.(*AirHandler)
	if !ok {
		return AirHandler{}, false
	}
	return *tb, true
}

func getHeatPump() (HeatPump, bool) {
	h := wsCache.get("heatpump")
	th, ok := h.(*HeatPump)
	if !ok {
		return HeatPump{}, false
	}
	return *th, true
}

func getDamperPosition() (DamperPosition, bool) {
	h := wsCache.get("damperpos")
	th, ok := h.(*DamperPosition)
	if !ok {
		return DamperPosition{}, false
	}
	return *th, true
}

func statePoller(monArray []uint16) {
	mon_i := 0
	for {
		// called once for all zones
		c1, c1ok := getZonesConfig()
		c2, c2ok := getVacationConfig()

		if c1ok {
			wsCache.update("tstat", c1)
			pf := fmt.Sprintf("mqtt/%s", instanceName)
			var hum uint8
			for zi := range c1.Zones {
				zp := fmt.Sprintf("%s/zone/%d", pf, c1.Zones[zi].ZoneNumber)
				mqttCache.update(zp+"/currentTemp", c1.Zones[zi].CurrentTemp)
				mqttCache.update(zp+"/humidity", c1.Zones[zi].CurrentHumidity)
				hum = c1.Zones[zi].CurrentHumidity
				mqttCache.update(zp+"/coolSetpoint", c1.Zones[zi].CoolSetpoint)
				mqttCache.update(zp+"/heatSetpoint", c1.Zones[zi].HeatSetpoint)
				mqttCache.update(zp+"/fanMode", c1.Zones[zi].FanMode)
				mqttCache.update(zp+"/hold", *c1.Zones[zi].Hold)
				mqttCache.update(zp+"/overrideDurationMins", c1.Zones[zi].OvrdDurationMins)
				if c2ok && *c2.Active {
					mqttCache.update(zp+"/preset", "vacation")
				} else {
					mqttCache.update(zp+"/preset", c1.Zones[zi].Preset)
				}
			}

			if hum > 0 {
				mqttCache.update(pf+"/humidity", hum)
			}
			mqttCache.update(pf+"/outdoorTemp", c1.OutdoorTemp)
			mqttCache.update(pf+"/mode", c1.Mode)
			// mqttCache.update(pf+"/action", c1.Action) // replaced by action set from snoop messages
			mqttCache.update(pf+"/rawMode", c1.RawMode)
			mqttCache.update(pf+"/dispZone", c1.DispZone)
			mqttCache.update(pf+"/dispDOW", c1.DispDOW)
			// calculate time skew
			tnow := time.Now()
			tsys := tnow.Hour() * 60 + tnow.Minute()
			tdiff := int16(c1.DispTime) - int16(tsys)
			if tdiff > 720 {
				tdiff = tdiff - 1440
			} else if tdiff < -720 {
				tdiff = tdiff + 1440
			}
			mqttCache.update(pf+"/dispTimeDiff", tdiff)
		}

		if c2ok {
			wsCache.update("vacation", c2)
			pf := fmt.Sprintf("mqtt/%s/vacation", instanceName)
			mqttCache.update(pf+"/active", *c2.Active)
			mqttCache.update(pf+"/days", *c2.Days)
			mqttCache.update(pf+"/hours", *c2.Hours)
			mqttCache.update(pf+"/minTemp", *c2.MinTemperature)
			mqttCache.update(pf+"/maxTemp", *c2.MaxTemperature)
			mqttCache.update(pf+"/minHumidity", *c2.MinHumidity)
			mqttCache.update(pf+"/maxHumidity", *c2.MaxHumidity)
			mqttCache.update(pf+"/fanMode", *c2.FanMode)
		}


		// rotate through the registoer monitor probes, if any
		if len(monArray) > 0 {
			getRawData(0x2001, []byte{ 0x00, byte(monArray[mon_i] >> 8 & 0xff), byte(monArray[mon_i] & 0xff) })
			mon_i = (mon_i + 1) % len(monArray)
		}

		time.Sleep(time.Second * 1)
	}
}

func statsPoller() {
	for {
		// called once for all zones
		ss := infinity.getStatsString()
		log.Info("#STATS# ", ss)

		time.Sleep(time.Second * 15)
	}
}

func attachSnoops() {
	// Snoop Heat Pump responses
	infinity.snoopResponse(0x5000, 0x51ff, func(frame *InfinityFrame) {
		data := frame.data[3:]
		heatPump, ok := getHeatPump()
		if ok {
			if bytes.Equal(frame.data[0:3], []byte{0x00, 0x3e, 0x01}) {
				heatPump.CoilTemp = float32(binary.BigEndian.Uint16(data[2:4])) / float32(16)
				heatPump.OutsideTemp = float32(binary.BigEndian.Uint16(data[0:2])) / float32(16)
				log.Debugf("heat pump coil temp is: %f", heatPump.CoilTemp)
				log.Debugf("heat pump outside temp is: %f", heatPump.OutsideTemp)
				wsCache.update("heatpump", &heatPump)
				mqttCache.update(fmt.Sprintf("mqtt/%s/coilTemp", instanceName), heatPump.CoilTemp)
				mqttCache.update(fmt.Sprintf("mqtt/%s/outsideTemp", instanceName), heatPump.OutsideTemp)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x3e, 0x02}) {
				heatPump.Stage = data[0] >> 1
				log.Debugf("HP stage is: %d", heatPump.Stage)
				wsCache.update("heatpump", &heatPump)
				mqttCache.update(fmt.Sprintf("mqtt/%s/coolStage", instanceName), heatPump.Stage)
			}
		}
	})

	// Snoop Air Handler responses
	infinity.snoopResponse(0x4000, 0x42ff, func(frame *InfinityFrame) {
		data := frame.data[3:]
		airHandler, ok := getAirHandler()
		if ok {
			if bytes.Equal(frame.data[0:3], []byte{0x00, 0x03, 0x06}) {
				airHandler.BlowerRPM = binary.BigEndian.Uint16(data[1:3])
				log.Debugf("blower RPM is: %d", airHandler.BlowerRPM)
				wsCache.update("blower", &airHandler)
				mqttCache.update(fmt.Sprintf("mqtt/%s/blowerRPM", instanceName), airHandler.BlowerRPM)
			} else if bytes.Equal(frame.data[0:3], []byte{0x00, 0x03, 0x16}) {
				airHandler.HeatStage = uint8(data[0])
				airHandler.AirFlowCFM = binary.BigEndian.Uint16(data[4:6])
				airHandler.StaticPressure = float32(float32(int(float32(binary.BigEndian.Uint16(data[7:9])) / float32(65536) * 10000 + 0.5))/10000.0)
				airHandler.ElecHeat = data[0]&0x03 != 0
				switch {
				case data[2] & 0x03 != 0:
					airHandler.Action = "cooling"
				case data[0] & 0x03 != 0:
					airHandler.Action = "heating"
				default:
					airHandler.Action = "idle"
				}
				log.Debugf("air flow CFM is: %d", airHandler.AirFlowCFM)
				wsCache.update("blower", &airHandler)
				mqttCache.update(fmt.Sprintf("mqtt/%s/heatStage", instanceName), airHandler.HeatStage)
				mqttCache.update(fmt.Sprintf("mqtt/%s/action", instanceName), airHandler.Action)
				mqttCache.update(fmt.Sprintf("mqtt/%s/airflowCFM", instanceName), airHandler.AirFlowCFM)
				mqttCache.update(fmt.Sprintf("mqtt/%s/staticPressure", instanceName), airHandler.StaticPressure)
			}
		}
	})

	// Snoop zone controllers 0x6001 and 0x6101 (up to 8 zones total)
	infinity.snoopResponse(0x6000, 0x61ff, func(frame *InfinityFrame) {
		// log.Debug("DamperMsg: ", data)
		data := frame.data[3:]
		damperPos, ok := getDamperPosition()
		if ok {
			if bytes.Equal(frame.data[0:3], []byte{0x00, 0x03, 0x19}) {
				var tdw float32
				for zi := range damperPos.DamperPos {
					if data[zi] != 0xff {
						damperPos.DamperPos[zi] = uint8(data[zi])
						mqttCache.update(fmt.Sprintf("mqtt/%s/zone/%d/damperPos", instanceName, zi+1), uint(damperPos.DamperPos[zi]) * 100 / 15)
						tdw += zoneWeight[zi] * float32(damperPos.DamperPos[zi])
					}
				}
				// calculate the airflow factor per zone if we have something
				if tdw > 0 {
					for zi := range damperPos.DamperPos {
						if data[zi] != 0xff {
							damperPos.DamperPos[zi] = uint8(data[zi])
							mqttCache.update(fmt.Sprintf("mqtt/%s/zone/%d/flowWeight", instanceName, zi+1), (zoneWeight[zi] * float32(damperPos.DamperPos[zi]) / tdw))
						}
					}
				}
				log.Debug("zone damper positions: ", damperPos.DamperPos)
				wsCache.update("damperpos", &damperPos)
			}
		}
	})
}


func (l *Logger) Open() (ok bool) {
	ok = true

	tds := time.Now().Format("06010215")
	rlfn := fmt.Sprintf("resplog.%s", tds)
	f, err := os.OpenFile(rlfn, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open resp log file '%s': %s", rlfn, err)
		ok = false
	} else {
		log.Infof("Opened resp log file '%s'", rlfn)
		of := l.f
		l.f = f
		l.tds = tds
		if of != nil {
			of.Close()
		}
	}
	l.basems = time.Now().UnixMilli()
	return
}

func (l *Logger) CheckRotate() {
	if l != nil && l.tds != "" && l.tds != time.Now().Format("06010215") {
		l.Open()
	}
}

func (l *Logger) Close() {
	l.CheckRotate()
	if l.f != nil {
		err := l.f.Close()
		if (err != nil) {
			log.Warnf("Error on closing resp logger: '%s'", err)
		} else {
			l.f = nil
		}
	}
}

func (l *Logger) Log(frame *InfinityFrame) {
	l.CheckRotate()
	if l.f != nil {
		l.f.WriteString(fmt.Sprintf("[%s] ", time.Now().Format(time.Stamp)));
		_, err := l.f.WriteString(frame.String())
		if err != nil { log.Error("Logger WriteString failed: ", err) }
		l.f.WriteString("\n")
		err = l.f.Sync()
		if err != nil { log.Error("Logger Sync failed: ", err) }
	}
}

func (l *Logger) LogS(s string) {
	if l.f != nil {
		l.f.WriteString(fmt.Sprintf("[%s] ", time.Now().Format(time.Stamp)));
		_, err := l.f.WriteString(s)
		if err != nil { log.Error("s.Logger WriteString failed: ", err) }
		l.f.WriteString("\n")
		err = l.f.Sync()
		if err != nil { log.Error("s.Logger Sync failed: ", err) }
	}
}

func main() {
	httpPort := flag.Int("httpport", 8080, "HTTP port to listen on")
	serialPort := flag.String("serial", "", "path to serial port")
	mqttBrokerUrl := flag.String("mqtt", "", "url for mqtt broker")
	ductCap := flag.String("ductCap", "12,11,11,11,11,11,11,11,11", "duct capacities as comma-separated values for leakage,z1,...,zN")
	instance := flag.String("instance", "infinitive", "unique system instance name")
	doRespLog := flag.Bool("rlog", false, "enable resp log")
	doDebugLog := flag.Bool("debug", false, "enable debug log level")

	flag.Parse()

	// validate the instance name
	if instance == nil || len(*instance) == 0 || len(*instance) > 32 || strings.ContainsAny(*instance, " $#+*/") {
		fmt.Print("invalid instance name")
		flag.PrintDefaults()
		os.Exit(1)
	}
	instanceName = *instance

	if len(*serialPort) == 0 {
		fmt.Print("must provide serial\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	loglevel := log.InfoLevel
	if doDebugLog != nil && *doDebugLog { loglevel = log.DebugLevel }
	log.SetLevel(loglevel)

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	if doRespLog != nil && *doRespLog {
		if !RLogger.Open() {
			panic("unable to open resp log file")
		}
		defer RLogger.Close()
	}

	infinity = &InfinityProtocol{device: *serialPort}
	airHandler := new(AirHandler)
	heatPump := new(HeatPump)
	damperPos := new(DamperPosition)
	wsCache.update("blower", airHandler)
	wsCache.update("heatpump", heatPump)
	wsCache.update("damperpos", damperPos)

	// init zone airflow weights (doesn't seem to be pollable so need to configure these)
	//  the system provides a zone leakage % as well as % capacity for each zone, which add to 100%
	//  refer to Advanced(hold)->Checkout->Zoning->DuctAssessment
	// from this we calculate per-zone relative weights which add to 1, and are used to calculate
	// per-zone airflow weights given the damper reports later on
	zoneRelPct := [8]float32{0}
	zoneLeakagePct := float32(0)
	zoneTotalPct := float32(0)

	for i, v := range strings.Split(*ductCap, ",") {
		nv, nerr := strconv.Atoi(v)
		if nerr != nil || nv < 0 || nv > 100 {
			fmt.Println("Invalid ductCap percentage ", v)
			os.Exit(1)
		}
		zoneTotalPct += float32(nv)
		if i == 0 {
			zoneLeakagePct = float32(nv)
		} else if i <= 8 {
			zoneRelPct[i-1] = float32(nv)
		} else {
			fmt.Println("Too many values given in ductCap")
			os.Exit(1)
		}
	}

	if zoneTotalPct != 100 {
		fmt.Println("ductCap percentages must total 100%")
		os.Exit(1)
	}

	// calculate zone weights, will total 100
	zoneTotalRelPct := float32(0)
	for _, v := range zoneRelPct {
		zoneTotalRelPct += v
	}

	if zoneTotalRelPct == float32(0) {
		fmt.Println("At least one zone must have a nonzero ductCap percentage")
		os.Exit(1)
	}

	for i, v := range zoneRelPct {
		zoneWeight[i] = (zoneLeakagePct * (v / zoneTotalRelPct) + zoneRelPct[i])/100
	}

	log.Infoln("ductCap zone Weights:", zoneWeight)

	rawMonTable := []uint16{
		// 0x3c01, 0x3c03, 0x3c0a, 0x3c0b, 0x3c0c, 0x3c0d, 0x3c0e, 0x3c0f, 0x3c14, 0x3d02, 0x3d03, 
		0x3b05, 0x3b06, 0x3b0e, 0x3b0f, 0x3d02, 0x3d03,
	}

	attachSnoops()
	err := infinity.Open()
	if err != nil {
		log.Panicf("error opening serial port: %s", err.Error())
	}

	if mqttBrokerUrl != nil {
		ConnectMqtt(*mqttBrokerUrl, os.Getenv("MQTTPASS"))
	}

	go statePoller(rawMonTable)
	go statsPoller()
	webserver(*httpPort)
}
