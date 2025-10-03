# THIS FORK IS A WORK IN PROGRESS

This fork of infinitive has added read/write API and UI for multi-zone Infinity systems, and support for status
and control via MQTT.  It currently works well (as of May 2025) and is
reported by various users to support systems of up to 6 zones but it should work on unzoned or up to 4 or 8 zones.
The UI and MQTT API adapt to show the zones that appear to be in use.

This code has been adapted from extensive previous work of others.  It should still work fine on non-zoned systems but there may be
some cosmetic cleanup needed; similarly, it was designed around a heatpump
system but my extensions have been tested on a system with AC and a gas-fueled heater, so status might not be optimally reported on
an HP system any more.  Please provide feedback or fixes.

MQTT support has been added; the schema (topics and data representation) have been crafted to work well with the MQTT Climate integration,
in an attempt to simplify Home Assitant integration (extending to zones and to reduce polling and improve responsiveness) without custom
python integration code.  MQTT Discovery messages are emitted so Home Assistant will automatically create or update all
the needed Climate and Sensor entities for your system.  Of course, the MQTT interface could also be useful on its own,
or just not configured if you don't want to use it.

Active development and testing are still under way.  In particular we still need to look into the following:
  * Precisely detect Dehumidify action - currently using a heuristic
  * Auto-detect the zone airflow weighting (please let me know if you find a way!)
  * MQTT: maybe support a read-only option
  * MQTT: add controls to change per-zone overrideDuration
  * HA sensors (other than the Climate entities) created by MQTT Discovery are hardwired to units of °F, but should adapt to the system's temp units
  * Consider changing MQTT discovery to be device-based rather than just unconnected entities
  * Consider moving the per-zone "bonus" sensors into a single JSON attributes object compatible with MQTT Climate integration

This README has been updated with some info about this fork but more needs to be written.

## Breaking Changes in May, 2025

We have recently changed the names of some of the entities created by MQTT Discovery to be more consistent and more friendly for
multi-zone and multi-instance installations.  In particular:

  * `Vacation Mode ...` sensors and buttons have changed to `HVAC Vacation Mode ...`, so their entity IDs changed from `vacation_mode_*` to `hvac_vacation_mode_*`
  * All entities that start with "HVAC" will change to the instance name, with spaces substituted instead of underscores, if the instance name is set.  So for example, if instance name is `Upstairs_AC` then sensors will be named for example `Upstairs AC Vacation Mode ...` with entity IDs like `upstairs_ac_vacation_mode_*`
  * Climate entities on a zoned system will still be named for each zone; however on a non-zoned system they will now be named for the instance name (underscores translated as above) or, if not set, "HVAC".
  * Per-zone "bonus sensors" are now named the same as the climate entities they relate to; for example, the sensor that used to be named `Zone 1 Damper Position` will now be `Downstairs Damper Position`, or `Upstairs AC Damper Position` or `HVAC Damper Position`, matching the corresponding climate entities.

# infinitive
Infinitive impersonates a SAM on a Carrier Infinity system management bus. 

This fork implements read/write and UI for multiple-zone systems.  It is of course backward compatible to a 1-zone system.

## **DISCLAIMER**
**The software and hardware described here interacts with a proprietary system using information gleaned by reverse engineering.  Although it works fine for me, no guarantee or warranty is provided.  Use at your own risk to your HVAC system and yourself.**

## Getting started
#### Hardware setup

This fork has been developed and tested on a Raspberry Pi 3B+ running current Raspberry Pi OS, and an external USB RS485 dongle.

Older notes:

I've done all my development on a Raspberry Pi, although any reasonably performant Linux system with an RS-485 interface should work.  I chose the Pi 3 since the built-in WiFi saved me the hassle of running Ethernet to my furnace.  I'm not sure if older Pis have enough horsepower to run the Infinitive software.  If you give it a shot and are successful, please let me know so I can update this information.

In addition to a Linux system, you'll need to source an adapter to communicate on the RS-485 bus.  I am using a FTDI chipset USB to RS-485 adapter that I bought from Amazon.  There are a variety of adapters on Amazon and eBay, although it may take a few attempts to find one that works reliably.

Once you have a RS-485 adapter you'll need to connect it to your ABCD bus. The easiest way to do this is by attaching new wires to the A and B terminals of the ABCD bus connector inside your furnace and connecting them to your adapter. The A and B lines are used for RS-485 communication, while C and D are 24V AC power. **Do not connect your RS-485 adapter to the C and D terminals unless you want to see its magic smoke.** 

#### Software
NOTE: this fork is not yet getting binary releases - you will need to build it yourself for now (see below).  Please open an Issue if you would like to express interest in getting builds released.

Start Infinitive, at minimum providing the HTTP port to listen on for the management interface and the path to the correct serial device.

```
$ ./infinitive -httpport=8080 -serial=/dev/ttyUSB0 
```

Logs are written to stderr.  If the RS-485 adapter is properly connected to your ABCD bus you should immediately see Infinitive logging messages indicating it is receiving data, such as:

```
INFO[0000] read frame: 2001 -> 4001: READ     000302    
INFO[0000] read frame: 4001 -> 2001: RESPONSE 000302041100000414000004020000 
INFO[0000] read frame: 2001 -> 4001: READ     000316    
INFO[0000] read frame: 4001 -> 2001: RESPONSE 0003160000000003ba004a2f780100037a 
```
Browse to your host system's IP, with the port you provided on the command line, and you should see a page that looks similar to the following:

<img src="https://raw.githubusercontent.com/lurgh/infinitive/master/screenshot.png"/>

There is a brief delay between altering a setting and Infinitive updating the information displayed.  This is due to Infinitive polling the thermostat settings once per second.

Once it is working you may want to install how to install it under systemd to run as a daemon.
@mww012 did a great writeup of this procedure - see https://github.com/mww012/hass-infinitive/blob/master/info.md

Here's the updated `infinitive.service` with the MQTT enabled:
```
[Unit]
Description=Infinitive Service
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=root
Environment="MQTTPASS=mqtt-password"
ExecStart=/path/to/infinitive -httpport=8080 -serial=/dev/ttyUSB0 -mqtt=tcp://mqtt-username@mqtt-broker-host:1883

[Install]
WantedBy=multi-user.target

```

#### Additional options

These additional options may be useful to you:

  * Enable req/resp logging:
```
$ infinitive ... -rlog
```
In addition to all normal operations, this option causes infinitive to log all requests and responses seen on the serial bus in an hourly log file named 'resplog.YYMMDDHH' which will be created in the current directory.  This is intended
to capture serial bus data for offline analysis.

  * Enable debug level logging:
```
$ infinitive ... -debug
```
This sets the log level to Debug rather than the default Info, causing quite a bit more verbose logging.

  * Enable MQTT data publication, HA MQTT discovery, and MQTT command subscriptions:
```
$ MQTTPASS=mqtt-password infinitive ... -mqtt tcp://mqtt-username@mqtt-broker-host:1883
```
password and username are optional, as needed by your MQTT broker.  Password is passed in the environment so as
not to be visible in "ps" etc.

See below for MQTT schema and more notes about using it.

  * Set the duct capacities of a multi-zone system
```
$ infinitive ... -ductCap=DL,Z1,Z2,...,Zn
```
This option sets the duct capacity ratings which will be used for airflow weighting calculations.  This is optional and only
needed on a multi-zone system, and when you care about the accuracy of the flowWeight values reported per zone.  If it is not
supplied, Infinitive will assume equal capacities so the per-zone flowWeight reports will probably not match the
per-zone airflows shown on the controller.

The capacities are integer percentages and must add up to 100.
DL is the DUCT LEAKAGE percent, and Z1, Z2, etc are the per-zone capacity percentages, all as shown on your system controller.
You need not specify capacities for zones that don't exist on your system.  For example a valid argument would look like
`-ductCap=12,56,32`, meaning 12% leakage, 56% zone 1 capacity, and 32% zone 2 capacity.

Refer to the section below on **Airflow Weights Per Zone** for how to obtain these values from your controller.

  * Set a different instance name (useful when you need to run multiple instances of infinitive that will publish to the same MQTT broker)
```
$ infinitive ... -instance=name
```
This sets the instance name to be 'name', typically something other than 'infinitive', and which should be human-readable since it will be used within Home Assistant entity names. 
Any characters other than ' ', '*', '+', '/', '$', and '#' are accepted.  Infinitive does not constrain the
length but it is best to keep it short since there are often tight constraints within MQTT agents.

The instance name currently just affects MQTT; it is used in these ways:
    * it is a prefix on every MQTT topic published or subscribed; therefore it allows multiple instances
      of Infinitive to pub/sub on different topic name trees, e.g. "HVAC_Upstairs" and "HVAC_Downstairs", without conflicting
    * it is used to create distinct discovery topics, unique IDs and entity names for all HA entities created by MQTT Discovery; therefore it
      separate instances of HA entities will be created automatically, with distinct names, referring to the distinct MQTT topics
      mentioned above, wiithout confusion or conflict
    * it is used as the name of the Climate entity for unzoned systems (or when the zone name is "ZONE 1")

When the instance name is uesd to create entity names, underscore "_" characters will be changed to spaces.
So for example. "Upstairs_AC" will create entities named "Upstairs AC Action", etc.

Use this option with care: when Infinitive is started with a new
Instance name, it will create a complete set of HA entities based on that name.  If this wasn't what you intended,
you may need to manually remove the retained discovery messages from your MQTT server in order to remove the unwanted entities.
See the MQTT section below for more information.

  * Enable reporting of 'drying' (dehumidify) action
```
$ infinitive ... -drying
```
This enables Infinitive to report an HVAC action of 'drying' when the system is in DEHUMIDIFY mode.  Unfortunately, we have not found a way to get
the thermostat to tell us it is in this mode.  When the System Status display (via the right-side button) shows DEHUMIDIFY, the thermostat just
reports a cooling mode that is indistinguishable from actual COOLING.  So to implement this, we apply a heuristic to determine when to report
'drying" as the current action instad of 'cooling'.  But only if this option has been set from the command line.

The current test requires all these to be true:
** the system is in a cooling stage
** the current humidity (measured at the thermostat) is above its setpoint
** all zones' temperatures are at or below their cooling setpoint

This heuristic may be adjusted based on future learnings, or hopefully replaced by something more precise.

This version of 'action' is reported in the /api/airhandler endpoint, and via MQTT at /infinitive/action.  There are
legacy 'Action' fields in the zone data reports which are calculated differently and will not reflect 'drying' mode.

## Building from source

(This section needs some updates and refinement)

If you'd like to build Infinitive from source, first confirm you have a working Go environment - current minimum version is 1.21.  Ensure your GOPATH and GOHOME are set correctly, then:

```
$ go get github.com/lurgh/infinitive
$ go build github.com/lurgh/infinitive
```

Alternatively you can clone the github repo and type "make".

Note: If you make changes to the code or other resources in the assets directory you will need to rebuild the bindata_assetfs.go file. You will need the go-bindata-assetfs utility.
 
1. Install go-bindata-assetfs into your go folders

Details, and installation instructions are available here.

https://github.com/elazarl/go-bindata-assetfs

but can be summarized as:

```
$ go install github.com/go-bindata/go-bindata/...
$ go install github.com/elazarl/go-bindata-assetfs/...
```

2. Rebuild bindata_assetfs.go

You can use the "make" command to rebuild the assets if you change the sources. 

## JSON "REST" API

Infinitive exposes a JSON API to retrieve and manipulate thermostat parameters.  There are features implemented in the MQTT API that have not made their way here yet
but would be easy enough to add if there is interest.  This API has been extended to support multiple-zone systems efficiently but is intended to be backward-compatible with the 1-zone API available in upstream versions of infinitive.

#### GET /api/zone/[Z]/config

Replace [Z] with any zone number 1-8.  If you want data for multiple zones, it's more efficient to use "GET /api/zones/config" to get all at once.


```json
{
   "currentTemp": 70,
   "currentHumidity": 50,
   "outdoorTemp": 50,
   "mode": "heat",
   "stage":2,
   "fanMode": "auto",
   "hold": true,
   "targetHumidity": 52,
   "zoneName": "Downstairs",
   "overrideDuration": "1:50",
   "overrideDurationMins": 110,
   "heatSetpoint": 68,
   "coolSetpoint": 74,
   "rawMode": 64
}
```
rawMode included for debugging purposes. It encodes stage and mode. 

Note that paramers stage, mode, outdoorTemp, and rawMode are global across all zones but for historical reasons they are present
in the per-zone query.

#### PUT /api/zone/[Z]/config

Replace [Z] with any zone number 1-8.  One or more parameters to write should be included in the JSON body.  Parameters that are not
mentioned are not changed.  The only parameters that are settable are "fanMode", "heatSetpoint", "coolSetpoint", and "hold", as well as the global
parameter "mode".

```json
{
   "mode": "auto",
   "fanMode": "auto",
   "hold": true,
   "heatSetpoint": 68,
   "coolSetpoint": 74
}
```

Valid write values for `mode` are `off`, `auto`, `heat`, and `cool`.
Additional read values for mode are `electric` and `heatpump` indicating "heat pump only" or "electric heat only" have been selected at the thermostat 
Values for `fanMode` are `auto`, `low`, `med`, and `high`.

#### GET /api/zones/config

This retrieves and returns data for all zones at once in a single JSON structure.  It's more efficient to use this
when you need multiple zones' data.  However, PUT is not supported with this structure currently so any PUTs must be done
using the per-zone API.

Note that this includes an array of the per-zone structures which includes the zone number in each one - that is, the zone number
is not necessarily related directly to the index in this array.  Also note that the "global" parameters (outdoorTemp, mode, stage, rawMode)
are properly at the top level of this
dictionary.  These parameters may for now also be inside the per-zone structures but should not be used and will eventually be removed.

```json
{
   "zones": [
      {
         "zoneNumber":1,
	 "currentTemp":79,
	 "currentHumidity":46,
	 "targetHumidity":0,
	 "zoneName":"Downstairs",
	 "fanMode":"low",
	 "hold":true,
	 "heatSetpoint":72,
	 "coolSetpoint":82,
	 "overrideDuration":"",
	 "overrideDurationMins":0,
	 "outdoorTemp":0,
	 "mode":"",
	 "stage":0,
	 "rawMode":0
      },
      {
         "zoneNumber":2,
	 "currentTemp":82,
	 "currentHumidity":46,
	 "targetHumidity":0,
	 "zoneName":"Upstairs",
	 "fanMode":"med",
	 "hold":false,
	 "heatSetpoint":73,
	 "coolSetpoint":83,
	 "overrideDuration":"1:25",
	 "overrideDurationMins":85,
	 "outdoorTemp":0,
	 "mode":"",
	 "stage":0,
	 "rawMode":0
      }
   ],
   "outdoorTemp":74,
   "mode":"cool",
   "stage":0,
   "rawMode":1
}

```
rawMode included for debugging purposes. It encodes stage and mode. 

#### GET /api/airhandler

This call is also supported as "GET /api/zone/1/airhandler" for backward compatibility but this is not per-zone data.  Note there is more airflow information available now thru the MQTT interface which could be added here if needed.

```json
{
	"blowerRPM":217,
	"airFlowCFM":420,
    "staticPressure":0.0765,
    "heatStage":0,
	"elecHeat":false,
    "action":"idle"
}
```

#### GET /api/heatpump

This call is also supported as "GET /api/zone/1/heatpump" for backward compatibility but this is not per-zone data.

```json
{
	"coilTemp":28.8125,
	"outsideTemp":31.375,
	"stage":2
}
```


#### GET /api/zone/1/vacation

(This API endpoint has not been changed from the original code but needs updates)

```
{
   "active":false,
   "days":0,
   "minTemperature":56,
   "maxTemperature":84,
   "minHumidity":15,
   "maxHumidity":60,
   "fanMode":"auto"
}
```

#### PUT /api/zone/1/vacation

(This API endpoint has not been changed from the original code but needs updates)

```
{
   "days":0,
   "minTemperature":56,
   "maxTemperature":84,
   "minHumidity":15,
   "maxHumidity":60,
   "fanMode":"auto"
}
```

All parameters are optional.  A single parameter may be updated by sending a JSON document containing only that parameter.  Vacation mode is disabled by setting `days` to `0`.  Valid values for `fanMode` are `auto`, `low`, `med`, and `high`.

## MQTT API

MQTT is a pub/sub bus that is used in many home automation settings.  To use it you will need to have an MQTT broker running
in your environment.  There are various simple ways to accompish this but they are beyond the scope here.

This MQTT API is read/write and it assumes that
it is running within a private, trusted environment - that is, there are not specific access controls beyond what
is provided to access the broker.  We recommend at least using password authentication on your broker.

All topics are published with the `retain` flag set so any new client will get all current values; updates are only posted
at startup or as individual values change.  This does mean that clients could be susceptible to seeing old data if the
service is no longer running, so it is important that clients monitor the `/available` topic to determine the validity of the MQTT data.

Communication with the MQTT broker is reasonably robust in the sense that a down MQTT broker will not
block startup, and we will reconnect in event of network drops, restarts or similar.  
MQTT connection state and communication with the MQTT broker are logged in the stderr log.

When enabled by providing the MQTT broker URI and optional password, the following topics are supported.  Note that
the initial topic level 'infinitive' can be replaced with something unique by specifying the '-instance'
command-line option.

### Topics Published

System-global topics:
* `infinitive/available`: status of infinitive's connection to MQTT: `online` or `offline`
* `infinitive/outdoorTemp`: Outside temp as reported by thermostat, whole number degrees
* `infinitive/mode`: System main mode normalized for Home Assistant, currently one of: `off`, `cool`, `heat`, `auto`
* `infinitive/action`: Current action, Home Assistant compatible, currently one of: `off`, `heating`, `cooling`, `drying` (optional), `idle`
* `infinitive/rawMode`: numeric representation of mode and action, a uint8 value - possibly useful to developers for discovery but does not cover all action states
* `infinitive/humidity`: current humidity as reported by thermostat, in percent RH
* `infinitive/coolStage`: compressor operating stage reported by outdoor unit, as a number 0/1/2
* `infinitive/heatStage`: furnace operating stage, as a number 0/1/2; in HP systems this represents electric/emergency heat
* `infinitive/blowerRPM`: blower speed reported by inside unit, in RPM, 0 when off
* `infinitive/airflowCFM`: airflow speed reported by inside unit, in cf/m, 0 when off
* `infinitive/staticPressure`: static pressure reported by inside unit, in inches wc

Global Vacation topics, apply to all zones:
* `infinitive/vacation/active`: flag whether Vacation mode is in effect - `true` or `false`
* `infinitive/vacation/days`: days remaining in Vacation mode (0 if not; rounded up to next whole day)
* `infinitive/vacation/hours`: hours left in Vacation mode (0 if not set; duplicates time from /days just with 1-hour resolution)
* `infinitive/vacation/minTemp`: will be used as the heat setpoint when in Vacation mode
* `infinitive/vacation/maxTemp`: will be used as the cool setpoint when in Vacation mode
* `infinitive/vacation/minHumidity`: minimum humidity paramater when in Vacation mode
* `infinitive/vacation/maxHumidity`: maximum humidity paramater when in Vacation mode
* `infinitive/vacation/fanMode`: will be used as the fan mode when in Vacation mode: `low`, `med`, `high`, `auto`

Experimental, may change or disappear over time:
* `infinitive/coilTemp`: coil temp reported by outdoor unit, in 0.125-degree resolution
* `infinitive/outsideTemp`: outside temp reported by outdoor unit, in 0.125-degree resolution
* `infinitive/elecHeat`: bool flag indicating HP air handler is operating on electric heat
* `infinitive/dispZone`: zone number currently displayed on the tstat
* `infinitive/dispDOW`: numeric value of the day of week as displayed on the tstat, 0=Sunday
* `infinitive/dispTimeDiff`: number of minutes skew between the tstat time and the host system time (-720 - +719)

Reported per zone, where X is a zone number 1-8:
* `infinitive/zone/X/currentTemp`: current temperature as reported by thermostat, in whole degrees
* `infinitive/zone/X/humidity`: current humidity as reported by thermostat, in percent RH
* `infinitive/zone/X/coolSetpoint`: current cool set point, in whole degrees
* `infinitive/zone/X/heatSetpoint`: current heat set point, in whole degrees
* `infinitive/zone/X/fanMode`: current fan mode setting, Home Assistant compatible: `low`, `med`, `high`, `auto`
* `infinitive/zone/X/hold`: bool flag for Hold setting, `false` or `true` (not really useful with HA -- use `preset` instead)
* `infinitive/zone/X/preset`: HA-style "preset" flag; currently `hold`, `vacation`, or `none`
* `infinitive/zone/X/damperPos`: zone damper position reported by zoning unit, 0-100 as whole number percent where 100 is fully open
* `infinitive/zone/X/flowWeight`: airflow allocation factor for this zone as a decimal fraction (0-1) - multiply the total airflowCFM
  by this number to get the reported airflow for this zone.
* `infinitive/zone/X/overrideDurationMins`: minutes remaining on zone setting override, zero if none
* `infinitive/zone/X/temp16`: high resolution, unsmoothed temperature, as a decimal number to .0625 degree precision

HomeAssistant MQTT Discovery topics published:
* `homeassistant/sensor/infinitive/*/config`: discovery topics, one per sensor, for:
  * all the "global" sensors: `outdoorTemp`, `humidity`, `rawMode`, `blowerRPM`, `airflowCFM`, `staticPressure`, `coolStage`, `heatStage`, `action`
  * all the vacation sensors: `vacation/active`, `vacation/days`, `vacation/hours`, `vacation/minTemp`, `vacation/maxTemp`, `vacation/minHumidity`, `vacation/maxHumidity`, `vacation/fanMode`
  * per-zone "bonus" sensors (not supported by the Climate integration): `damperPos`, `flowWeight`, `overrideDurationMins`, `temp16`
* `homeassistant/button/infinitive/*/config`: discovery topics to create "buttons" as a convenience to manipulate vacation timing:
  * "HVAC Vacation Cancel", "HVAC Vacation Add 1 Hour", "HVAC Vacation Subtract 1 Hour", "HVAC Vacation 1 Hour", and so on (total of 18 buttons)
* `homeassistant/climate/infinitive/*/config`: discovery topics, one per zone, for an MQTT HVAC climate entity, which includes:
  * Zone name, temperature unit and increment, and enumerations of the modes, fan modes, and preset modes
  * MQTT topic names for receiving current mode, action, fan mode, preset mode, temperature, humnidity, and setpoints
  * MQTT topic names for setting mode, fan mode, preset mode, and setpoints
  * MQTT "availability" topic name which causes the climate entity to track as Unavailable when infinitive is not running or not able to reach the MQTT broker

If the MQTT integration and MQTT Discovery are enabled in your Home Assistant instance, a dozen or more sensors, 18 buttons, and one
MQTT climate entity per zone, will be created.  The discovery messages are sent once each time Infinitive starts up, so restart it if you need
them to be re-sent, such as after clearing out retained messages.

The Climate and Sensor entities that are created per-zone will be named the same as the zone name within your system; if your system is not zoned,
they will be named based on the instance name if provided, otherwise "HVAC".  The Sensor and Button entities that are global per system will be named
based on the instance name if provided, otherwise "HVAC".

If your MQTT broker has accumulated retained discovery messages, you will need to delete those retained messages in order to cause
Home Assistant to delete the unwanted entities.  There are various ways to do it but the most general approach is to publish new retained messages to the same discovery topic names with an empty message field.

For example, if you use the `mosquitto` MQTT broker, you may use a command such as this to remove all of Infinitive's retained discovery messages, which will immediately cause Home Assistant to delete all the old dynamically-created entities, and new ones will be re-created on next Infinitive restart:

```
mosquitto_sub -v -t homeassistant/+/infinitive/\# --remove-retained
```

If you choose not to enable discovery in Home Assistant, you can create your sensors and climate entities manually in the config file.  For example:

```
# optional, if not using MQTT Discovery
mqtt:
  - climate:
      name: Downstairs
      modes:
        - "off"
        - "cool"
        - "heat"
        - "auto"
      fan_modes:
        - "high"
        - "med"
        - "low"
        - "auto"
      preset_modes:
        - "hold"
        - "vacation"
      current_humidity_topic: infinitive/zone/1/humidity
      current_temperature_topic: infinitive/zone/1/currentTemp
      fan_mode_state_topic: infinitive/zone/1/fanMode
      mode_state_topic: infinitive/mode
      action_topic: infinitive/action
      temperature_high_state_topic: infinitive/zone/1/coolSetpoint
      temperature_low_state_topic: infinitive/zone/1/heatSetpoint
      fan_mode_command_topic: infinitive/zone/1/fanMode/set
      mode_command_topic: infinitive/mode/set
      temperature_high_command_topic: infinitive/zone/1/coolSetpoint/set
      temperature_low_command_topic: infinitive/zone/1/heatSetpoint/set
      preset_mode_state_topic: infinitive/zone/1/preset
      preset_mode_command_topic: infinitive/zone/1/preset/set
      availability_topic: infinitive/available
      temp_step: 1
      temperature_unit: F
      unique_id: hvac-zone-1x

```

### Topics Subscribed

An MQTT client may publish to these topics in order to change operating
configuration.  These acitons are taken immediately and there is no reply
per se but any successful changes will result in infinitive publishing a
data update for that parameter to reflect the change, once the thermostat
publishes updated status to reflect it.  Logs will indicate if there
are errors in processing the request.  The response can be delayed by
up to 1 second, due to the thermostat polling interval.

Global topics:
* `infinitive/mode/set`: Set the main operating mode (same options as above)
* `infinitive/vacation/hours/set`: set Vacation mode time in hours; set to 0 to cancel, or prefix with '+' or '-' for relative settings
* `infinitive/vacation/days/set`: set Vacation mode time in days; set to 0 to cancel, or prefix with '+' or '-' for relative settings

Experimental topics:
* `infinitive/dispZone/set`: set the system to display a particular zone (where zone number is 1-8 and presumably must exist)

Zone topics:
* `infinitive/zone/X/coolSetpoint/set`: set the cool set point, as above
* `infinitive/zone/X/heatSetpoint/set`: set the heat set point, as above
* `infinitive/zone/X/fanMode/set`: set the fan mode setting, same options as above
* `infinitive/zone/X/hold/set`: set the zone hold setting, same options as above
* `infinitive/zone/X/preset/set`: set the zone "preset" setting, `hold` or `none`; `vacation` cannot be set here but setting `hold` will unset it

Again remember that the "infinitive/" prefix on these subscribed names will be changed to the instance name if one is provided.  This allows multiple instances of Infinitive to coexist on one MQTT bus.

## Details
#### ABCD bus
Infinity systems use a proprietary binary protocol for data exchange between system components.  These message are sent across an RS-485 serial bus which Carrier refers to as the ABCD bus.  Most systems usually includes an air-conditioning unit or heat pump, furnace, and thermostat.  The thermostat is responsible for enumerating other components of the system and managing their operation. 

The protocol has been reverse engineered as Carrier has not published a protocol specification.  The following resources provided invaluable assistance with my reverse engineering efforts:

* [Cocoontech's Carrier Infinity Thread](http://cocoontech.com/forums/topic/11372-carrier-infinity/)
* [Infinitude Wiki](https://github.com/nebulous/infinitude/wiki/Infinity-Protocol-Main)
* [Carrier's SAM documentation](https://www.shareddocs.com/hvac/docs/1009/Public/09/SAM01-04XA.pdf), latest version from 2021 also describes Touch systems

Infinitive reads and writes information from the Infinity thermostat.  It also gathers data by passively observing traffic exchanged between the thermostat and other system components.

#### Bus Logging

By adding the -rlog command line option, you can request infinitive to log every request and response seen on the serial bus into a log file, for offline analysis.  We have some primitive tools for analyzing this data which we may add to the repo at some point.  It has been very helpful for finding some more tricks in the protocol.

#### Changing global parameters

We observed that changing global parameters including operating mode (heat/cool/auto/off), as well as some experimental and future ones
that are set via the same bus register (displayed zone, day of week, unoccupied zones), has a side effect of setting back the
system clock by up to one minute.  This was discovered when automating changes to the operating mode multiple times per day, and the
thermostat would lose a couple minutes more each day.  A fix has been added, to reset the time to the infinitive system time any
time those parameters are set.  All the more reason to keep your system clock set correctly!

#### Protocol Notes
Building on the work documented above, a numer of additional details about the protocol have been discovered.  These notes are
based on observations of the protocol exchanges on a 2-zone system with 2-stage gas furnace, 2-stage AC compressor, and media filter.
They are noted here just as a place to track progress.

Register 3b.06: some numbers, then dealer name and phone; numbers probably correspond to settings from the UI/SAM such as filter reminder, UV reminder, Humidifier reminder, Backlight, units F/C, auto mode enabled, sys heat/cool/heatcool, deadband, cycles/hr, programmable fan option

Register 3b.07 - 3b.0d: seven 1-day schedules each corresponding to a day of week, encoded in 160 bytes as
* for each of 8 zones
  * for each of 4 time periods
    * uint16 start time (min past midnight)
    * uint8 heatSP
    * uint8 coolSP
    * uint8 0xff (optional fan setting or placeholder for it?)

Register 3c.03: looks like semi-random garbage remnants of other response data, some spliced together inconsistently, as if buffering remnants

Register 3c.0a: list of the 8 zone names (repeats content from 003b03); some extra 0 padding whcih doesn't seem to vary

Register 3c.0b: unclear but consistent values over time

Register 3c.0c: consistently all zeros

Register 3c.0d: unclear but (1) bytes 01-02 increment hourly but stop incrementing at 870 (90 days); other values change occasionally
```
   003c0d5a0000005a00a50000000c00a50000000c00a50000005a00000000000000
   003c0d5a0001005a00a50000000c00a50000000c00a50000005a00000000000000
   003c0d5a08700a5a5aa50000000c00a50000000c00a50000005a00000000000000
```

Register 3c.0e: consistent similar patterns to 3c0d (a lot of 5a, c0) but has not changed at all in days
```
   003c0e00005a005a0000a5000c0000a5000c000007005a0000000000
```

Register 3c.0f: last byte mostly counts hours since midnight; next-to-last counts days.  not consistent, can't tell what base is
```
   003c0f000704080015b811
```

Register 3c.14: consistent, unchanged for days
```
   003c1401000000ff010000000001000100000000000000000000000000000000000000
```

Register 3c.16: snooped from air handler to thermostat:
```
   000316 00 00 02 0002c1004de95a01000203
          HS    CC
```
HS = Furnace heat stage (00=off, 01/02/03=low/med/hi)
CC = Cooling flag (may be HP actually), 02 = cooling on my system, doesn't reflect stage

Register 3d.02: read from Thermostat, contains actual zone temps, apparently both raw (to 1/16 dF) and displayable (whole number,
apparently smoothed and not just rounded)
```
   003d02 a5 0101 046f 46 0104 0462 46 000000000000000000000000000000000000000000000000000000000000
             FFFF TTTT TT ...
```
unknown flag (0xa5)
Repeated per each of 8 zones:
  FFFF = unknown flags (0x0101, 0x0104) - may indicate Tstat, Temp Sensor, Smart Sensor
  TTTT = raw current temp x16
  TT = smoothed current temp for display

Register 3d.03: read from Thermostat: actuals incl outdoor temp and two humidity metrics
```
  003d03 a5 01 0337 0100 31 00 31 a5a5a55a5aa5
               OOOO      HH    HR
```
OOOO = outside temp x16
HH = Humidity (indoor, smoothed) in %RH
HR = Humidity (indoor, raw) in %RH

Register 04.1f is sent as a WRITE from the thermostat to the smart sensor.  Appears to contain:
```
  00041f 10 03 00000000 42 52 000004000000000000000000
            TP          HS CS
```
  TP = Program time period for zone (0 = Wake thru 3 = Sleep)
  HS = Heat setpoint for zone
  CS = Cool setpoint for zone

Register 04.20 is regularly broadcast as a WRITE to f1f1 by the thermostat.  Fields look to be:
```
  000420 c002c0000f01 056b 0102 2f 3c5000405600000000
                      TTTT      HH
```
  TTTT = 16x outdoor temp in dF
  HH = indoor humidity in %RH

#### Bryant Evolution
I believe Infinitive should work with Bryant Evolution systems as they use the same ABCD bus.  Please let me know if you have success using Infinitive on a Bryant system.

#### Notes About Multi-Zone Systems

Multi-zone systems are supported in this version.  We have tested it on a 2-zone system and others have gone up to 6 zones.  It should work up to the
apparent 8-zone limit of the Infinity architecture.  Please get in touch if you have success or difficulty with a zoned system.

The UI will automatically show all the zones, listed in order of their index number.  The REST and internal APIs can access a single zone's data at a time, or all zones
in one go; if your application wants all the zone data then it's more efficient to use the latter since the per-zone APIs will be slower owing to each
one needing make redundant requests to the system.  The all-zones API is read-only; use the per-zone PUT method to make changes to a zone's configuration.
The MQTT API has global and per-zone data as documented above.  The websocket API (used by the UI) is read-only and includes global data and all zones.

There is some cleverness wired into infinitive which calculates per-zone airflow fractions when the fan is running.  Currently this is published only through the MQTT API.

##### Airflow Weights Per Zone

Infinitive can calculate the airflow going to each zone based on the zone damper states, but to do so
accurately requires the duct assessment data from the controller which characterizes the airflow capacity of each
zone's ducting.  With that data, Infinitive will show the same per-zone airflow that the controller shows; without it,
Infinitive assumes all zones have equal capacity so will likely not match what the controller reports.

The airflow weighting calculation depends on the zone assessment (static pressure measurements)
done periodically by the system, combined with the damper open percentages at the moment.  The author
has been unable to find data emitted by the control to supply the static factors, so if you want the
per-zone airflow to be calculated correctly, you'll need to obtain these factors from your controller, and pass them
on the command line using the `-ductCap` option.  Here is how to do it:

* Open the `INSALL/SERVICE` menu on your controller by holding the Advanced (settings) button for 10 seconds
* Move the highlight to `CHECKOUT` and press `SELECT`
* Move the highlight to `ZONING` and press `SELECT`
* Move the highlight to `DUCT ASSESSMENT` and press `SELECT`

You will see a table of `ZONE`, `CAPACITY` (percent), and a final `DAMPER LEAKAGE` value.  Note these values.  Be aware that they can change over time since
the Duct Assessment is run daily and at each power-up, but it is not likely to change much unless your ductwork is modified or system settings are
changed.

Now craft a `-ductCap` argument by arranging the values in a list: first the leakage percent, then the zone percents in zone-number order from 1 to up to 8.
For example, the author's 2-zone system shows
| zone           | capacity |
| -------------- | -------- |
| Downstairs     | 56%      |
| Upstairs       | 32%      |
| DAMPER LEAKAGE | 12%      |
So the arg will be `-ductCap=12,56,32`

Note: we only have seen a small sample size but it looks like the system actually holds greater than integer precision for these values; comparing
the calculated airflow values to what is
displayed on the system UI, it seems that the author's system's duct capacity numbers are closer to 55.5 and 32.5, so the calculated zone airflow is
a few cfm off from what the system reports, while at one time we were spot-on with integers.  We will live with this for now.  Still hoping we will
find a way to get these numbers (in full precision) from the bus but not holding our breath.

#### Unimplemented features

Vacation mode temp/fan settings are read-only.  No access is provided for the scueduling features.  APIs are understood so these would be fairly easily added if there is interest.


#### Issues
##### rPi USB stack
The USB to RS-485 adapter I'm using periodically locks up due to what appear to be USB stack issues on the Raspberry Pi 3.  When this happens, reads on the serial file descriptor block forever and the kernel logs the following:
```
[491862.396039] ftdi_sio ttyUSB0: usb_serial_generic_read_bulk_callback - urb stopped: -32
```
Infinitive reopens the serial interface when it hasn't received any data in 5 seconds to workaround the issue.  Alternatively, forcing the Pi USB stack to USB 1.1 mode resolves the issue.  If you want to go this route, add `dwc_otg.speed=1` to `/boot/config.txt` and reboot the Pi.

##### Bogus data (fixed)
There was a long-standing problem wherein occasionally Infinitive's UI would display incorrect data via the web interface for a second.  This was due to a bug in the go code and has been fixed in this fork.  Leaving this note so others familiar with the README will see it.

#### See Also
[Infinitude](https://github.com/nebulous/infinitude) is another solution for managing Carrier HVAC systems.  It impersonates Carrier web services and provides an alternate interface for controlling Carrier Internet-enabled touchscreen thermostats.  It also supports passive snooping of the RS-485 bus and can decode and display some of the data.  Note if infinitude is running on the same machine, it may open the serial port and interfere with infinitive's communications with the bus.

#### Contact & Acknowledgements

Please log an issue in Github if you have questions or requests related to the work in this fork.

Upstream fork: @jkp717 did some initial work on multi-zone support which inspired me to extend what they started.

Original author: Andrew Danforth (<adanforth@gmail.com>)
