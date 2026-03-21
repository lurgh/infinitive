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
	"sync"
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
	ZoneOff         bool   `json:"zoneOff"`
	Hold            *bool  `json:"hold"`
	OverrideActive bool `json:"overrideActive"`
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

type zoneResponseSpoofProfile struct {
	payload []byte
	count   int
	gap     time.Duration
}

type zoneResponseSpoofState struct {
	mu       sync.Mutex
	profiles map[int]zoneResponseSpoofProfile
}

var activeZoneResponseSpoofs = zoneResponseSpoofState{profiles: map[int]zoneResponseSpoofProfile{}}

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

type busCaptureFlag struct {
	enabled bool
	auto    bool
	path    string
}

func (f *busCaptureFlag) String() string {
	if f == nil {
		return ""
	}
	return f.path
}

func (f *busCaptureFlag) Set(value string) error {
	switch strings.TrimSpace(value) {
	case "", "true":
		f.enabled = true
		f.auto = true
		f.path = ""
	case "false":
		f.enabled = false
		f.auto = false
		f.path = ""
	default:
		f.enabled = true
		f.auto = false
		f.path = value
	}
	return nil
}

func (f *busCaptureFlag) IsBoolFlag() bool {
	return true
}

// system instance name (default: "infinitive")
//  used as the root mqtt topic name and to unique-ify entities
var instanceName string

// system-level option to report 'drying' HVAC action
//  we default to false to maintain backward compatibility
var showDrying bool = false

const maxOverrideDurationMins = 2184

func holdTime(ht uint16) string {
	if ht == 0 {
		return ""
	}
	return fmt.Sprintf("%d:%02d", ht/60, ht % 60)
}

func zoneModeString(mode uint8, zoneOff bool) string {
	globalMode := rawModeToString(mode & 0xf)
	if globalMode == "off" || zoneOff {
		return "off"
	}
	return globalMode
}

// Writes to TStatCurrentParams can skew the thermostat clock, so every write
// that touches this table also refreshes day/time from the host.
func writeCurrentConfig(params TStatCurrentParams, flags uint16) bool {
	tnow := time.Now().Add(time.Second * 30)
	params.DispDOW = uint8(tnow.Weekday())
	params.DispTimeMin = uint16(tnow.Hour()*60 + tnow.Minute())
	flags |= flagCurrentDispDOW | flagCurrentDispTime

	return infinity.WriteTable(devTSTAT, params, flags)
}

func writeCurrentConfigAsTstat(params TStatCurrentParams, flags uint16) bool {
	tnow := time.Now().Add(time.Second * 30)
	params.DispDOW = uint8(tnow.Weekday())
	params.DispTimeMin = uint16(tnow.Hour()*60 + tnow.Minute())
	flags |= flagCurrentDispDOW | flagCurrentDispTime

	return infinity.WriteTableAs(devTSTAT, devTSTAT, params, flags)
}

func writeGlobalMode(mode string) bool {
	rawMode, ok := stringModeToRaw(mode)
	if !ok {
		log.Errorf("writeGlobalMode: invalid mode '%s'", mode)
		return false
	}

	params := TStatCurrentParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		log.Errorf("writeGlobalMode: unable to read current params")
		return false
	}

	if params.Mode&0xf == rawMode {
		log.Infof("writeGlobalMode: already in mode '%s'", mode)
		return true
	}

	params.Mode = rawMode
	log.Infof("writeGlobalMode: writing mode '%s' raw=0x%02x", mode, rawMode)
	return writeCurrentConfig(params, flagCurrentMode)
}

// On this thermostat, clearing a timed "hold until" override is done through
// the same zone-hold write used to resume schedule. A duration write of 0 does
// not reliably clear S1Z1OVR on its own, so overrideDurationMins=0 must map to
// ZoneHold=false.
func writeZoneResumeSchedule(zoneNumber int) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	params := TStatZoneParams{}
	return infinity.WriteTableZ(devTSTAT, params, uint8(zoneNumber-1), 0x02)
}

func writeLegacyZoneMode(zoneNumber int, mode string) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	params := TStatCurrentParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	zi := zoneNumber - 1
	zbit := uint8(0x01 << zi)
	flags := uint16(0)

	switch mode {
	case "off":
		if params.ZoneUnocc&zbit != 0 {
			return true
		}
		params.ZoneUnocc |= zbit
		flags |= flagCurrentZoneUnocc
	case "heat", "cool", "auto":
		rawMode, _ := stringModeToRaw(mode)
		if params.ZoneUnocc&zbit != 0 {
			params.ZoneUnocc &^= zbit
			flags |= flagCurrentZoneUnocc
		}
		if params.Mode&0xf != rawMode {
			params.Mode = rawMode
			flags |= flagCurrentMode
		}
	default:
		return false
	}

	if flags == 0 {
		return true
	}

	return writeCurrentConfig(params, flags)
}

func writeLegacyZoneOff(zoneNumber int, zoneOff bool) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	params := TStatCurrentParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	zbit := uint8(0x01 << (zoneNumber - 1))
	if zoneOff {
		if params.ZoneUnocc&zbit != 0 {
			return true
		}
		params.ZoneUnocc |= zbit
	} else {
		if params.ZoneUnocc&zbit == 0 {
			return true
		}
		params.ZoneUnocc &^= zbit
	}

	return writeCurrentConfig(params, flagCurrentZoneUnocc)
}

func writeLegacyZoneOffAsTstat(zoneNumber int, zoneOff bool) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	params := TStatCurrentParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	zbit := uint8(0x01 << (zoneNumber - 1))
	if zoneOff {
		if params.ZoneUnocc&zbit != 0 {
			return true
		}
		params.ZoneUnocc |= zbit
	} else {
		if params.ZoneUnocc&zbit == 0 {
			return true
		}
		params.ZoneUnocc &^= zbit
	}

	return writeCurrentConfigAsTstat(params, flagCurrentZoneUnocc)
}

func rewriteRemoteZoneSetpoints(zoneNumber int) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	zi := zoneNumber - 1
	cfg := TStatZoneParams{}
	if !infinity.ReadTable(devTSTAT, &cfg) {
		log.Errorf("rewriteRemoteZoneSetpoints: unable to read thermostat zone params for zone %d", zoneNumber)
		return false
	}

	heatSetpoint := cfg.ZHeatSetpoint[zi]
	coolSetpoint := cfg.ZCoolSetpoint[zi]
	if heatSetpoint == 0 && coolSetpoint == 0 {
		log.Errorf("rewriteRemoteZoneSetpoints: zone %d has no setpoints to rewrite", zoneNumber)
		return false
	}

	log.Warnf(
		"rewriteRemoteZoneSetpoints: zone %d re-enable fallback via 003b03 heat=%d cool=%d",
		zoneNumber,
		heatSetpoint,
		coolSetpoint,
	)

	if heatSetpoint > 0 {
		params := TStatZoneParams{}
		params.ZHeatSetpoint[zi] = heatSetpoint
		if !infinity.WriteTableZ(devTSTAT, params, uint8(zi), 0x04) {
			log.Errorf("rewriteRemoteZoneSetpoints: failed heat setpoint rewrite for zone %d", zoneNumber)
			return false
		}
	}

	if coolSetpoint > 0 {
		params := TStatZoneParams{}
		params.ZCoolSetpoint[zi] = coolSetpoint
		if !infinity.WriteTableZ(devTSTAT, params, uint8(zi), 0x08) {
			log.Errorf("rewriteRemoteZoneSetpoints: failed cool setpoint rewrite for zone %d", zoneNumber)
			return false
		}
	}

	return true
}

// clearZoneHold explicitly writes the ZoneHold field false for a single zone.
//
// This matters because writing overrideDuration=0 is a no-op on the bus for
// these thermostats. An explicit 003b03 write with flag 0x02 reliably clears
// the thermostat's internal "temporary override active" state, even when the
// read-side Hold bit already appears false in the API.
func clearZoneHold(zoneNumber int) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	zi := zoneNumber - 1
	params := TStatZoneParams{}
	log.Infof("clearZoneHold: zone %d clearing hold bit via 003b03", zoneNumber)
	return infinity.WriteTableZ(devTSTAT, params, uint8(zi), 0x02)
}

// writeRemoteZoneSetpointWhileOff handles the common user action of changing a
// remote zone's heat/cool setpoint while that zone is OFF.
//
// On the physical remote thermostat, pressing temp +/- while the zone is OFF
// implicitly re-enables the zone. On the bus, that means we must first stop
// spoofing OFF 00041e responses, then write the requested setpoint, then clear
// ZoneHold to remove the thermostat's automatic temporary override timer.
func writeRemoteZoneSetpointWhileOff(zoneNumber int, flag uint16, value uint8) bool {
	if zoneNumber <= 1 || zoneNumber > 8 {
		return false
	}

	zi := zoneNumber - 1
	clearZoneResponseSpoof(zoneNumber)

	params := TStatZoneParams{}
	switch flag {
	case 0x04:
		params.ZHeatSetpoint[zi] = value
	case 0x08:
		params.ZCoolSetpoint[zi] = value
	default:
		log.Errorf("writeRemoteZoneSetpointWhileOff: unsupported flag 0x%x for zone %d", flag, zoneNumber)
		return false
	}

	log.Infof("writeRemoteZoneSetpointWhileOff: zone %d setpoint write while OFF flag=0x%x value=%d", zoneNumber, flag, value)
	if !infinity.WriteTableZ(devTSTAT, params, uint8(zi), flag) {
		log.Errorf("writeRemoteZoneSetpointWhileOff: failed setpoint write for zone %d", zoneNumber)
		return false
	}

	if !clearZoneHold(zoneNumber) {
		log.Warnf("writeRemoteZoneSetpointWhileOff: zone %d setpoint write enabled zone but failed to clear hold", zoneNumber)
	}

	return true
}

func writeZoneOverrideDurationOnly(zoneNumber int, durationMins uint16) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	zi := zoneNumber - 1
	params := TStatZoneParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	params.ZOvrdDuration[zi] = durationMins
	return infinity.WriteTableZAs(devTSTAT, devTSTAT, params, uint8(zi), 0x100)
}

func writeZoneOverrideDurationAsTstat(zoneNumber int, durationMins uint16, heatSetpoint uint8, coolSetpoint uint8) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	zi := zoneNumber - 1
	params := TStatZoneParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	params.ZoneHold &^= 1 << zi
	params.ZOvrdDuration[zi] = durationMins
	params.ZHeatSetpoint[zi] = heatSetpoint
	params.ZCoolSetpoint[zi] = coolSetpoint
	return infinity.WriteTableZAs(devTSTAT, devTSTAT, params, uint8(zi), 0x10e)
}

func syncRemoteZoneOffConfig(zoneNumber int, payload []byte) bool {
	return true
}

func writeZoneOff(zoneNumber int, zoneOff bool) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	if zoneNumber == 1 {
		return writeLegacyZoneOff(zoneNumber, zoneOff)
	}

	if zoneOff {
		if spoofPayload, ok := buildRemoteZoneOffSpoofPayload(zoneNumber); ok {
			setZoneResponseSpoof(zoneNumber, spoofPayload, 4, 15*time.Millisecond)
			log.Infof("writeZoneOff: zone %d armed remote 00041e spoof payload=%s", zoneNumber, hex.EncodeToString(spoofPayload))
		}

		if current, _, ok := remoteZoneCache.zoneOff(zoneNumber); ok && current {
			log.Infof("writeZoneOff: zone %d already disabled", zoneNumber)
			return true
		}

		payload, reason, ok := remoteZoneCache.offPayload(zoneNumber)
		if !ok {
			log.Errorf("writeZoneOff: zone %d disable requested before any cached 00041f payload was seen", zoneNumber)
			return false
		}

		if !syncRemoteZoneOffConfig(zoneNumber, payload) {
			return false
		}

		return writeRemoteZonePayload(zoneNumber, payload, "disablebit/"+reason)
	}

	clearZoneResponseSpoof(zoneNumber)

	if current, _, ok := remoteZoneCache.zoneOff(zoneNumber); ok && !current {
		log.Infof("writeZoneOff: zone %d already enabled", zoneNumber)
		return true
	}

	// Re-enabling a remote zone is intentionally a two-step thermostat-side
	// sequence:
	//
	//  1. Rewrite the currently scheduled setpoints back into 003b03.
	//     That causes the master thermostat to adopt the zone as enabled again
	//     and emit the matching 00041f ON payload to the remote zone stat.
	//
	//  2. Immediately clear ZoneHold with a dedicated 0x02 write.
	//     Without this second step, the thermostat keeps a 2-hour temporary
	//     override active as a side effect of the setpoint rewrite.
	//
	// We have not found a single bus command that reproduces the desired
	// "zone on, following schedule, no override timer" end state in one shot.
	// On the physical remote thermostat, the equivalent user flow is also
	// two-step:
	//
	//  1. Press temp + or temp - to re-enable the zone.
	//  2. Press hold - repeatedly to drive the override duration back to zero.
	//
	// This software path mirrors that observed behavior at the bus level.
	log.Infof("writeZoneOff: zone %d enabling via thermostat setpoint rewrite followed by hold clear", zoneNumber)
	if rewriteRemoteZoneSetpoints(zoneNumber) {
		if !clearZoneHold(zoneNumber) {
			log.Warnf("writeZoneOff: zone %d enabled but failed to clear hold after thermostat setpoint rewrite", zoneNumber)
		}
		return true
	}

	if payload, reason, ok := remoteZoneCache.onPayload(zoneNumber); ok {
		log.Warnf("writeZoneOff: zone %d thermostat setpoint rewrite failed, falling back to cached ON payload", zoneNumber)
		return writeRemoteZonePayload(zoneNumber, payload, "enablebit/"+reason)
	}

	log.Errorf("writeZoneOff: zone %d unable to enable via thermostat setpoint rewrite or cached ON payload", zoneNumber)
	return false
}

func writeZoneMode(zoneNumber int, mode string) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}

	if zoneNumber == 1 {
		log.Infof("writeZoneMode: zone %d mode '%s' using thermostat zone-mode fallback", zoneNumber, mode)
		return writeLegacyZoneMode(zoneNumber, mode)
	}

	switch mode {
	case "off":
		return writeZoneOff(zoneNumber, true)
	case "heat", "cool", "auto":
		if !writeGlobalMode(mode) {
			log.Errorf("writeZoneMode: unable to set global mode '%s' while enabling zone %d", mode, zoneNumber)
			return false
		}

		return writeZoneOff(zoneNumber, false)
	default:
		return false
	}
}

// Timed hold writes must start from a live copy of table 003b03 and carry the
// current setpoints, otherwise unrelated zone fields can be clobbered.
// A duration of 0 is the API/"resume schedule" form and is translated here to
// the ZoneHold=false write, because writing a raw 0 duration does not reliably
// clear the timed override state on the thermostat.
func writeZoneOverrideDuration(zoneNumber int, durationMins uint16, heatSetpoint uint8, coolSetpoint uint8) bool {
	if zoneNumber < 1 || zoneNumber > 8 {
		return false
	}
	if durationMins == 0 {
		return writeZoneResumeSchedule(zoneNumber)
	}
	if durationMins > maxOverrideDurationMins {
		durationMins = maxOverrideDurationMins
	}

	zi := zoneNumber - 1
	params := TStatZoneParams{}
	if !infinity.ReadTable(devTSTAT, &params) {
		return false
	}

	params.ZOvrdDuration[zi] = durationMins
	params.ZHeatSetpoint[zi] = heatSetpoint
	params.ZCoolSetpoint[zi] = coolSetpoint

	return infinity.WriteTableZ(devTSTAT, params, uint8(zi), 0x10c)
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

func zoneOffForConfig(zoneNumber int, params *TStatCurrentParams) bool {
	if zoneNumber > 1 {
		if zoneOff, _, ok := remoteZoneCache.zoneOff(zoneNumber); ok {
			return zoneOff
		}
		return false
	}

	return params.ZoneUnocc&0x01 != 0
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
			overrideActive := ((cfg.ZTimedOvrdState & (0x01 << zi)) != 0)
			overrideDurationMins := cfg.ZOvrdDuration[zi]
			overrideDuration := holdTime(overrideDurationMins)
			zoneOff := zoneOffForConfig(zi+1, &params)
			presetz := "none"

			if holdz {
				presetz = "hold"
			}
			if !overrideActive {
				overrideDurationMins = 0
				overrideDuration = ""
			}

			zName := string(bytes.Trim(cfg.ZName[zi][:], " \000"))

			zoneArr[zc] = TStatZoneConfig{
				ZoneNumber:       uint8(zi+1),
				CurrentTemp:      params.ZCurrentTemp[zi],
				CurrentHumidity:  params.ZCurrentHumidity[zi],
				FanMode:          rawFanModeToString(cfg.ZFanMode[zi]),
				ZoneOff:          zoneOff,
				Hold:             &holdz,
				OverrideActive: overrideActive,
				Preset:           presetz,
				HeatSetpoint:     cfg.ZHeatSetpoint[zi],
				CoolSetpoint:     cfg.ZCoolSetpoint[zi],
				TargetHumidity:   cfg.ZTargetHumidity[zi],
				OvrdDuration:     overrideDuration,
				OvrdDurationMins: overrideDurationMins,
				Mode:             zoneModeString(params.Mode, zoneOff),
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
		log.Infof("putConfig: request zone %d %s=%s", zn, param, value)
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
				if zn > 1 {
					if zoneOff, _, ok := remoteZoneCache.zoneOff(zn); ok && zoneOff {
						return writeRemoteZoneSetpointWhileOff(zn, 0x08, uint8(val))
					}
				}
				params.ZCoolSetpoint[zi] = uint8(val)
				flags |= 0x08
			}
		case "heatSetpoint":
			if val, err := strconv.ParseUint(value, 10, 8); err != nil {
				log.Errorf("putConfig: invalid heat setpoint value '%s' for zone %d", value, zn)
				return false
			} else {
				if zn > 1 {
					if zoneOff, _, ok := remoteZoneCache.zoneOff(zn); ok && zoneOff {
						return writeRemoteZoneSetpointWhileOff(zn, 0x04, uint8(val))
					}
				}
				params.ZHeatSetpoint[zi] = uint8(val)
				flags |= 0x04
			}
		case "overrideDurationMins":
			base := uint16(0)
			if len(value) == 0 {
				log.Errorf("putConfig: invalid overrideDurationMins value '%s' for zone %d", value, zn)
				return false
			}
			if value[0] == '+' || value[0] == '-' {
				if cur, ok := getZNConfig(zi); !ok {
					log.Errorf("putConfig: unable to read current zone config for relative overrideDurationMins write, zone %d", zn)
					return false
				} else {
					base = cur.OvrdDurationMins
				}
			}
			if val, err := strconv.ParseInt(value, 10, 32); err != nil {
				log.Errorf("putConfig: invalid overrideDurationMins value '%s' for zone %d", value, zn)
				return false
			} else {
				val += int64(base)
				if val < 0 {
					val = 0
				} else if val > maxOverrideDurationMins {
					log.Infof("putConfig: clamping overrideDurationMins from %d to %d for zone %d", val, maxOverrideDurationMins, zn)
					val = maxOverrideDurationMins
				}
				if cur, ok := getZNConfig(zi); !ok {
					log.Errorf("putConfig: unable to read current zone config for overrideDurationMins write, zone %d", zn)
					return false
				} else if !writeZoneOverrideDuration(zn, uint16(val), cur.HeatSetpoint, cur.CoolSetpoint) {
					log.Errorf("putConfig: failed to write overrideDurationMins=%d for zone %d", val, zn)
					return false
				} else {
					return true
				}
			}
		case "mode":
			if !writeZoneMode(zn, value) {
				log.Errorf("putConfig: invalid mode value '%s' for zone %d", value, zn)
				return false
			}
			return true
		case "zoneOff":
			switch value {
				case "true":
					return writeZoneOff(zn, true)
				case "false":
					return writeZoneOff(zn, false)
				default:
					log.Errorf("putConfig: invalid zoneOff value '%s' for zone %d", value, zn)
					return false
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
			log.Infof("putConfig: zone %d submitting 003b03 write for %s=%s with flags=0x%x", zn, param, value, flags)
			infinity.WriteTableZ(devTSTAT, params, uint8(zi), flags)
		}

		return true
	} else if zn == 0 {
		log.Infof("putConfig: request global %s=%s", param, value)
		switch param {
		case "mode":
			return writeGlobalMode(value)
		case "dispZone":
			p := TStatCurrentParams{}
			if val, err := strconv.ParseUint(value, 10, 8); err != nil || val < 1 || val > 2 {
				log.Errorf("putConfig: invalid dispZone value '%s'", value)
				return false
			} else {
				p.DispZone = uint8(val)
				flags = flagCurrentDispZone
				return writeCurrentConfig(p, flags)
			}
		default:
			log.Errorf("putConfig: invalid parameter name '%s'", param)
			return false
		}
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
	overrideActive := cfg.ZTimedOvrdState & (0x01 << zi) != 0
	overrideDurationMins := cfg.ZOvrdDuration[zi]
	overrideDuration := holdTime(overrideDurationMins)
	zoneOff := zoneOffForConfig(zi+1, &params)
	presetz := "none"

	if hold {
		presetz = "hold"
	}
	if !overrideActive {
		overrideDurationMins = 0
		overrideDuration = ""
	}

	return &TStatZoneConfig{
		CurrentTemp:     params.ZCurrentTemp[zi],
		CurrentHumidity: params.ZCurrentHumidity[zi],
		OutdoorTemp:     params.OutdoorAirTemp,
		Mode:            zoneModeString(params.Mode, zoneOff),
		Stage:           params.Mode >> 5,
		Action:          rawActionToString(params.Mode >> 5),
		FanMode:         rawFanModeToString(cfg.ZFanMode[zi]),
		ZoneOff:         zoneOff,
		Hold:            &hold,
		OverrideActive: overrideActive,
		Preset:          presetz,
		HeatSetpoint:    cfg.ZHeatSetpoint[zi],
		CoolSetpoint:    cfg.ZCoolSetpoint[zi],
		OvrdDuration:    overrideDuration,
		OvrdDurationMins: overrideDurationMins,
		ZoneName:        string(bytes.Trim(cfg.ZName[zi][:], " \000")),
		TargetHumidity:  cfg.ZTargetHumidity[zi],
		RawMode:         params.Mode,
	}, true
}

func waitForZoneTestReady(zoneNumber int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := getZNConfig(zoneNumber - 1); ok {
			if zoneNumber == 1 {
				return true
			}
			if _, _, ok := remoteZoneCache.zoneOff(zoneNumber); ok {
				return true
			}
		}
		time.Sleep(time.Second)
	}
	return false
}

func logZoneTestState(label string, zoneNumber int) (*TStatZoneConfig, bool) {
	cfg, ok := getZNConfig(zoneNumber - 1)
	if !ok {
		log.Errorf("zoneofftest: unable to read zone %d config for %s", zoneNumber, label)
		return nil, false
	}

	log.Infof(
		"zoneofftest %s zone=%d mode=%s zoneOff=%t hold=%t duration=%d heat=%d cool=%d name=%q",
		label,
		zoneNumber,
		cfg.Mode,
		cfg.ZoneOff,
		cfg.Hold != nil && *cfg.Hold,
		cfg.OvrdDurationMins,
		cfg.HeatSetpoint,
		cfg.CoolSetpoint,
		cfg.ZoneName,
	)
	return cfg, true
}

func setZoneResponseSpoof(zoneNumber int, payload []byte, count int, gap time.Duration) {
	activeZoneResponseSpoofs.mu.Lock()
	defer activeZoneResponseSpoofs.mu.Unlock()
	activeZoneResponseSpoofs.profiles[zoneNumber] = zoneResponseSpoofProfile{
		payload: append([]byte(nil), payload...),
		count:   count,
		gap:     gap,
	}
}

func clearZoneResponseSpoof(zoneNumber int) {
	activeZoneResponseSpoofs.mu.Lock()
	defer activeZoneResponseSpoofs.mu.Unlock()
	delete(activeZoneResponseSpoofs.profiles, zoneNumber)
}

func maybeSpoofZoneResponse(frame *InfinityFrame) {
	zoneNumber, ok := remoteZoneNumber(frame.src)
	if !ok || frame.dst != devTSTAT {
		return
	}
	if !bytes.Equal(frame.data[0:3], []byte{0x00, 0x04, 0x1e}) {
		return
	}

	activeZoneResponseSpoofs.mu.Lock()
	profile, ok := activeZoneResponseSpoofs.profiles[zoneNumber]
	activeZoneResponseSpoofs.mu.Unlock()
	if !ok {
		return
	}

	payload := append([]byte(nil), profile.payload...)
	if len(payload) != remoteZonePayloadLen || profile.count <= 0 {
		return
	}
	if len(frame.data) >= 3+remoteZonePayloadLen && bytes.Equal(frame.data[3:3+remoteZonePayloadLen], payload) {
		return
	}

	go func() {
		for i := 0; i < profile.count; i++ {
			log.Debugf("zonespoof: zone=%d spoofing 00041e response step=%d/%d payload=%s", zoneNumber, i+1, profile.count, hex.EncodeToString(payload))
			infinity.SendAs(frame.src, devTSTAT, opRESPONSE, append([]byte{0x00, 0x04, 0x1e}, payload...))
			if i+1 < profile.count && profile.gap > 0 {
				time.Sleep(profile.gap)
			}
		}
	}()
}

func buildRemoteZoneOffSpoofPayload(zoneNumber int) ([]byte, bool) {
	snapshot, ok := remoteZoneCache.snapshot(zoneNumber)
	if !ok || !snapshot.HaveResponse {
		return nil, false
	}

	payload := copyRemoteZonePayload(snapshot.ResponsePayload)
	payload[0] = 0x50
	payload[1] = 0x04
	payload[2] = 0x04
	payload[3] = 0x00
	payload[4] = 0x00
	payload[5] = 0x00
	return payload, true
}

func runZoneOffDirectTest(zoneNumber int, zoneOff bool, duration time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 1 || zoneNumber > 8 {
		log.Errorf("zoneofftest: invalid zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 5 * time.Second
	}

	log.Infof("zoneofftest: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zoneofftest: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before", zoneNumber); !ok {
		return 1
	}

	log.Infof("zoneofftest: writing zone %d zoneOff=%t", zoneNumber, zoneOff)
	if !writeZoneOff(zoneNumber, zoneOff) {
		log.Errorf("zoneofftest: writeZoneOff failed for zone %d", zoneNumber)
		return 1
	}

	deadline := time.Now().Add(duration)
	for {
		cfg, ok := logZoneTestState("poll", zoneNumber)
		if !ok {
			return 1
		}
		if cfg.ZoneOff != zoneOff {
			log.Errorf("zoneofftest: zone %d snapped back to zoneOff=%t", zoneNumber, cfg.ZoneOff)
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	finalCfg, ok := logZoneTestState("final", zoneNumber)
	if !ok {
		return 1
	}
	if finalCfg.ZoneOff != zoneOff {
		log.Errorf("zoneofftest: zone %d snapped back to zoneOff=%t", zoneNumber, finalCfg.ZoneOff)
		return 1
	}

	log.Infof("zoneofftest: zone %d remained at zoneOff=%t for %s", zoneNumber, zoneOff, duration)
	return 0
}

func runZoneOverrideDurationProbe(zoneNumber int, durationMins uint16, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 1 || zoneNumber > 8 {
		log.Errorf("zoneovrdtest: invalid zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	log.Infof("zoneovrdtest: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zoneovrdtest: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-ovrd", zoneNumber); !ok {
		return 1
	}

	log.Infof("zoneovrdtest: writing zone %d overrideDurationMins=%d", zoneNumber, durationMins)
	cur, ok := getZNConfig(zoneNumber - 1)
	if !ok {
		log.Errorf("zoneovrdtest: unable to read current zone %d config", zoneNumber)
		return 1
	}
	if !writeZoneOverrideDuration(zoneNumber, durationMins, cur.HeatSetpoint, cur.CoolSetpoint) {
		log.Errorf("zoneovrdtest: write failed for zone %d duration=%d", zoneNumber, durationMins)
		return 1
	}

	deadline := time.Now().Add(monitor)
	for {
		if _, ok := logZoneTestState("poll-ovrd", zoneNumber); !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	return 0
}

func runZonePutDirectTest(zoneNumber int, param string, value string, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 1 || zoneNumber > 8 {
		log.Errorf("zoneputtest: invalid zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	log.Infof("zoneputtest: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zoneputtest: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-put", zoneNumber); !ok {
		return 1
	}

	log.Infof("zoneputtest: writing zone %d %s=%s", zoneNumber, param, value)
	if !putConfig(strconv.Itoa(zoneNumber), param, value) {
		log.Errorf("zoneputtest: write failed for zone %d %s=%s", zoneNumber, param, value)
		return 1
	}

	deadline := time.Now().Add(monitor)
	for {
		if _, ok := logZoneTestState("poll-put", zoneNumber); !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	return 0
}

func runZoneRawDirectTest(zoneNumber int, payloadHex string, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 2 || zoneNumber > 8 {
		log.Errorf("zonerawtest: invalid remote zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	payload, ok := parseRemoteZonePayloadHex(payloadHex)
	if !ok {
		log.Errorf("zonerawtest: invalid 00041f payload %q", payloadHex)
		return 2
	}

	log.Infof("zonerawtest: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zonerawtest: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-raw", zoneNumber); !ok {
		return 1
	}

	log.Infof("zonerawtest: writing zone %d remote payload=%s", zoneNumber, payloadHex)
	if !writeRemoteZonePayload(zoneNumber, payload, "rawtest") {
		log.Errorf("zonerawtest: write failed for zone %d payload=%s", zoneNumber, payloadHex)
		return 1
	}

	deadline := time.Now().Add(monitor)
	for {
		if _, ok := logZoneTestState("poll-raw", zoneNumber); !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	return 0
}

func runZoneRawSequenceDirectTest(zoneNumber int, payloadsCSV string, gap time.Duration, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 2 || zoneNumber > 8 {
		log.Errorf("zonerawseq: invalid remote zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if gap < 0 {
		gap = 0
	}

	payloadHexes := strings.Split(payloadsCSV, ",")
	if len(payloadHexes) == 0 {
		log.Errorf("zonerawseq: no payloads provided")
		return 2
	}

	payloads := make([][]byte, 0, len(payloadHexes))
	for _, payloadHex := range payloadHexes {
		payload, ok := parseRemoteZonePayloadHex(payloadHex)
		if !ok {
			log.Errorf("zonerawseq: invalid 00041f payload %q", payloadHex)
			return 2
		}
		payloads = append(payloads, payload)
	}

	log.Infof("zonerawseq: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zonerawseq: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-rawseq", zoneNumber); !ok {
		return 1
	}

	for i, payload := range payloads {
		reason := fmt.Sprintf("rawseq-%d/%d", i+1, len(payloads))
		log.Infof("zonerawseq: writing zone %d step=%d payload=%s", zoneNumber, i+1, payloadHexes[i])
		if !writeRemoteZonePayload(zoneNumber, payload, reason) {
			log.Errorf("zonerawseq: write failed for zone %d step=%d payload=%s", zoneNumber, i+1, payloadHexes[i])
			return 1
		}
		if i+1 < len(payloads) && gap > 0 {
			time.Sleep(gap)
		}
	}

	deadline := time.Now().Add(monitor)
	for {
		if _, ok := logZoneTestState("poll-rawseq", zoneNumber); !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	return 0
}

func runZoneStepsDirectTest(zoneNumber int, stepsCSV string, gap time.Duration, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 1 || zoneNumber > 8 {
		log.Errorf("zonesteps: invalid zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if gap < 0 {
		gap = 0
	}

	steps := strings.Split(stepsCSV, ",")
	if len(steps) == 0 {
		log.Errorf("zonesteps: no steps provided")
		return 2
	}

	log.Infof("zonesteps: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zonesteps: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-steps", zoneNumber); !ok {
		return 1
	}

	for i, step := range steps {
		step = strings.TrimSpace(step)
		switch {
		case strings.HasPrefix(step, "put:"):
			spec := strings.TrimPrefix(step, "put:")
			parts := strings.SplitN(spec, "=", 2)
			if len(parts) != 2 {
				log.Errorf("zonesteps: invalid put step %q", step)
				return 2
			}
			log.Infof("zonesteps: step=%d put %s=%s", i+1, parts[0], parts[1])
			if !putConfig(strconv.Itoa(zoneNumber), parts[0], parts[1]) {
				log.Errorf("zonesteps: put failed at step=%d %s=%s", i+1, parts[0], parts[1])
				return 1
			}
		case strings.HasPrefix(step, "raw:"):
			payloadHex := strings.TrimPrefix(step, "raw:")
			payload, ok := parseRemoteZonePayloadHex(payloadHex)
			if !ok {
				log.Errorf("zonesteps: invalid raw step payload %q", payloadHex)
				return 2
			}
			log.Infof("zonesteps: step=%d raw payload=%s", i+1, payloadHex)
			if !writeRemoteZonePayload(zoneNumber, payload, fmt.Sprintf("steps-%d", i+1)) {
				log.Errorf("zonesteps: raw write failed at step=%d payload=%s", i+1, payloadHex)
				return 1
			}
		default:
			log.Errorf("zonesteps: unsupported step %q", step)
			return 2
		}
		if i+1 < len(steps) && gap > 0 {
			time.Sleep(gap)
		}
	}

	deadline := time.Now().Add(monitor)
	for {
		if _, ok := logZoneTestState("poll-steps", zoneNumber); !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(pollInterval)
	}

	return 0
}

func runZoneSpoofDirectTest(zoneNumber int, responsePayloadHex string, spoofCount int, spoofGap time.Duration, monitor time.Duration, pollInterval time.Duration) int {
	if zoneNumber < 2 || zoneNumber > 8 {
		log.Errorf("zonespooftest: invalid remote zone %d", zoneNumber)
		return 2
	}
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}
	if spoofCount <= 0 {
		spoofCount = 1
	}

	payload, ok := parseRemoteZonePayloadHex(responsePayloadHex)
	if !ok {
		log.Errorf("zonespooftest: invalid 00041e payload %q", responsePayloadHex)
		return 2
	}

	log.Infof("zonespooftest: waiting for zone %d state to become available", zoneNumber)
	if !waitForZoneTestReady(zoneNumber, 20*time.Second) {
		log.Errorf("zonespooftest: timed out waiting for zone %d state", zoneNumber)
		return 1
	}

	if _, ok := logZoneTestState("before-spoof", zoneNumber); !ok {
		return 1
	}

	setZoneResponseSpoof(zoneNumber, payload, spoofCount, spoofGap)
	defer clearZoneResponseSpoof(zoneNumber)

	log.Infof("zonespooftest: writing zone %d zoneOff=true and arming spoof payload=%s count=%d gap=%s", zoneNumber, responsePayloadHex, spoofCount, spoofGap)
	if !writeZoneOff(zoneNumber, true) {
		log.Errorf("zonespooftest: writeZoneOff failed for zone %d", zoneNumber)
		return 1
	}

	deadline := time.Now().Add(monitor)
	for {
		cfg, ok := logZoneTestState("poll-spoof", zoneNumber)
		if !ok {
			return 1
		}
		if time.Now().After(deadline) {
			break
		}
		if !cfg.ZoneOff {
			log.Errorf("zonespooftest: zone %d snapped back to zoneOff=%t", zoneNumber, cfg.ZoneOff)
			return 1
		}
		time.Sleep(pollInterval)
	}

	finalCfg, ok := logZoneTestState("final-spoof", zoneNumber)
	if !ok {
		return 1
	}
	if !finalCfg.ZoneOff {
		log.Errorf("zonespooftest: zone %d snapped back to zoneOff=%t", zoneNumber, finalCfg.ZoneOff)
		return 1
	}
	log.Infof("zonespooftest: zone %d remained at zoneOff=true for %s", zoneNumber, monitor)
	return 0
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
		// hours is limited to 0-32768, including after incremental
		if val, err := strconv.ParseInt(value, 10, 16); err != nil {
			log.Errorf("putVacationConfig: invalid hours value '%s'", value)
			return false
		} else {
			val = min(max(0, val + int64(bHours)), int64(32767))
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

func getTstatTemps() (*APITStatTemps, bool) {
	tst := TStatTemps{}
	ok := infinity.ReadTable(devTSTAT, &tst)
	if !ok {
		return nil, false
	}

	return tst.toAPI(), true
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
	cyc_i := 0
	potentially_drying := true	// prove this wrong

	for {
		// called once for all zones
		c1, c1ok := getZonesConfig()
		c2, c2ok := getVacationConfig()

		pf := fmt.Sprintf("mqtt/%s", instanceName)

		if c1ok {
			wsCache.update("tstat", c1)
			var hum uint8
			for zi := range c1.Zones {
				zp := fmt.Sprintf("%s/zone/%d", pf, c1.Zones[zi].ZoneNumber)
				mqttCache.update(zp+"/currentTemp", c1.Zones[zi].CurrentTemp)
				mqttCache.update(zp+"/humidity", c1.Zones[zi].CurrentHumidity)
				hum = c1.Zones[zi].CurrentHumidity
				mqttCache.update(zp+"/coolSetpoint", c1.Zones[zi].CoolSetpoint)
				mqttCache.update(zp+"/heatSetpoint", c1.Zones[zi].HeatSetpoint)
				mqttCache.update(zp+"/targetHumidity", c1.Zones[zi].TargetHumidity)
				mqttCache.update(zp+"/fanMode", c1.Zones[zi].FanMode)
				mqttCache.update(zp+"/mode", c1.Zones[zi].Mode)
				mqttCache.update(zp+"/zoneOff", c1.Zones[zi].ZoneOff)
				mqttCache.update(zp+"/hold", *c1.Zones[zi].Hold)
				mqttCache.update(zp+"/overrideActive", c1.Zones[zi].OverrideActive)
				mqttCache.update(zp+"/overrideDurationMins", c1.Zones[zi].OvrdDurationMins)
				if c2ok && *c2.Active {
					mqttCache.update(zp+"/preset", "vacation")
				} else {
					mqttCache.update(zp+"/preset", c1.Zones[zi].Preset)
				}

				if hum <= c1.Zones[zi].TargetHumidity || c1.Zones[zi].CurrentTemp > c1.Zones[zi].CoolSetpoint {
					potentially_drying = false
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

			// publish the potentially_drying state for use later when cooling is detected
			mqttCache.update("local/potentially_drying", potentially_drying)
		}

		if c2ok {
			wsCache.update("vacation", c2)
			mqttCache.update(pf+"/vacation/active", *c2.Active)
			mqttCache.update(pf+"/vacation/days", *c2.Days)
			mqttCache.update(pf+"/vacation/hours", *c2.Hours)
			mqttCache.update(pf+"/vacation/minTemp", *c2.MinTemperature)
			mqttCache.update(pf+"/vacation/maxTemp", *c2.MaxTemperature)
			mqttCache.update(pf+"/vacation/minHumidity", *c2.MinHumidity)
			mqttCache.update(pf+"/vacation/maxHumidity", *c2.MaxHumidity)
			mqttCache.update(pf+"/vacation/fanMode", *c2.FanMode)
		}

		// things to poll less-often
		if c1ok && cyc_i == 0 {
			c3, c3ok := getTstatTemps()
			if c3ok {
				for zi := range c1.Zones {
					zp := fmt.Sprintf("%s/zone/%d", pf, c1.Zones[zi].ZoneNumber)
					mqttCache.update(zp+"/temp16", c3.Zones[zi].Temp16)
				}
			}
		}
		cyc_i = (cyc_i + 1) % 10

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
	infinity.snoopResponse(0x2201, 0x2801, func(frame *InfinityFrame) {
		if bytes.Equal(frame.data[0:3], []byte{0x00, 0x04, 0x1e}) {
			remoteZoneCache.rememberResponse(frame.src, frame.data[3:])
			maybeSpoofZoneResponse(frame)
		}
	})

	infinity.snoopWrite(devTSTAT, devTSTAT, func(frame *InfinityFrame) {
		if bytes.Equal(frame.data[0:3], []byte{0x00, 0x04, 0x1f}) {
			remoteZoneCache.rememberWrite(frame.dst, frame.data[3:], "thermostat")
		}
	})

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
					// see whether to report this as drying instead
					pd := mqttCache.get("local/potentially_drying")
					if showDrying && pd == true {
						airHandler.Action = "drying"
					} else {
						airHandler.Action = "cooling"
					}
					log.Debugf("cooling stage while potentially_drying=%v, showDrying=%v, so action=%s", pd, showDrying, airHandler.Action)
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
	var busCapturePath busCaptureFlag
	flag.Var(&busCapturePath, "buscap", "capture decoded bus traffic to a JSONL file")
	zoneOffTestZone := flag.Int("zoneofftest", 0, "direct bus test mode: zone number to toggle and monitor without HTTP")
	zoneOffTestState := flag.String("zoneoffteststate", "off", "direct bus test mode target state: off or on")
	zoneOffTestDuration := flag.Duration("zoneofftestduration", time.Minute, "direct bus test mode monitor duration")
	zoneOffTestPoll := flag.Duration("zoneofftestpoll", 5*time.Second, "direct bus test mode poll interval")
	zoneOvrdTestZone := flag.Int("zoneovrdtest", 0, "direct bus test mode: zone number to probe override duration writes")
	zoneOvrdTestMins := flag.Uint("zoneovrdmins", 0, "direct bus test mode: override duration value to write")
	zoneOvrdTestDuration := flag.Duration("zoneovrdtestduration", 15*time.Second, "direct bus test mode monitor duration")
	zoneOvrdTestPoll := flag.Duration("zoneovrdtestpoll", 2*time.Second, "direct bus test mode poll interval")
	zonePutTestZone := flag.Int("zoneputtest", 0, "direct bus test mode: zone number for a single config write")
	zonePutTestParam := flag.String("zoneputparam", "", "direct bus test mode: zone config parameter name")
	zonePutTestValue := flag.String("zoneputvalue", "", "direct bus test mode: zone config parameter value")
	zonePutTestDuration := flag.Duration("zoneputtestduration", 10*time.Second, "direct bus test mode monitor duration")
	zonePutTestPoll := flag.Duration("zoneputtestpoll", 2*time.Second, "direct bus test mode poll interval")
	zoneRawTestZone := flag.Int("zonerawtest", 0, "direct bus test mode: remote zone number for a raw 00041f write")
	zoneRawTestPayload := flag.String("zonerawpayload", "", "direct bus test mode: 20-byte remote 00041f payload as 40 hex chars")
	zoneRawTestDuration := flag.Duration("zonerawtestduration", 10*time.Second, "direct bus test mode monitor duration")
	zoneRawTestPoll := flag.Duration("zonerawtestpoll", 2*time.Second, "direct bus test mode poll interval")
	zoneRawSeqZone := flag.Int("zonerawseq", 0, "direct bus test mode: remote zone number for a raw 00041f sequence")
	zoneRawSeqPayloads := flag.String("zonerawseqpayloads", "", "direct bus test mode: comma-separated 20-byte remote 00041f payloads as 40 hex chars each")
	zoneRawSeqGap := flag.Duration("zonerawseqgap", 200*time.Millisecond, "direct bus test mode: delay between raw sequence writes")
	zoneRawSeqDuration := flag.Duration("zonerawseqduration", 10*time.Second, "direct bus test mode monitor duration")
	zoneRawSeqPoll := flag.Duration("zonerawseqpoll", 2*time.Second, "direct bus test mode poll interval")
	zoneStepsZone := flag.Int("zonesteps", 0, "direct bus test mode: zone number for a mixed put/raw step sequence")
	zoneStepsSpec := flag.String("zonestepsspec", "", "direct bus test mode: comma-separated steps like put:coolSetpoint=89,raw:<40hex>")
	zoneStepsGap := flag.Duration("zonestepsgap", 250*time.Millisecond, "direct bus test mode: delay between mixed steps")
	zoneStepsDuration := flag.Duration("zonestepsduration", 10*time.Second, "direct bus test mode monitor duration")
	zoneStepsPoll := flag.Duration("zonestepspoll", 2*time.Second, "direct bus test mode poll interval")
	zoneSpoofTestZone := flag.Int("zonespooftest", 0, "direct bus test mode: remote zone number to disable while spoofing 00041e responses")
	zoneSpoofPayload := flag.String("zonespoofpayload", "", "direct bus test mode: spoofed remote 00041e payload as 40 hex chars")
	zoneSpoofCount := flag.Int("zonespoofcount", 3, "direct bus test mode: spoof repeats after each real 00041e response")
	zoneSpoofGap := flag.Duration("zonespoofgap", 20*time.Millisecond, "direct bus test mode: gap between spoofed 00041e responses")
	zoneSpoofDuration := flag.Duration("zonespooftestduration", 20*time.Second, "direct bus test mode monitor duration")
	zoneSpoofPoll := flag.Duration("zonespooftestpoll", 2*time.Second, "direct bus test mode poll interval")
	ductCap := flag.String("ductCap", "12,11,11,11,11,11,11,11,11", "duct capacities as comma-separated values for leakage,z1,...,zN")
	instance := flag.String("instance", "infinitive", "unique system instance name")
	doRespLog := flag.Bool("rlog", false, "enable resp log")
	doDebugLog := flag.Bool("debug", false, "enable debug log level")
	showDryingOpt := flag.Bool("drying", false, "enable reporting of Drying HVAC action")

	flag.Parse()

	// validate the instance name
	if instance == nil || len(*instance) == 0 || len(*instance) > 32 || strings.ContainsAny(*instance, " $#+*/") {
		fmt.Print("invalid instance name")
		flag.PrintDefaults()
		os.Exit(1)
	}
	instanceName = *instance

	if showDryingOpt != nil && *showDryingOpt {
		showDrying = true
	}
	log.Infoln("Option showDrying: ", showDrying)

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

	if (*zoneOffTestZone > 0 || *zoneOvrdTestZone > 0 || *zonePutTestZone > 0 || *zoneRawTestZone > 0 || *zoneRawSeqZone > 0 || *zoneStepsZone > 0 || *zoneSpoofTestZone > 0) && !busCapturePath.enabled {
		busCapturePath.enabled = true
		busCapturePath.auto = true
	}

	if busCapturePath.enabled {
		path := busCapturePath.path
		if busCapturePath.auto {
			path = autoCapturePath()
		}
		if !busCapture.Open(path) {
			panic("unable to open bus capture file")
		}
		defer busCapture.Close()
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
		0x3b05, 0x3b06, 0x3b0e, 0x3b0f, 0x3d03,
	}

	attachSnoops()
	err := infinity.Open()
	if err != nil {
		log.Panicf("error opening serial port: %s", err.Error())
	}

	if *zoneOffTestZone > 0 {
		go statePoller(rawMonTable)
		switch *zoneOffTestState {
		case "off":
			os.Exit(runZoneOffDirectTest(*zoneOffTestZone, true, *zoneOffTestDuration, *zoneOffTestPoll))
		case "on":
			os.Exit(runZoneOffDirectTest(*zoneOffTestZone, false, *zoneOffTestDuration, *zoneOffTestPoll))
		default:
			log.Panicf("invalid zoneoffteststate %q", *zoneOffTestState)
		}
	}

	if *zoneOvrdTestZone > 0 {
		go statePoller(rawMonTable)
		os.Exit(runZoneOverrideDurationProbe(*zoneOvrdTestZone, uint16(*zoneOvrdTestMins), *zoneOvrdTestDuration, *zoneOvrdTestPoll))
	}

	if *zonePutTestZone > 0 {
		if *zonePutTestParam == "" {
			log.Panicf("zoneputparam is required when using -zoneputtest")
		}
		go statePoller(rawMonTable)
		os.Exit(runZonePutDirectTest(*zonePutTestZone, *zonePutTestParam, *zonePutTestValue, *zonePutTestDuration, *zonePutTestPoll))
	}

	if *zoneRawTestZone > 0 {
		if *zoneRawTestPayload == "" {
			log.Panicf("zonerawpayload is required when using -zonerawtest")
		}
		go statePoller(rawMonTable)
		os.Exit(runZoneRawDirectTest(*zoneRawTestZone, *zoneRawTestPayload, *zoneRawTestDuration, *zoneRawTestPoll))
	}

	if *zoneRawSeqZone > 0 {
		if *zoneRawSeqPayloads == "" {
			log.Panicf("zonerawseqpayloads is required when using -zonerawseq")
		}
		go statePoller(rawMonTable)
		os.Exit(runZoneRawSequenceDirectTest(*zoneRawSeqZone, *zoneRawSeqPayloads, *zoneRawSeqGap, *zoneRawSeqDuration, *zoneRawSeqPoll))
	}

	if *zoneStepsZone > 0 {
		if *zoneStepsSpec == "" {
			log.Panicf("zonestepsspec is required when using -zonesteps")
		}
		go statePoller(rawMonTable)
		os.Exit(runZoneStepsDirectTest(*zoneStepsZone, *zoneStepsSpec, *zoneStepsGap, *zoneStepsDuration, *zoneStepsPoll))
	}

	if *zoneSpoofTestZone > 0 {
		if *zoneSpoofPayload == "" {
			log.Panicf("zonespoofpayload is required when using -zonespooftest")
		}
		go statePoller(rawMonTable)
		os.Exit(runZoneSpoofDirectTest(*zoneSpoofTestZone, *zoneSpoofPayload, *zoneSpoofCount, *zoneSpoofGap, *zoneSpoofDuration, *zoneSpoofPoll))
	}

	if mqttBrokerUrl != nil {
		ConnectMqtt(*mqttBrokerUrl, os.Getenv("MQTTPASS"))
	}

	go statePoller(rawMonTable)
	go statsPoller()
	webserver(*httpPort)
}
