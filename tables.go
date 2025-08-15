package main

type InfinityTableAddr [3]byte
type InfinityTable interface {
	addr() InfinityTableAddr
}

type TStatCurrentParams struct {
	ZCurrentTemp      [8]uint8
	ZCurrentHumidity  [8]uint8
	Unknown1          uint8
	OutdoorAirTemp    uint8
	ZoneUnocc         uint8 // bitflags
	Mode              uint8 // flag x10
	DispSchedPeriod   uint8
	Unknown2          uint8
	DispDOW           uint8 // flag 0x80
	DispTimeMin       uint16 // flag 0x100 (untested)
	DispZone          uint8 // flag 0x200
}

func (params TStatCurrentParams) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x3B, 0x02}
}

type TStatZoneParams struct {
	ZFanMode         [8]uint8
	ZoneHold         uint8 // bitflags
	ZHeatSetpoint    [8]uint8
	ZCoolSetpoint    [8]uint8
	ZTargetHumidity  [8]uint8
	FanAutoCfg       uint8
	Unknown          uint8
	ZOvrdDuration    [8]uint16
	ZName            [8][12]byte
}

func (params TStatZoneParams) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x3B, 0x03}
}

// Damper status response from 4-zone damper controller
// response to READ 00 03 19
// Damper controls are 4-zone and on the first one, the first 4 zones are
// represented with the next 4 being 0xff.  Assuming the
// second damper controller (6101) populates the 2nd 4 zones in their
// natural positions.
type DamperParams struct {
	ZDamperPosition [8]uint8
}

func (params DamperParams) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x03, 0x19}
}

type TStatVacationParams struct {
	Active         uint8
	Hours          uint16
	MinTemperature uint8
	MaxTemperature uint8
	MinHumidity    uint8
	MaxHumidity    uint8
	FanMode        uint8 // matches fan mode from TStatZoneParams
}

func (params TStatVacationParams) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x3B, 0x04}
}

type APIVacationConfig struct {
	Active         *bool   `json:"active"`
	Days           *uint8  `json:"days"`
	Hours          *uint16 `json:"hours"`
	MinTemperature *uint8  `json:"minTemperature"`
	MaxTemperature *uint8  `json:"maxTemperature"`
	MinHumidity    *uint8  `json:"minHumidity"`
	MaxHumidity    *uint8  `json:"maxHumidity"`
	FanMode        *string `json:"fanMode"`
}

func (params TStatVacationParams) toAPI() APIVacationConfig {
	api := APIVacationConfig{Hours: &params.Hours,
		MinTemperature: &params.MinTemperature,
		MaxTemperature: &params.MaxTemperature,
		MinHumidity:    &params.MinHumidity,
		MaxHumidity:    &params.MaxHumidity}

	active := bool(params.Active == 1)
	api.Active = &active

	days := uint8((params.Hours + 23) / 24)
	api.Days = &days

	mode := rawFanModeToString(params.FanMode)
	api.FanMode = &mode

	return api
}

func (params *TStatVacationParams) fromAPI(config *APIVacationConfig) uint16 {
	flags := uint16(0)

	if config.Active != nil {
		params.Active = 0
		if *config.Active == true {
			params.Active = 1
		}
		flags |= 0x01
	}

	if config.Hours != nil {
		params.Hours = *config.Hours
		flags |= 0x02
	}

	if config.Days != nil {
		params.Hours = uint16(*config.Days) * uint16(24)
		flags |= 0x02
	}

	if config.MinTemperature != nil {
		params.MinTemperature = *config.MinTemperature
		flags |= 0x04
	}

	if config.MaxTemperature != nil {
		params.MaxTemperature = *config.MaxTemperature
		flags |= 0x08
	}

	if config.MinHumidity != nil {
		params.MinHumidity = *config.MinHumidity
		flags |= 0x10
	}

	if config.MaxHumidity != nil {
		params.MaxHumidity = *config.MaxHumidity
		flags |= 0x20
	}

	if config.FanMode != nil {
		mode, _ := stringFanModeToRaw(*config.FanMode)
		// FIXME: check for ok here
		params.FanMode = mode
		flags |= 0x40
	}

	return flags
}

type TStatSettings struct {
	BacklightSetting uint8
	AutoMode         uint8
	Unknown1         uint8
	DeadBand         uint8
	CyclesPerHour    uint8
	SchedulePeriods  uint8
	ProgramsEnabled  uint8
	TempUnits        uint8
	Unknown2         uint8
	DealerName       [20]byte
	DealerPhone      [20]byte
}

func (params TStatSettings) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x3B, 0x06}
}


type TStatTemps struct {
	Zones [8]struct {
		Unknown  [2]uint8
		Temp16   [2]uint8
		Temp8    uint8
	}
}

type APITStatTemps struct {
	Zones [8]struct {
		Unknown  uint16
		Temp16   float32
		Temp     uint8
	}
}

func (params TStatTemps) addr() InfinityTableAddr {
	return InfinityTableAddr{0x00, 0x3D, 0x02}
}

func (params *TStatTemps) toAPI() *APITStatTemps {
	to := APITStatTemps{}

	for i, _ := range to.Zones {
		to.Zones[i].Unknown = uint16(params.Zones[i].Unknown[0]) << 8 + uint16(params.Zones[i].Unknown[1])
		to.Zones[i].Temp16 = float32(params.Zones[i].Temp16[0]) * 16 + float32(params.Zones[i].Temp16[1]) / 16
		to.Zones[i].Temp = params.Zones[i].Temp8
	}

	return &to
}


