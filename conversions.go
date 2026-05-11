package main

func rawModeToString(mode uint8) string {
	switch mode {
	case 0:
		return "heat" // heat source: "system in control"
	case 1:
		return "cool"
	case 2:
		return "auto"
	case 3:
		return "electric" // electric heat only - fan coil system
		// on a furnace system perhaps this is furnace only - untested
	case 4:
		return "heatpump" // heat pump only
	case 5:
		return "off"
	default:
		return "unknown"
	}
}

func stringModeToRaw(mode string) (uint8, bool) {
	switch mode {
	case "heat":
		return 0, true
	case "cool":
		return 1, true
	case "auto":
		return 2, true
	case "off":
		return 5, true
	default:
		return 5, false
	}
}

func rawFanModeToString(mode uint8) string {
	switch mode {
	case 0:
		return "auto"
	case 1:
		return "low"
	case 2:
		return "med"
	case 3:
		return "high"
	default:
		return "unknown"
	}
}

func stringFanModeToRaw(mode string) (uint8, bool) {
	switch mode {
	case "auto":
		return 0, true
	case "low":
		return 1, true
	case "med":
		return 2, true
	case "high":
		return 3, true
	default:
		return 0, false
	}
}

func rawActionToString(action uint8) string {
	switch action {
	case 0:
		return "idle" // heat source: "system in control"
	case 1:
		return "cooling"
	case 2:
		return "cooling"
	default:
		return "heating" // electric heat only - fan coil system
		// on a furnace system perhaps this is furnace only - untested
	}
}
