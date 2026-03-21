package main

import (
	"bytes"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const remoteZonePayloadLen = 20

type RemoteZoneSnapshot struct {
	ZoneNumber      int
	Device          uint16
	ResponsePayload [remoteZonePayloadLen]byte
	HaveResponse    bool
	WritePayload    [remoteZonePayloadLen]byte
	HaveWrite       bool
	LastOnWrite     [remoteZonePayloadLen]byte
	HaveLastOnWrite bool
	LastOffWrite    [remoteZonePayloadLen]byte
	HaveLastOffWrite bool
	UpdatedAt       time.Time
	LastSource      string
}

type remoteZoneCacheType struct {
	cacheMutex sync.Mutex
	zones      map[int]RemoteZoneSnapshot
}

var remoteZoneCache = remoteZoneCacheType{zones: make(map[int]RemoteZoneSnapshot)}

func remoteZoneDevice(zoneNumber int) (uint16, bool) {
	if zoneNumber <= 1 || zoneNumber > 8 {
		return 0, false
	}
	return devTSTAT + uint16(zoneNumber<<8), true
}

func remoteZoneNumber(device uint16) (int, bool) {
	if device <= devTSTAT || device&0x00ff != 0x0001 {
		return 0, false
	}

	zoneNumber := int((device - devTSTAT) >> 8)
	if zoneNumber <= 1 || zoneNumber > 8 {
		return 0, false
	}

	return zoneNumber, true
}

func remoteZoneOffState(state uint8) bool {
	return state&0x40 != 0
}

func remoteZoneStateName(state uint8) string {
	if remoteZoneOffState(state) {
		return "OFF"
	}
	return "ON"
}

func copyRemoteZonePayload(arr [remoteZonePayloadLen]byte) []byte {
	payload := make([]byte, len(arr))
	copy(payload, arr[:])
	return payload
}

func payloadToRemoteZoneArray(payload []byte) ([remoteZonePayloadLen]byte, bool) {
	arr := [remoteZonePayloadLen]byte{}
	if len(payload) != remoteZonePayloadLen {
		return arr, false
	}
	copy(arr[:], payload)
	return arr, true
}

func currentRemoteZoneOff(snapshot RemoteZoneSnapshot) (bool, bool) {
	if snapshot.HaveWrite {
		return remoteZoneOffState(snapshot.WritePayload[0]), true
	}
	if snapshot.HaveResponse {
		return remoteZoneOffState(snapshot.ResponsePayload[0]), true
	}
	return false, false
}

func logRemoteZonePayload(event string, zoneNumber int, device uint16, oldPayload []byte, newPayload []byte) {
	if len(newPayload) != remoteZonePayloadLen {
		return
	}
	if len(oldPayload) == remoteZonePayloadLen && bytes.Equal(oldPayload, newPayload) {
		return
	}

	if len(oldPayload) != remoteZonePayloadLen {
		log.Infof(
			"zone %d %s dev=0x%04x state=%s raw=0x%02x payload=%s",
			zoneNumber,
			event,
			device,
			remoteZoneStateName(newPayload[0]),
			newPayload[0],
			hex.EncodeToString(newPayload),
		)
		return
	}

	if oldPayload[0] != newPayload[0] {
		log.Infof(
			"zone %d %s dev=0x%04x state=%s->%s raw=0x%02x->0x%02x payload=%s",
			zoneNumber,
			event,
			device,
			remoteZoneStateName(oldPayload[0]),
			remoteZoneStateName(newPayload[0]),
			oldPayload[0],
			newPayload[0],
			hex.EncodeToString(newPayload),
		)
		return
	}

	log.Debugf(
		"zone %d %s dev=0x%04x state=%s raw=0x%02x payload=%s",
		zoneNumber,
		event,
		device,
		remoteZoneStateName(newPayload[0]),
		newPayload[0],
		hex.EncodeToString(newPayload),
	)
}

func (c *remoteZoneCacheType) snapshot(zoneNumber int) (RemoteZoneSnapshot, bool) {
	c.cacheMutex.Lock()
	defer c.cacheMutex.Unlock()

	snapshot, ok := c.zones[zoneNumber]
	return snapshot, ok
}

func (c *remoteZoneCacheType) zoneOff(zoneNumber int) (bool, string, bool) {
	snapshot, ok := c.snapshot(zoneNumber)
	if !ok {
		return false, "", false
	}

	zoneOff, ok := currentRemoteZoneOff(snapshot)
	if !ok {
		return false, "", false
	}

	return zoneOff, snapshot.LastSource, true
}

func (c *remoteZoneCacheType) rememberResponse(device uint16, payload []byte) {
	zoneNumber, ok := remoteZoneNumber(device)
	if !ok {
		return
	}

	arr, ok := payloadToRemoteZoneArray(payload)
	if !ok {
		log.Warnf("zone %d invalid 00041e payload len=%d from dev=0x%04x", zoneNumber, len(payload), device)
		return
	}

	c.cacheMutex.Lock()
	snapshot := c.zones[zoneNumber]
	oldPayload := []byte(nil)
	if snapshot.HaveResponse {
		oldPayload = copyRemoteZonePayload(snapshot.ResponsePayload)
	}
	snapshot.ZoneNumber = zoneNumber
	snapshot.Device = device
	snapshot.ResponsePayload = arr
	snapshot.HaveResponse = true
	snapshot.UpdatedAt = time.Now()
	snapshot.LastSource = "00041e"
	c.zones[zoneNumber] = snapshot
	c.cacheMutex.Unlock()

	logRemoteZonePayload("remote 00041e", zoneNumber, device, oldPayload, payload)
}

func (c *remoteZoneCacheType) rememberWrite(device uint16, payload []byte, source string) {
	zoneNumber, ok := remoteZoneNumber(device)
	if !ok {
		return
	}

	arr, ok := payloadToRemoteZoneArray(payload)
	if !ok {
		log.Warnf("zone %d invalid 00041f payload len=%d from dev=0x%04x", zoneNumber, len(payload), device)
		return
	}

	c.cacheMutex.Lock()
	snapshot := c.zones[zoneNumber]
	oldPayload := []byte(nil)
	if snapshot.HaveWrite {
		oldPayload = copyRemoteZonePayload(snapshot.WritePayload)
	}
	snapshot.ZoneNumber = zoneNumber
	snapshot.Device = device
	snapshot.WritePayload = arr
	snapshot.HaveWrite = true
	if source == "thermostat" {
		if remoteZoneOffState(arr[0]) {
			snapshot.LastOffWrite = arr
			snapshot.HaveLastOffWrite = true
		} else {
			snapshot.LastOnWrite = arr
			snapshot.HaveLastOnWrite = true
		}
	}
	snapshot.UpdatedAt = time.Now()
	snapshot.LastSource = "00041f"
	c.zones[zoneNumber] = snapshot
	c.cacheMutex.Unlock()

	event := "remote 00041f"
	if len(source) > 0 {
		event = event + "/" + source
	}
	logRemoteZonePayload(event, zoneNumber, device, oldPayload, payload)
}

func buildRemoteZoneOffFallback(zoneNumber int, snapshot RemoteZoneSnapshot) ([]byte, string, bool) {
	if snapshot.HaveLastOffWrite {
		return copyRemoteZonePayload(snapshot.LastOffWrite), "last-off-write", true
	}

	if snapshot.HaveWrite || snapshot.HaveResponse {
		base := snapshot.ResponsePayload
		if snapshot.HaveWrite {
			base = snapshot.WritePayload
		}
		payload := copyRemoteZonePayload(base)
		rawState := byte(0x50)
		if payload[0]&0x80 != 0 {
			rawState = 0xd0
		}
		payload[0] = rawState
		payload[1] = 0x03
		payload[2] = 0x00
		payload[3] = 0x00
		payload[4] = 0x00
		payload[5] = 0x00
		payload[10] = 0x04
		return payload, "family-off", true
	}

	return nil, "", false
}

func (c *remoteZoneCacheType) offPayload(zoneNumber int) ([]byte, string, bool) {
	snapshot, ok := c.snapshot(zoneNumber)
	if !ok {
		return nil, "", false
	}

	if payload, reason, ok := buildRemoteZoneOffFallback(zoneNumber, snapshot); ok {
		return payload, reason, true
	}

	return nil, "", false
}

func (c *remoteZoneCacheType) onPayload(zoneNumber int) ([]byte, string, bool) {
	snapshot, ok := c.snapshot(zoneNumber)
	if !ok {
		return nil, "", false
	}

	if snapshot.HaveLastOnWrite {
		return copyRemoteZonePayload(snapshot.LastOnWrite), "last-on-write", true
	}
	if snapshot.HaveWrite && !remoteZoneOffState(snapshot.WritePayload[0]) {
		return copyRemoteZonePayload(snapshot.WritePayload), "last-write", true
	}

	return nil, "", false
}

func writeRemoteZonePayload(zoneNumber int, payload []byte, reason string) bool {
	device, ok := remoteZoneDevice(zoneNumber)
	if !ok {
		log.Errorf("zone %d remote 00041f write (%s) has no remote device", zoneNumber, reason)
		return false
	}
	if len(payload) != remoteZonePayloadLen {
		log.Errorf("zone %d remote 00041f write (%s) has invalid payload len=%d", zoneNumber, reason, len(payload))
		return false
	}

	log.Infof(
		"zone %d remote 00041f write (%s) dev=0x%04x state=%s raw=0x%02x payload=%s",
		zoneNumber,
		reason,
		device,
		remoteZoneStateName(payload[0]),
		payload[0],
		hex.EncodeToString(payload),
	)

	if !infinity.WriteAs(devTSTAT, device, []byte{0x00, 0x04, 0x1f}, payload[0:3], payload[3:]) {
		log.Errorf("zone %d remote 00041f write (%s) failed", zoneNumber, reason)
		return false
	}

	remoteZoneCache.rememberWrite(device, payload, "api")
	return true
}

func parseRemoteZonePayloadHex(s string) ([]byte, bool) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil, false
	}
	payload, err := hex.DecodeString(s)
	if err != nil || len(payload) != remoteZonePayloadLen {
		return nil, false
	}
	return payload, true
}
