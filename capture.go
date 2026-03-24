package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type BusCapture struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

var busCapture BusCapture

func autoCapturePath() string {
	return fmt.Sprintf("buscap-%s.jsonl", time.Now().Format("20060102-150405"))
}

type busCaptureRecord struct {
	Timestamp string `json:"ts"`
	UnixMs    int64  `json:"unix_ms"`
	Direction string `json:"dir"`
	RawHex    string `json:"raw_hex"`

	Valid *bool `json:"valid,omitempty"`
	Note  string `json:"note,omitempty"`

	Src     uint16 `json:"src,omitempty"`
	SrcHex  string `json:"src_hex,omitempty"`
	Dst     uint16 `json:"dst,omitempty"`
	DstHex  string `json:"dst_hex,omitempty"`
	Op      uint8  `json:"op,omitempty"`
	OpHex   string `json:"op_hex,omitempty"`
	OpName  string `json:"op_name,omitempty"`
	DataHex string `json:"data_hex,omitempty"`
}

func (c *BusCapture) Open(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	finalPath, err := nextCapturePath(path)
	if err != nil {
		log.Errorf("failed to select bus capture file '%s': %s", path, err)
		return false
	}

	f, err := os.OpenFile(finalPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		log.Errorf("failed to open bus capture file '%s': %s", finalPath, err)
		return false
	}

	if c.f != nil {
		_ = c.f.Close()
	}
	c.f = f
	c.path = finalPath
	log.Infof("Opened bus capture file '%s'", finalPath)
	return true
}

func nextCapturePath(requested string) (string, error) {
	path := strings.TrimSpace(requested)
	if path == "" {
		path = "buscap.jsonl"
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path, nil
	} else if err != nil {
		return "", err
	}

	ext := filepath.Ext(path)
	base := strings.TrimSuffix(path, ext)
	ts := time.Now().Format("20060102-150405")

	for i := 0; i < 1000; i++ {
		suffix := fmt.Sprintf("-%s", ts)
		if i > 0 {
			suffix = fmt.Sprintf("-%s-%03d", ts, i)
		}
		candidate := base + suffix + ext
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}

	return "", fmt.Errorf("unable to find free capture filename based on %s", path)
}

func (c *BusCapture) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.f != nil {
		if err := c.f.Close(); err != nil {
			log.Warnf("error closing bus capture file '%s': %s", c.path, err)
		}
		c.f = nil
	}
}

func (c *BusCapture) LogFrame(direction string, raw []byte, frame *InfinityFrame, valid bool, note string) {
	rec := busCaptureRecord{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		UnixMs:    time.Now().UnixMilli(),
		Direction: direction,
		RawHex:    hex.EncodeToString(raw),
		Valid:     &valid,
		Note:      note,
	}
	if frame != nil {
		rec.Src = frame.src
		rec.SrcHex = fmt.Sprintf("0x%04x", frame.src)
		rec.Dst = frame.dst
		rec.DstHex = fmt.Sprintf("0x%04x", frame.dst)
		rec.Op = frame.op
		rec.OpHex = fmt.Sprintf("0x%02x", frame.op)
		rec.OpName = frame.opString()
		rec.DataHex = hex.EncodeToString(frame.data)
	}

	c.logRecord(rec)
}

func (c *BusCapture) logRecord(rec busCaptureRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.f == nil {
		return
	}

	b, err := json.Marshal(rec)
	if err != nil {
		log.Errorf("bus capture json marshal failed: %s", err)
		return
	}
	b = append(b, '\n')

	if _, err = c.f.Write(b); err != nil {
		log.Errorf("bus capture write failed: %s", err)
		return
	}
	if err = c.f.Sync(); err != nil {
		log.Errorf("bus capture sync failed: %s", err)
	}
}
