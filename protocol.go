package main

import (
	"bytes"
	"encoding/binary"
	"time"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/tarm/serial"
)

const (
	devTSTAT = uint16(0x2001)
	devSAM   = uint16(0x9201)
)

const responseTimeout = 500
const responseRetries = 5

type snoopCallback func(*InfinityFrame)

type InfinityProtocolRawRequest struct {
	data *[]byte
}

type InfinityProtocolSnoop struct {
	srcMin uint16
	srcMax uint16
	cb     snoopCallback
}

type protocolStats struct {
	rcvs	int64	// candidate frames received
	frerrs	int64	// framing errors
	frames	int64	// valid frames received
	fself	int64	// frames addressed to me
	fother	int64	// frames addressed to others
	fsnoop	int64	// frames addressed to others, snooped

	sresp	int64	// response msgs ordered
	srd	int64	// action (originated) msgs ordered
	srdt	int64	// action (originated) msgs ordered
	swr	int64	// action (originated) msgs ordered

	aact	int64	// actions originated
	aretr	int64	// actions retransmitted
	aother	int64	// something other than expected response when resp expected
	aok1	int64	// actions processed OK w/o retrans
	aokN	int64	// actions processed OK a retrans
	aokms	int64	// total milliseconds of elapsed time for successful transactions (aok1 + aokN)
	afail	int64	// actions failed
	afailms	int64	// total milliseconds of elapsed time for failed transactions (afail)
}

type InfinityProtocol struct {
	device     string
	port       *serial.Port
	responseCh chan *InfinityFrame
	actionCh   chan *Action
	snoops     []InfinityProtocolSnoop
	statTime   int64		// time stats cleared (unix ms
	stats	   *protocolStats
}

type Action struct {
	requestFrame  *InfinityFrame
	responseFrame *InfinityFrame
	ok            bool
	ch            chan bool
}

var readTimeout = time.Second * 5

func (p *InfinityProtocol) openSerial() error {
	log.Printf("opening serial interface: %s", p.device)
	if p.port != nil {
		p.port.Close()
	}

	c := &serial.Config{Name: p.device, Baud: 38400, ReadTimeout: readTimeout}
	var err error
	p.port, err = serial.OpenPort(c)
	if err != nil {
		return err
	}

	return nil
}

func (p *InfinityProtocol) Open() error {
	err := p.openSerial()
	if err != nil {
		return err
	}

	p.responseCh = make(chan *InfinityFrame, 32)
	p.actionCh = make(chan *Action)

	p.stats = new(protocolStats)

	go p.reader()
	go p.broker()

	return nil
}

func (p *InfinityProtocol) handleFrame(frame *InfinityFrame) *InfinityFrame {
	// log.Printf("read frame: %s", frame)
	RLogger.Log(frame)

	switch frame.op {
	case opRESPONSE:
		if frame.dst == devSAM {
			p.stats.fself++
			p.responseCh <- frame
		} else {
			p.stats.fother++
		}

		if len(frame.data) > 3 {
			for _, s := range p.snoops {
				if frame.src >= s.srcMin && frame.src <= s.srcMax {
					s.cb(frame)
				}
			}
		}
	case opWRITE:
		if frame.src == devTSTAT && frame.dst == devSAM {
			p.stats.fself++
			return writeAck
		} else {
			p.stats.fother++
		}
	}

	return nil
}

func (p *InfinityProtocol) reader() {
	defer panic("exiting InfinityProtocol reader, this should never happen")

	msg := []byte{}
	buf := make([]byte, 1024)

	for {
		if p.port == nil {
			msg = []byte{}
			p.openSerial()
		}

		n, err := p.port.Read(buf)
		if n == 0 || err != nil {
			log.Printf("error reading from serial port: %s", err.Error())
			if p.port != nil {
				p.port.Close()
			}
			p.port = nil
			continue
		}

		p.stats.rcvs++

		// log.Printf("%q", buf[:n])
		msg = append(msg, buf[:n]...)
		// log.Printf("buf len is: %v", len(msg))

		for {
			if len(msg) < 10 {
				break
			}
			l := int(msg[4]) + 10
			if len(msg) < l {
				break
			}
			buf := msg[:l]

			frame := &InfinityFrame{}
			if frame.decode(buf) {
				p.stats.frames++
				response := p.handleFrame(frame)
				if response != nil {
					p.stats.sresp++
					p.sendFrame(response.encode())
				}
				// Intentionally didn't do msg = msg[l:] to avoid potential
				// memory leak.  Not sure if it makes a difference...
				msg = msg[:copy(msg, msg[l:])]
			} else {
				p.stats.frerrs++
				// Corrupt message, move ahead one byte and continue parsing
				msg = msg[:copy(msg, msg[1:])]
			}
		}
	}
}

func (p *InfinityProtocol) broker() {
	defer panic("exiting InfinityProtocol broker, this should never happen")

	for {
		// log.Debug("entering action select")
		select {
		case action := <-p.actionCh:
			p.performAction(action)
		case <-p.responseCh:
			log.Warn("dropping unexpected response")
		}
	}
}

func (p *InfinityProtocol) performAction(action *Action) {
	// log.Infof("encoded frame: %s", action.requestFrame)
	encodedFrame := action.requestFrame.encode()

	p.stats.aact++
	stime := time.Now()

	p.sendFrame(encodedFrame)
	ticker := time.NewTicker(time.Millisecond * responseTimeout)
	defer ticker.Stop()
	for tries := 0; tries < responseRetries; {
		select {
		case res := <-p.responseCh:
			// at this point we just know it's an opRRESPONSE but could be to someone else
			// or to us from a different thread
			if res.src != action.requestFrame.dst {
				p.stats.aother++
				continue
			}

			// if it was a READ, the table must match; if it was a WRITE the resp len must be 1 and the resp must be 00
			if action.requestFrame.op == opREAD {
				// check for a write resp coming in for a read req, can happen if the write resp was delayed and we timed out waiting for it
				if len(res.data) < 3 {
					p.stats.aother++
					continue;
				}

				reqTable := action.requestFrame.data[0:3]
				resTable := res.data[0:3]

				if !bytes.Equal(reqTable, resTable) {
					p.stats.aother++
					continue
				}
			} else if action.requestFrame.op == opWRITE {
				if res.dataLen != 1 || !bytes.Equal(res.data[0:1], []byte{00}) {
					p.stats.aother++
					continue
				}
			}

			if tries == 0 { p.stats.aok1++ } else {p.stats.aokN++ }
			p.stats.aokms = p.stats.aokms + time.Since(stime).Milliseconds()

			action.responseFrame = res
			// log.Printf("got response!")
			action.ok = true
			action.ch <- true
			// log.Printf("sent action!")
			return
		case <-ticker.C:
			log.Debug("timeout waiting for response, retransmitting frame")
			p.stats.aretr++
			p.sendFrame(encodedFrame)
			tries++
		}
	}

	log.Printf("action timed out")
	p.stats.afailms = p.stats.afailms + time.Since(stime).Milliseconds()
	p.stats.afail++
	action.ch <- false
}

func (p *InfinityProtocol) send(dst uint16, op uint8, requestData []byte, response interface{}) bool {
	f := InfinityFrame{src: devSAM, dst: dst, op: op, data: requestData}
	act := &Action{requestFrame: &f, ch: make(chan bool)}

	// Send action to action handling goroutine
	p.actionCh <- act
	// Wait for response
	ok := <-act.ch

	if ok && op == opREAD && act.responseFrame != nil && act.responseFrame.data != nil && len(act.responseFrame.data) > 6 {
		raw, ok := response.(InfinityProtocolRawRequest)
		reginfosz := 6
		if act.responseFrame.data[1] == 0x3d {
			// register 3dXX seemingly has a 4-byte reg header not 6
			reginfosz = 4
		}
		if ok {
			// log.Printf(">>>> handling a RawRequest")
			*raw.data = append(*raw.data, act.responseFrame.data[reginfosz:]...)
			// log.Printf("raw data length is: %d", len(*raw.data))
		} else {
			// log.Printf("%+v", act.responseFrame.data)
			r := bytes.NewReader(act.responseFrame.data[reginfosz:])
			e := binary.Read(r, binary.BigEndian, response)
			if e != nil {
				log.Errorf("p.send Read failed from %d bytes: %v", len(act.responseFrame.data[reginfosz:]), e)
			}
			// log.Printf("YAH %+v", response)
		}
	}

	return ok
}

func (p *InfinityProtocol) Write(dst uint16, table []byte, addr []byte, params interface{}) bool {
	buf := new(bytes.Buffer)
	buf.Write(table[:])
	buf.Write(addr[:])
	binary.Write(buf, binary.BigEndian, params)

	p.stats.swr++
	return p.send(dst, opWRITE, buf.Bytes(), nil)
}

// Update a non-zoned table
func (p *InfinityProtocol) WriteTable(dst uint16, table InfinityTable, flags uint16) bool {
	addr := table.addr()
	fl := []byte{0x00, uint8(flags >> 8), uint8(flags)}
	return p.Write(dst, addr[:], fl, table)
}

// Update a table, specifying the zone index number (0 = Zone 1, 1 = Zone 2, etc).
func (p *InfinityProtocol) WriteTableZ(dst uint16, table InfinityTable, zflag uint8, flags uint16) bool {
	addr := table.addr()
	fl := []byte{zflag, uint8(flags >> 8), uint8(flags)}
	return p.Write(dst, addr[:], fl, table)
}

func (p *InfinityProtocol) Read(dst uint16, addr InfinityTableAddr, params interface{}) bool {
	p.stats.srd++
	return p.send(dst, opREAD, addr[:], params)
}

func (p *InfinityProtocol) ReadTable(dst uint16, table InfinityTable) bool {
	addr := table.addr()
	p.stats.srdt++
	return p.send(dst, opREAD, addr[:], table)
}

func (p *InfinityProtocol) sendFrame(buf []byte) bool {
	// Ensure we're not in the middle of reopening the serial port due to an error.
	if p.port == nil {
		return false
	}

	// log.Debugf("transmitting frame: %x", buf)
	_, err := p.port.Write(buf)
	if err != nil {
		log.Errorf("error writing to serial: %s", err.Error())
		p.port.Close()
		p.port = nil
		return false
	}
	return true
}

func (p *InfinityProtocol) snoopResponse(srcMin uint16, srcMax uint16, cb snoopCallback) {
	s := InfinityProtocolSnoop{srcMin: srcMin, srcMax: srcMax, cb: cb}
	p.snoops = append(p.snoops, s)
}

func (p *InfinityProtocol) getStatsString() string {

	ostats := p.stats
	p.stats = new(protocolStats)

	// calculate avgs
	if ostats.aok1 > 0 || ostats.aokN > 0 {
		ostats.aokms = ostats.aokms / (ostats.aok1 + ostats.aokN)
	}

	if ostats.afail > 0 {
		ostats.afailms = ostats.afailms / ostats.afail
	}
	ss := fmt.Sprintf("%+v", ostats)

	return ss
}

