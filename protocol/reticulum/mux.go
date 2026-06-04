package reticulum

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/log"
)

const (
	MaxReticulumMessage = 400 //410 is passing somehow, 420 already bad: should be 400 as stated in LXMF-rs/crates/libs/rns-core/src/packet.rs:14
	muxHeaderSize       = 3
	maxFragPayload      = MaxReticulumMessage - muxHeaderSize // 197

	// Control packet type bytes (high bit set). From PLAN.md + new types.
	TypeAuthCtrl     byte = 0x80 // auth exchange message
	TypeReauthReq    byte = 0x81 // re-auth request
	TypeNewConn      byte = 0x82 // new virtual connection; payload = "host:port"
	TypeCloseConn    byte = 0x83 // close virtual connection; no payload
	TypeRequestAuth  byte = 0x84 // challenge request (future)
	TypeResponseAuth byte = 0x85 // auth response (future)
)

// isControl reports whether a type byte represents a control packet (high bit set).
// Data packets use the full 7 lower bits for fragmentation info.
func isControl(b byte) bool { return b&0x80 != 0 }

// encodeDataByte packs fragmentation info for a data packet into the lower 7 bits.
//
// Bit layout: 0b0_[isLast 1 bit]_[partIndex 6 bits]
// isLast: 1 if this is the final fragment, 0 otherwise
// partIndex: 0-indexed (0 = first fragment), supports up to 64 fragments
//
// Panics on out-of-range inputs.
func encodeDataByte(totalParts, partIndex int) byte {
	if totalParts < 1 || totalParts > 64 || partIndex < 0 || partIndex >= totalParts {
		panic(fmt.Sprintf("encodeDataByte: invalid totalParts=%d partIndex=%d", totalParts, partIndex))
	}
	isLast := 0
	if partIndex == totalParts-1 {
		isLast = 1
	}
	return byte((isLast << 6) | (partIndex & 0x3F))
}

// decodeDataByte extracts fragmentation info from a data-packet type byte.
// Returns isLast (true if final fragment) and the 0-indexed partIndex.
func decodeDataByte(b byte) (isLast bool, partIndex int) {
	isLast = (b>>6)&1 != 0
	partIndex = int(b & 0x3F)
	return
}

// muxPacket is a decoded Reticulum mux message.
type muxPacket struct {
	typeByte byte
	connID   uint16
	payload  []byte
	// Decoded fragmentation fields; valid only when !isControl(typeByte).
	isLast    bool // true if this is the final fragment
	partIndex int  // 0-indexed
}

// encodePacket serialises a muxPacket to wire bytes (header + payload, no framing).
func encodePacket(p muxPacket) []byte {
	buf := make([]byte, muxHeaderSize+len(p.payload))
	buf[0] = p.typeByte
	binary.BigEndian.PutUint16(buf[1:3], p.connID)
	copy(buf[3:], p.payload)
	return buf
}

// decodePacket parses wire bytes into a muxPacket.
func decodePacket(b []byte) (muxPacket, error) {
	if len(b) < muxHeaderSize {
		return muxPacket{}, fmt.Errorf("mux: packet too short: %d bytes", len(b))
	}
	p := muxPacket{
		typeByte: b[0],
		connID:   binary.BigEndian.Uint16(b[1:3]),
		payload:  append([]byte(nil), b[3:]...),
	}
	if !isControl(p.typeByte) {
		p.isLast, p.partIndex = decodeDataByte(p.typeByte)
	}
	return p, nil
}

// fragment slices data into chunks of at most maxFragPayload bytes.
// Empty or nil input returns a single empty chunk (needed for TypeCloseConn payloads).
func fragment(data []byte) [][]byte {
	if len(data) == 0 {
		return [][]byte{{}}
	}
	var parts [][]byte
	for len(data) > 0 {
		size := maxFragPayload
		if size > len(data) {
			size = len(data)
		}
		parts = append(parts, data[:size])
		data = data[size:]
	}
	return parts
}

// fragBuffer accumulates fragments for one logical message.
// Only accessed from the readLoop goroutine; no locking needed.
type fragBuffer struct {
	parts   [][]byte
	gotLast bool
	lastIdx int
}

// addPart records one fragment. Returns the assembled payload and true when the final
// fragment has been received and all preceding parts are present.
func (fb *fragBuffer) addPart(partIndex int, isLast bool, data []byte) ([]byte, bool) {
	for len(fb.parts) <= partIndex {
		fb.parts = append(fb.parts, nil)
	}
	if fb.parts[partIndex] == nil {
		fb.parts[partIndex] = append([]byte(nil), data...)
	}
	if isLast {
		fb.gotLast = true
		fb.lastIdx = partIndex
	}
	if !fb.gotLast {
		return nil, false
	}
	for i := 0; i <= fb.lastIdx; i++ {
		if i >= len(fb.parts) || fb.parts[i] == nil {
			return nil, false
		}
	}
	assembled := make([]byte, 0, (fb.lastIdx+1)*maxFragPayload)
	for i := 0; i <= fb.lastIdx; i++ {
		assembled = append(assembled, fb.parts[i]...)
	}
	*fb = fragBuffer{}
	return assembled, true
}

// muxSession manages one Reticulum connection and multiplexes virtual connections.
type muxSession struct {
	inner      net.Conn
	writeMu    sync.Mutex // serialises writes so all fragments of one message are consecutive
	mu         sync.Mutex // protects conns
	nextConnID uint32     // accessed via sync/atomic; cast to uint16; exhausted if > 65535
	closedFlag uint32     // 0 = open, 1 = closed (atomic)
	conns      map[uint16]*muxConn
	logger     log.ContextLogger
	incomingCh chan *muxConn // non-nil on server side; new virtual conns arrive here
}

// newMuxSessionClient creates a client-side mux session (no incomingCh).
func newMuxSessionClient(inner net.Conn, logger log.ContextLogger) *muxSession {
	s := &muxSession{
		inner:  inner,
		conns:  make(map[uint16]*muxConn),
		logger: logger,
	}
	go s.readLoop()
	return s
}

// newMuxSessionServer creates a server-side mux session with an incomingCh.
func newMuxSessionServer(inner net.Conn, logger log.ContextLogger) *muxSession {
	s := &muxSession{
		inner:      inner,
		conns:      make(map[uint16]*muxConn),
		logger:     logger,
		incomingCh: make(chan *muxConn, 64),
	}
	go s.readLoop()
	return s
}

// isClosed reports whether this session has been shut down.
func (s *muxSession) isClosed() bool {
	return atomic.LoadUint32(&s.closedFlag) != 0
}

// OpenConn creates a new virtual connection and announces it to the peer (client side).
func (s *muxSession) OpenConn(dest string) (*muxConn, error) {
	id64 := atomic.AddUint32(&s.nextConnID, 1)
	if id64 > 65535 {
		return nil, fmt.Errorf("mux: connection ID space exhausted")
	}
	id := uint16(id64)

	mc := newMuxConn(id, s, dest)

	s.mu.Lock()
	s.conns[id] = mc
	s.mu.Unlock()

	if err := s.writeCtrl(TypeNewConn, id, []byte(dest)); err != nil {
		s.mu.Lock()
		delete(s.conns, id)
		s.mu.Unlock()
		mc.closeOnce.Do(func() { close(mc.done) })
		return nil, fmt.Errorf("mux: send TypeNewConn: %w", err)
	}
	return mc, nil
}

// writeCtrl sends a single control packet under writeMu.
func (s *muxSession) writeCtrl(typ byte, id uint16, payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.inner.Write(encodePacket(muxPacket{typeByte: typ, connID: id, payload: payload}))
	return err
}

// writeData fragments data and sends all packets under writeMu, ensuring
// no other write can interleave between fragments of the same message.
func (s *muxSession) writeData(id uint16, data []byte) error {
	parts := fragment(data)
	n := len(parts)
	if n > 64 {
		return fmt.Errorf("mux: payload requires %d fragments (max 64)", n)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for i, part := range parts {
		pkt := muxPacket{
			typeByte: encodeDataByte(n, i),
			connID:   id,
			payload:  part,
		}
		if _, err := s.inner.Write(encodePacket(pkt)); err != nil {
			return err
		}
	}
	return nil
}

// removeConn removes a virtual connection from the session map.
func (s *muxSession) removeConn(id uint16) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

// readLoop is the single goroutine that reads packets and dispatches them.
func (s *muxSession) readLoop() {
	defer s.closeAll()

	fragBufs := make(map[uint16]*fragBuffer)
	buf := make([]byte, MaxReticulumMessage)

	for {
		n, err := s.inner.Read(buf)
		if err != nil {
			if s.logger != nil {
				s.logger.Debug("mux readLoop exit: ", err)
			}
			return
		}

		pkt, err := decodePacket(buf[:n])
		if err != nil {
			if s.logger != nil {
				s.logger.Error("mux: decode error: ", err)
			}
			continue
		}

		if isControl(pkt.typeByte) {
			switch pkt.typeByte {
			case TypeNewConn:
				dest := string(pkt.payload)
				mc := newMuxConn(pkt.connID, s, dest)
				s.mu.Lock()
				s.conns[pkt.connID] = mc
				s.mu.Unlock()
				if s.incomingCh != nil {
					select {
					case s.incomingCh <- mc:
					default:
						if s.logger != nil {
							s.logger.Error("mux: incomingCh full, dropping conn ", pkt.connID)
						}
						mc.closeOnce.Do(func() { close(mc.done) })
						s.removeConn(pkt.connID)
					}
				}
			case TypeCloseConn:
				delete(fragBufs, pkt.connID)
				s.closeConnRemote(pkt.connID)
			default:
				if s.logger != nil {
					s.logger.Debug("mux: unhandled control 0x", fmt.Sprintf("%02x", pkt.typeByte))
				}
			}
			continue
		}

		// Data packet: accumulate fragments.
		fb := fragBufs[pkt.connID]
		if fb == nil {
			fb = &fragBuffer{}
			fragBufs[pkt.connID] = fb
		}
		assembled, done := fb.addPart(pkt.partIndex, pkt.isLast, pkt.payload)
		if !done {
			continue
		}
		delete(fragBufs, pkt.connID)

		s.mu.Lock()
		mc := s.conns[pkt.connID]
		s.mu.Unlock()
		if mc == nil {
			continue // connection already closed
		}
		select {
		case mc.readCh <- assembled:
		case <-mc.done:
			// conn closed by local side; discard
		}
	}
}

// closeConnRemote closes a virtual conn in response to a TypeCloseConn from the peer.
func (s *muxSession) closeConnRemote(id uint16) {
	s.mu.Lock()
	mc := s.conns[id]
	delete(s.conns, id)
	s.mu.Unlock()
	if mc != nil {
		mc.closeOnce.Do(func() { close(mc.done) })
	}
}

// closeAll marks the session closed and signals all virtual connections.
func (s *muxSession) closeAll() {
	if !atomic.CompareAndSwapUint32(&s.closedFlag, 0, 1) {
		return
	}
	s.mu.Lock()
	conns := s.conns
	s.conns = make(map[uint16]*muxConn)
	s.mu.Unlock()

	for _, mc := range conns {
		mc.closeOnce.Do(func() { close(mc.done) })
	}
	if s.incomingCh != nil {
		close(s.incomingCh)
	}
}

// Close shuts down the session and its underlying connection.
func (s *muxSession) Close() error {
	s.closeAll()
	return s.inner.Close()
}

// muxConn is a virtual TCP connection multiplexed over a muxSession.
// Implements net.Conn.
type muxConn struct {
	id      uint16
	session *muxSession
	dest    string

	readCh    chan []byte   // assembled message payloads; never closed (use done instead)
	readBuf   []byte        // leftover bytes from the last readCh receive
	done      chan struct{} // closed exactly once via closeOnce
	closeOnce sync.Once

	localAddr  net.Addr
	remoteAddr net.Addr
}

func newMuxConn(id uint16, s *muxSession, dest string) *muxConn {
	return &muxConn{
		id:         id,
		session:    s,
		dest:       dest,
		readCh:     make(chan []byte, 64),
		done:       make(chan struct{}),
		localAddr:  reticulumAddr{network: "reticulum-mux", str: fmt.Sprintf("mux:%d", id)},
		remoteAddr: reticulumAddr{network: "reticulum-mux", str: dest},
	}
}

// Read reads data from the virtual connection. Blocks until data is available or the
// connection is closed.
func (c *muxConn) Read(b []byte) (int, error) {
	for {
		if len(c.readBuf) > 0 {
			n := copy(b, c.readBuf)
			c.readBuf = c.readBuf[n:]
			return n, nil
		}
		select {
		case data := <-c.readCh:
			c.readBuf = data
		case <-c.done:
			// Drain one pending message if available before returning EOF.
			select {
			case data := <-c.readCh:
				c.readBuf = data
			default:
				return 0, io.EOF
			}
		}
	}
}

// Write sends data over the virtual connection, fragmenting if necessary.
func (c *muxConn) Write(b []byte) (int, error) {
	if err := c.session.writeData(c.id, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close signals this virtual connection is closed and notifies the peer.
func (c *muxConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.session.writeCtrl(TypeCloseConn, c.id, nil) // best-effort
		c.session.removeConn(c.id)
	})
	return nil
}

func (c *muxConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *muxConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *muxConn) SetDeadline(t time.Time) error      { return nil }
func (c *muxConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *muxConn) SetWriteDeadline(t time.Time) error { return nil }
