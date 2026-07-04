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
	// MaxReticulumMessage is the maximum total mux-packet size (header + payload) that
	// is transmitted as a single Reticulum data_packet.  It must fit after Fernet
	// encryption within the narrowest interface MTU in the path.
	//
	// The LoRa/RNode serial link MTU is 220 bytes.  A data_packet's on-wire size is:
	//   HEADER_MAXSIZE(35) + IFAC_MIN_SIZE(1) + IV(16) + ciphertext + HMAC(32)
	// where ciphertext = ceil(plaintext/16)*16.  Solving for the largest plaintext
	// that still fits in 220 bytes (using worst-case AES padding of 16):
	//   220 − 35 − 1 − 16 − 32 − 16 = 120 bytes.
	//
	// This is the same formula as rns-core LXMF_MAX_PAYLOAD = 400 for MTU=500 (TCP).
	// Larger mux packets are sent via the Reticulum Resource protocol instead.
	MaxReticulumMessage = 120
	muxHeaderSize       = 3
	maxFragPayload      = MaxReticulumMessage - muxHeaderSize // 117

	// Control packet type bytes (high bit set).
	TypeAuthCtrl     byte = 0x80 // auth exchange message
	TypeReauthReq    byte = 0x81 // re-auth request
	TypeNewConn      byte = 0x82 // new virtual connection; payload = "host:port"
	TypeCloseConn    byte = 0x83 // close virtual connection; no payload
	TypeRequestAuth  byte = 0x84 // challenge request
	TypeResponseAuth byte = 0x85 // auth response
	// TypeLargeData carries a payload that exceeds the 64-fragment limit (~25 KB).
	// The bridge routes this to the Reticulum Resource protocol; the receiver gets
	// the reassembled data via a single on_data callback.
	TypeLargeData byte = 0xC0

	maxRetransmitDelay = 30 * time.Second
	defaultFragTimeout = 2 * time.Second
)

// isControl reports whether a type byte represents a control packet (high bit set).
func isControl(b byte) bool { return b&0x80 != 0 }

// encodeDataByte packs fragmentation info into the lower 7 bits.
// Bit layout: 0b0_[isLast 1 bit]_[partIndex 6 bits]
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
func decodeDataByte(b byte) (isLast bool, partIndex int) {
	isLast = (b>>6)&1 != 0
	partIndex = int(b & 0x3F)
	return
}

// muxPacket is a decoded mux message.
type muxPacket struct {
	typeByte  byte
	connID    uint16
	payload   []byte
	isLast    bool
	partIndex int
}

// encodePacket serialises a muxPacket to wire bytes.
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

// fragmentWith slices data into chunks of at most mfp bytes.
func fragmentWith(data []byte, mfp int) [][]byte {
	if len(data) == 0 {
		return [][]byte{{}}
	}
	var parts [][]byte
	for len(data) > 0 {
		size := mfp
		if size > len(data) {
			size = len(data)
		}
		parts = append(parts, data[:size])
		data = data[size:]
	}
	return parts
}

// fragment slices data using the package-level LoRa default maxFragPayload.
func fragment(data []byte) [][]byte { return fragmentWith(data, maxFragPayload) }

// fragmentData slices data using this session's maxFragPayload.
func (s *muxSession) fragmentData(data []byte) [][]byte {
	return fragmentWith(data, s.maxFragPayload)
}

// fragBuffer accumulates fragments for one logical message.
// Only accessed from the readLoop goroutine; no locking needed.
type fragBuffer struct {
	parts        [][]byte
	gotLast      bool
	lastIdx      int
	lastActivity time.Time
	// delivered is set after the assembled message has been sent to readCh.
	// Subsequent retransmits of the same connID are ACKed but not re-delivered,
	// preventing duplicate HTTP handler invocations when the sender retransmits
	// before its ACK arrives.
	delivered bool
}

// addPart records one fragment. Returns the assembled payload and true when complete.
func (fb *fragBuffer) addPart(partIndex int, isLast bool, data []byte) ([]byte, bool) {
	fb.lastActivity = time.Now()
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

// ── Send queue ───────────────────────────────────────────────────────────────

// fragKey uniquely identifies one fragment within a session.
type fragKey struct {
	connID    uint16
	partIndex uint8
}

// msgSend tracks the in-order send completion of one logical message's fragments.
// writeData blocks on `done` so the *next* message on the connection is only sent
// after this one has finished going out — guaranteeing the receiver never sees a
// later message (fast fragment path) overtake an earlier one (e.g. a direct
// TypeLargeData/resource write). This is why no per-message sequence number is
// needed: ordering is enforced at the source by serialising messages per conn.
type msgSend struct {
	done   chan struct{} // closed once the last fragment has been written to inner
	failed atomic.Bool   // set if any fragment's inner write returned an error
}

// queuedFrag is one item in the send queue.
type queuedFrag struct {
	encoded   []byte
	key       fragKey
	msg       *msgSend // shared by all fragments of one message
	lastInMsg bool     // true for the final fragment; closes msg.done when sent
}

// sendQueue serialises fragment sends through a single worker goroutine so that
// fragments reach the inner conn in the order they were enqueued.
type sendQueue struct {
	ch chan *queuedFrag
}

func newSendQueue(bufSize int) *sendQueue {
	return &sendQueue{ch: make(chan *queuedFrag, bufSize)}
}

// run is the send-queue worker goroutine. It drains queued fragments and calls
// s.doWrite for each. Exits when s.done is closed.
func (q *sendQueue) run(s *muxSession) {
	for {
		select {
		case <-s.done:
			return
		case f := <-q.ch:
			s.doWrite(f)
		}
	}
}

// ── muxSession ───────────────────────────────────────────────────────────────

// muxSession manages one Reticulum connection and multiplexes virtual connections.
type muxSession struct {
	inner        net.Conn
	innerWriteMu sync.Mutex // protects individual s.inner.Write() calls
	mu           sync.Mutex // protects conns
	nextConnID   uint32     // accessed via sync/atomic; cast to uint16
	closedFlag   uint32     // 0 = open, 1 = closed (atomic)
	done         chan struct{}
	conns        map[uint16]*muxConn
	logger       log.ContextLogger
	incomingCh   chan *muxConn // non-nil on server side

	// maxMsg is the max total mux-packet size (header + payload) for this session.
	// Derived from the link's packet_mdu minus Fernet overhead; varies per interface type.
	maxMsg         int
	maxFragPayload int // maxMsg - muxHeaderSize

	sendQ       *sendQueue
	fragTimeout time.Duration

	// statFragsSent counts fragments written to inner, for observability.
	statFragsSent atomic.Uint64
}

// newMuxSession is the internal constructor used by both public constructors and tests.
// maxMsg is the maximum total mux-packet size (header + payload) for this session;
// pass MaxReticulumMessage for the LoRa default.
func newMuxSession(
	inner net.Conn,
	logger log.ContextLogger,
	isServer bool,
	maxMsg int,
) *muxSession {
	if maxMsg <= muxHeaderSize {
		maxMsg = MaxReticulumMessage
	}
	s := &muxSession{
		inner:          inner,
		conns:          make(map[uint16]*muxConn),
		logger:         logger,
		done:           make(chan struct{}),
		maxMsg:         maxMsg,
		maxFragPayload: maxMsg - muxHeaderSize,
		sendQ:          newSendQueue(4096),
		fragTimeout:    defaultFragTimeout,
	}
	if isServer {
		s.incomingCh = make(chan *muxConn, 1024)
	}
	go s.readLoop()
	go s.sendQ.run(s)
	// Reliability, ordering, acknowledgement and flow control are handled by the
	// Reticulum channel under the bridge, so the mux keeps no window, in-flight
	// table, fragment ACKs, or retransmit loop.
	return s
}

func newMuxSessionClient(inner net.Conn, logger log.ContextLogger, maxMsg int) *muxSession {
	return newMuxSession(inner, logger, false, maxMsg)
}

func newMuxSessionServer(inner net.Conn, logger log.ContextLogger, maxMsg int) *muxSession {
	return newMuxSession(inner, logger, true, maxMsg)
}

// isClosed reports whether this session has been shut down.
func (s *muxSession) isClosed() bool {
	return atomic.LoadUint32(&s.closedFlag) != 0
}

// OpenConn creates a new virtual connection and announces it to the peer.
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

// writeCtrl sends a single control packet directly (bypasses the send queue and window).
// Safe to call concurrently.
func (s *muxSession) writeCtrl(typ byte, id uint16, payload []byte) error {
	s.innerWriteMu.Lock()
	defer s.innerWriteMu.Unlock()
	_, err := s.inner.Write(encodePacket(muxPacket{typeByte: typ, connID: id, payload: payload}))
	return err
}

// writeData sends data for virtual connection id.
// Payloads exceeding the 64-fragment limit are sent as TypeLargeData via the
// Reticulum Resource protocol. Smaller payloads are fragmented and enqueued to
// the send queue; the worker hands each fragment to the inner conn, which carries
// it over the Reticulum channel that provides reliability and flow control.
func (s *muxSession) writeData(id uint16, data []byte) error {
	if len(data) > s.maxFragPayload*64 {
		if s.logger != nil {
			s.logger.Trace("mux: conn=", id, " TypeLargeData data_len=", len(data))
		}
		err := s.writeCtrl(TypeLargeData, id, data)
		if err != nil && s.logger != nil {
			s.logger.Warn("mux: conn=", id, " TypeLargeData failed data_len=", len(data), " err=", err)
		}
		return err
	}
	parts := s.fragmentData(data)
	n := len(parts)
	tracker := &msgSend{done: make(chan struct{})}
	for i, part := range parts {
		pkt := muxPacket{typeByte: encodeDataByte(n, i), connID: id, payload: part}
		f := &queuedFrag{
			encoded:   encodePacket(pkt),
			key:       fragKey{connID: id, partIndex: uint8(i)},
			msg:       tracker,
			lastInMsg: i == n-1,
		}
		select {
		case s.sendQ.ch <- f:
		case <-s.done:
			return io.ErrClosedPipe
		}
	}
	// Block until every fragment of THIS message has been written to the inner
	// conn. Only then may muxConn.Write return and the next message be sent, so
	// messages never interleave on the wire and the receiver reassembles the byte
	// stream in order without needing a sequence number.
	select {
	case <-tracker.done:
		if tracker.failed.Load() {
			return fmt.Errorf("mux: conn=%d message send failed", id)
		}
		return nil
	case <-s.done:
		return io.ErrClosedPipe
	}
}

// doWrite writes one queued fragment to the inner conn. Reliability,
// acknowledgement, flow control and retransmission are the Reticulum channel's
// job (the bridge sends each fragment over the link's reliable channel), so the
// mux keeps no window, in-flight table, fragment ACKs, or retransmit loop.
func (s *muxSession) doWrite(f *queuedFrag) {
	// Unblock the writing muxConn.Write once the final fragment of the message has
	// been pushed to the inner conn. writeData waits on this so a connection's
	// messages are handed to the link in order.
	if f.lastInMsg && f.msg != nil {
		defer close(f.msg.done)
	}

	s.innerWriteMu.Lock()
	_, err := s.inner.Write(f.encoded)
	s.innerWriteMu.Unlock()
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("mux: conn=", f.key.connID, " part=", f.key.partIndex, " write failed: ", err)
		}
		if f.msg != nil {
			f.msg.failed.Store(true)
		}
		s.logStats("fragment write error")
		return
	}
	s.statFragsSent.Add(1)
	s.logStats("fragment sent")
}

// removeConn removes a virtual connection from the session map.
func (s *muxSession) removeConn(id uint16) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}

// readLoop is the single goroutine that reads packets and dispatches them.
//
// When inner implements packetReader, full messages are read without size
// limits — required for TypeLargeData payloads that arrive via the Resource
// protocol. Falls back to the fixed-size Read path for byte-stream conns
// (net.Pipe in tests), which only handles payloads that fit in MaxReticulumMessage.
func (s *muxSession) readLoop() {
	defer s.closeAll()

	var readOne func() ([]byte, error)
	if pr, ok := s.inner.(packetReader); ok {
		readOne = pr.ReadPacket
	} else {
		buf := make([]byte, MaxReticulumMessage)
		readOne = func() ([]byte, error) {
			n, err := s.inner.Read(buf)
			if err != nil {
				return nil, err
			}
			msg := make([]byte, n)
			copy(msg, buf[:n])
			return msg, nil
		}
	}

	fragBufs := make(map[uint16]*fragBuffer)
	gcCounter := 0

	for {
		msg, err := readOne()
		if err != nil {
			if s.logger != nil {
				s.logger.Debug("mux readLoop exit: ", err)
			}
			return
		}

		pkt, err := decodePacket(msg)
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
			case TypeLargeData:
				// Full payload delivered atomically via Resource protocol.
				s.mu.Lock()
				mc := s.conns[pkt.connID]
				s.mu.Unlock()
				if mc == nil {
					continue
				}
				select {
				case mc.readCh <- pkt.payload:
				case <-mc.done:
				}
			default:
				if s.logger != nil {
					s.logger.Debug("mux: unhandled control 0x", fmt.Sprintf("%02x", pkt.typeByte))
				}
			}
			continue
		}

		// Data packet: reassemble. No ACK is sent — the Reticulum channel under
		// the bridge already acknowledges and orders delivery.
		fb := fragBufs[pkt.connID]
		if fb == nil {
			fb = &fragBuffer{}
			fragBufs[pkt.connID] = fb
		} else if fb.delivered {
			// Already fully delivered. Update lastActivity so the GC doesn't expire
			// the delivery marker too early, suppressing any late retransmit.
			fb.lastActivity = time.Now()
			continue
		}
		assembled, done := fb.addPart(pkt.partIndex, pkt.isLast, pkt.payload)
		if !done {
			// Periodically GC stale incomplete buffers and expired delivery markers.
			gcCounter++
			if gcCounter >= 100 {
				gcCounter = 0
				now := time.Now()
				for cid, b := range fragBufs {
					if !b.gotLast && now.Sub(b.lastActivity) > s.fragTimeout {
						delete(fragBufs, cid)
					}
					// Keep delivered markers for maxRetransmitDelay so any
					// remaining sender retransmits are suppressed, then GC.
					if b.delivered && now.Sub(b.lastActivity) > maxRetransmitDelay {
						delete(fragBufs, cid)
					}
				}
			}
			continue
		}
		// Mark delivered so retransmits of this connID don't re-trigger upper layer.
		// The parts slice is cleared to free memory; the struct stays in fragBufs
		// as a delivery marker until it is GC'd by the loop above.
		fb.delivered = true
		fb.parts = nil
		fb.lastActivity = time.Now()

		s.mu.Lock()
		mc := s.conns[pkt.connID]
		s.mu.Unlock()
		if mc == nil {
			continue
		}
		select {
		case mc.readCh <- assembled:
		case <-mc.done:
		}
	}
}

// logStats emits the current mux counters at DEBUG level with a reason label.
// Called on every significant send event so the log reflects state changes
// immediately rather than on a fixed timer.
func (s *muxSession) logStats(reason string) {
	if s.logger == nil {
		return
	}
	s.logger.Debug(fmt.Sprintf(
		"mux stats [%s]: queued=%d sent=%d",
		reason, len(s.sendQ.ch), s.statFragsSent.Load(),
	))
}

// closeConnRemote closes a virtual conn in response to a TypeCloseConn or retransmit failure.
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
	close(s.done)

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

// ── muxConn ──────────────────────────────────────────────────────────────────

// muxConn is a virtual TCP connection multiplexed over a muxSession.
type muxConn struct {
	id      uint16
	session *muxSession
	dest    string

	readCh    chan []byte
	readBuf   []byte
	done      chan struct{}
	closeOnce sync.Once
	writeMu   sync.Mutex // serialises Write() calls: fragments must reach the
	// receiver in the order they were written, and two goroutines must not
	// interleave their fragment sequences for the same connID.

	localAddr  net.Addr
	remoteAddr net.Addr
}

func newMuxConn(id uint16, s *muxSession, dest string) *muxConn {
	return &muxConn{
		id:         id,
		session:    s,
		dest:       dest,
		readCh:     make(chan []byte, 1024),
		done:       make(chan struct{}),
		localAddr:  reticulumAddr{network: "reticulum-mux", str: fmt.Sprintf("mux:%d", id)},
		remoteAddr: reticulumAddr{network: "reticulum-mux", str: dest},
	}
}

// Read reads data from the virtual connection.
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
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.session.writeData(c.id, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close signals this virtual connection is closed and notifies the peer.
func (c *muxConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.session.writeCtrl(TypeCloseConn, c.id, nil)
		c.session.removeConn(c.id)
	})
	return nil
}

func (c *muxConn) LocalAddr() net.Addr  { return c.localAddr }
func (c *muxConn) RemoteAddr() net.Addr { return c.remoteAddr }

func (c *muxConn) SetDeadline(t time.Time) error      { return nil }
func (c *muxConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *muxConn) SetWriteDeadline(t time.Time) error { return nil }
