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
	maxFragPayload      = MaxReticulumMessage - muxHeaderSize

	// Control packet type bytes (high bit set).
	TypeAuthCtrl     byte = 0x80 // auth exchange message
	TypeReauthReq    byte = 0x81 // re-auth request
	TypeNewConn      byte = 0x82 // new virtual connection; payload = "host:port"
	TypeCloseConn    byte = 0x83 // close virtual connection; no payload
	TypeRequestAuth  byte = 0x84 // challenge request
	TypeResponseAuth byte = 0x85 // auth response
	TypeFragAck      byte = 0x86 // fragment acknowledgement; payload = 1-byte partIndex

	defaultWindowSize    = 16
	defaultMaxRetries    = 3
	defaultRetryInterval = 500 * time.Millisecond
	defaultFragTimeout   = 2 * time.Second
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

// fragment slices data into chunks of at most maxFragPayload bytes.
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
	parts        [][]byte
	gotLast      bool
	lastIdx      int
	lastActivity time.Time
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

type fragPriority int

const (
	prioRetransmit fragPriority = 0 // lost fragment being re-sent
	prioInProgress fragPriority = 1 // fragment 2..N of an already-started message
	prioNew        fragPriority = 2 // first fragment of a brand-new message
)

// fragKey uniquely identifies one fragment within a session.
type fragKey struct {
	connID    uint16
	partIndex uint8
}

// inFlightEntry tracks a fragment that has been sent but not yet ACKed.
type inFlightEntry struct {
	encoded []byte
	key     fragKey
	retries int
	retryAt time.Time
}

// queuedFrag is one item in the send queue.
type queuedFrag struct {
	encoded      []byte
	key          fragKey
	isRetransmit bool // true → already holds a window slot; skip window acquire
	prio         fragPriority
}

// sendQueue is a 3-level priority queue backed by buffered channels.
type sendQueue struct {
	retransmit chan *queuedFrag
	inProgress chan *queuedFrag
	newMsg     chan *queuedFrag
}

func newSendQueue(bufSize int) *sendQueue {
	return &sendQueue{
		retransmit: make(chan *queuedFrag, bufSize),
		inProgress: make(chan *queuedFrag, bufSize),
		newMsg:     make(chan *queuedFrag, bufSize),
	}
}

func (q *sendQueue) enqueue(f *queuedFrag) {
	switch f.prio {
	case prioRetransmit:
		q.retransmit <- f
	case prioInProgress:
		q.inProgress <- f
	default:
		q.newMsg <- f
	}
}

// run is the send-queue worker goroutine. It drains items in priority order and
// calls s.doWrite for each. Exits when s.done is closed.
func (q *sendQueue) run(s *muxSession) {
	for {
		// Fast path: drain retransmit without blocking.
		select {
		case f := <-q.retransmit:
			s.doWrite(f)
			continue
		default:
		}
		// Fast path: drain inProgress before new.
		select {
		case f := <-q.inProgress:
			s.doWrite(f)
			continue
		default:
		}
		// Blocking wait; retransmit and inProgress still have priority via the loop.
		select {
		case <-s.done:
			return
		case f := <-q.retransmit:
			s.doWrite(f)
		case f := <-q.inProgress:
			s.doWrite(f)
		case f := <-q.newMsg:
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

	windowSize    int
	windowSem     chan struct{} // semaphore: len = concurrent-writes-in-progress; cap = windowSize
	inFlight      sync.Map      // fragKey → *inFlightEntry (used for retransmit on RF links)
	sendQ         *sendQueue
	retryInterval time.Duration
	maxRetries    int
	fragTimeout   time.Duration

	// Counters for observability.
	statFragsSent    atomic.Uint64 // total fragments written to inner
	statAcksReceived atomic.Uint64 // total TypeFragAck packets received from peer
	statAcksSent     atomic.Uint64 // total TypeFragAck packets sent to peer
}

// newMuxSession is the internal constructor used by both public constructors and tests.
func newMuxSession(
	inner net.Conn,
	logger log.ContextLogger,
	isServer bool,
	windowSize int,
	retryInterval time.Duration,
	maxRetries int,
) *muxSession {
	s := &muxSession{
		inner:         inner,
		conns:         make(map[uint16]*muxConn),
		logger:        logger,
		done:          make(chan struct{}),
		windowSize:    windowSize,
		windowSem:     make(chan struct{}, windowSize),
		sendQ:         newSendQueue(4096),
		retryInterval: retryInterval,
		maxRetries:    maxRetries,
		fragTimeout:   defaultFragTimeout,
	}
	if isServer {
		s.incomingCh = make(chan *muxConn, 1024)
	}
	go s.readLoop()
	go s.sendQ.run(s)
	go s.retransmitLoop()
	return s
}

func newMuxSessionClient(inner net.Conn, logger log.ContextLogger) *muxSession {
	return newMuxSession(inner, logger, false, defaultWindowSize, defaultRetryInterval, defaultMaxRetries)
}

func newMuxSessionServer(inner net.Conn, logger log.ContextLogger) *muxSession {
	return newMuxSession(inner, logger, true, defaultWindowSize, defaultRetryInterval, defaultMaxRetries)
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

// writeData enqueues all fragments of data onto the send queue for conn id.
// Returns immediately; actual writes happen in the send-queue worker goroutine.
//
// All fragments use prioNew so they land in the same FIFO channel, preserving
// intra-message order. Using prioInProgress for frag 1+ caused the worker to
// send continuations before the first fragment, which silently dropped frag 0
// on the receiver when a second Write() arrived on the same connID.
func (s *muxSession) writeData(id uint16, data []byte) error {
	parts := fragment(data)
	n := len(parts)
	if n > 64 {
		return fmt.Errorf("mux: payload requires %d fragments (max 64)", n)
	}
	for i, part := range parts {
		f := &queuedFrag{
			encoded: encodePacket(muxPacket{
				typeByte: encodeDataByte(n, i),
				connID:   id,
				payload:  part,
			}),
			key:  fragKey{connID: id, partIndex: uint8(i)},
			prio: prioNew,
		}
		s.sendQ.enqueue(f)
	}
	return nil
}

// doWrite sends one queued fragment. For new (non-retransmit) fragments it acquires
// a window slot first, blocking until one is available or the session closes.
// The slot is released immediately after inner.Write() returns: on TCP the write
// succeeding is sufficient; on RF the TypeFragAck path handles it instead.
func (s *muxSession) doWrite(f *queuedFrag) {
	if !f.isRetransmit {
		// Acquire a window slot; block until one is free or session closes.
		select {
		case s.windowSem <- struct{}{}:
		case <-s.done:
			return
		}
		// Register in in-flight map so the retransmit timer and TypeFragAck can find it.
		s.inFlight.Store(f.key, &inFlightEntry{
			encoded: f.encoded,
			key:     f.key,
			retryAt: time.Now().Add(s.retryInterval),
		})
	} else {
		// Retransmit: skip if already ACKed (entry removed) since we enqueued it.
		if _, ok := s.inFlight.Load(f.key); !ok {
			return
		}
	}

	s.innerWriteMu.Lock()
	_, err := s.inner.Write(f.encoded)
	s.innerWriteMu.Unlock()
	if err != nil && s.logger != nil {
		s.logger.Debug("mux: doWrite error: ", err)
	}

	// Release the window slot once the write is queued. TypeFragAck-based release
	// is a no-op when the inFlight entry is already gone (returns false).
	if !f.isRetransmit {
		if _, ok := s.inFlight.LoadAndDelete(f.key); ok {
			s.statFragsSent.Add(1)
			<-s.windowSem
		}
	}
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
	gcCounter := 0

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
			case TypeFragAck:
				// Peer acknowledged one of our fragments; release that window slot.
				if len(pkt.payload) == 1 {
					s.statAcksReceived.Add(1)
					key := fragKey{connID: pkt.connID, partIndex: pkt.payload[0]}
					if _, ok := s.inFlight.LoadAndDelete(key); ok {
						<-s.windowSem
					}
				}
			default:
				if s.logger != nil {
					s.logger.Debug("mux: unhandled control 0x", fmt.Sprintf("%02x", pkt.typeByte))
				}
			}
			continue
		}

		// Data packet: send ACK to peer non-blocking so readLoop never stalls.
		// On TCP the sender already released its window slot after the write, so
		// a dropped ACK here is harmless. On RF a dropped ACK triggers a retransmit.
		if s.innerWriteMu.TryLock() {
			_, _ = s.inner.Write(encodePacket(muxPacket{
				typeByte: TypeFragAck,
				connID:   pkt.connID,
				payload:  []byte{byte(pkt.partIndex)},
			}))
			s.innerWriteMu.Unlock()
			s.statAcksSent.Add(1)
		}

		fb := fragBufs[pkt.connID]
		if fb == nil {
			fb = &fragBuffer{}
			fragBufs[pkt.connID] = fb
		}
		assembled, done := fb.addPart(pkt.partIndex, pkt.isLast, pkt.payload)
		if !done {
			// Periodically GC stale incomplete buffers.
			gcCounter++
			if gcCounter >= 100 {
				gcCounter = 0
				now := time.Now()
				for cid, b := range fragBufs {
					if !b.gotLast && now.Sub(b.lastActivity) > s.fragTimeout {
						delete(fragBufs, cid)
					}
				}
			}
			continue
		}
		delete(fragBufs, pkt.connID)

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

// retransmitLoop periodically scans in-flight fragments and re-enqueues timed-out ones.
// It also logs mux stats at every tick so operators can see window utilisation.
func (s *muxSession) retransmitLoop() {
	ticker := time.NewTicker(s.retryInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.checkRetransmits()
			if s.logger != nil {
				writing := len(s.windowSem)
				queued := len(s.sendQ.retransmit) + len(s.sendQ.inProgress) + len(s.sendQ.newMsg)
				s.logger.Debug(fmt.Sprintf(
					"mux stats: writing=%d/%d queued=%d sent=%d acks_rx=%d acks_tx=%d",
					writing, s.windowSize, queued,
					s.statFragsSent.Load(),
					s.statAcksReceived.Load(),
					s.statAcksSent.Load(),
				))
			}
		}
	}
}

// checkRetransmits re-enqueues fragments whose retryAt has elapsed.
// Fragments exceeding maxRetries cause their connection to be closed.
func (s *muxSession) checkRetransmits() {
	now := time.Now()
	var toClose []uint16
	s.inFlight.Range(func(k, v any) bool {
		entry := v.(*inFlightEntry)
		if now.Before(entry.retryAt) {
			return true
		}
		entry.retries++
		if entry.retries > s.maxRetries {
			toClose = append(toClose, entry.key.connID)
			s.inFlight.Delete(k)
			// Release the window slot this fragment was holding.
			select {
			case <-s.windowSem:
			default:
			}
			return true
		}
		// Update deadline before enqueuing to prevent double-enqueue on next tick.
		entry.retryAt = now.Add(s.retryInterval)
		select {
		case s.sendQ.retransmit <- &queuedFrag{
			encoded:      entry.encoded,
			key:          entry.key,
			isRetransmit: true,
			prio:         prioRetransmit,
		}:
		default:
			// Retransmit channel full; will retry on next tick.
		}
		return true
	})
	for _, id := range toClose {
		if s.logger != nil {
			s.logger.Warn("mux: conn ", id, " closed after ", s.maxRetries, " retransmit failures")
		}
		s.closeConnRemote(id)
	}
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
