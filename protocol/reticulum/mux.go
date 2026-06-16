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
	TypeFragAck      byte = 0x86 // fragment acknowledgement; payload = 1-byte partIndex
	// TypeLargeData carries a payload that exceeds the 64-fragment limit (~25 KB).
	// The bridge routes this to the Reticulum Resource protocol; the receiver gets
	// the reassembled data via a single on_data callback.
	TypeLargeData byte = 0xC0

	defaultWindowSize    = 16
	defaultMaxRetries    = 6
	defaultRetryInterval = 500 * time.Millisecond
	maxRetransmitDelay   = 30 * time.Second
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
	sentAt  time.Time // when first sent; used to measure RTT on ACK
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

// ewma is a thread-safe Exponentially Weighted Moving Average.
// alpha controls smoothing: smaller α → slower adaptation, larger α → faster.
// A common choice is α = 0.125 (1/8): new = 7/8·old + 1/8·sample.
type ewma struct {
	val   atomic.Int64
	alpha float64
	floor int64 // lower bound enforced after every update; 0 = no floor
}

// newEWMA creates an EWMA with the given smoothing factor, initial value, and floor.
func newEWMA(alpha float64, initial, floor int64) *ewma {
	e := &ewma{alpha: alpha, floor: floor}
	e.val.Store(initial)
	return e
}

// Update incorporates sample into the moving average using a CAS retry loop.
func (e *ewma) Update(sample int64) {
	for {
		old := e.val.Load()
		n := int64(float64(old)*(1-e.alpha) + float64(sample)*e.alpha)
		if e.floor > 0 && n < e.floor {
			n = e.floor
		}
		if e.val.CompareAndSwap(old, n) {
			return
		}
	}
}

// Value returns the current EWMA value.
func (e *ewma) Value() int64 {
	return e.val.Load()
}

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
	maxMsg        int
	maxFragPayload int // maxMsg - muxHeaderSize

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

	// rttEst is the EWMA round-trip-time estimate in nanoseconds (alpha=0.125).
	// Initialised from retryInterval; updated on every TypeFragAck and on fragment
	// give-up (failed packets: time from first send to give-up is treated as a lower
	// bound sample).  Retransmit timeout = 2 × rttEst, floored at retryInterval.
	rttEst *ewma

	// bwEst is the EWMA throughput estimate in bytes/second (alpha=0.125).
	// Increases when ACKs arrive (link is delivering), decreases on retransmit
	// timeouts (congestion / loss).  Nil when pacing is disabled (test sessions).
	bwEst atomic.Pointer[ewma]

	// lastSendAt is the unix-nano timestamp of the most recent doWrite; used for
	// pacing so sends are spaced by at least len(frag)/bwEst seconds.
	lastSendAt atomic.Int64
}

// newMuxSession is the internal constructor used by both public constructors and tests.
// maxMsg is the maximum total mux-packet size (header + payload) for this session;
// pass MaxReticulumMessage for the LoRa default.
// withPacing enables bandwidth-based send pacing; pass true for real LoRa/RF links,
// false for test sessions (net.Pipe) where pacing would make tests take minutes.
func newMuxSession(
	inner net.Conn,
	logger log.ContextLogger,
	isServer bool,
	windowSize int,
	retryInterval time.Duration,
	maxRetries int,
	maxMsg int,
	withPacing bool,
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
		windowSize:     windowSize,
		windowSem:      make(chan struct{}, windowSize),
		sendQ:          newSendQueue(4096),
		retryInterval:  retryInterval,
		maxRetries:     maxRetries,
		fragTimeout:    defaultFragTimeout,
	}
	s.rttEst = newEWMA(0.5, int64(retryInterval), int64(retryInterval))
	// SF8 BW62.5 (retryInterval=500ms, maxMsg=120): initial ≈ 58 B/s → ~2 s per fragment,
	// leaving airtime for ACKs and return-path traffic.
	if withPacing {
		initial := max(int64(s.maxFragPayload)*int64(time.Second)/(4*int64(retryInterval)), 1)
		s.bwEst.Store(newEWMA(0.5, initial, 1))
	}
	if isServer {
		s.incomingCh = make(chan *muxConn, 1024)
	}
	go s.readLoop()
	go s.sendQ.run(s)
	go s.retransmitLoop()
	return s
}

func newMuxSessionClient(inner net.Conn, logger log.ContextLogger, maxMsg int) *muxSession {
	return newMuxSession(inner, logger, false, defaultWindowSize, defaultRetryInterval, defaultMaxRetries, maxMsg, true)
}

func newMuxSessionServer(inner net.Conn, logger log.ContextLogger, maxMsg int) *muxSession {
	return newMuxSession(inner, logger, true, defaultWindowSize, defaultRetryInterval, defaultMaxRetries, maxMsg, true)
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
// the send queue so that doWrite handles windowing and inFlight registration;
// this enables TypeFragAck-driven flow control and retransmission on lossy links.
func (s *muxSession) writeData(id uint16, data []byte) error {
	if len(data) > s.maxFragPayload*64 {
		return s.writeCtrl(TypeLargeData, id, data)
	}
	parts := s.fragmentData(data)
	n := len(parts)
	for i, part := range parts {
		pkt := muxPacket{typeByte: encodeDataByte(n, i), connID: id, payload: part}
		key := fragKey{connID: id, partIndex: uint8(i)}
		// Continuation fragments (i > 0) are marked prioInProgress so that doWrite
		// skips bandwidth pacing for them. All parts still go into newMsg to preserve
		// send order; prio is only used for the pacing decision in doWrite.
		prio := prioNew
		if i > 0 {
			prio = prioInProgress
		}
		f := &queuedFrag{encoded: encodePacket(pkt), key: key, prio: prio}
		select {
		case s.sendQ.newMsg <- f:
		case <-s.done:
			return io.ErrClosedPipe
		}
	}
	return nil
}

// doWrite sends one queued fragment. For new (non-retransmit) fragments it acquires
// a window slot first, blocking until one is available or the session closes, then
// registers the fragment in inFlight. The window slot is held until the peer sends a
// TypeFragAck (released by readLoop) or checkRetransmits gives up after maxRetries.
// On reliable connections (TCP, net.Pipe) ACKs arrive quickly so the window is never
// exhausted; on lossy RF links the window provides backpressure and inFlight enables
// retransmission of lost fragments.
func (s *muxSession) doWrite(f *queuedFrag) {
	if !f.isRetransmit {
		// Acquire a window slot; blocks until one is free or the session closes.
		select {
		case s.windowSem <- struct{}{}:
		case <-s.done:
			return
		}
		now := time.Now()
		s.inFlight.Store(f.key, &inFlightEntry{
			encoded: f.encoded,
			key:     f.key,
			sentAt:  now,
			retryAt: now.Add(fragRetransmitDelay(0, time.Duration(s.rttEst.Value()))),
		})
	} else {
		// Retransmit: skip if already ACKed (entry removed) since we enqueued it.
		if _, ok := s.inFlight.Load(f.key); !ok {
			return
		}
	}

	// Pace sends to the estimated link throughput so the LoRa channel is not
	// saturated with our traffic, leaving airtime for ACKs and the return path.
	// Continuation fragments (prioInProgress) are never paced: all parts of a
	// multi-fragment message must arrive for reassembly, so we cannot afford to
	// hold them back longer than the window/ACK cycle already does.
	if bw := s.bwEst.Load(); bw != nil && f.prio != prioInProgress {
		bwVal := bw.Value()
		if bwVal > 0 {
			fragBytes := int64(len(f.encoded))
			pacingInterval := time.Duration(fragBytes * int64(time.Second) / bwVal)
			if lastNano := s.lastSendAt.Load(); lastNano > 0 {
				elapsed := time.Duration(time.Now().UnixNano() - lastNano)
				if elapsed < pacingInterval {
					select {
					case <-time.After(pacingInterval - elapsed):
					case <-s.done:
						return
					}
				}
			}
		}
	}
	s.lastSendAt.Store(time.Now().UnixNano())

	s.innerWriteMu.Lock()
	_, err := s.inner.Write(f.encoded)
	s.innerWriteMu.Unlock()
	if err != nil {
		// Reticulum returned an error: it definitively failed to deliver this
		// fragment (timed out on its side, or link error).  Free the window slot
		// and inFlight entry immediately — no ACK will ever arrive.
		if s.logger != nil {
			s.logger.Warn("mux: conn=", f.key.connID, " part=", f.key.partIndex, " write failed (fragment lost): ", err)
		}
		s.inFlight.Delete(f.key)
		select {
		case <-s.windowSem:
		default:
		}
		s.logStats("fragment lost (write error)")
		return
	}
	if !f.isRetransmit {
		s.statFragsSent.Add(1)
		s.logStats("fragment sent")
	} else {
		s.logStats("retransmit sent")
	}
	// inFlight entry and window slot are held until the TypeFragAck handler
	// (readLoop) calls LoadAndDelete + releases windowSem, or checkRetransmits
	// gives up and releases both after maxRetries.
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
			case TypeFragAck:
				// Peer acknowledged one of our fragments; measure RTT, update EWMA, release slot.
				if len(pkt.payload) == 1 {
					s.statAcksReceived.Add(1)
					key := fragKey{connID: pkt.connID, partIndex: pkt.payload[0]}
					if v, ok := s.inFlight.LoadAndDelete(key); ok {
						entry := v.(*inFlightEntry)
						rtt := time.Since(entry.sentAt)
						s.rttEst.Update(int64(rtt))
						// Update bandwidth EWMA on ACK (if pacing is active).
						if rtt > time.Millisecond {
							if bw := s.bwEst.Load(); bw != nil {
								sample := int64(len(entry.encoded)) * int64(time.Second) / int64(rtt)
								bw.Update(sample)
							}
						}
						<-s.windowSem
							s.logStats("ack received")
						}
					}
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
		} else if fb.delivered {
			// Already fully delivered.  The ACK was already sent above so the
			// sender will eventually stop retransmitting.  Update lastActivity
			// so the GC doesn't expire the marker too early.
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
// Called on every significant event (send, ACK, retransmit/give-up) so the log
// reflects state changes immediately rather than on a fixed timer.
func (s *muxSession) logStats(reason string) {
	if s.logger == nil {
		return
	}
	writing := len(s.windowSem)
	queued := len(s.sendQ.retransmit) + len(s.sendQ.inProgress) + len(s.sendQ.newMsg)
	s.logger.Debug(fmt.Sprintf(
		"mux stats [%s]: writing=%d/%d queued=%d sent=%d acks_rx=%d acks_tx=%d rtt_est=%s bw_est=%d B/s",
		reason,
		writing, s.windowSize, queued,
		s.statFragsSent.Load(),
		s.statAcksReceived.Load(),
		s.statAcksSent.Load(),
		time.Duration(s.rttEst.Value()).Round(time.Millisecond),
		func() int64 {
			if bw := s.bwEst.Load(); bw != nil {
				return bw.Value()
			}
			return 0
		}(),
	))
}

// retransmitLoop periodically scans in-flight fragments and re-enqueues timed-out ones.
func (s *muxSession) retransmitLoop() {
	ticker := time.NewTicker(s.retryInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.checkRetransmits()
		}
	}
}

// fragRetransmitDelay returns the backoff delay before the next retransmit.
// Each failure doubles the wait (exponential backoff): 2^retries × rttEst,
// capped at maxRetransmitDelay. retries is the attempt number after incrementing
// (so 1 for the first retransmit, 2 for the second, etc.).
func fragRetransmitDelay(retries int, rttEst time.Duration) time.Duration {
	shift := uint(retries)
	if shift > 5 {
		shift = 5 // prevent overflow; 2^5=32 is already above maxRetransmitDelay/rttEst for any sane rttEst
	}
	d := time.Duration(2<<shift) * rttEst
	if d > maxRetransmitDelay {
		d = maxRetransmitDelay
	}
	return d
}

// checkRetransmits re-enqueues fragments whose retryAt has elapsed.
// Fragments exceeding maxRetries cause their connection to be closed.
//
// NOTE: commented out — Reticulum sends a LinkProof for every data_packet,
// so the link layer already handles delivery confirmation and retransmit.
// Re-enable if we switch away from data_packet or need app-layer guarantees.
func (s *muxSession) checkRetransmits() {
	// now := time.Now()
	// var toClose []uint16
	// s.inFlight.Range(func(k, v any) bool {
	// 	entry := v.(*inFlightEntry)
	// 	if now.Before(entry.retryAt) {
	// 		return true
	// 	}
	// 	entry.retries++
	// 	if entry.retries > s.maxRetries {
	// 		toClose = append(toClose, entry.key.connID)
	// 		s.inFlight.Delete(k)
	// 		// Treat total wait (first send → give-up) as an RTT sample.
	// 		s.rttEst.Update(int64(time.Since(entry.sentAt)))
	// 		// Give-up is a strong congestion signal; EWMA toward 0.
	// 		if bw := s.bwEst.Load(); bw != nil {
	// 			bw.Update(0)
	// 		}
	// 		s.logStats("packet lost (all retries exhausted)")
	// 		// Release the window slot this fragment was holding.
	// 		select {
	// 		case <-s.windowSem:
	// 		default:
	// 		}
	// 		return true
	// 	}
	// 	// Timeout: nudge rttEst up.
	// 	s.rttEst.Update(2 * s.rttEst.Value())
	// 	// Retransmit timeout is a mild congestion signal; EWMA toward 0.
	// 	if bw := s.bwEst.Load(); bw != nil {
	// 		bw.Update(0)
	// 	}
	// 	// Exponential backoff: each failure doubles the wait.
	// 	rttEst := time.Duration(s.rttEst.Value())
	// 	delay := fragRetransmitDelay(entry.retries, rttEst)
	// 	entry.retryAt = now.Add(delay)
	// 	s.logStats("retransmit queued (no ACK)")
	// 	select {
	// 	case s.sendQ.retransmit <- &queuedFrag{
	// 		encoded:      entry.encoded,
	// 		key:          entry.key,
	// 		isRetransmit: true,
	// 		prio:         prioRetransmit,
	// 	}:
	// 	default:
	// 		// Retransmit channel full; will retry on next tick.
	// 	}
	// 	return true
	// })
	// for _, id := range toClose {
	// 	if s.logger != nil {
	// 		s.logger.Warn("mux: conn ", id, " closed after ", s.maxRetries, " retransmit failures")
	// 	}
	// 	s.closeConnRemote(id)
	// }
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
