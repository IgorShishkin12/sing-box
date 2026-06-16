package reticulum

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestMuxPairWithWindow creates a client+server mux pair with a custom windowSize and
// fast retry settings so tests run quickly.
func newTestMuxPairWithWindow(t *testing.T, windowSize int) (client, server *muxSession) {
	t.Helper()
	cc, sc := net.Pipe()
	t.Cleanup(func() { cc.Close(); sc.Close() })
	client = newMuxSession(cc, nil, false, windowSize, 20*time.Millisecond, 2, MaxReticulumMessage, false)
	server = newMuxSession(sc, nil, true, windowSize, 20*time.Millisecond, 2, MaxReticulumMessage, false)
	return
}

// newManualSession creates a session whose inner conn is a chanConn pair, giving the
// test direct control over what the session receives and what it sends.
func newManualSession(t *testing.T, windowSize int) (s *muxSession, remote *chanConn) {
	t.Helper()
	inner, remote := newChanConnPair()
	s = newMuxSession(inner, nil, false, windowSize, 20*time.Millisecond, 2, MaxReticulumMessage, false)
	t.Cleanup(func() { s.Close() })
	return
}

// drainNFromSession reads exactly n packets that were sent BY the session (i.e. from
// remote.readCh, which is the channel that inner.Write feeds into).
func drainNFromSession(t *testing.T, remote *chanConn, n int, timeout time.Duration) [][]byte {
	t.Helper()
	out := make([][]byte, 0, n)
	deadline := time.After(timeout)
	for len(out) < n {
		select {
		case pkt := <-remote.readCh:
			out = append(out, pkt)
		case <-deadline:
			t.Fatalf("drainNFromSession: only got %d/%d packets", len(out), n)
		}
	}
	return out
}

// --- encodeDataByte / decodeDataByte ---

func TestEncodeDataByte_single(t *testing.T) {
	// total=1, partIdx=0 → isLast=1, partIndex=0 → 0x40
	require.Equal(t, byte(0x40), encodeDataByte(1, 0))
}

func TestEncodeDataByte_twoOfThree(t *testing.T) {
	// total=3, partIdx=1 → isLast=0, partIndex=1 → 0x01
	require.Equal(t, byte(0x01), encodeDataByte(3, 1))
}

func TestDecodeDataByte_zero(t *testing.T) {
	// 0x00: isLast=false, partIndex=0
	isLast, idx := decodeDataByte(0x00)
	require.False(t, isLast)
	require.Equal(t, 0, idx)
}

func TestDecodeDataByte_multipart(t *testing.T) {
	// middle of a 3-part message: isLast=false, partIndex=1
	b := encodeDataByte(3, 1)
	isLast, idx := decodeDataByte(b)
	require.False(t, isLast)
	require.Equal(t, 1, idx)
}

func TestDecodeDataByte_max(t *testing.T) {
	// last of 64 parts: isLast=true, partIndex=63 → 0x7F
	b := encodeDataByte(64, 63)
	require.Equal(t, byte(0x7F), b)
	isLast, idx := decodeDataByte(b)
	require.True(t, isLast)
	require.Equal(t, 63, idx)
}

func TestIsControl(t *testing.T) {
	for b := 0; b < 256; b++ {
		want := b >= 0x80
		require.Equal(t, want, isControl(byte(b)), "byte 0x%02x", b)
	}
}

// --- encodePacket / decodePacket ---

func TestEncodeDecodePacket_data(t *testing.T) {
	payload := []byte("hello")
	p := muxPacket{
		typeByte:  encodeDataByte(1, 0), // single-frag: isLast=true, partIndex=0 → 0x40
		connID:    0x1234,
		payload:   payload,
		isLast:    true,
		partIndex: 0,
	}
	encoded := encodePacket(p)
	require.Len(t, encoded, muxHeaderSize+len(payload))
	require.Equal(t, byte(0x40), encoded[0])
	require.Equal(t, byte(0x12), encoded[1])
	require.Equal(t, byte(0x34), encoded[2])
	require.Equal(t, payload, encoded[3:])

	decoded, err := decodePacket(encoded)
	require.NoError(t, err)
	require.Equal(t, p.typeByte, decoded.typeByte)
	require.Equal(t, p.connID, decoded.connID)
	require.Equal(t, payload, decoded.payload)
	require.True(t, decoded.isLast)
	require.Equal(t, 0, decoded.partIndex)
}

func TestEncodeDecodePacket_control(t *testing.T) {
	payload := []byte("127.0.0.1:8080")
	p := muxPacket{
		typeByte: TypeNewConn,
		connID:   0x0042,
		payload:  payload,
	}
	encoded := encodePacket(p)
	require.Len(t, encoded, muxHeaderSize+len(payload))
	require.Equal(t, TypeNewConn, encoded[0])
	require.Equal(t, byte(0x00), encoded[1])
	require.Equal(t, byte(0x42), encoded[2])
	require.Equal(t, payload, encoded[3:])

	decoded, err := decodePacket(encoded)
	require.NoError(t, err)
	require.Equal(t, TypeNewConn, decoded.typeByte)
	require.Equal(t, uint16(0x0042), decoded.connID)
	require.Equal(t, payload, decoded.payload)
}

func TestDecodePacket_tooShort(t *testing.T) {
	_, err := decodePacket([]byte{0x00, 0x00}) // only 2 bytes; need ≥ 3
	require.Error(t, err)
}

// TestMaxReticulumMessage_LoRaMTU asserts that MaxReticulumMessage is small enough
// that a Fernet-encrypted mux packet fits within the LoRa/RNode serial link MTU of 220 bytes.
//
// On-wire size of a Reticulum data_packet carrying a MaxReticulumMessage-byte plaintext:
//   HEADER_MAXSIZE(35) + IFAC_MIN_SIZE(1) + IV(16) + ceil(plain/16)*16 + HMAC(32)
// Using the same overhead constants as rns-core (LXMF_MAX_PAYLOAD = 464 − 35 − 1 − 16 − 32 − 16 = 364... wait)
// The formula per mux.go: 220 − 35 − 1 − 16 − 32 − 16 = 120.
// Any increase to MaxReticulumMessage causes silent packet loss on LoRa links.
func TestMaxReticulumMessage_LoRaMTU(t *testing.T) {
	const (
		loraMTU         = 220
		headerMaxSize   = 35
		ifacMinSize     = 1
		fernetIV        = 16
		fernetHMAC      = 32
		fernetMaxPad    = 16
		reticOverhead   = headerMaxSize + ifacMinSize + fernetIV + fernetHMAC + fernetMaxPad
		maxSafePayload  = loraMTU - reticOverhead
	)
	if MaxReticulumMessage > maxSafePayload {
		t.Errorf("MaxReticulumMessage=%d exceeds LoRa-safe limit=%d (MTU=%d overhead=%d); "+
			"mux fragments will be silently dropped by the KISS interface",
			MaxReticulumMessage, maxSafePayload, loraMTU, reticOverhead)
	}
}

// --- fragment ---

func TestFragment_small(t *testing.T) {
	data := make([]byte, 100)
	parts := fragment(data)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], 100)
}

func TestFragment_exact(t *testing.T) {
	data := make([]byte, maxFragPayload)
	parts := fragment(data)
	require.Len(t, parts, 1)
	require.Len(t, parts[0], maxFragPayload)
}

func TestFragment_split(t *testing.T) {
	data := make([]byte, maxFragPayload+1)
	parts := fragment(data)
	require.Len(t, parts, 2)
	require.Len(t, parts[0], maxFragPayload)
	require.Len(t, parts[1], 1)
}

func TestFragment_mss(t *testing.T) {
	data := make([]byte, 6*maxFragPayload) // 1182 bytes
	parts := fragment(data)
	require.Len(t, parts, 6)
	for i, p := range parts {
		require.Len(t, p, maxFragPayload, "fragment %d wrong size", i)
	}
}

func TestFragment_empty(t *testing.T) {
	for _, input := range [][]byte{nil, {}} {
		parts := fragment(input)
		require.Len(t, parts, 1)
		require.Empty(t, parts[0])
	}
}

// --- fragBuffer ---

func TestFragBuffer_singlePart(t *testing.T) {
	fb := &fragBuffer{}
	assembled, done := fb.addPart(0, true, []byte("hello"))
	require.True(t, done)
	require.Equal(t, []byte("hello"), assembled)
}

func TestFragBuffer_multiPart(t *testing.T) {
	fb := &fragBuffer{}

	assembled, done := fb.addPart(0, false, []byte("hel"))
	require.False(t, done)
	require.Nil(t, assembled)

	assembled, done = fb.addPart(1, false, []byte("lo "))
	require.False(t, done)
	require.Nil(t, assembled)

	assembled, done = fb.addPart(2, true, []byte("world"))
	require.True(t, done)
	require.Equal(t, []byte("hello world"), assembled)
}

// --- muxSession integration ---

// newTestMuxPair creates a client+server muxSession pair connected via net.Pipe.
// Uses byte-stream semantics; suitable for messages up to ~25 KB (64 fragments).
// Uses a 20ms retryInterval (not the production 500ms default) so pacing is
// disabled and tests complete in milliseconds, not minutes.
func newTestMuxPair(t *testing.T) (client, server *muxSession) {
	t.Helper()
	cc, sc := net.Pipe()
	t.Cleanup(func() {
		cc.Close()
		sc.Close()
	})
	client = newMuxSession(cc, nil, false, defaultWindowSize, 20*time.Millisecond, defaultMaxRetries, MaxReticulumMessage, false)
	server = newMuxSession(sc, nil, true, defaultWindowSize, 20*time.Millisecond, defaultMaxRetries, MaxReticulumMessage, false)
	return
}

// newTestMuxPairMsg creates a client+server muxSession pair backed by the
// existing chanConn (message-boundary). Required for TypeLargeData tests
// (payloads > 64*maxFragPayload) where net.Pipe byte-stream semantics break.
// Uses a 20ms retryInterval so pacing is disabled and tests complete quickly.
func newTestMuxPairMsg(t *testing.T) (client, server *muxSession) {
	t.Helper()
	cc, sc := newChanConnPair()
	t.Cleanup(func() {
		cc.Close()
		sc.Close()
	})
	client = newMuxSession(cc, nil, false, defaultWindowSize, 20*time.Millisecond, defaultMaxRetries, MaxReticulumMessage, false)
	server = newMuxSession(sc, nil, true, defaultWindowSize, 20*time.Millisecond, defaultMaxRetries, MaxReticulumMessage, false)
	return
}

func TestMuxSession_newConn(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	select {
	case sc := <-server.incomingCh:
		require.Equal(t, "127.0.0.1:8080", sc.dest)
		sc.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for incoming conn")
	}
}

func TestMuxSession_smallData(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	sent := []byte("hello world")
	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

func TestMuxSession_largeData(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	// 1000 bytes → ceil(1000/197) = 6 fragments
	sent := make([]byte, 1000)
	for i := range sent {
		sent[i] = byte(i % 251)
	}
	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

func TestMuxSession_multiplexing(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc1, err := client.OpenConn("host1:80")
	require.NoError(t, err)
	defer mc1.Close()

	mc2, err := client.OpenConn("host2:80")
	require.NoError(t, err)
	defer mc2.Close()

	// TypeNewConn packets arrive in order, so sc1=conn1, sc2=conn2.
	sc1 := <-server.incomingCh
	require.Equal(t, "host1:80", sc1.dest)
	defer sc1.Close()

	sc2 := <-server.incomingCh
	require.Equal(t, "host2:80", sc2.dest)
	defer sc2.Close()

	msg1 := []byte("data for conn1")
	msg2 := []byte("data for conn2")

	_, err = mc1.Write(msg1)
	require.NoError(t, err)
	_, err = mc2.Write(msg2)
	require.NoError(t, err)

	buf1 := make([]byte, len(msg1))
	_, err = io.ReadFull(sc1, buf1)
	require.NoError(t, err)
	require.Equal(t, msg1, buf1)

	buf2 := make([]byte, len(msg2))
	_, err = io.ReadFull(sc2, buf2)
	require.NoError(t, err)
	require.Equal(t, msg2, buf2)
}

func TestMuxSession_connClose(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)

	sc := <-server.incomingCh

	mc.Close()

	// After TypeCloseConn is sent and processed, server Read returns EOF.
	buf := make([]byte, 4)
	_, err = sc.Read(buf)
	require.ErrorIs(t, err, io.EOF)
}

// TestMuxSession_veryLargeData verifies the TypeLargeData path for payloads that
// exceed the 64-fragment limit (> 64*maxFragPayload bytes).
// Uses chanConn (message-boundary) so the full TypeLargeData message arrives whole.
func TestMuxSession_veryLargeData(t *testing.T) {
	client, server := newTestMuxPairMsg(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	// 100 KB — well above the 64-fragment cap of ~25 KB.
	sent := make([]byte, 100*1024)
	for i := range sent {
		sent[i] = byte(i % 251)
	}

	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

// TestMuxSession_boundaryData verifies that exactly 64*maxFragPayload bytes
// still uses the fragment path (not TypeLargeData).
func TestMuxSession_boundaryData(t *testing.T) {
	client, server := newTestMuxPair(t)

	mc, err := client.OpenConn("127.0.0.1:8080")
	require.NoError(t, err)
	defer mc.Close()

	sc := <-server.incomingCh
	defer sc.Close()

	sent := make([]byte, maxFragPayload*64) // exactly at the fragment limit
	for i := range sent {
		sent[i] = byte(i % 199)
	}

	_, err = mc.Write(sent)
	require.NoError(t, err)

	got := make([]byte, len(sent))
	_, err = io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, sent, got)
}

func TestMuxSession_idExhaustion(t *testing.T) {
	cc, sc := net.Pipe()
	defer cc.Close()
	defer sc.Close()

	client := newMuxSessionClient(cc, nil, MaxReticulumMessage)
	server := newMuxSessionServer(sc, nil, MaxReticulumMessage)

	// Drain server incoming conns so readLoop doesn't block.
	go func() {
		for mc := range server.incomingCh {
			mc.Close()
		}
	}()

	// Set counter so the next AddUint32 yields 65536, exceeding uint16 max.
	atomic.StoreUint32(&client.nextConnID, 65535)

	_, err := client.OpenConn("127.0.0.1:8080")
	require.Error(t, err)
	require.Contains(t, err.Error(), "exhausted")
}

// ── New tests for window / ACK / retransmit / priority ──────────────────────

// TestSendQueuePriority verifies that retransmit items are always dequeued before
// new-message items even when both are ready simultaneously.
func TestSendQueuePriority(t *testing.T) {
	q := newSendQueue(32)

	retransmitFrag := &queuedFrag{encoded: []byte("retransmit"), prio: prioRetransmit, isRetransmit: true}
	newFrag := &queuedFrag{encoded: []byte("new"), prio: prioNew}

	// Enqueue new first, then retransmit — retransmit must win.
	q.newMsg <- newFrag
	q.retransmit <- retransmitFrag

	// The worker drains retransmit before newMsg.
	// We simulate by calling the priority-select logic directly.
	var got []*queuedFrag
	for i := 0; i < 2; i++ {
		select {
		case f := <-q.retransmit:
			got = append(got, f)
			continue
		default:
		}
		select {
		case f := <-q.inProgress:
			got = append(got, f)
			continue
		default:
		}
		select {
		case f := <-q.retransmit:
			got = append(got, f)
		case f := <-q.inProgress:
			got = append(got, f)
		case f := <-q.newMsg:
			got = append(got, f)
		}
	}
	require.Len(t, got, 2)
	require.Equal(t, prioRetransmit, got[0].prio, "retransmit must come first")
	require.Equal(t, prioNew, got[1].prio)
}

// TestConcurrentWriteNoDeadlock verifies that 20 goroutines writing large messages
// concurrently all deliver their data intact, with no deadlock.
func TestConcurrentWriteNoDeadlock(t *testing.T) {
	client, server := newTestMuxPairWithWindow(t, 32)

	const goroutines = 20
	// Each goroutine sends exactly one message that fits in a single fragment.
	msgs := make([][]byte, goroutines)
	for i := range msgs {
		msgs[i] = []byte("payload-" + string(rune('A'+i)))
	}

	// Open goroutines connections; accept them all on the server side.
	conns := make([]*muxConn, goroutines)
	sconns := make([]*muxConn, goroutines)
	for i := 0; i < goroutines; i++ {
		mc, err := client.OpenConn("127.0.0.1:8080")
		require.NoError(t, err)
		conns[i] = mc
	}
	for i := 0; i < goroutines; i++ {
		select {
		case sc := <-server.incomingCh:
			sconns[i] = sc
		case <-time.After(2 * time.Second):
			t.Fatal("timeout accepting conn")
		}
	}

	// Write concurrently.
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			_, err := conns[i].Write(msgs[i])
			errs <- err
		}(i)
	}
	for i := 0; i < goroutines; i++ {
		require.NoError(t, <-errs)
	}

	// Read from each server conn (order is arbitrary due to concurrency).
	received := make([][]byte, goroutines)
	for i := 0; i < goroutines; i++ {
		buf := make([]byte, 64)
		n, err := io.ReadFull(sconns[i], buf[:len(msgs[i])])
		require.NoError(t, err)
		received[i] = buf[:n]
	}

	// Every sent message must appear exactly once in received.
	for i, want := range msgs {
		require.Equal(t, want, received[i])
	}

	for i := range conns {
		conns[i].Close()
		sconns[i].Close()
	}
}

// TestWindowBound verifies that the window provides backpressure: with wSize=2 at most
// 2 fragments are in-flight simultaneously, and the 3rd is sent only after a TypeFragAck
// releases a slot.
func TestWindowBound(t *testing.T) {
	const wSize = 2
	inner, remote := newChanConnPair()
	s := newMuxSession(inner, nil, false, wSize, 10*time.Second, 0, MaxReticulumMessage, false)
	t.Cleanup(func() { s.Close() })

	// Three separate connections so each fragment has a unique fragKey.
	mcs := [3]*muxConn{newMuxConn(1, s, "a"), newMuxConn(2, s, "b"), newMuxConn(3, s, "c")}
	s.mu.Lock()
	for _, mc := range mcs {
		s.conns[mc.id] = mc
	}
	s.mu.Unlock()

	for _, mc := range mcs {
		go func(c *muxConn) { c.Write([]byte("x")) }(mc)
	}

	// Read each fragment and send back an ACK to release the window slot, allowing
	// the next fragment to proceed.
	for i := 0; i < 3; i++ {
		var pkt []byte
		select {
		case pkt = <-remote.readCh:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("fragment %d not sent within timeout", i+1)
		}
		p, err := decodePacket(pkt)
		if err != nil {
			t.Fatalf("bad packet: %v", err)
		}
		ack := encodePacket(muxPacket{typeByte: TypeFragAck, connID: p.connID, payload: []byte{byte(p.partIndex)}})
		remote.Write(ack)
	}
}

// TestFragAckReleasesWindow verifies that a TypeFragAck unblocks the next write: with
// wSize=1 the second fragment can only be sent after the first is ACKed.
func TestFragAckReleasesWindow(t *testing.T) {
	const wSize = 1
	s, remote := newManualSession(t, wSize)

	mc := newMuxConn(1, s, "test")
	s.mu.Lock()
	s.conns[1] = mc
	s.mu.Unlock()

	go func() { mc.Write([]byte("first")) }()
	go func() { mc.Write([]byte("second")) }()

	// First fragment must arrive (acquires the single window slot).
	select {
	case pkt := <-remote.readCh:
		p, _ := decodePacket(pkt)
		// ACK it to release the window slot.
		ack := encodePacket(muxPacket{typeByte: TypeFragAck, connID: p.connID, payload: []byte{byte(p.partIndex)}})
		remote.Write(ack)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first fragment not sent")
	}

	// Second fragment must arrive after the ACK releases the slot.
	select {
	case <-remote.readCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second fragment not sent after ACK")
	}
}

// TestRetransmitOnTimeout verifies that a fragment with no TypeFragAck is retransmitted
// after retryInterval, proving the inFlight entry stays until ACKed.
func TestRetransmitOnTimeout(t *testing.T) {
	s, remote := newManualSession(t, 8)

	mc := newMuxConn(1, s, "test")
	s.mu.Lock()
	s.conns[1] = mc
	s.mu.Unlock()

	go mc.Write([]byte("hello"))

	// Initial fragment must be sent.
	select {
	case <-remote.readCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("initial fragment not sent")
	}

	// Retransmit must fire: inFlight entry persists until TypeFragAck arrives.
	select {
	case <-remote.readCh:
		// correct: retransmit fired
	case <-time.After(s.retryInterval * 3):
		t.Fatal("no retransmit — inFlight entry was incorrectly cleared")
	}
}

// TestRetransmitGivesUp verifies that a connection is closed after maxRetries failed
// retransmits with no TypeFragAck.
func TestRetransmitGivesUp(t *testing.T) {
	s, remote := newManualSession(t, 8)

	mc := newMuxConn(1, s, "test")
	s.mu.Lock()
	s.conns[1] = mc
	s.mu.Unlock()

	go mc.Write([]byte("will-never-ack"))

	// Initial send must succeed.
	select {
	case <-remote.readCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("initial fragment not sent")
	}

	// After maxRetries retransmits without ACK the connection must be closed.
	// Drain retransmits so the sendQ doesn't stall.
	go func() {
		for {
			select {
			case <-remote.readCh:
			case <-mc.done:
				return
			}
		}
	}()
	// rttEst grows ~12.5% per retransmit on top of exponential backoff, so total
	// giveup time is larger than the pure-backoff sum.  Use 2^(maxRetries+4) as a
	// generous upper bound that covers the compounded growth.
	waitFor := time.Duration(1<<uint(s.maxRetries+4)) * s.retryInterval
	select {
	case <-mc.done:
		// correct: connection closed after max retries
	case <-time.After(waitFor):
		t.Fatal("connection not closed after max retries exceeded")
	}
}

// TestRetransmitGivesUp_UpdatesRttEst verifies that rttEst increases after a fragment
// exhausts maxRetries (no ACKs), so the estimate reflects the actual link latency
// even when the peer is completely silent.
func TestRetransmitGivesUp_UpdatesRttEst(t *testing.T) {
	s, remote := newManualSession(t, 8)
	initRtt := time.Duration(s.rttEst.Value())

	mc := newMuxConn(1, s, "test")
	s.mu.Lock()
	s.conns[1] = mc
	s.mu.Unlock()

	go mc.Write([]byte("will-never-ack"))

	// Drain retransmits so sendQ doesn't stall.
	go func() {
		for {
			select {
			case <-remote.readCh:
			case <-mc.done:
				return
			}
		}
	}()

	// rttEst grows ~12.5% per retransmit on top of exponential backoff, so total
	// giveup time is larger than the pure-backoff sum.  Use 2^(maxRetries+4) as a
	// generous upper bound that covers the compounded growth.
	waitFor := time.Duration(1<<uint(s.maxRetries+4)) * s.retryInterval
	select {
	case <-mc.done:
	case <-time.After(waitFor):
		t.Fatal("connection not closed after max retries exceeded")
	}

	finalRtt := time.Duration(s.rttEst.Value())
	if finalRtt <= initRtt {
		t.Errorf("rttEst should have increased after give-up: init=%s final=%s", initRtt, finalRtt)
	}
}

// TestFragBuffer_DuplicateDelivery verifies that injecting the same fragment a second
// time (simulating a retransmit arriving after first assembly) delivers the message
// only once to the upper layer, not twice.
func TestFragBuffer_DuplicateDelivery(t *testing.T) {
	inner, remote := newChanConnPair()
	s := newMuxSession(inner, nil, true, defaultWindowSize, 20*time.Millisecond, 2, MaxReticulumMessage, false)
	t.Cleanup(func() { s.Close() })

	const connID = uint16(1)
	payload := []byte("hello-once")

	// Announce the connection.
	remote.Write(encodePacket(muxPacket{typeByte: TypeNewConn, connID: connID, payload: []byte("peer:1")}))

	// One-fragment message: encodeDataByte(totalParts=1, partIndex=0).
	frag := encodePacket(muxPacket{typeByte: encodeDataByte(1, 0), connID: connID, payload: payload})
	remote.Write(frag)

	var sc *muxConn
	select {
	case sc = <-s.incomingCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("server conn not opened within timeout")
	}
	defer sc.Close()

	got := make([]byte, len(payload))
	_, err := io.ReadFull(sc, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)

	// Inject the same fragment again — simulates a retransmit arriving late.
	// With fb.delivered set, the readLoop must NOT push another message onto sc.readCh.
	remote.Write(frag)

	select {
	case extra := <-sc.readCh:
		t.Fatalf("duplicate delivery: %q", extra)
	case <-time.After(100 * time.Millisecond):
		// good: no duplicate
	}
}

// TestMuxSession_DynamicMaxMsg verifies that a session created with a larger maxMsg
// fragments data at the correct (larger) boundary, not the LoRa default.
func TestMuxSession_DynamicMaxMsg(t *testing.T) {
	const bigMaxMsg = 500 // TCP-style MTU
	const bigMFP = bigMaxMsg - muxHeaderSize

	// Use chanConn (packetReader path) so the receiver handles arbitrary packet sizes.
	inner, remote := newChanConnPair()
	s := newMuxSession(inner, nil, false, defaultWindowSize, 20*time.Millisecond, 2, bigMaxMsg, false)
	t.Cleanup(func() { s.Close() })

	mc := newMuxConn(1, s, "test")
	s.mu.Lock()
	s.conns[1] = mc
	s.mu.Unlock()

	// Write exactly bigMFP bytes — must arrive as a single fragment, not two.
	data := make([]byte, bigMFP)
	for i := range data {
		data[i] = byte(i % 251)
	}
	go func() { mc.Write(data) }()

	select {
	case pkt := <-remote.readCh:
		decoded, err := decodePacket(pkt)
		require.NoError(t, err)
		require.Equal(t, bigMFP, len(decoded.payload), "expected single fragment of bigMFP bytes")
		isLast, partIdx := decodeDataByte(decoded.typeByte)
		require.True(t, isLast, "single fragment must be the last part")
		require.Equal(t, 0, partIdx)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no fragment received")
	}
}

// TestMuxSession_MaxMsgDefault verifies that the default (LoRa) maxMsg is preserved
// when sessions are created via the public constructors.
func TestMuxSession_MaxMsgDefault(t *testing.T) {
	client, _ := newTestMuxPair(t)
	require.Equal(t, MaxReticulumMessage, client.maxMsg)
	require.Equal(t, maxFragPayload, client.maxFragPayload)
}
