package reticulum

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

var errAuthTimeout = errors.New("auth timeout")

// chanAuthIO implements AuthIO using a channel pair; no network needed.
type chanAuthIO struct {
	in  <-chan []byte
	out chan<- []byte
}

func (c *chanAuthIO) ReadMsg() (byte, []byte, error) {
	msg, ok := <-c.in
	if !ok {
		return 0, nil, io.EOF
	}
	if len(msg) == 0 {
		return 0, nil, io.EOF
	}
	return msg[0], msg[1:], nil
}

func (c *chanAuthIO) ReadMsgDeadline(deadline time.Time) (byte, []byte, error) {
	select {
	case msg, ok := <-c.in:
		if !ok || len(msg) == 0 {
			return 0, nil, io.EOF
		}
		return msg[0], msg[1:], nil
	case <-time.After(time.Until(deadline)):
		return 0, nil, errAuthTimeout
	}
}

func (c *chanAuthIO) WriteMsg(typeByte byte, payload []byte) error {
	msg := make([]byte, 1+len(payload))
	msg[0] = typeByte
	copy(msg[1:], payload)
	c.out <- msg
	return nil
}

// newChanAuthPair returns two paired AuthIO endpoints (a, b).
func newChanAuthPair() (*chanAuthIO, *chanAuthIO) {
	ch1 := make(chan []byte, 8)
	ch2 := make(chan []byte, 8)
	return &chanAuthIO{in: ch1, out: ch2}, &chanAuthIO{in: ch2, out: ch1}
}

func TestAuth_Success(t *testing.T) {
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password", "id-a", "id-b") }()
	go func() { errs <- Auth(b, "password", "id-b", "id-a") }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestAuth_WrongPassword(t *testing.T) {
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password-a", "id-a", "id-b") }()
	go func() { errs <- Auth(b, "password-b", "id-b", "id-a") }()
	errCount := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			errCount++
		}
	}
	if errCount == 0 {
		t.Error("expected at least one error for wrong password, got none")
	}
}

func TestAuth_WrongPeerID(t *testing.T) {
	// Same password but mismatched peer IDs — auth should fail.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	// a thinks peer is "id-wrong", b presents "id-b"
	go func() { errs <- Auth(a, "password", "id-a", "id-wrong") }()
	go func() { errs <- Auth(b, "password", "id-b", "id-a") }()
	errCount := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			errCount++
		}
	}
	if errCount == 0 {
		t.Error("expected at least one error for mismatched peer ID, got none")
	}
}

func TestAuth_ShortSalt(t *testing.T) {
	// One side sends a salt that is too short.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password", "id-a", "id-b") }()
	go func() {
		// Send a TypeRequestAuth with only 10 bytes (should be 32).
		b.WriteMsg(TypeRequestAuth, make([]byte, 10)) //nolint:errcheck
		errs <- nil
	}()
	var authErr error
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			authErr = err
		}
	}
	if authErr == nil {
		t.Fatal("expected error for short salt, got nil")
	}
}

func TestAuth_WrongTypeByte(t *testing.T) {
	// One side sends the wrong type byte in round 1.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password", "id-a", "id-b") }()
	go func() {
		// Send TypeResponseAuth (0x85) instead of TypeRequestAuth (0x84).
		b.WriteMsg(TypeResponseAuth, make([]byte, 32)) //nolint:errcheck
		errs <- nil
	}()
	var authErr error
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			authErr = err
		}
	}
	if authErr == nil {
		t.Fatal("expected error for wrong type byte, got nil")
	}
}

func noSleep(_ time.Duration) {}

func recordSleep(calls *[]time.Duration) func(time.Duration) {
	return func(d time.Duration) { *calls = append(*calls, d) }
}

func TestRetryDelay(t *testing.T) {
	cases := []struct {
		policy  RetryPolicy
		attempt int
		wantOK  bool
		wantD   time.Duration
	}{
		{RetryNone, 0, false, 0},
		{"", 0, false, 0},
		{RetryLinear, 0, true, 5 * time.Second},
		{RetryLinear, 99, true, 5 * time.Second},
		{RetryExp, 0, true, 4 * time.Second},
		{RetryExp, 1, true, 8 * time.Second},
		{RetryExp, 2, true, 16 * time.Second},
		{RetryExp, 3, true, 32 * time.Second},
	}
	for _, c := range cases {
		d, ok := retryDelay(c.policy, c.attempt)
		if ok != c.wantOK {
			t.Errorf("retryDelay(%q, %d): ok=%v want %v", c.policy, c.attempt, ok, c.wantOK)
		}
		if ok && d != c.wantD {
			t.Errorf("retryDelay(%q, %d): delay=%v want %v", c.policy, c.attempt, d, c.wantD)
		}
	}
}

func TestAuthWithRetry_NoneMatchesAuth(t *testing.T) {
	// RetryNone with correct passwords — should succeed, same as Auth().
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- authWithRetry(a, "pw", "id-a", "id-b", RetryNone, noSleep) }()
	go func() { errs <- authWithRetry(b, "pw", "id-b", "id-a", RetryNone, noSleep) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

// selfServingAuthIO acts as a synthetic peer for testing authWithRetry without
// needing a real channel-pair partner. It responds to the caller's auth exchange:
//   - WriteMsg(TypeRequestAuth, salt) captures the caller's ownSalt.
//   - ReadMsg on odd calls (round 1) returns TypeRequestAuth + fixed peer salt.
//   - ReadMsg on even calls (round 2) returns TypeResponseAuth + a wrong MAC for
//     the first failFor attempts, then the correct MAC so auth succeeds.
//
// authAttempt calls WriteMsg before ReadMsg in each round (never concurrently),
// so no mutex is needed.
type selfServingAuthIO struct {
	password     string
	peerID       string // this mock's identity (= caller's peerID argument)
	failFor      int
	readCount    int
	capturedSalt []byte
}

func (s *selfServingAuthIO) WriteMsg(typeByte byte, payload []byte) error {
	if typeByte == TypeRequestAuth {
		s.capturedSalt = append([]byte{}, payload...)
	}
	return nil
}

func (s *selfServingAuthIO) ReadMsg() (byte, []byte, error) {
	s.readCount++
	if s.readCount%2 == 1 {
		// Round 1: return a fixed peer challenge (zero salt is fine for tests).
		return TypeRequestAuth, make([]byte, 32), nil
	}
	// Round 2: attempt index is (readCount/2 - 1), zero-based.
	attempt := s.readCount/2 - 1
	if attempt < s.failFor {
		return TypeResponseAuth, make([]byte, 32), nil // wrong MAC → auth fails
	}
	// Correct MAC: what the caller expects = macBound(password, peerID, ownSalt).
	return TypeResponseAuth, macBound(s.password, s.peerID, s.capturedSalt), nil
}

func (s *selfServingAuthIO) ReadMsgDeadline(_ time.Time) (byte, []byte, error) {
	// selfServingAuthIO always answers immediately; ignore the deadline.
	return s.ReadMsg()
}

func TestAuthWithRetry_NoneFailsFast(t *testing.T) {
	// RetryNone with a peer that returns wrong MAC — error returned, sleep never called.
	mock := &selfServingAuthIO{password: "pw", peerID: "id-b", failFor: 1}
	var calls []time.Duration
	err := authWithRetry(mock, "pw", "id-a", "id-b", RetryNone, recordSleep(&calls))
	if err == nil {
		t.Error("expected error for RetryNone after auth failure, got nil")
	}
	if len(calls) != 0 {
		t.Errorf("RetryNone should not sleep, got %d sleep calls", len(calls))
	}
}

func TestAuthWithRetry_LinearRetry(t *testing.T) {
	// First attempt returns wrong MAC, second succeeds. Verify sleep called once with 5s.
	mock := &selfServingAuthIO{password: "pw", peerID: "id-b", failFor: 1}
	var calls []time.Duration
	if err := authWithRetry(mock, "pw", "id-a", "id-b", RetryLinear, recordSleep(&calls)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(calls) != 1 || calls[0] != 5*time.Second {
		t.Errorf("sleep calls: got %v, want [5s]", calls)
	}
}

func TestAuthWithRetry_ExpDelays(t *testing.T) {
	// Three failures then success — verify delay sequence is [4s, 8s, 16s].
	mock := &selfServingAuthIO{password: "pw", peerID: "id-b", failFor: 3}
	var calls []time.Duration
	if err := authWithRetry(mock, "pw", "id-a", "id-b", RetryExp, recordSleep(&calls)); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	want := []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second}
	if len(calls) != len(want) {
		t.Fatalf("sleep calls: got %v, want %v", calls, want)
	}
	for i, d := range want {
		if calls[i] != d {
			t.Errorf("sleep[%d]: got %v, want %v", i, calls[i], d)
		}
	}
}

// orderRecordingIO wraps a chanAuthIO and records the sequence of WriteMsg/ReadMsg
// calls so tests can assert write-before-read ordering within each round.
type orderRecordingIO struct {
	mu  sync.Mutex
	ops []string
	*chanAuthIO
}

func (o *orderRecordingIO) WriteMsg(typeByte byte, payload []byte) error {
	o.mu.Lock()
	if typeByte == TypeRequestAuth {
		o.ops = append(o.ops, "W1")
	} else {
		o.ops = append(o.ops, "W2")
	}
	o.mu.Unlock()
	return o.chanAuthIO.WriteMsg(typeByte, payload)
}

func (o *orderRecordingIO) ReadMsg() (byte, []byte, error) {
	typB, data, err := o.chanAuthIO.ReadMsg()
	o.mu.Lock()
	if typB == TypeRequestAuth {
		o.ops = append(o.ops, "R1")
	} else if typB == TypeResponseAuth {
		o.ops = append(o.ops, "R2")
	}
	o.mu.Unlock()
	return typB, data, err
}

func (o *orderRecordingIO) ReadMsgDeadline(deadline time.Time) (byte, []byte, error) {
	typB, data, err := o.chanAuthIO.ReadMsgDeadline(deadline)
	o.mu.Lock()
	if err == nil && typB == TypeRequestAuth {
		o.ops = append(o.ops, "R1")
	}
	o.mu.Unlock()
	return typB, data, err
}

// TestAuth_WriteBeforeRead verifies that WriteMsg is always called before ReadMsg
// in each round. This is the key invariant that prevents false auth timeouts on
// high-latency links: the idle timer in ReadMsg starts only after our own
// transmission completes, not while waiting for the TX queue to drain.
func TestAuth_WriteBeforeRead(t *testing.T) {
	a, b := newChanAuthPair()
	rec := &orderRecordingIO{chanAuthIO: a}

	errs := make(chan error, 2)
	go func() { errs <- Auth(rec, "pw", "id-a", "id-b") }()
	go func() { errs <- Auth(b, "pw", "id-b", "id-a") }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	rec.mu.Lock()
	ops := append([]string{}, rec.ops...)
	rec.mu.Unlock()

	want := []string{"W1", "R1", "W2", "R2"}
	if !reflect.DeepEqual(ops, want) {
		t.Errorf("operation order: got %v, want %v (write must precede read in each round)", ops, want)
	}
}

// slowWriteIO wraps a chanAuthIO and adds a configurable delay to WriteMsg,
// simulating a slow TX queue (e.g. LoRa interface draining radio-config ACKs).
type slowWriteIO struct {
	delay time.Duration
	*chanAuthIO
}

func (s *slowWriteIO) WriteMsg(typeByte byte, payload []byte) error {
	time.Sleep(s.delay)
	return s.chanAuthIO.WriteMsg(typeByte, payload)
}

func (s *slowWriteIO) ReadMsgDeadline(deadline time.Time) (byte, []byte, error) {
	return s.chanAuthIO.ReadMsgDeadline(deadline)
}

// TestAuth_SlowWriteNoTimeout verifies that a slow WriteMsg does not cause a
// false auth timeout. Previously, the ReadMsg timer started before WriteMsg
// completed, causing timeouts on LoRa links where TX takes several seconds.
func TestAuth_SlowWriteNoTimeout(t *testing.T) {
	const writeDelay = 50 * time.Millisecond
	a, b := newChanAuthPair()
	slow := &slowWriteIO{delay: writeDelay, chanAuthIO: a}

	errs := make(chan error, 2)
	go func() { errs <- Auth(slow, "pw", "id-a", "id-b") }()
	go func() { errs <- Auth(b, "pw", "id-b", "id-a") }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("auth failed with slow write (delay=%v): %v", writeDelay, err)
		}
	}
}

// retransmitMockIO simulates a peer that doesn't respond to Round 1 for the
// first N ReadMsgDeadline calls (returns errAuthTimeout), then succeeds.
// Round 2 ReadMsg always returns the correct MAC immediately.
type retransmitMockIO struct {
	mu           sync.Mutex
	writeCalls   int        // how many times WriteMsg(TypeRequestAuth) was called
	deadlineCalls int       // how many times ReadMsgDeadline was called
	failFor      int        // number of ReadMsgDeadline calls that return timeout
	capturedSalt []byte
	password     string
	peerID       string
}

func (r *retransmitMockIO) WriteMsg(typeByte byte, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if typeByte == TypeRequestAuth {
		r.writeCalls++
		r.capturedSalt = append([]byte{}, payload...)
	}
	return nil
}

func (r *retransmitMockIO) ReadMsgDeadline(_ time.Time) (byte, []byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deadlineCalls++
	if r.deadlineCalls <= r.failFor {
		return 0, nil, errAuthTimeout
	}
	return TypeRequestAuth, make([]byte, 32), nil
}

func (r *retransmitMockIO) ReadMsg() (byte, []byte, error) {
	r.mu.Lock()
	salt := append([]byte{}, r.capturedSalt...)
	r.mu.Unlock()
	return TypeResponseAuth, macBound(r.password, r.peerID, salt), nil
}

// TestAuth_Round1Retransmit verifies that authAttempt retransmits TypeRequestAuth
// when ReadMsgDeadline returns timeout, and succeeds once the peer responds.
func TestAuth_Round1Retransmit(t *testing.T) {
	mock := &retransmitMockIO{password: "pw", peerID: "id-peer", failFor: 2}
	if err := authAttempt(mock, "pw", "id-self", "id-peer", make([]byte, 32)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mock.mu.Lock()
	writes := mock.writeCalls
	deadlines := mock.deadlineCalls
	mock.mu.Unlock()
	// 2 timeouts + 1 success = 3 ReadMsgDeadline calls, 3 WriteMsg(Round1) calls.
	if writes != 3 {
		t.Errorf("WriteMsg(TypeRequestAuth) called %d times, want 3", writes)
	}
	if deadlines != 3 {
		t.Errorf("ReadMsgDeadline called %d times, want 3", deadlines)
	}
}

// staleRound1MockIO simulates a peer whose Round 2 ReadMsg returns a stale
// TypeRequestAuth before the real TypeResponseAuth.
type staleRound1MockIO struct {
	readCount    int
	capturedSalt []byte
	password     string
	peerID       string
}

func (s *staleRound1MockIO) WriteMsg(typeByte byte, payload []byte) error {
	if typeByte == TypeRequestAuth {
		s.capturedSalt = append([]byte{}, payload...)
	}
	return nil
}

func (s *staleRound1MockIO) ReadMsgDeadline(_ time.Time) (byte, []byte, error) {
	// Round 1 always succeeds immediately.
	return TypeRequestAuth, make([]byte, 32), nil
}

func (s *staleRound1MockIO) ReadMsg() (byte, []byte, error) {
	s.readCount++
	if s.readCount == 1 {
		// First Round 2 read: return a stale TypeRequestAuth retransmit.
		return TypeRequestAuth, make([]byte, 32), nil
	}
	// Second call: real Round 2 response.
	return TypeResponseAuth, macBound(s.password, s.peerID, s.capturedSalt), nil
}

// TestAuth_Round2_IgnoresStaleRound1 verifies that a stale TypeRequestAuth
// received during Round 2 is skipped and auth still succeeds.
func TestAuth_Round2_IgnoresStaleRound1(t *testing.T) {
	mock := &staleRound1MockIO{password: "pw", peerID: "id-peer"}
	if err := authAttempt(mock, "pw", "id-self", "id-peer", make([]byte, 32)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.readCount != 2 {
		t.Errorf("ReadMsg called %d times, want 2 (1 stale + 1 real)", mock.readCount)
	}
}
