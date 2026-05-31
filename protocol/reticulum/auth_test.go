package reticulum

import (
	"io"
	"testing"
)

// chanAuthIO implements AuthIO using a channel pair; no network needed.
// done is a shared channel between the pair — closing it via Close() unblocks
// both sides' pending ReadMsg/WriteMsg, simulating a connection teardown.
type chanAuthIO struct {
	in   <-chan []byte
	out  chan<- []byte
	done chan struct{}
}

func (c *chanAuthIO) ReadMsg() (byte, []byte, error) {
	select {
	case msg, ok := <-c.in:
		if !ok {
			return 0, nil, io.EOF
		}
		return msg[0], msg[1:], nil
	case <-c.done:
		return 0, nil, io.ErrClosedPipe
	}
}

func (c *chanAuthIO) WriteMsg(typeByte byte, payload []byte) error {
	msg := make([]byte, 1+len(payload))
	msg[0] = typeByte
	copy(msg[1:], payload)
	select {
	case c.out <- msg:
		return nil
	case <-c.done:
		return io.ErrClosedPipe
	}
}

// Close unblocks both sides of the pair by closing the shared done channel.
func (c *chanAuthIO) Close() {
	select {
	case <-c.done: // already closed
	default:
		close(c.done)
	}
}

// newChanAuthPair returns two paired AuthIO endpoints sharing a done channel.
func newChanAuthPair() (*chanAuthIO, *chanAuthIO) {
	ch1 := make(chan []byte, 8)
	ch2 := make(chan []byte, 8)
	done := make(chan struct{})
	return &chanAuthIO{in: ch1, out: ch2, done: done},
		&chanAuthIO{in: ch2, out: ch1, done: done}
}

func TestAuth_Success(t *testing.T) {
	// Each side uses its own identity and verifies the other's.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "correct-password", "identity-a", "identity-b") }()
	go func() { errs <- Auth(b, "correct-password", "identity-b", "identity-a") }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestAuth_WrongPassword(t *testing.T) {
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password-A", "identity-a", "") }()
	go func() { errs <- Auth(b, "password-B", "identity-b", "") }()

	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			errCount++
		}
	}
	if errCount == 0 {
		t.Error("expected errors for mismatched passwords, got none")
	}
}

func TestAuth_PeerIdentityMismatch(t *testing.T) {
	// Side a expects peer to be "expected-b" but peer claims "identity-b".
	// a returns error after round 1; calling a.Close() unblocks b which is
	// waiting for a's round-2 MAC that will never arrive.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() {
		err := Auth(a, "password", "identity-a", "expected-b")
		a.Close() // unblock b, which is waiting for a's round-2 MAC
		errs <- err
	}()
	go func() { errs <- Auth(b, "password", "identity-b", "") }()

	var gotErr bool
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			gotErr = true
		}
	}
	if !gotErr {
		t.Error("expected error for peer identity mismatch, got none")
	}
}

func TestAuth_ShortRound1(t *testing.T) {
	// One side sends a round1 message shorter than 64 bytes; peer rejects it.
	a, b := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- Auth(a, "password", "identity-a", "") }()
	go func() {
		_ = b.WriteMsg(TypeRequestAuth, []byte("short")) // only 5 bytes, want 32
		b.ReadMsg()                     //nolint:errcheck  drain so side a's write doesn't stall
		errs <- nil
	}()

	var gotErr bool
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			gotErr = true
		}
	}
	if !gotErr {
		t.Error("expected at least one error for short round1, got none")
	}
}
