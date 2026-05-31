package reticulum

import (
	"io"
	"testing"
)

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
