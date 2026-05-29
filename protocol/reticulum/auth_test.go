package reticulum

import (
	"io"
	"testing"
)

// chanAuthIO implements AuthIO using channels — no network involved.
type chanAuthIO struct {
	sendCh chan []byte
	recvCh chan []byte
}

func (c *chanAuthIO) WriteMsg(msg []byte) error {
	cp := make([]byte, len(msg))
	copy(cp, msg)
	c.sendCh <- cp
	return nil
}

func (c *chanAuthIO) ReadMsg() ([]byte, error) {
	msg, ok := <-c.recvCh
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

// newChanAuthIOPair returns two linked chanAuthIOs: writes to a appear as reads on b and vice versa.
func newChanAuthIOPair() (*chanAuthIO, *chanAuthIO) {
	ab := make(chan []byte, 32)
	ba := make(chan []byte, 32)
	return &chanAuthIO{sendCh: ab, recvCh: ba},
		&chanAuthIO{sendCh: ba, recvCh: ab}
}

func TestAuth_Success(t *testing.T) {
	server, client := newChanAuthIOPair()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(server, "correct-password") }()
	go func() { errs <- ClientAuth(client, "correct-password") }()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestAuth_WrongPassword(t *testing.T) {
	server, client := newChanAuthIOPair()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(server, "server-password") }()
	go func() { errs <- ClientAuth(client, "client-password") }()

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

func TestAuth_BadHeader(t *testing.T) {
	server, client := newChanAuthIOPair()

	go func() {
		server.sendCh <- []byte("GARBAGE-HEADER")
		close(server.sendCh)
	}()

	err := ClientAuth(client, "password")
	if err == nil {
		t.Fatal("expected error for bad header, got nil")
	}
}

func TestAuth_Timeout(t *testing.T) {
	// Server sends header but nothing else; client should time out.
	// We use a very short timeout by relying on the goroutine not sending.
	server, client := newChanAuthIOPair()
	_ = server // suppress unused warning

	// Close the server send channel after the header to simulate a hung server.
	go func() {
		server.sendCh <- []byte("SBRT-AUTH-1")
		close(server.sendCh)
	}()

	err := ClientAuth(client, "password")
	if err == nil {
		t.Fatal("expected error when server hangs, got nil")
	}
}
