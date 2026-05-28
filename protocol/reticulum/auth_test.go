package reticulum

import (
	"io"
	"testing"
)

// chanAuthIO implements AuthIO using a pair of Go channels, making it
// suitable for unit tests without needing a real network connection or
// framing layer.
type chanAuthIO struct {
	send chan string
	recv chan string
}

func (c *chanAuthIO) WriteMsg(text string) (err error) {
	// Recover from a send on a closed channel (happens when the peer has
	// already exited and signalled EOF by closing its send channel).
	defer func() {
		if r := recover(); r != nil {
			err = io.EOF
		}
	}()
	c.send <- text
	return nil
}

func (c *chanAuthIO) ReadMsg() (string, error) {
	s, ok := <-c.recv
	if !ok {
		return "", io.EOF
	}
	return s, nil
}

// newChanAuthPair returns two linked AuthIO endpoints.
func newChanAuthPair() (*chanAuthIO, *chanAuthIO) {
	ch1 := make(chan string, 16)
	ch2 := make(chan string, 16)
	return &chanAuthIO{send: ch1, recv: ch2},
		&chanAuthIO{send: ch2, recv: ch1}
}

func TestAuth_Success(t *testing.T) {
	serverIO, clientIO := newChanAuthPair()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(serverIO, "correct-password") }()
	go func() { errs <- ClientAuth(clientIO, "correct-password") }()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestAuth_WrongPassword(t *testing.T) {
	serverIO, clientIO := newChanAuthPair()

	errs := make(chan error, 2)
	go func() {
		err := ServerAuth(serverIO, "server-password")
		// Signal the client that we're done by closing our send channel.
		// This causes the client's next ReadMsg to return io.EOF instead of
		// blocking forever.
		close(serverIO.send)
		errs <- err
	}()
	go func() { errs <- ClientAuth(clientIO, "client-password") }()

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
	serverIO, clientIO := newChanAuthPair()

	go func() {
		_ = serverIO.WriteMsg("GARBAGE-HEADER")
		close(serverIO.send)
	}()

	err := ClientAuth(clientIO, "password")
	if err == nil {
		t.Fatal("expected error for bad header, got nil")
	}
}
