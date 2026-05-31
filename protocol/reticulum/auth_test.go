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

func (c *chanAuthIO) ReadMsg() ([]byte, error) {
	msg, ok := <-c.in
	if !ok {
		return nil, io.EOF
	}
	return msg, nil
}

func (c *chanAuthIO) WriteMsg(b []byte) error {
	cpy := make([]byte, len(b))
	copy(cpy, b)
	c.out <- cpy
	return nil
}

// newChanAuthPair returns two paired AuthIO endpoints (server, client).
func newChanAuthPair() (*chanAuthIO, *chanAuthIO) {
	ch1 := make(chan []byte, 8)
	ch2 := make(chan []byte, 8)
	return &chanAuthIO{in: ch1, out: ch2}, &chanAuthIO{in: ch2, out: ch1}
}

func TestAuth_Success(t *testing.T) {
	server, client := newChanAuthPair()
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
	server, client := newChanAuthPair()
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

func TestAuth_BadChallenge(t *testing.T) {
	// Client receives a garbage challenge (wrong version byte).
	server, client := newChanAuthPair()
	errs := make(chan error, 2)
	go func() {
		// Send a bad challenge manually.
		bad := make([]byte, 33)
		bad[0] = 0xFF // wrong version
		server.WriteMsg(bad)
		errs <- nil
	}()
	go func() { errs <- ClientAuth(client, "password") }()

	var clientErr error
	for i := 0; i < 2; i++ {
		err := <-errs
		if err != nil {
			clientErr = err
		}
	}
	if clientErr == nil {
		t.Fatal("expected client error for bad challenge, got nil")
	}
}

func TestAuth_ServerRejectsShortResponse(t *testing.T) {
	// Server receives a too-short client response.
	server, client := newChanAuthPair()
	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(server, "password") }()
	go func() {
		// Read the server challenge, send a short response.
		client.ReadMsg() //nolint:errcheck
		client.WriteMsg([]byte("short"))
		errs <- nil
	}()

	var serverErr error
	for i := 0; i < 2; i++ {
		err := <-errs
		if err != nil {
			serverErr = err
		}
	}
	if serverErr == nil {
		t.Fatal("expected server error for short response, got nil")
	}
}
