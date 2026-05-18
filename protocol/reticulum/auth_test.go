package reticulum

import (
	"net"
	"testing"
)

func TestAuth_Success(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(serverConn, "correct-password") }()
	go func() { errs <- ClientAuth(clientConn, "correct-password") }()

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestAuth_WrongPassword(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	errs := make(chan error, 2)
	go func() { errs <- ServerAuth(serverConn, "server-password") }()
	go func() { errs <- ClientAuth(clientConn, "client-password") }()

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
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	go func() {
		serverConn.Write([]byte("GARBAGE-HEADER\n"))
		serverConn.Close()
	}()

	err := ClientAuth(clientConn, "password")
	if err == nil {
		t.Fatal("expected error for bad header, got nil")
	}
}
