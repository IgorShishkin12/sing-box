package reticulum

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReticulumConnClosedRead(t *testing.T) {
	c := &reticulumConn{handle: 0}
	_, err := c.Read(make([]byte, 4))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

func TestReticulumConnClosedWrite(t *testing.T) {
	c := &reticulumConn{handle: 0}
	_, err := c.Write([]byte("x"))
	require.ErrorIs(t, err, io.ErrClosedPipe)
}

// connEntry.deliver must never silently drop: it enqueues immediately when there
// is room, reports a closed connection, and applies bounded backpressure when the
// buffer is full instead of discarding the packet. Regression test for the silent
// large-request data loss (e2e_serial62 issues a & c).
func TestConnEntryDeliverFastPath(t *testing.T) {
	e := &connEntry{ch: make(chan []byte, 1), done: make(chan struct{})}
	require.Equal(t, deliverOK, e.deliver([]byte("a"), time.Second))
	require.Equal(t, []byte("a"), <-e.ch)
}

func TestConnEntryDeliverClosed(t *testing.T) {
	e := &connEntry{ch: make(chan []byte), done: make(chan struct{})}
	close(e.done)
	require.Equal(t, deliverClosed, e.deliver([]byte("a"), time.Second))
}

func TestConnEntryDeliverBackpressureThenDrain(t *testing.T) {
	e := &connEntry{ch: make(chan []byte, 1), done: make(chan struct{})}
	e.ch <- []byte("first") // fill the buffer

	// Drain after a short delay so the blocked send can complete.
	go func() {
		time.Sleep(20 * time.Millisecond)
		<-e.ch
	}()

	require.Equal(t, deliverBackpressured, e.deliver([]byte("second"), time.Second))
	require.Equal(t, []byte("second"), <-e.ch)
}

func TestConnEntryDeliverDropOnTimeout(t *testing.T) {
	e := &connEntry{ch: make(chan []byte, 1), done: make(chan struct{})}
	e.ch <- []byte("first") // fill and never drain
	require.Equal(t, deliverDropped, e.deliver([]byte("second"), 20*time.Millisecond))
}
