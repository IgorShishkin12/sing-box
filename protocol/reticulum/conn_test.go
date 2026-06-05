package reticulum

import (
	"io"
	"testing"

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
