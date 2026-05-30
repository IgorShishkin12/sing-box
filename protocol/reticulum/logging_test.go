package reticulum

import (
	"context"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

// recordingLogger records all log calls so tests can assert on them.
type recordingLogger struct {
	mu    sync.Mutex
	calls []string
}

func (l *recordingLogger) record(level string) {
	l.mu.Lock()
	l.calls = append(l.calls, level)
	l.mu.Unlock()
}

func (l *recordingLogger) countLevel(level string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, c := range l.calls {
		if c == level {
			n++
		}
	}
	return n
}

func (l *recordingLogger) Trace(args ...any)                              { l.record("trace") }
func (l *recordingLogger) Debug(args ...any)                              { l.record("debug") }
func (l *recordingLogger) Info(args ...any)                               { l.record("info") }
func (l *recordingLogger) Warn(args ...any)                               { l.record("warn") }
func (l *recordingLogger) Error(args ...any)                              { l.record("error") }
func (l *recordingLogger) Fatal(args ...any)                              { l.record("fatal") }
func (l *recordingLogger) Panic(args ...any)                              { l.record("panic") }
func (l *recordingLogger) TraceContext(_ context.Context, args ...any)    { l.record("trace") }
func (l *recordingLogger) DebugContext(_ context.Context, args ...any)    { l.record("debug") }
func (l *recordingLogger) InfoContext(_ context.Context, args ...any)     { l.record("info") }
func (l *recordingLogger) WarnContext(_ context.Context, args ...any)     { l.record("warn") }
func (l *recordingLogger) ErrorContext(_ context.Context, args ...any)    { l.record("error") }
func (l *recordingLogger) FatalContext(_ context.Context, args ...any)    { l.record("fatal") }
func (l *recordingLogger) PanicContext(_ context.Context, args ...any)    { l.record("panic") }

var _ log.ContextLogger = (*recordingLogger)(nil)

func newTestInboundLogger(t *testing.T, opts option.ReticulumInboundOptions, logger log.ContextLogger) *Inbound {
	t.Helper()
	ib, err := NewInbound(context.Background(), nil, logger, "test", opts)
	require.NoError(t, err)
	return ib.(*Inbound)
}

func newTestOutboundLogger(t *testing.T, opts option.ReticulumOutboundOptions, logger log.ContextLogger) *Outbound {
	t.Helper()
	o, err := NewOutbound(context.Background(), nil, logger, "test", opts)
	require.NoError(t, err)
	return o.(*Outbound)
}

func TestInboundStart_LogsInfo(t *testing.T) {
	logger := &recordingLogger{}
	i := newTestInboundLogger(t, option.ReticulumInboundOptions{Name: "test-inbound"}, logger)
	// Fails at BridgeInit in stub mode, but the first Info log fires before that.
	_ = i.Start(adapter.StartStateInitialize)
	require.GreaterOrEqual(t, logger.countLevel("info"), 1, "expected Info log during Start")
}

func TestOutboundStart_LogsInfo(t *testing.T) {
	logger := &recordingLogger{}
	o := newTestOutboundLogger(t, option.ReticulumOutboundOptions{Destination: "aabbccdd"}, logger)
	_ = o.Start(adapter.StartStateInitialize)
	require.GreaterOrEqual(t, logger.countLevel("info"), 1, "expected Info log during Start")
}

func TestOutboundClose_LogsDebug(t *testing.T) {
	logger := &recordingLogger{}
	o := newTestOutboundLogger(t, option.ReticulumOutboundOptions{Name: "x"}, logger)
	require.NoError(t, o.Close())
	require.GreaterOrEqual(t, logger.countLevel("debug"), 1, "expected Debug log on Close")
}
