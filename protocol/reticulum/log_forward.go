package reticulum

import (
	"os"

	"github.com/sagernet/sing-box/log"
)

// setRustLogLevelIfUnset sets RUST_LOG if it is not already set in the
// environment. Must be called before BridgeInit because Rust reads RUST_LOG
// exactly once during tracing subscriber initialization.
//
// Priority: existing env var > explicit rustLog > sing-box log level.
func setRustLogLevelIfUnset(logger log.ContextLogger, rustLog string) {
	if os.Getenv("RUST_LOG") != "" {
		return
	}
	if rustLog != "" {
		os.Setenv("RUST_LOG", rustLog)
		return
	}
	level := log.LevelInfo // safe default if level cannot be read
	if lv, ok := logger.(interface{ Level() log.Level }); ok {
		level = lv.Level()
	}
	os.Setenv("RUST_LOG", rustLogFilter(level))
}

// rustLogFilter maps a sing-box log level to a tracing EnvFilter directive.
func rustLogFilter(level log.Level) string {
	switch level {
	case log.LevelTrace:
		return "trace,serde=off" // suppress noisy serde framework spans at trace
	case log.LevelDebug:
		return "debug,serde=off"
	case log.LevelInfo:
		return "info"
	case log.LevelWarn:
		return "warn"
	default: // LevelError, LevelFatal, LevelPanic
		return "error"
	}
}
