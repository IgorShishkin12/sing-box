package reticulum

import (
	"os"

	"github.com/sagernet/sing-box/log"
)

// setRustLogLevelIfUnset sets RUST_LOG from the sing-box log level if it is
// not already set in the environment. Must be called before BridgeInit because
// Rust reads RUST_LOG exactly once during tracing subscriber initialization.
func setRustLogLevelIfUnset(logger log.ContextLogger) {
	if os.Getenv("RUST_LOG") != "" {
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
