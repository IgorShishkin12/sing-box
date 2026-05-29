package reticulum

// pkg_logger.go — package-level logger shared across all build modes.
// In with_reticulum builds it is wired up by BridgeSetLogger; in stub
// builds it remains nil and all log calls are no-ops.

import (
	"sync/atomic"

	"github.com/sagernet/sing-box/log"
)

var bridgeLoggerVal atomic.Value // stores log.ContextLogger

func pkgTrace(args ...interface{}) {
	if l, _ := bridgeLoggerVal.Load().(log.ContextLogger); l != nil {
		l.Trace(args...)
	}
}

func pkgWarn(args ...interface{}) {
	if l, _ := bridgeLoggerVal.Load().(log.ContextLogger); l != nil {
		l.Warn(args...)
	}
}
