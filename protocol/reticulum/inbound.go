package reticulum

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
)

func buildConfigJSON(inlineConfig *option.ReticulumConfig, configPath string) (string, error) {
	if inlineConfig != nil {
		b, err := json.Marshal(inlineConfig)
		if err != nil {
			return "", E.Cause(err, "failed to marshal reticulum config")
		}
		return string(b), nil
	}
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return "", E.Cause(err, "failed to read reticulum config from ", configPath)
		}
		return string(data), nil
	}
	return "{}", nil
}

func RegisterInbound(registry *inbound.Registry) {
	inbound.Register[option.ReticulumInboundOptions](registry, C.TypeReticulum, NewInbound)
}

type Inbound struct {
	inbound.Adapter
	router      adapter.Router
	logger      log.ContextLogger
	options     option.ReticulumInboundOptions
	listenerHdl uint64
	doneCh      chan struct{}

	// authMu serialises full SBRT-AUTH-1 exchanges so two simultaneous
	// first-connections from the same peer don't race each other.
	authMu sync.Mutex

	// trustStore caches authenticated peer identities persistently on disk.
	trustStore *TrustStore

	mu        sync.Mutex
	closed    bool
	accepting bool
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ReticulumInboundOptions) (adapter.Inbound, error) {
	return &Inbound{
		Adapter: inbound.NewAdapter(C.TypeReticulum, tag),
		router:  router,
		logger:  logger,
		options: options,
		doneCh:  make(chan struct{}),
	}, nil
}

func (h *Inbound) trustStorePath() string {
	if h.options.ReticulumConfig != nil && h.options.ReticulumConfig.StoragePath != "" {
		return h.options.ReticulumConfig.StoragePath + "/trust_store.json"
	}
	return "" // in-memory only when no storage path is configured
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return E.New("reticulum inbound: already closed")
	}

	h.trustStore = NewTrustStore(h.trustStorePath())

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}
	if err := BridgeInit(configJSON); err != nil {
		return E.Cause(err, "bridge init failed")
	}

	listenHash := h.options.Destination
	if listenHash == "" {
		listenHash = h.options.Name
	}
	if listenHash == "" {
		return E.New("reticulum inbound: destination or name must be set")
	}

	handle, err := BridgeListen(listenHash)
	if err != nil {
		return E.Cause(err, "bridge listen failed")
	}
	h.listenerHdl = handle
	h.accepting = true

	go h.acceptLoop()
	return nil
}

func (h *Inbound) acceptLoop() {
	for {
		select {
		case ev := <-globalAcceptCh:
			if ev.listenerID != h.listenerHdl {
				// Not for this listener — put it back for other inbounds.
				globalAcceptCh <- ev
				continue
			}
			go h.handleConn(ev.connID, ev.peerHash)
		case <-h.doneCh:
			return
		}
	}
}

func (h *Inbound) handleConn(connID uint64, peerHash string) {
	raw := newReticulumConn(connID, "inbound", peerHash)
	fc := newFramedConn(raw)
	go fc.dispatch()

	defer fc.Close()

	if h.options.Password != "" {
		if err := h.negotiateAuth(fc, peerHash); err != nil {
			h.logger.Error("reticulum auth failed: ", err)
			return
		}
	} else {
		fc.OpenGate()
	}

	destAddr, err := readDestHeader(fc)
	if err != nil {
		h.logger.Error("reticulum: read dest header: ", err)
		return
	}

	if h.router != nil {
		metadata := adapter.InboundContext{
			Network:     "tcp",
			Destination: M.ParseSocksaddr(destAddr),
		}
		h.router.RouteConnectionEx(context.Background(), fc, metadata, nil)
	}
}

// negotiateAuth exchanges trust hints with the peer and either opens the gate
// immediately (both sides trust each other) or runs a full SBRT-AUTH-1
// handshake.
func (h *Inbound) negotiateAuth(fc *framedConn, peerHash string) error {
	// Determine our side's trust state.
	myTrust := byte(0x00)
	if h.trustStore.IsTrusted(peerHash, h.options.Password) {
		myTrust = 0x01
	}

	// Send our hint first (non-blocking in reticulum), then read peer's.
	if err := fc.WriteMsg(string([]byte{myTrust})); err != nil {
		return E.Cause(err, "write trust hint")
	}

	peerMsg, err := fc.ReadMsg()
	if err != nil {
		return E.Cause(err, "read trust hint")
	}
	if len(peerMsg) == 0 {
		return E.New("empty trust hint from peer")
	}
	peerTrust := peerMsg[0]

	if myTrust == 0x01 && peerTrust == 0x01 {
		// Both sides trust each other — skip the full handshake.
		fc.OpenGate()
		return nil
	}

	// Full handshake required. Serialise per-inbound so at most one
	// SBRT-AUTH-1 exchange happens at a time (prevents protocol races when
	// two connections from the same peer arrive simultaneously).
	h.authMu.Lock()
	err = ServerAuth(fc, h.options.Password)
	h.authMu.Unlock()
	if err != nil {
		return err
	}

	if err := h.trustStore.Store(peerHash, h.options.Password); err != nil {
		// Non-fatal: trust is held in memory even if disk write fails.
		h.logger.Warn("trust store write failed: ", err)
	}
	fc.OpenGate()
	return nil
}

func (h *Inbound) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	h.accepting = false
	close(h.doneCh)
	if h.listenerHdl != 0 {
		BridgeClose(h.listenerHdl)
	}
	BridgeShutdown()
	return nil
}

// Ensure *reticulumConn satisfies net.Conn (compile-time check).
var _ net.Conn = (*reticulumConn)(nil)
