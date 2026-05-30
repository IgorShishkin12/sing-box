package reticulum

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
)

// buildConfigJSON returns the JSON string to pass to BridgeInit.
// Priority: inline config > path config > minimal default "{}".
// Rust requires a non-null, non-empty config string.
func buildConfigJSON(inlineConfig *option.ReticulumConfig, configPath string) (string, error) {
	if inlineConfig != nil {
		b, err := json.Marshal(inlineConfig)
		if err != nil {
			return "", fmt.Errorf("failed to marshal reticulum config: %w", err)
		}
		return string(b), nil
	}
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return "", fmt.Errorf("failed to read reticulum config from %s: %w", configPath, err)
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
	}, nil
}

func (h *Inbound) trustStorePath() string {
	if h.options.ReticulumConfig != nil && h.options.ReticulumConfig.StoragePath != "" {
		return h.options.ReticulumConfig.StoragePath + "/trust_store.json"
	}
	return ""
}

func (h *Inbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("reticulum inbound: already closed")
	}

	listenHash := h.options.Destination
	if listenHash == "" {
		listenHash = h.options.Name
	}
	if listenHash == "" {
		return fmt.Errorf("reticulum inbound: destination or name must be set")
	}

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}

	h.logger.Info("reticulum inbound: starting, listening on ", listenHash)

	BridgeSetLogger(h.logger)

	if err := BridgeInit(configJSON); err != nil {
		return fmt.Errorf("bridge init failed: %w", err)
	}

	h.trustStore = NewTrustStore(h.trustStorePath())

	taskID, err := BridgeListen(listenHash)
	if err != nil {
		return fmt.Errorf("bridge listen failed: %w", err)
	}

	handle, err := BridgePollTask(taskID, 30*time.Second)
	if err != nil {
		return fmt.Errorf("listener poll failed: %w", err)
	}
	h.listenerHdl = handle

	h.logger.Info("reticulum inbound: listener ready")
	h.accepting = true

	go h.acceptLoop()
	return nil
}

func (h *Inbound) acceptLoop() {
	for {
		h.mu.Lock()
		if h.closed || !h.accepting {
			h.mu.Unlock()
			return
		}
		h.mu.Unlock()

		taskID, err := BridgeAccept(h.listenerHdl)
		if err != nil {
			h.logger.Error("accept error: ", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Use a long timeout: the first client may take 60-90 s for name
		// resolution + link establishment. If we time out before the connection
		// arrives, the Rust accept_wait task silently claims it and the handle
		// is never retrieved. 120 s gives plenty of margin.
		handle, err := BridgePollTask(taskID, 120*time.Second)
		if err != nil {
			h.logger.Error("poll after accept failed: ", err)
			continue
		}

		h.logger.Debug("accepted connection, handle=", handle)

		// Spawn per-connection goroutine immediately so acceptLoop can loop
		// back to BridgeAccept without waiting for auth+routing to complete.
		go h.handleConn(handle)
	}
}

func (h *Inbound) handleConn(handle uint64) {
	raw := newReticulumConn(handle, "inbound", fmt.Sprintf("listener-%d", h.listenerHdl))
	fc := newFramedConn(raw)
	go fc.dispatch()
	defer fc.Close()

	if h.options.Password != "" {
		if err := h.negotiateAuth(fc, "" /* peerHash — available in Phase 2 */); err != nil {
			h.logger.Error("reticulum auth failed: ", err)
			return
		}
	} else {
		fc.OpenGate()
	}

	destAddr, err := readDestHeader(fc)
	if err != nil {
		h.logger.Error("read dest header: ", err)
		return
	}

	h.logger.Info("inbound connection to ", destAddr)

	if h.router != nil {
		metadata := adapter.InboundContext{
			Network:     "tcp",
			Destination: M.ParseSocksaddr(destAddr),
		}
		h.router.RouteConnectionEx(context.Background(), fc, metadata, nil)
	}
}

// negotiateAuth exchanges trust hints and runs a full SBRT-AUTH-1 handshake
// if needed, then opens the data gate.
func (h *Inbound) negotiateAuth(fc *framedConn, peerHash string) error {
	myTrust := byte(0x00)
	if h.trustStore.IsTrusted(peerHash, h.options.Password) {
		myTrust = 0x01
	}

	if err := fc.WriteMsg(string([]byte{myTrust})); err != nil {
		return fmt.Errorf("write trust hint: %w", err)
	}

	peerMsg, err := fc.ReadMsg()
	if err != nil {
		return fmt.Errorf("read trust hint: %w", err)
	}
	if len(peerMsg) == 0 {
		return fmt.Errorf("empty trust hint from peer")
	}
	peerTrust := peerMsg[0]

	if myTrust == 0x01 && peerTrust == 0x01 {
		fc.OpenGate()
		return nil
	}

	h.authMu.Lock()
	err = ServerAuth(fc, h.options.Password)
	h.authMu.Unlock()
	if err != nil {
		return err
	}

	if err := h.trustStore.Store(peerHash, h.options.Password); err != nil {
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
	if h.listenerHdl != 0 {
		BridgeClose(h.listenerHdl)
	}
	BridgeShutdown()
	return nil
}
