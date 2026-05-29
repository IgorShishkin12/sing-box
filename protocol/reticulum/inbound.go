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
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
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
	listenerTask int
	listenerHdl  uint64
	accepting   bool
	mu          sync.Mutex
	closed      bool
	trustStore  *TrustStore
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ReticulumInboundOptions) (adapter.Inbound, error) {
	return &Inbound{
		Adapter:    inbound.NewAdapter(C.TypeReticulum, tag),
		router:     router,
		logger:     logger,
		options:    options,
		trustStore: NewTrustStore(),
	}, nil
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

	taskID, err := BridgeListen(listenHash)
	if err != nil {
		return fmt.Errorf("bridge listen failed: %w", err)
	}
	h.listenerTask = taskID

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
			h.logger.Error("reticulum: accept error: ", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		handle, err := BridgePollTask(taskID, 30*time.Second)
		if err != nil {
			h.logger.Error("reticulum: poll after accept failed: ", err)
			continue
		}

		// Get peer hash for trust store lookup (may be empty if unavailable).
		peerHash, _ := BridgeGetListenerHash(h.listenerHdl)

		h.logger.Debug("reticulum: accepted connection, peer=", peerHash, " handle=", handle)

		raw := newReticulumConn(handle, "inbound", fmt.Sprintf("listener-%d", h.listenerHdl))
		fc := newFramedConn(raw, h.logger)

		// Handle each connection concurrently so the accept loop can
		// immediately issue the next BridgeAccept.
		go h.handleConn(fc, peerHash)
	}
}

func (h *Inbound) handleConn(fc *framedConn, peerHash string) {
	defer fc.Close()

	h.logger.Debug("reticulum: handling inbound connection, peer=", peerHash)

	if h.options.Password != "" {
		if err := negotiateServerAuth(fc, h.options.Password, peerHash, h.trustStore, h.logger); err != nil {
			h.logger.Error("reticulum auth failed: ", err)
			return
		}
	}

	fc.OpenGate()

	destAddr, err := readDestHeader(fc)
	if err != nil {
		h.logger.Error("reticulum: read dest header: ", err)
		return
	}

	h.logger.Info("reticulum: inbound connection from ", peerHash, " to ", destAddr)

	if h.router != nil {
		metadata := adapter.InboundContext{
			Network:     "tcp",
			Destination: M.ParseSocksaddr(destAddr),
		}
		h.router.RouteConnectionEx(context.Background(), fc, metadata, nil)
	}
}

// negotiateServerAuth performs the server-side auth negotiation.
// If the peer is in the trust store, skips full auth; otherwise runs ServerAuth.
func negotiateServerAuth(fc *framedConn, password, peerHash string, ts *TrustStore, logger log.ContextLogger) error {
	if peerHash != "" {
		tok := TrustToken(password, peerHash)
		if ts.Check(peerHash, tok) {
			logger.Debug("reticulum: trusted peer, skipping auth: ", peerHash)
			// Trusted peer: send hint byte 0x01 and skip full auth.
			if err := fc.WriteMsg([]byte{0x01}); err != nil {
				return fmt.Errorf("write trust hint: %w", err)
			}
			return nil
		}
	}
	logger.Debug("reticulum: running full auth for peer: ", peerHash)
	// Unknown peer: send hint byte 0x00 and do full auth.
	if err := fc.WriteMsg([]byte{0x00}); err != nil {
		return fmt.Errorf("write auth hint: %w", err)
	}
	if err := ServerAuth(fc, password); err != nil {
		return err
	}
	if peerHash != "" {
		ts.Store(peerHash, TrustToken(password, peerHash))
	}
	logger.Debug("reticulum: auth succeeded for peer: ", peerHash)
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
