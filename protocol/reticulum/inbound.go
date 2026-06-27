package reticulum

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

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
		if err := inlineConfig.Validate(); err != nil {
			return "", fmt.Errorf("invalid reticulum config: %w", err)
		}
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
	mu          sync.Mutex
	closed      bool
	doneCh      chan struct{}
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

	handle, err := BridgeListen(listenHash)
	if err != nil {
		return fmt.Errorf("bridge listen failed: %w", err)
	}
	h.listenerHdl = handle

	h.logger.Info("reticulum inbound: listener ready, handle=", handle)

	go h.acceptLoop()
	return nil
}

func (h *Inbound) acceptLoop() {
	for {
		select {
		case <-h.doneCh:
			return
		case ev := <-globalAcceptCh:
			if ev.listenerID != h.listenerHdl {
				// Not for us — put back and yield so other listeners can pick it up.
				select {
				case globalAcceptCh <- ev:
				default:
				}
				continue
			}
			go h.handleConn(ev.connID)
		}
	}
}

func (h *Inbound) handleConn(connID uint64) {
	h.logger.Debug("accepted connection, handle=", connID)
	handle := connID

	raw := newReticulumConn(handle, "inbound", fmt.Sprintf("listener-%d", h.listenerHdl), h.logger)
	fc := newFramedConn(raw)

	if h.options.Password != "" {
		ownID, err := BridgeTransportHash()
		if err != nil {
			h.logger.Warn("auth: own identity unavailable: ", err)
			ownID = ""
		}
		peerID, err := BridgeConnIdentifiedPeer(handle)
		if err != nil {
			h.logger.Warn("auth: peer identity unavailable (identify exchange may have failed): ", err)
			peerID = ""
		}
		policy := RetryPolicy(h.options.AuthRetry)
		if policy == "" {
			policy = RetryNone
		}
		if err := AuthWithRetry(fc, h.options.Password, ownID, peerID, policy); err != nil {
			h.logger.Error("reticulum auth failed: ", err)
			fc.Close()
			return
		}
	}
	fc.OpenGate()

	session := newMuxSessionServer(fc, h.logger, BridgeConnMaxPayload(handle))
	for mc := range session.incomingCh {
		mc := mc
		go func() {
			h.logger.Info("inbound virtual connection to ", mc.dest)
			if h.router != nil {
				metadata := adapter.InboundContext{
					Network:     "tcp",
					Destination: M.ParseSocksaddr(mc.dest),
				}
				h.router.RouteConnectionEx(context.Background(), mc, metadata, nil)
			}
		}()
	}
}

func (h *Inbound) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	close(h.doneCh)
	if h.listenerHdl != 0 {
		BridgeClose(h.listenerHdl)
	}
	BridgeShutdown()
	return nil
}
