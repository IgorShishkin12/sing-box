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
	router       adapter.Router
	logger       log.ContextLogger
	options      option.ReticulumInboundOptions
	listenerTask int
	listenerHdl  uint64
	accepting    bool
	mu           sync.Mutex
	closed       bool
}

func NewInbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ReticulumInboundOptions) (adapter.Inbound, error) {
	return &Inbound{
		Adapter: inbound.NewAdapter(C.TypeReticulum, tag),
		router:  router,
		logger:  logger,
		options: options,
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
			h.logger.Error("accept error: ", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		handle, err := BridgePollTask(taskID, 30*time.Second)
		if err != nil {
			h.logger.Error("poll after accept failed: ", err)
			continue
		}

		h.logger.Debug("accepted connection, handle=", handle)

		conn := newReticulumConn(handle, "inbound", fmt.Sprintf("listener-%d", h.listenerHdl), h.logger)

		if h.options.Password != "" {
			if err := ServerAuth(conn, h.options.Password); err != nil {
				h.logger.Error("reticulum auth failed: ", err)
				conn.Close()
				continue
			}
		}

		session := newMuxSessionServer(conn, h.logger)
		go h.handleSession(session)
	}
}

// handleSession dispatches incoming virtual connections from a mux session.
func (h *Inbound) handleSession(s *muxSession) {
	for mc := range s.incomingCh {
		go h.routeVirtualConn(mc)
	}
}

// routeVirtualConn routes one virtual connection to the configured destination.
func (h *Inbound) routeVirtualConn(mc *muxConn) {
	defer mc.Close()
	h.logger.Info("inbound virtual connection to ", mc.dest)
	if h.router != nil {
		metadata := adapter.InboundContext{
			Network:     "tcp",
			Destination: M.ParseSocksaddr(mc.dest),
		}
		h.router.RouteConnectionEx(context.Background(), mc, metadata, nil)
	}
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
