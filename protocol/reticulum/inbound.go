package reticulum

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
)

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
	doneCh      chan struct{}

	// Auth state: cache authenticated peer identities so we only challenge
	// each identity once; serialize the challenge itself with a mutex.
	authedPeers sync.Map   // peerHash(string) → struct{}
	authMu      sync.Mutex

	mu     sync.Mutex
	closed bool
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

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}
	if err := BridgeInit(configJSON); err != nil {
		return fmt.Errorf("bridge init failed: %w", err)
	}

	listenHash := h.options.Destination
	if listenHash == "" {
		listenHash = h.options.Name
	}
	if listenHash == "" {
		return fmt.Errorf("reticulum inbound: destination or name must be set")
	}

	handle, err := BridgeListen(listenHash)
	if err != nil {
		return fmt.Errorf("bridge listen failed: %w", err)
	}
	h.listenerHdl = handle

	go h.acceptLoop()
	return nil
}

func (h *Inbound) acceptLoop() {
	for {
		select {
		case ev := <-globalAcceptCh:
			if ev.listenerID != h.listenerHdl {
				// Not for this listener — put it back for other inbounds.
				// (In practice there is only one inbound, but be correct.)
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
	conn := newReticulumConn(connID, "inbound", peerHash)

	if h.options.Password != "" {
		if _, cached := h.authedPeers.Load(peerHash); !cached {
			// Serialize auth challenges so at most one HMAC exchange runs at a time.
			h.authMu.Lock()
			err := ServerAuth(conn, h.options.Password)
			h.authMu.Unlock()
			if err != nil {
				h.logger.Error("reticulum auth failed: ", err)
				conn.Close()
				return
			}
			h.authedPeers.Store(peerHash, struct{}{})
		}
	}

	destAddr, err := readDestHeader(conn)
	if err != nil {
		h.logger.Error("reticulum: read dest header: ", err)
		conn.Close()
		return
	}

	if h.router != nil {
		metadata := adapter.InboundContext{
			Network:     "tcp",
			Destination: M.ParseSocksaddr(destAddr),
		}
		h.router.RouteConnectionEx(context.Background(), conn, metadata, nil)
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

// Ensure *reticulumConn satisfies net.Conn (compile-time check).
var _ net.Conn = (*reticulumConn)(nil)
