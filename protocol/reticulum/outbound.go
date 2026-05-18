package reticulum

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.ReticulumOutboundOptions](registry, C.TypeReticulum, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	network      []string
	router       adapter.Router
	logger       log.ContextLogger
	options      option.ReticulumOutboundOptions
	bridgeInited bool
	resolvedHash string
	mu           sync.Mutex
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ReticulumOutboundOptions) (adapter.Outbound, error) {
	return &Outbound{
		Adapter: outbound.NewAdapterWithDialerOptions(C.TypeReticulum, tag, options.Network.Build(), options.DialerOptions),
		network: options.Network.Build(),
		router:  router,
		logger:  logger,
		options: options,
	}, nil
}

func (h *Outbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateInitialize {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bridgeInited {
		return nil
	}

	if h.options.Destination != "" {
		h.resolvedHash = h.options.Destination
	} else if h.options.Name == "" {
		return fmt.Errorf("reticulum outbound: destination or name must be set")
	}

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}
	if err := BridgeInit(configJSON); err != nil {
		return fmt.Errorf("bridge init failed: %w", err)
	}
	h.bridgeInited = true
	return nil
}

func (h *Outbound) Close() error {
	BridgeShutdown()
	return nil
}

func (h *Outbound) Network() []string { return h.network }

func (h *Outbound) Dependencies() []string { return nil }

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	h.mu.Lock()
	inited := h.bridgeInited
	h.mu.Unlock()
	if !inited {
		if err := h.Start(adapter.StartStateInitialize); err != nil {
			return nil, err
		}
	}

	h.mu.Lock()
	destHash := h.resolvedHash
	h.mu.Unlock()

	if destHash == "" {
		hash, err := BridgeResolveName(h.options.Name)
		if err != nil {
			return nil, fmt.Errorf("reticulum: resolve %q: %w", h.options.Name, err)
		}
		h.mu.Lock()
		h.resolvedHash = hash
		h.mu.Unlock()
		destHash = hash
	}

	_, resultCh, err := BridgeDial(destHash)
	if err != nil {
		return nil, err
	}

	var connID uint64
	select {
	case connID = <-resultCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if connID == 0 {
		return nil, ErrBridgeDialFailed
	}

	localName := "outbound"
	if h.options.Name != "" {
		localName = h.options.Name
	}
	conn := newReticulumConn(connID, localName, destHash)

	if h.options.Password != "" {
		if err := ClientAuth(conn, h.options.Password); err != nil {
			conn.Close()
			return nil, fmt.Errorf("reticulum auth failed: %w", err)
		}
	}

	if err := writeDestHeader(conn, destination.String()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("reticulum: write dest header: %w", err)
	}

	return conn, nil
}

func (h *Outbound) ListenPacket(_ context.Context, _ M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}
