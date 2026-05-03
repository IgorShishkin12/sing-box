package reticulum

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

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
	if stage == adapter.StartStateInitialize {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.bridgeInited {
			return nil
		}
		// If config path is provided, load and init bridge with it.
		if h.options.ReticulumConfigPath != "" {
			data, err := os.ReadFile(h.options.ReticulumConfigPath)
			if err != nil {
				return fmt.Errorf("failed to read reticulum config: %w", err)
			}
			if err := BridgeInit(string(data)); err != nil {
				return fmt.Errorf("bridge init failed: %w", err)
			}
		} else if h.options.ReticulumConfig != nil {
			// Marshal inline config
			b, err := json.Marshal(h.options.ReticulumConfig)
			if err != nil {
				return fmt.Errorf("failed to marshal config: %w", err)
			}
			if err := BridgeInit(string(b)); err != nil {
				return fmt.Errorf("bridge init failed: %w", err)
			}
		} else {
			// Default init
			if err := BridgeInit(""); err != nil {
				return fmt.Errorf("bridge init failed: %w", err)
			}
		}
		h.bridgeInited = true
	}
	return nil
}

func (h *Outbound) Close() error {
	BridgeShutdown()
	return nil
}

func (h *Outbound) Network() []string {
	return h.network
}

func (h *Outbound) Dependencies() []string {
	return nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	h.mu.Lock()
	inited := h.bridgeInited
	h.mu.Unlock()
	if !inited {
		if err := h.Start(adapter.StartStateInitialize); err != nil {
			return nil, err
		}
	}

	// Use destination string as the hash to dial
	dest := destination.String()
	taskID, err := BridgeDial(dest)
	if err != nil {
		return nil, err
	}

	// Poll with context deadline
	deadline, ok := ctx.Deadline()
	timeout := 30 * time.Second
	if ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
	}

	handle, err := BridgePollTask(taskID, timeout)
	if err != nil {
		return nil, err
	}

	localName := "outbound"
	if h.options.Name != "" {
		localName = h.options.Name
	}
	remoteName := dest
	return newReticulumConn(handle, localName, remoteName), nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}



