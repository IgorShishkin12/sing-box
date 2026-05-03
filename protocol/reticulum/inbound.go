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
)




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
		return fmt.Errorf("inbound closed")
	}

	// Init bridge if needed
	if h.options.ReticulumConfigPath != "" {
		data, err := os.ReadFile(h.options.ReticulumConfigPath)
		if err != nil {
			return fmt.Errorf("failed to read reticulum config: %w", err)
		}
		if err := BridgeInit(string(data)); err != nil {
			return fmt.Errorf("bridge init failed: %w", err)
		}
	} else if h.options.ReticulumConfig != nil {
		b, err := json.Marshal(h.options.ReticulumConfig)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}
		if err := BridgeInit(string(b)); err != nil {
			return fmt.Errorf("bridge init failed: %w", err)
		}
	} else {
		if err := BridgeInit(""); err != nil {
			return fmt.Errorf("bridge init failed: %w", err)
		}
	}

	// Determine listen hash
	listenHash := h.options.Destination
	if listenHash == "" {
		if h.options.Name != "" {
			listenHash = h.options.Name
		} else {
			listenHash = "default-listen"
		}
	}

	// Call BridgeListen
	taskID, err := BridgeListen(listenHash)
	if err != nil {
		return fmt.Errorf("bridge listen failed: %w", err)
	}
	h.listenerTask = taskID

	// Poll for listener handle
	timeout := 30 * time.Second
	handle, err := BridgePollTask(taskID, timeout)
	if err != nil {
		return fmt.Errorf("listener poll failed: %w", err)
	}
	h.listenerHdl = handle
	h.accepting = true

	// Start accept loop
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

		// Accept new connection via bridge
		taskID, err := BridgeAccept(h.listenerHdl)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		handle, err := BridgePollTask(taskID, 30*time.Second)
		if err != nil {
			continue
		}

		conn := newReticulumConn(handle, "inbound", fmt.Sprintf("listener-%d", h.listenerHdl))
		if h.router != nil {
			h.router.RouteConnectionEx(context.Background(), conn, adapter.InboundContext{}, nil)

		}
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


