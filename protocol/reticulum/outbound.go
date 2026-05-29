package reticulum

import (
	"context"
	"fmt"
	"net"
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

// serializedConn wraps a net.Conn and releases a per-destination mutex on Close,
// ensuring at most one active Reticulum connection per destination at a time.
type serializedConn struct {
	net.Conn
	mu   *sync.Mutex
	once sync.Once
}

func (sc *serializedConn) Close() error {
	err := sc.Conn.Close()
	sc.once.Do(func() { sc.mu.Unlock() })
	return err
}

type Outbound struct {
	outbound.Adapter
	network      []string
	router       adapter.Router
	logger       log.ContextLogger
	options      option.ReticulumOutboundOptions
	bridgeInited bool
	resolvedHash string // Reticulum destination hash, cached after first resolution
	mu           sync.Mutex
	trustStore   *TrustStore
	dialMu       sync.Map // destHash → *sync.Mutex; serializes dials per destination
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.ReticulumOutboundOptions) (adapter.Outbound, error) {
	return &Outbound{
		Adapter:    outbound.NewAdapterWithDialerOptions(C.TypeReticulum, tag, options.Network.Build(), options.DialerOptions),
		network:    options.Network.Build(),
		router:     router,
		logger:     logger,
		options:    options,
		trustStore: NewTrustStore(),
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

	h.logger.Info("reticulum outbound: starting, destination=", h.options.Destination, " name=", h.options.Name)

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}

	BridgeSetLogger(h.logger)
	if err := BridgeInit(configJSON); err != nil {
		return fmt.Errorf("bridge init failed: %w", err)
	}
	h.bridgeInited = true
	h.logger.Info("reticulum outbound: bridge initialized")
	return nil
}

func (h *Outbound) Close() error {
	h.logger.Debug("reticulum outbound: shutting down")
	BridgeShutdown()
	return nil
}

func (h *Outbound) Network() []string {
	return h.network
}

func (h *Outbound) Dependencies() []string {
	return nil
}

// perDestMu returns (and lazily creates) the per-destination mutex.
func (h *Outbound) perDestMu(destHash string) *sync.Mutex {
	v, _ := h.dialMu.LoadOrStore(destHash, &sync.Mutex{})
	return v.(*sync.Mutex)
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

	h.mu.Lock()
	destHash := h.resolvedHash
	h.mu.Unlock()

	if destHash == "" {
		h.logger.Debug("reticulum: resolving name ", h.options.Name)
		hash, err := BridgeResolveName(h.options.Name)
		if err != nil {
			return nil, fmt.Errorf("reticulum: resolve %q: %w", h.options.Name, err)
		}
		h.logger.Debug("reticulum: resolved ", h.options.Name, " → ", hash)
		h.mu.Lock()
		h.resolvedHash = hash
		h.mu.Unlock()
		destHash = hash
	}

	h.logger.DebugContext(ctx, "reticulum: dialing ", destHash, " for ", destination)

	// Serialize dials per destination: only one active Reticulum connection at a
	// time prevents concurrent goroutines from racing on the shared link.
	mu := h.perDestMu(destHash)
	mu.Lock()

	taskID, err := BridgeDial(destHash)
	if err != nil {
		h.logger.ErrorContext(ctx, "reticulum: dial error: ", err)
		mu.Unlock()
		return nil, err
	}

	deadline, ok := ctx.Deadline()
	timeout := 30 * time.Second
	if ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			mu.Unlock()
			return nil, context.DeadlineExceeded
		}
	}

	handle, err := BridgePollTask(taskID, timeout)
	if err != nil {
		mu.Unlock()
		return nil, err
	}

	h.logger.InfoContext(ctx, "reticulum: connected to ", destHash)

	localName := "outbound"
	if h.options.Name != "" {
		localName = h.options.Name
	}
	raw := newReticulumConn(handle, localName, destHash)
	fc := newFramedConn(raw, h.logger)

	if h.options.Password != "" {
		if err := negotiateClientAuth(fc, h.options.Password, destHash, h.trustStore, h.logger); err != nil {
			fc.Close()
			mu.Unlock()
			return nil, fmt.Errorf("reticulum auth failed: %w", err)
		}
	}

	fc.OpenGate()

	if err := writeDestHeader(fc, destination.String()); err != nil {
		fc.Close()
		mu.Unlock()
		return nil, fmt.Errorf("reticulum: write dest header: %w", err)
	}

	// Wrap conn so the per-dest mutex is released when the connection closes.
	return &serializedConn{Conn: fc, mu: mu}, nil
}

// negotiateClientAuth performs the client-side auth negotiation.
// Reads the server's trust hint; if trusted (0x01), skips full auth.
func negotiateClientAuth(fc *framedConn, password, destHash string, ts *TrustStore, logger log.ContextLogger) error {
	hint, err := fc.ReadMsg()
	if err != nil {
		return fmt.Errorf("read trust hint: %w", err)
	}
	if len(hint) > 0 && hint[0] == 0x01 {
		logger.Debug("reticulum: server trusts us, skipping auth")
		return nil
	}
	logger.Debug("reticulum: running full client auth")
	if err := ClientAuth(fc, password); err != nil {
		return err
	}
	if destHash != "" {
		ts.Store(destHash, TrustToken(password, destHash))
	}
	return nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}
