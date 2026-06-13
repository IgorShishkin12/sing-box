package reticulum

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
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
	resolvedHash string // Reticulum destination hash, cached after first resolution
	mu           sync.Mutex
	sessionMu    sync.Mutex
	session      *muxSession
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

	h.logger.Info("reticulum outbound: starting, destination=", h.options.Destination, " name=", h.options.Name)

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}

	rustLog := ""
	if h.options.ReticulumConfig != nil {
		rustLog = h.options.ReticulumConfig.RustLog
	}
	setRustLogLevelIfUnset(h.logger, rustLog)
	BridgeSetLogger(h.logger)
	if err := BridgeInit(configJSON); err != nil {
		return fmt.Errorf("bridge init failed: %w", err)
	}
	h.bridgeInited = true
	h.logger.Info("reticulum outbound: bridge initialized")

	if h.options.AuthOnStart {
		if err := h.connectEager(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

// connectEager resolves the destination (if needed) and establishes the mux session
// immediately rather than waiting for the first DialContext call.
// Caller must hold h.mu when h.resolvedHash may be written.
func (h *Outbound) connectEager(ctx context.Context) error {
	destHash := h.resolvedHash
	if destHash == "" {
		hash, err := BridgeResolveName(h.options.Name)
		if err != nil {
			return fmt.Errorf("resolve %q on start: %w", h.options.Name, err)
		}
		h.resolvedHash = hash
		destHash = hash
	}
	if _, err := h.getOrCreateSession(ctx, destHash); err != nil {
		return fmt.Errorf("connect on start: %w", err)
	}
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

// getOrCreateSession returns the shared mux session, creating it if needed.
// Must be called with h.mu already released (it acquires sessionMu internally).
func (h *Outbound) getOrCreateSession(ctx context.Context, destHash string) (*muxSession, error) {
	h.sessionMu.Lock()
	defer h.sessionMu.Unlock()

	if h.session != nil && !h.session.isClosed() {
		return h.session, nil
	}

	h.logger.DebugContext(ctx, "opening new mux session to ", destHash)

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
	handle := connID

	localName := "outbound"
	if h.options.Name != "" {
		localName = h.options.Name
	}
	raw := newReticulumConn(handle, localName, destHash, h.logger)
	fc := newFramedConn(raw)

	if h.options.Password != "" {
		ownID, err := BridgeTransportHash()
		if err != nil {
			h.logger.WarnContext(ctx, "auth: own identity unavailable: ", err)
			ownID = ""
		}
		peerID, err := BridgeConnIdentifiedPeer(handle)
		if err != nil {
			h.logger.WarnContext(ctx, "auth: peer identity unavailable (identify exchange may have failed): ", err)
			peerID = ""
		}
		policy := RetryPolicy(h.options.AuthRetry)
		if policy == "" {
			policy = RetryNone
		}
		if err := AuthWithRetry(fc, h.options.Password, ownID, peerID, policy); err != nil {
			fc.Close()
			return nil, fmt.Errorf("reticulum auth failed: %w", err)
		}
	}
	fc.OpenGate()

	h.session = newMuxSessionClient(fc, h.logger)
	h.logger.InfoContext(ctx, "mux session established to ", destHash)
	return h.session, nil
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
		h.logger.Debug("resolving name ", h.options.Name)
		hash, err := BridgeResolveName(h.options.Name)
		if err != nil {
			return nil, fmt.Errorf("resolve %q: %w", h.options.Name, err)
		}
		h.logger.Debug("resolved ", h.options.Name, " → ", hash)
		h.mu.Lock()
		h.resolvedHash = hash
		h.mu.Unlock()
		destHash = hash
	}

	h.logger.DebugContext(ctx, "dialing ", destHash, " for ", destination)

	session, err := h.getOrCreateSession(ctx, destHash)
	if err != nil {
		h.logger.ErrorContext(ctx, "session error: ", err)
		return nil, err
	}

	mc, err := session.OpenConn(destination.String())
	if err != nil {
		return nil, fmt.Errorf("mux OpenConn: %w", err)
	}
	return mc, nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}
