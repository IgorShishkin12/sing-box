package reticulum

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

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
	resolvedHash string
	trustStore   *TrustStore
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

func (h *Outbound) trustStorePath() string {
	if h.options.ReticulumConfig != nil && h.options.ReticulumConfig.StoragePath != "" {
		return h.options.ReticulumConfig.StoragePath + "/trust_store.json"
	}
	return ""
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
	h.trustStore = NewTrustStore(h.trustStorePath())
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
	ts := h.trustStore
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
		ts = h.trustStore
		h.mu.Unlock()
		destHash = hash
	}

	h.logger.DebugContext(ctx, "dialing ", destHash, " for ", destination)

	taskID, err := BridgeDial(destHash)
	if err != nil {
		h.logger.ErrorContext(ctx, "dial error: ", err)
		return nil, err
	}

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

	h.logger.InfoContext(ctx, "connected to ", destHash)

	localName := "outbound"
	if h.options.Name != "" {
		localName = h.options.Name
	}
	raw := newReticulumConn(handle, localName, destHash)
	fc := newFramedConn(raw)
	go fc.dispatch()

	if h.options.Password != "" {
		if err := h.negotiateAuth(fc, destHash, ts); err != nil {
			fc.Close()
			return nil, fmt.Errorf("reticulum auth failed: %w", err)
		}
	} else {
		fc.OpenGate()
	}

	if err := writeDestHeader(fc, destination.String()); err != nil {
		fc.Close()
		return nil, fmt.Errorf("write dest header: %w", err)
	}

	return fc, nil
}

// negotiateAuth exchanges trust hints and runs a full SBRT-AUTH-1 handshake
// if needed, then opens the data gate.
func (h *Outbound) negotiateAuth(fc *framedConn, destHash string, ts *TrustStore) error {
	myTrust := byte(0x00)
	if ts != nil && ts.IsTrusted(destHash, h.options.Password) {
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
		return fmt.Errorf("empty trust hint from server")
	}
	peerTrust := peerMsg[0]

	if myTrust == 0x01 && peerTrust == 0x01 {
		fc.OpenGate()
		return nil
	}

	if err := ClientAuth(fc, h.options.Password); err != nil {
		return err
	}

	if ts != nil {
		if err := ts.Store(destHash, h.options.Password); err != nil {
			h.logger.Warn("trust store write failed: ", err)
		}
	}
	fc.OpenGate()
	return nil
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}
