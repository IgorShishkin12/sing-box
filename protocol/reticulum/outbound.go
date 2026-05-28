package reticulum

import (
	"context"
	"net"
	"sync"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
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
		return E.New("reticulum outbound: destination or name must be set")
	}

	configJSON, err := buildConfigJSON(h.options.ReticulumConfig, h.options.ReticulumConfigPath)
	if err != nil {
		return err
	}
	if err := BridgeInit(configJSON); err != nil {
		return E.Cause(err, "bridge init failed")
	}
	h.trustStore = NewTrustStore(h.trustStorePath())
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
	ts := h.trustStore
	h.mu.Unlock()

	if destHash == "" {
		hash, err := BridgeResolveName(h.options.Name)
		if err != nil {
			return nil, E.Cause(err, "reticulum: resolve ", h.options.Name)
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
	raw := newReticulumConn(connID, localName, destHash)
	fc := newFramedConn(raw)
	go fc.dispatch()

	if h.options.Password != "" {
		if err := h.negotiateAuth(fc, destHash, ts); err != nil {
			fc.Close()
			return nil, E.Cause(err, "reticulum auth failed")
		}
	} else {
		fc.OpenGate()
	}

	if err := writeDestHeader(fc, destination.String()); err != nil {
		fc.Close()
		return nil, E.Cause(err, "reticulum: write dest header")
	}

	return fc, nil
}

// negotiateAuth exchanges trust hints with the server and either opens the gate
// immediately or runs a full SBRT-AUTH-1 handshake.
func (h *Outbound) negotiateAuth(fc *framedConn, destHash string, ts *TrustStore) error {
	myTrust := byte(0x00)
	if ts != nil && ts.IsTrusted(destHash, h.options.Password) {
		myTrust = 0x01
	}

	// Write our hint, then read server's.
	if err := fc.WriteMsg(string([]byte{myTrust})); err != nil {
		return E.Cause(err, "write trust hint")
	}

	peerMsg, err := fc.ReadMsg()
	if err != nil {
		return E.Cause(err, "read trust hint")
	}
	if len(peerMsg) == 0 {
		return E.New("empty trust hint from server")
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

func (h *Outbound) ListenPacket(_ context.Context, _ M.Socksaddr) (net.PacketConn, error) {
	return nil, N.ErrUnknownNetwork
}
