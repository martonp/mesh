package tatanka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/admin"
	"github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// privateKeyFileName is the name of the file that contains the private key
	// for the tatanka node.
	privateKeyFileName = "p.key"

	// forwardRelayProtocol is the protocol used to forward a relay message between two tatanka nodes.
	forwardRelayProtocol = "/tatanka/forward-relay/1.0.0"

	// discoveryProtocol is the protocol used to query a tatanka node for the addresses of
	// a given peer.
	discoveryProtocol = "/tatanka/discovery/1.0.0"

	// whitelistProtocol is the protocol used to verify the whitelist alignment of a tatanka node.
	whitelistProtocol = "/tatanka/whitelist/1.0.0"

	// quotaHandshakeProtocol is the protocol used to exchange quota information between tatanka nodes.
	quotaHandshakeProtocol = "/tatanka/quota-handshake/1.0.0"
)

// Config is the configuration for the tatanka node
type Config struct {
	DataDir       string
	Logger        slog.Logger
	ListenIP      string
	ListenPort    int
	MetricsPort   int
	AdminPort     int
	WhitelistPath string

	// Oracle Configuration
	CMCKey           string
	TatumKey         string
	BlockcypherToken string
	CoinGeckoKey     string
	CoinGeckoPlan    string
}

// Option is a functional option for configuring TatankaNode.
type Option func(*TatankaNode)

// WithHost sets a pre-created libp2p host (e.g., for testing with mocks).
func WithHost(h host.Host) Option {
	return func(n *TatankaNode) {
		n.node = h
	}
}

// Oracle defines the requirements for implementing an oracle.
type Oracle interface {
	Run(ctx context.Context)
	Merge(update *oracle.OracleUpdate, senderID string) *oracle.MergeResult
	Price(ticker oracle.Ticker) (float64, bool)
	FeeRate(network oracle.Network) (*big.Int, bool)
	GetLocalQuotas() map[string]*sources.QuotaStatus
	UpdatePeerSourceQuota(peerID string, quota *oracle.TimestampedQuotaStatus, source string)
	OracleSnapshot() *oracle.OracleSnapshot
}

// TatankaNode is a permissioned node in the tatanka mesh
type TatankaNode struct {
	config       *Config
	node         host.Host
	log          slog.Logger
	whitelist    atomic.Value // *whitelist
	readyCh      chan struct{}
	readyOnce    sync.Once
	readyErr     atomic.Value // error
	privateKey   crypto.PrivKey
	bondVerifier *bondVerifier
	bondStorage  bondStorage

	gossipSub               *gossipSub
	clientConnectionManager *clientConnectionManager
	subscriptionManager     *subscriptionManager
	pushStreamManager       *pushStreamManager
	connectionManager       *meshConnectionManager
	adminServer             *admin.Server

	metricsServer *http.Server

	oracle Oracle
}

// NewTatankaNode creates a new TatankaNode with the given configuration and options.
func NewTatankaNode(config *Config, opts ...Option) (*TatankaNode, error) {
	privateKey, err := getOrCreatePrivateKey(filepath.Join(config.DataDir, privateKeyFileName))
	if err != nil {
		return nil, err
	}

	whitelist, err := loadWhitelist(config.WhitelistPath)
	if err != nil {
		return nil, err
	}

	t := &TatankaNode{
		config:                  config,
		log:                     config.Logger,
		privateKey:              privateKey,
		clientConnectionManager: newClientConnectionManager(config.Logger),
		subscriptionManager:     newSubscriptionManager(),
		bondVerifier:            newBondVerifier(),
		bondStorage:             newMemoryBondStorage(time.Now),
		readyCh:                 make(chan struct{}),
	}
	t.whitelist.Store(whitelist)

	for _, opt := range opts {
		opt(t)
	}

	return t, nil
}

func (t *TatankaNode) getWhitelist() *whitelist {
	return t.whitelist.Load().(*whitelist)
}

func (t *TatankaNode) getWhitelistPeers() map[peer.ID]struct{} {
	return t.getWhitelist().allPeerIDs()
}

func (t *TatankaNode) handleBroadcastMessage(msg *protocolsPb.PushMessage) {
	clients := t.subscriptionManager.clientsForTopic(msg.Topic)
	if len(clients) > 0 {
		t.pushStreamManager.distribute(clients, msg)
	}
}

func (t *TatankaNode) handleClientConnectionMessage(update *clientConnectionUpdate) {
	t.clientConnectionManager.updateClientConnectionInfo(update)
}

// Run starts the tatanka node and blocks until the context is done.
func (t *TatankaNode) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}

	// Setup libp2p node if not provided in options.
	if t.node == nil {
		listenAddrs := []string{
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", t.config.ListenPort),
			fmt.Sprintf("/ip6/::/tcp/%d", t.config.ListenPort),
		}
		var err error
		t.node, err = libp2p.New(
			libp2p.Identity(t.privateKey),
			libp2p.ListenAddrStrings(listenAddrs...),
			// EnableRelayService for p2p communication between clients
			libp2p.EnableRelayService(),
		)
		if err != nil {
			t.markReady(err)
			return err
		}
	}

	t.log.Infof("Node ID: %s", t.node.ID().String())

	listenAddrs := t.node.Network().ListenAddresses()
	t.log.Infof("Listening on: ")
	for _, addr := range listenAddrs {
		t.log.Infof("  %s", addr.String())
	}

	var err error
	t.gossipSub, err = newGossipSub(ctx, &gossipSubCfg{
		node:                          t.node,
		log:                           t.config.Logger,
		getWhitelistPeers:             t.getWhitelistPeers,
		handleBroadcastMessage:        t.handleBroadcastMessage,
		handleClientConnectionMessage: t.handleClientConnectionMessage,
		handleOracleUpdate:            t.handleOracleUpdate,
		handleQuotaHeartbeat:          t.handleQuotaHeartbeat,
	})
	if err != nil {
		t.markReady(err)
		return err
	}

	t.pushStreamManager = newPushStreamManager(t.config.Logger, func(client peer.ID, timestamp time.Time, connected bool) {
		err := t.gossipSub.publishClientConnectionMessage(ctx, &clientConnectionUpdate{
			clientID:   client,
			reporterID: t.node.ID(),
			timestamp:  timestamp.UnixMilli(),
			connected:  connected,
		})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			t.log.Errorf("Publishing client connection message failed: %v", err)
		}
	})

	// Only create oracle if not provided (e.g., via test setup)
	if t.oracle == nil {
		t.oracle, err = oracle.New(&oracle.Config{
			Log:              t.config.Logger,
			CMCKey:           t.config.CMCKey,
			TatumKey:         t.config.TatumKey,
			BlockcypherToken: t.config.BlockcypherToken,
			CoinGeckoKey:     t.config.CoinGeckoKey,
			CoinGeckoPlan:    t.config.CoinGeckoPlan,
			DataDir:          t.config.DataDir,
			NodeID:           t.node.ID().String(),
			PublishUpdate:    t.gossipSub.publishOracleUpdate,
			OnStateUpdate: func(update *oracle.OracleSnapshot) {
				if t.adminServer != nil {
					t.adminServer.BroadcastOracleUpdate("oracle_update", update)
				}
			},
			PublishQuotaHeartbeat: t.gossipSub.publishQuotaHeartbeat,
		})
		if err != nil {
			return fmt.Errorf("failed to create oracle: %v", err)
		}
	}

	// Create admin callback function and setup the admin server if configured.
	adminCallback := func(peerID peer.ID, connected bool, whitelistMismatch bool, addresses []string, peerWhitelist []string) {
	}
	if t.config.AdminPort > 0 {
		adminAddr := fmt.Sprintf(":%d", t.config.AdminPort)
		server := admin.NewServer(t.config.Logger, adminAddr, t.oracle)
		whitelistIDs := t.getWhitelist().allPeerIDs()
		whitelist := make([]string, 0, len(whitelistIDs))
		for id := range whitelistIDs {
			whitelist = append(whitelist, id.String())
		}
		server.UpdateWhitelist(whitelist)

		adminCallback = func(peerID peer.ID, connected, whitelistMismatch bool, addresses []string, peerWhitelist []string) {
			state := admin.StateDisconnected
			switch {
			case connected:
				state = admin.StateConnected
			case whitelistMismatch:
				state = admin.StateWhitelistMismatch
			}
			server.UpdateConnectionState(peerID, state, addresses, peerWhitelist)
		}

		t.adminServer = server
	}

	t.connectionManager = newMeshConnectionManager(
		t.config.Logger, t.node, t.getWhitelist(), adminCallback,
		func() map[string]*pb.QuotaStatus {
			return quotaStatusesToPb(t.oracle.GetLocalQuotas())
		},
		func(peerID peer.ID, quotas map[string]*pb.QuotaStatus) {
			for source, q := range quotas {
				t.oracle.UpdatePeerSourceQuota(peerID.String(), pbToTimestampedQuotaStatus(q), source)
			}
		},
	)

	t.log.Infof("Admin interface available (or not) on :%d", t.config.AdminPort)

	t.setupStreamHandlers()
	t.setupObservability()

	go func() {
		t.log.Infof("Metrics available on :%d/metrics", t.config.MetricsPort)
		t.log.Infof("Profiler available on :%d/debug/pprof", t.config.MetricsPort)
		if err := t.metricsServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				t.log.Errorf("Failed to start metrics server: %v", err)
			}
		}
	}()

	// Start admin server if configured
	if t.adminServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.log.Infof("Admin interface available on :%d", t.config.AdminPort)
			if err := t.adminServer.Start(ctx); err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					t.log.Errorf("Failed to start admin server: %v", err)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := t.gossipSub.run(ctx); err != nil {
			t.log.Errorf("Gossip sub failed: %v", err)
		} else {
			t.log.Infof("Gossip sub stopped.")
		}
	}()

	// Maintain mesh connectivity
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.connectionManager.run(ctx)
	}()

	// Wait for the initial connectivity pass to finish before reporting ready.
	t.connectionManager.waitInitial(ctx)
	t.markReady(nil)

	// Run Oracle
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.oracle.Run(ctx)
	}()

	wg.Wait()

	t.log.Infof("Shutting down tatanka node...")
	err = t.metricsServer.Shutdown(ctx)
	if err != nil {
		return err
	}

	err = t.node.Close()
	if err != nil {
		return err
	}

	t.log.Infof("Tatanka node shutdown complete.")

	return nil
}

// WaitReady blocks until Run has finished initialization or the context is done.
func (t *TatankaNode) WaitReady(ctx context.Context) error {
	select {
	case <-t.readyCh:
		if err, ok := t.readyErr.Load().(error); ok && err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// markReady signals that initialization is complete. If an error occurred,
// it is stored for WaitReady callers. Only the first call takes effect.
func (t *TatankaNode) markReady(err error) {
	t.readyOnce.Do(func() {
		if err != nil {
			t.readyErr.Store(err)
		}
		close(t.readyCh)
	})
}

func getOrCreatePrivateKey(filePath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
			if err != nil {
				return nil, err
			}
			bytes, err := crypto.MarshalPrivateKey(priv)
			if err != nil {
				return nil, err
			}
			if err := os.WriteFile(filePath, bytes, 0600); err != nil {
				return nil, err
			}
			return priv, nil
		}
		return nil, err
	}
	priv, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, err
	}

	// TODO: zero data bytes, encrypt priv key on disk

	return priv, nil
}

func (t *TatankaNode) setupStreamHandlers() {
	t.setStreamHandler(protocols.PostBondsProtocol, t.handlePostBonds, requireNoPermission)
	t.setStreamHandler(forwardRelayProtocol, t.handleForwardRelay, t.isWhitelistPeer)
	t.setStreamHandler(protocols.ClientSubscribeProtocol, t.handleClientSubscribe, t.requireBonds)
	t.setStreamHandler(protocols.ClientPublishProtocol, t.handleClientPublish, t.requireBonds)
	t.setStreamHandler(protocols.ClientPushProtocol, t.handleClientPush, t.requireBonds)
	t.setStreamHandler(protocols.ClientRelayMessageProtocol, t.handleClientRelayMessage, t.requireBonds)
	t.setStreamHandler(protocols.AvailableMeshNodesProtocol, t.handleAvailableMeshNodes, t.requireBonds)
	t.setStreamHandler(discoveryProtocol, t.handleDiscovery, t.isWhitelistPeer)
	t.setStreamHandler(whitelistProtocol, t.handleWhitelist, t.isWhitelistPeer)
	t.setStreamHandler(quotaHandshakeProtocol, t.handleQuotaHandshake, t.isWhitelistPeer)
}

func (t *TatankaNode) setupObservability() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	t.metricsServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", t.config.MetricsPort),
		Handler: mux,
	}
}
