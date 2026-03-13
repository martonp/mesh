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

	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/subtrie"
	"github.com/bisoncraft/mesh/tatanka/admin"
	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/bisoncraft/mesh/tatanka/types"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// privateKeyFileName is the name of the file that contains the private key
	// for the tatanka node.
	privateKeyFileName = "p.key"

	// whitelistFileName is the name of the file that contains the whitelist for
	// the tatanka node.
	whitelistFileName = "whitelist.json"

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
	DataDir        string
	Logger         slog.Logger
	ListenPort     int
	MetricsPort    int
	AdminPort      int
	BootstrapAddrs []string
	WhitelistPeers []peer.ID
	// ForceWhitelist overwrites any existing whitelist on disk with the provided
	// whitelist when WhitelistPeers is non-empty.
	ForceWhitelist bool

	// Oracle Configuration
	CMCKey           string
	TatumKey         string
	BlockcypherToken string

	// NATMapping uses UPnP to discover the public IP and map the listen port
	// automatically. For nodes behind a consumer router.
	NATMapping bool
	// PublicIP is the public IP address to advertise when port forwarding is
	// configured manually.
	PublicIP string
}

// Option is a functional option for configuring TatankaNode.
type Option func(*TatankaNode)

// WithHost sets a pre-created libp2p host (e.g., for testing with mocks).
func WithHost(h host.Host) Option {
	return func(n *TatankaNode) {
		n.node = h
		n.nodeID = h.ID()
	}
}

// oracleService defines the requirements for implementing an oracle.
type oracleService interface {
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
	config        *Config
	node          host.Host
	nodeID        peer.ID
	log           slog.Logger
	initWhitelist *types.Whitelist
	readyCh       chan struct{}
	readyOnce     sync.Once
	readyErr      atomic.Value // error
	privateKey    crypto.PrivKey

	bondVerifier            *bondVerifier
	bondStorage             bondStorage
	peerstoreCache          *peerstoreCache
	gossipSub               *gossipSub
	clientConnectionManager *clientConnectionManager
	subTrie                 *subtrie.SubTrie
	pushStreamManager       *pushStreamManager
	whitelistManager        *whitelistManager
	connectionManager       *meshConnectionManager
	adminServer             *admin.Server
	adminNotify             adminNotifier
	metricsServer           *http.Server
	oracle                  oracleService
	natMapper               *natMapper
}

func initTatankaNode(dataDir string) (crypto.PrivKey, error) {
	if dataDir == "" {
		return nil, errors.New("no data directory provided")
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create data directory %q: %w", dataDir, err)
	}
	priv, err := getOrCreatePrivateKey(filepath.Join(dataDir, privateKeyFileName))
	if err != nil {
		return nil, err
	}
	return priv, nil
}

// InitTatankaNode ensures the data directory exists, generates (or loads) the
// node private key, and returns the corresponding peer ID.
func InitTatankaNode(dataDir string) (peer.ID, error) {
	priv, err := initTatankaNode(dataDir)
	if err != nil {
		return "", err
	}

	return peer.IDFromPrivateKey(priv)
}

// NewTatankaNode creates a new TatankaNode with the given configuration and options.
func NewTatankaNode(config *Config, opts ...Option) (*TatankaNode, error) {
	privateKey, err := initTatankaNode(config.DataDir)
	if err != nil {
		return nil, err
	}

	t := &TatankaNode{
		config:                  config,
		log:                     config.Logger,
		privateKey:              privateKey,
		clientConnectionManager: newClientConnectionManager(config.Logger),
		subTrie:                 subtrie.New(),
		bondVerifier:            newBondVerifier(),
		bondStorage:             newMemoryBondStorage(time.Now),
		readyCh:                 make(chan struct{}),
	}

	for _, opt := range opts {
		opt(t)
	}

	// Derive local peer ID from the host if one was injected (e.g. tests),
	// otherwise from the generated private key.
	var localPeerID peer.ID
	if t.node != nil {
		localPeerID = t.nodeID
	} else {
		localPeerID, err = peer.IDFromPrivateKey(privateKey)
		if err != nil {
			return nil, err
		}
	}

	t.initWhitelist, err = initWhitelist(config.DataDir, config.WhitelistPeers, config.ForceWhitelist, localPeerID)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// decodePeerIDStrings decodes a slice of peer ID strings into peer.ID values.
func decodePeerIDStrings(peers []string) ([]peer.ID, error) {
	peerIDs := make([]peer.ID, 0, len(peers))
	for _, p := range peers {
		pid, err := peer.Decode(p)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID %q: %w", p, err)
		}
		peerIDs = append(peerIDs, pid)
	}
	return peerIDs, nil
}

func (t *TatankaNode) handleBroadcastMessage(msg *protocolsPb.PushMessage) {
	peers := t.subTrie.PeersForTopic(msg.Topic)
	if len(peers) > 0 {
		t.pushStreamManager.distribute(peers, msg)
	}
}

func (t *TatankaNode) handleClientConnectionMessage(update *clientConnectionUpdate) {
	t.clientConnectionManager.updateClientConnectionInfo(update)
}

// peerStates computes the current admin state on demand by querying the
// connection manager and whitelist manager.
func (t *TatankaNode) peerStates() admin.AdminState {
	state := admin.AdminState{
		OurPeerID:      t.nodeID.String(),
		WhitelistState: t.whitelistManager.getLocalWhitelistState(),
		Peers:          make(map[string]admin.PeerInfo),
	}
	for pid, pi := range t.connectionManager.peerInfoSnapshot() {
		state.Peers[pid.String()] = pi
	}
	return state
}

// Run starts the tatanka node and blocks until the context is done.
func (t *TatankaNode) Run(ctx context.Context) error {
	t.adminNotify = noopAdminNotifier{}

	if err := t.initHost(); err != nil {
		t.markReady(err)
		return err
	}
	if err := t.initMessaging(ctx); err != nil {
		t.markReady(err)
		return err
	}
	if err := t.initOracle(); err != nil {
		t.markReady(err)
		return err
	}
	if err := t.initAdmin(ctx); err != nil {
		t.markReady(err)
		return err
	}
	if err := t.initConnectivity(); err != nil {
		t.markReady(err)
		return err
	}

	t.setupStreamHandlers()
	t.setupObservability()

	return t.serve(ctx)
}

// initHost creates the libp2p host if not already injected (e.g. via WithHost).
func (t *TatankaNode) initHost() error {
	if t.node != nil {
		return nil
	}

	var listenIP string
	if t.config.NATMapping || t.config.PublicIP != "" {
		listenIP = "0.0.0.0"
	} else {
		listenIP = "127.0.0.1"
		t.log.Infof("No NAT mapping or public IP configured; running in local dev mode")
	}
	listenAddr := fmt.Sprintf("/ip4/%s/tcp/%d", listenIP, t.config.ListenPort)

	opts := []libp2p.Option{
		libp2p.Identity(t.privateKey),
		libp2p.ListenAddrStrings(listenAddr),
		libp2p.EnableRelay(),
	}

	if t.config.NATMapping {
		nm, err := newNATMapper(t.log, t.config.ListenPort)
		if err != nil {
			return fmt.Errorf("failed to create NAT mapper: %w", err)
		}
		t.natMapper = nm

		opts = append(opts, libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			pubAddr := t.natMapper.publicAddr()
			if pubAddr == nil {
				return addrs
			}
			out := []ma.Multiaddr{pubAddr}
			for _, a := range addrs {
				if a.String() != pubAddr.String() {
					out = append(out, a)
				}
			}
			return out
		}))

		opts = append(opts, libp2p.EnableRelayService())
	} else if t.config.PublicIP != "" {
		pubAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", t.config.PublicIP, t.config.ListenPort))
		if err != nil {
			return fmt.Errorf("invalid public address: %w", err)
		}

		opts = append(opts, libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			out := []ma.Multiaddr{pubAddr}
			for _, a := range addrs {
				if a.String() != pubAddr.String() {
					out = append(out, a)
				}
			}
			return out
		}))

		opts = append(opts, libp2p.EnableRelayService())
	}

	var err error
	t.node, err = libp2p.New(opts...)
	if err != nil {
		return err
	}
	t.nodeID = t.node.ID()
	return nil
}

// initMessaging creates the gossipSub and pushStreamManager.
func (t *TatankaNode) initMessaging(ctx context.Context) error {
	t.log.Infof("Node ID: %s", t.nodeID.String())

	listenAddrs := t.node.Network().ListenAddresses()
	t.log.Infof("Listening on: ")
	for _, addr := range listenAddrs {
		t.log.Infof("  %s", addr.String())
	}

	var err error
	t.gossipSub, err = newGossipSub(ctx, &gossipSubCfg{
		node: t.node,
		log:  t.config.Logger,
		getWhitelistPeers: func() map[peer.ID]struct{} {
			return t.whitelistManager.getWhitelist().PeerIDs
		},
		handleBroadcastMessage:        t.handleBroadcastMessage,
		handleClientConnectionMessage: t.handleClientConnectionMessage,
		handleOracleUpdate:            t.handleOracleUpdate,
		handleQuotaHeartbeat:          t.handleQuotaHeartbeat,
		// t.whitelistManager and t.connectionManager are set in initConnectivity,
		// after gossipSub is created. The closure is safe because it is only
		// called during gossipSub.run, which starts in serve() after all init
		// phases.
		handleWhitelistUpdate: t.handleWhitelistUpdate,
	})
	if err != nil {
		return err
	}

	t.pushStreamManager = newPushStreamManager(t.config.Logger, func(client peer.ID, timestamp time.Time, connected bool) {
		err := t.gossipSub.publishClientConnectionMessage(ctx, &clientConnectionUpdate{
			clientID:   client,
			reporterID: t.nodeID,
			timestamp:  timestamp.UnixMilli(),
			connected:  connected,
		})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			t.log.Errorf("Publishing client connection message failed: %v", err)
		}
	})

	return nil
}

// initOracle creates the oracle if not already injected (e.g. via test setup).
func (t *TatankaNode) initOracle() error {
	if t.oracle != nil {
		return nil
	}

	var err error
	t.oracle, err = oracle.New(&oracle.Config{
		Log:              t.config.Logger,
		CMCKey:           t.config.CMCKey,
		TatumKey:         t.config.TatumKey,
		BlockcypherToken: t.config.BlockcypherToken,
		NodeID:           t.nodeID.String(),
		PublishUpdate:    t.gossipSub.publishOracleUpdate,
		OnStateUpdate: func(update *oracle.OracleSnapshot) {
			t.adminNotify.BroadcastOracleUpdate("oracle_update", update)
		},
		PublishQuotaHeartbeat: t.gossipSub.publishQuotaHeartbeat,
	})
	if err != nil {
		return fmt.Errorf("failed to create oracle: %v", err)
	}

	return nil
}

// initAdmin creates the admin server when configured (AdminPort > 0) and
// upgrades adminNotify from the noop to the live implementation.
func (t *TatankaNode) initAdmin(ctx context.Context) error {
	if t.config.AdminPort <= 0 {
		return nil
	}

	adminAddr := fmt.Sprintf("127.0.0.1:%d", t.config.AdminPort)
	server := admin.NewServer(&admin.Config{
		Log:    t.config.Logger,
		Addr:   adminAddr,
		PeerID: t.nodeID.String(),
		Oracle: t.oracle,
		GetState: func() admin.AdminState {
			return t.peerStates()
		},
		ProposeWhitelist: func(peers []string) error {
			peerIDs, err := decodePeerIDStrings(peers)
			if err != nil {
				return err
			}
			return t.whitelistManager.proposeWhitelist(types.NewWhitelist(peerIDs))
		},
		ClearProposal: func() {
			t.whitelistManager.clearProposal()
		},
		ForceWhitelist: func(peers []string) error {
			peerIDs, err := decodePeerIDStrings(peers)
			if err != nil {
				return err
			}
			return t.whitelistManager.forceWhitelist(types.NewWhitelist(peerIDs))
		},
	})

	t.adminServer = server
	t.adminNotify = &liveAdminNotifier{server: server}

	return nil
}

// initConnectivity creates the peerstore cache and mesh connection manager.
func (t *TatankaNode) initConnectivity() error {
	t.peerstoreCache = newPeerstoreCache(
		t.log,
		filepath.Join(t.config.DataDir, "peerstore.json"),
		t.node,
		func() map[peer.ID]struct{} {
			return t.whitelistManager.getWhitelist().PeerIDs
		},
	)
	t.peerstoreCache.load()

	if len(t.config.BootstrapAddrs) > 0 {
		if err := seedBootstrapAddrs(t.node, t.config.BootstrapAddrs); err != nil {
			return fmt.Errorf("failed to seed bootstrap addresses: %w", err)
		}
	}

	whitelistPath := filepath.Join(t.config.DataDir, whitelistFileName)
	t.whitelistManager = newWhitelistManager(&whitelistManagerConfig{
		log:    t.config.Logger,
		peerID: t.nodeID,
		isConnected: func(pid peer.ID) bool {
			return t.node.Network().Connectedness(pid) == network.Connected
		},
		whitelist: t.initWhitelist,
		whitelistUpdated: func(newWl *types.Whitelist) {
			if err := saveWhitelist(whitelistPath, newWl); err != nil {
				t.log.Errorf("Failed to save whitelist: %v", err)
			}
			t.connectionManager.reconcileTrackers()
			t.adminNotify.BroadcastWhitelistUpdate(admin.WhitelistUpdate{
				WhitelistState: t.whitelistManager.getLocalWhitelistState(),
			})
			t.peerstoreCache.save()
		},
		broadcastLocalState: func(ws *types.WhitelistState) {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			defer cancel()
			if err := t.gossipSub.publishWhitelistUpdate(ctx, whitelistStateToPb(ws)); err != nil {
				t.log.Errorf("Failed to publish whitelist update: %v", err)
			}
			t.adminNotify.BroadcastWhitelistState(ws)
		},
	})

	t.connectionManager = newMeshConnectionManager(&meshConnectionManagerConfig{
		log:  t.config.Logger,
		node: t.node,
		peerStateUpdated: func(pi admin.PeerInfo) {
			t.adminNotify.BroadcastPeerUpdate(pi)
		},
		getPeerWhitelistState: t.whitelistManager.getPeerWhitelistState,
		getLocalQuotas: func() map[string]*pb.QuotaStatus {
			return quotaStatusesToPb(t.oracle.GetLocalQuotas())
		},
		handlePeerQuotas: func(peerID peer.ID, quotas map[string]*pb.QuotaStatus) {
			for source, q := range quotas {
				t.oracle.UpdatePeerSourceQuota(peerID.String(), pbToTimestampedQuotaStatus(q), source)
			}
		},
		getLocalWhitelistState:   t.whitelistManager.getLocalWhitelistState,
		updatePeerWhitelistState: t.whitelistManager.updatePeerWhitelistState,
	})

	return nil
}

// serve launches all long-running goroutines, waits for the initial connectivity
// pass to complete, marks the node as ready, then blocks until context
// cancellation. After all goroutines exit it runs shutdown.
func (t *TatankaNode) serve(ctx context.Context) error {
	var wg sync.WaitGroup

	go func() {
		t.log.Infof("Metrics available on :%d/metrics", t.config.MetricsPort)
		t.log.Infof("Profiler available on :%d/debug/pprof", t.config.MetricsPort)
		if err := t.metricsServer.ListenAndServe(); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				t.log.Errorf("Failed to start metrics server: %v", err)
			}
		}
	}()

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.whitelistManager.run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.connectionManager.run(ctx)
	}()

	// Wait for the initial connectivity pass to finish before reporting ready.
	t.connectionManager.waitInitial(ctx)
	t.markReady(nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.oracle.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.peerstoreCache.run(ctx)
	}()

	if t.natMapper != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.natMapper.run(ctx)
		}()
	}

	wg.Wait()

	return t.shutdown()
}

// shutdown performs graceful teardown of the metrics server and libp2p host.
func (t *TatankaNode) shutdown() error {
	t.log.Infof("Shutting down tatanka node...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := t.metricsServer.Shutdown(shutdownCtx); err != nil {
		return err
	}

	if t.natMapper != nil {
		t.natMapper.close(5 * time.Second)
	}

	if err := t.node.Close(); err != nil {
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

// getOrCreatePrivateKey loads an existing private key from filePath, or
// generates a new Ed25519 key and writes it to filePath if none exists.
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
	t.setStreamHandler(protocols.ClientSubscribeProtocol, t.handleClientSubscribe, requireNoPermission)
	t.setStreamHandler(protocols.ClientUnsubscribeProtocol, t.handleClientUnsubscribe, requireNoPermission)
	t.setStreamHandler(forwardRelayProtocol, t.handleForwardRelay, t.isWhitelistPeer)
	t.setStreamHandler(protocols.ClientPublishProtocol, t.handleClientPublish, t.requireBonds)
	t.setStreamHandler(protocols.ClientPushProtocol, t.handleClientPush, t.requireBonds)
	t.setStreamHandler(protocols.ClientRelayMessageProtocol, t.handleClientRelayMessage, t.requireBonds)
	t.setStreamHandler(protocols.AvailableMeshNodesProtocol, t.handleAvailableMeshNodes, t.requireBonds)
	t.setStreamHandler(protocols.SearchTopicsProtocol, t.handleSearchTopics, t.requireBonds)
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
