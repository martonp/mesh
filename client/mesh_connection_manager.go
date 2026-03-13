package client

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

const (
	meshNodesRefreshInterval = 10 * time.Minute
	baseReconnectDelay       = 2 * time.Second
	maxReconnectDelay        = time.Minute
)

type meshConn interface {
	broadcast(ctx context.Context, topic string, data []byte) error
	subscribe(ctx context.Context, topics []string) error
	unsubscribe(ctx context.Context, topics []string) error
	postAllBonds(ctx context.Context) error
	addBonds(ctx context.Context, bonds []*bond.BondParams) error
	fetchAvailableMeshNodes(ctx context.Context) ([]peer.AddrInfo, error)
	searchTopics(ctx context.Context, filters []string) ([]string, error)
	kill()
	remotePeerID() peer.ID
	run(ctx context.Context) error
	waitReady(ctx context.Context)
}

// meshConnFactory creates new mesh connections.
type meshConnFactory func(peerID peer.ID) meshConn

type meshConnHolder struct {
	mc meshConn
}

// meshConnectionManager manages the lifecycle of mesh connections, including
// connection establishment, failover, and discovery of available mesh nodes.
type meshConnectionManager struct {
	host        host.Host
	log         slog.Logger
	connFactory meshConnFactory

	primaryConn atomic.Pointer[meshConnHolder]

	nodesMtx   sync.RWMutex
	knownNodes []peer.ID
}

// meshConnectionManagerConfig holds the configuration for creating a meshConnectionManager.
type meshConnectionManagerConfig struct {
	host        host.Host
	log         slog.Logger
	connFactory meshConnFactory
}

func newMeshConnectionManager(cfg *meshConnectionManagerConfig) *meshConnectionManager {
	m := &meshConnectionManager{
		host:        cfg.host,
		log:         cfg.log,
		connFactory: cfg.connFactory,
	}

	for _, peerID := range cfg.host.Peerstore().Peers() {
		if cfg.host.ID() == peerID {
			continue
		}
		m.knownNodes = append(m.knownNodes, peerID)
	}

	return m
}

// primaryConnection returns the current primary mesh connection, or an error if none is established.
func (m *meshConnectionManager) primaryConnection() (meshConn, error) {
	holder := m.primaryConn.Load()
	if holder == nil || holder.mc == nil {
		return nil, errNoMeshConnection
	}
	return holder.mc, nil
}

func (m *meshConnectionManager) setPrimaryConnection(mc meshConn) {
	if mc == nil {
		m.primaryConn.Store(nil)
		return
	}
	m.primaryConn.Store(&meshConnHolder{mc: mc})
}

// connectResult holds the result of a connection attempt.
type connectResult struct {
	conn       meshConn
	runErrCh   <-chan error
	connectErr error
}

// connectToNode creates a new mesh connection, starts it, and waits for it
// to become ready. Returns a connectResult with either a ready connection or an error.
func (m *meshConnectionManager) connectToNode(ctx context.Context, peerID peer.ID) connectResult {
	conn := m.connFactory(peerID)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- conn.run(ctx)
	}()

	readyCh := make(chan struct{}, 1)
	go func() {
		conn.waitReady(ctx)
		close(readyCh)
	}()

	select {
	case <-ctx.Done():
		return connectResult{connectErr: ctx.Err()}
	case err := <-runErrCh:
		return connectResult{connectErr: err}
	case <-readyCh:
		return connectResult{conn: conn, runErrCh: runErrCh}
	}
}

// fetchAndStoreNodes fetches the list of available mesh nodes from the
// connected tatanka node and stores their addresses in the peerstore.
func (m *meshConnectionManager) fetchAndStoreNodes(ctx context.Context) {
	primaryConn, err := m.primaryConnection()
	if err != nil {
		return
	}

	peers, err := primaryConn.fetchAvailableMeshNodes(ctx)
	if err != nil {
		m.log.Warnf("Failed to fetch available mesh nodes: %v", err)
		return
	}

	m.nodesMtx.Lock()
	defer m.nodesMtx.Unlock()

	existingPeers := make(map[peer.ID]struct{}, len(m.knownNodes))
	for _, pid := range m.knownNodes {
		existingPeers[pid] = struct{}{}
	}

	for _, p := range peers {
		if p.ID == m.host.ID() {
			continue
		}

		if _, exists := existingPeers[p.ID]; !exists {
			m.knownNodes = append(m.knownNodes, p.ID)
			existingPeers[p.ID] = struct{}{}
		}

		if len(p.Addrs) > 0 {
			m.host.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
		}
	}

	m.log.Debugf("Updated known mesh nodes: %d nodes", len(m.knownNodes))
}

// connectToAvailableNode tries to connect to any available node in the known
// nodes list. It copies the list and tries each node in order, returning true
// and setting the primary connection on the first successful attempt. Returns
// false if no connection could be established.
func (m *meshConnectionManager) connectToAvailableNode(ctx context.Context) (meshConn, <-chan error, bool) {
	m.nodesMtx.RLock()
	nodes := make([]peer.ID, len(m.knownNodes))
	copy(nodes, m.knownNodes)
	m.nodesMtx.RUnlock()

	// Shuffle the nodes list to avoid always connecting to the same node.
	// TODO: use a more efficient algorithm to pick which nodes to connect
	// to first.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	for _, peerID := range nodes {
		if len(m.host.Peerstore().Addrs(peerID)) == 0 {
			continue
		}

		result := m.connectToNode(ctx, peerID)
		if result.connectErr != nil {
			m.log.Errorf("Connection to %s failed: %v", peerID.ShortString(), result.connectErr)
			continue
		}

		m.setPrimaryConnection(result.conn)
		m.log.Infof("Connected to mesh node %s", peerID.ShortString())
		m.fetchAndStoreNodes(ctx)
		return result.conn, result.runErrCh, true
	}

	return nil, nil, false
}

// run maintains the mesh connection. It connects to the initial peer, fetches
// available mesh nodes, and handles failover to other nodes if the connection
// is lost.
func (m *meshConnectionManager) run(ctx context.Context) {
	reconnectTimer := time.NewTimer(0)
	refreshTimer := time.NewTimer(meshNodesRefreshInterval)
	defer reconnectTimer.Stop()
	defer refreshTimer.Stop()

	var runErrCh <-chan error
	backoff := baseReconnectDelay

	attemptConnect := func() {
		conn, errCh, ok := m.connectToAvailableNode(ctx)
		if ok {
			backoff = baseReconnectDelay
			m.setPrimaryConnection(conn)
			runErrCh = errCh
		} else {
			reconnectTimer.Reset(backoff)
			backoff *= 2
			if backoff > maxReconnectDelay {
				backoff = maxReconnectDelay
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-refreshTimer.C:
			m.fetchAndStoreNodes(ctx)
			refreshTimer.Reset(meshNodesRefreshInterval)
		case <-reconnectTimer.C:
			attemptConnect()
		case <-runErrCh:
			m.setPrimaryConnection(nil)
			runErrCh = nil
			attemptConnect()
		}
	}
}
