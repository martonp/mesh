package tatanka

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/tatanka/admin"
	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	// connectToPeerTimeout is the timeout for attempting to connect to a peer.
	connectToPeerTimeout = 10 * time.Second
	// baseRetryDelay is the initial backoff duration after a failed connection.
	baseRetryDelay = 2 * time.Second
	// maxRetryDelay is the maximum backoff duration.
	maxRetryDelay = time.Minute
	// discoveryAttemptFrequency controls how often we attempt peer discovery.
	// Discovery is attempted every N consecutive dial failures.
	discoveryAttemptFrequency = 5
)

var (
	errWhitelistMismatch = errors.New("whitelist mismatch")
	errDiscoveryNotFound = errors.New("discovery not found")
)

// retryState tracks exponential backoff state for connection retries.
type retryState struct {
	failures int
	backoff  time.Duration
}

// onSuccess resets the retry state after a successful connection.
func (r *retryState) onSuccess(base time.Duration) {
	r.failures = 0
	r.backoff = base
}

// onFailure increments the failure count and doubles the backoff (up to max).
func (r *retryState) onFailure() {
	r.failures++
	r.backoff *= 2
	if r.backoff > maxRetryDelay {
		r.backoff = maxRetryDelay
	}
}

// peerTracker manages the connection lifecycle of a single peer.
type peerTracker struct {
	peerID peer.ID
	m      *meshConnectionManager
	ctx    context.Context
	cancel context.CancelFunc
	// signalReconnect is used to signal the tracker to attempt to reconnect
	// immediately.
	signalReconnect chan struct{}
	// whitelistMismatch is set when the most recent connection attempt failed
	// due to whitelist mismatch.
	whitelistMismatch atomic.Bool
	// initialCh is closed after the first connection attempt.
	initialCh   chan struct{}
	initialOnce sync.Once
}

func (t *peerTracker) markInitial() {
	t.initialOnce.Do(func() {
		close(t.initialCh)
	})
}

// run starts the main event loop for a single peer connection.
func (t *peerTracker) run() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	retry := &retryState{backoff: t.m.retryBaseDelay}

	resetTimerWithJitter := func(duration time.Duration) {
		jitter := time.Duration(rand.Intn(int(duration / 10)))
		timer.Reset(duration + jitter)
	}

	success := func() {
		t.m.peerStateUpdated(t.m.getPeerInfo(t.peerID))
		t.markInitial()
		retry.onSuccess(t.m.retryBaseDelay)
		resetTimerWithJitter(maxRetryDelay)
	}

	forceReconnect := func() {
		retry.onSuccess(t.m.retryBaseDelay)
		timer.Reset(0)
	}

	failure := func() {
		t.m.peerStateUpdated(t.m.getPeerInfo(t.peerID))
		t.markInitial()
		resetTimerWithJitter(retry.backoff)
		retry.onFailure()
	}

	// shouldAttemptDiscovery determines if we should query other peers for
	// addresses of our target peer on this pass.
	//
	// - If whitelist mismatch occurred, discovery won't help (peer has wrong config).
	// - If we have addresses: try them first, only discover every N failures
	//   (addresses might just be temporarily unreachable).
	// - If we have no addresses: discover immediately on first attempt (pass 0),
	//   then every N failures.
	shouldAttemptDiscovery := func() bool {
		if t.whitelistMismatch.Load() {
			return false
		}

		hasAddrs := len(t.m.node.Peerstore().Addrs(t.peerID)) > 0

		if hasAddrs {
			return retry.failures != 0 && retry.failures%discoveryAttemptFrequency == 0
		}

		return retry.failures%discoveryAttemptFrequency == 0
	}

	for {
		select {
		case <-t.ctx.Done():
			return

		case <-t.signalReconnect:
			// Do not force a reconnect if we just disconnected due to a whitelist mismatch.
			if t.whitelistMismatch.Load() && t.m.node.Network().Connectedness(t.peerID) != network.Connected {
				continue
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			forceReconnect()

		case <-timer.C:
			if !t.whitelistMismatch.Load() && t.m.node.Network().Connectedness(t.peerID) == network.Connected {
				success()
				continue
			}

			if shouldAttemptDiscovery() {
				if !t.discoverAddresses() {
					failure()
					continue
				}
			}

			if len(t.m.node.Peerstore().Addrs(t.peerID)) == 0 {
				failure()
				continue
			}

			err := t.connect()
			if err == nil {
				t.m.log.Debugf("Connected to peer %s", t.peerID)
				success()
			} else {
				failure()
				t.m.log.Warnf("Failed to connect to peer %s (Attempt %d): %v", t.peerID, retry.failures, err)
			}
		}
	}
}

// connect dials the peer and performs the whitelist handshake.
func (t *peerTracker) connect() error {
	dialCtx, dialCancel := context.WithTimeout(t.ctx, connectToPeerTimeout)
	defer dialCancel()

	if err := t.m.node.Connect(dialCtx, peer.AddrInfo{ID: t.peerID}); err != nil {
		return err
	}

	err := t.verifyWhitelist()
	if err != nil {
		if errors.Is(err, errWhitelistMismatch) {
			t.whitelistMismatch.Store(true)
		}
		return fmt.Errorf("failed to verify whitelist for peer %s: %w", t.peerID, err)
	}

	// Whitelist verified — clear any prior mismatch.
	t.whitelistMismatch.Store(false)

	go t.exchangeOracleQuotas()

	return nil
}

// exchangeOracleQuotas sends local quota information to the peer and receives theirs.
func (t *peerTracker) exchangeOracleQuotas() {
	ctx, cancel := context.WithTimeout(t.ctx, 10*time.Second)
	defer cancel()

	stream, err := t.m.node.NewStream(ctx, t.peerID, quotaHandshakeProtocol)
	if err != nil {
		t.m.log.Debugf("Quota handshake stream to %s failed: %v", t.peerID, err)
		return
	}
	defer func() { _ = stream.Close() }()

	localQuotas := t.m.getLocalQuotas()
	req := &pb.QuotaHandshake{Quotas: localQuotas}
	if err := codec.WriteLengthPrefixedMessage(stream, req); err != nil {
		t.m.log.Debugf("Failed to send quota handshake to %s: %v", t.peerID, err)
		_ = stream.Reset()
		return
	}

	resp := &pb.QuotaHandshake{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		t.m.log.Debugf("Failed to read quota handshake from %s: %v", t.peerID, err)
		_ = stream.Reset()
		return
	}

	t.m.handlePeerQuotas(t.peerID, resp.Quotas)
}

// discoverAddresses asks connected whitelist peers for the address of the target.
func (t *peerTracker) discoverAddresses() bool {
	whitelist := t.m.getLocalWhitelistState().Current

	var wg sync.WaitGroup
	var success atomic.Bool

	for pid := range whitelist.PeerIDs {
		if pid == t.m.node.ID() || pid == t.peerID {
			continue
		}
		if t.m.node.Network().Connectedness(pid) != network.Connected {
			continue
		}

		wg.Add(1)
		go func(helper peer.ID) {
			defer wg.Done()
			addrs, err := t.sendDiscoveryRequest(helper, t.peerID)
			if err == nil && len(addrs) > 0 {
				t.m.log.Debugf("Discovered address for %s via %s", t.peerID, helper)
				t.m.node.Peerstore().AddAddrs(t.peerID, addrs, peerstore.PermanentAddrTTL)
				success.Store(true)
			} else if !errors.Is(err, errDiscoveryNotFound) {
				t.m.log.Warnf("Failed to discover addresses for %s via %s: %v", t.peerID, helper, err)
			}
		}(pid)
	}

	wg.Wait()

	return success.Load()
}

// verifyWhitelist performs a whitelist handshake with the peer. Both sides
// exchange a WhitelistState, check for a match, and protect or close the connection.
func (t *peerTracker) verifyWhitelist() error {
	ctx, cancel := context.WithTimeout(t.ctx, connectToPeerTimeout)
	defer cancel()

	stream, err := t.m.node.NewStream(ctx, t.peerID, whitelistProtocol)
	if err != nil {
		_ = t.m.node.Network().ClosePeer(t.peerID)
		return err
	}
	defer func() { _ = stream.Close() }()

	// 1. Send our state.
	state := t.m.getLocalWhitelistState()
	currentWl := state.Current
	proposedWl := state.Proposed
	ownState := &pb.WhitelistState{
		PeerIDs:   currentWl.PeerIDsBytes(),
		Timestamp: time.Now().UnixNano(),
		Ready:     state.Ready,
	}
	if proposedWl != nil {
		ownState.ProposedPeerIDs = proposedWl.PeerIDsBytes()
	}
	if err := codec.WriteLengthPrefixedMessage(stream, ownState); err != nil {
		_ = stream.Reset()
		_ = t.m.node.Network().ClosePeer(t.peerID)
		return err
	}

	// 2. Read peer's state.
	peerState := &pb.WhitelistState{}
	if err := codec.ReadLengthPrefixedMessage(stream, peerState); err != nil {
		_ = stream.Reset()
		_ = t.m.node.Network().ClosePeer(t.peerID)
		return err
	}

	// 3. An empty PeerIDs means the peer rejected our handshake (the
	// permission decorator sent a generic Response, not a WhitelistState).
	if len(peerState.PeerIDs) == 0 {
		t.m.updatePeerWhitelistState(t.peerID, nil, 0)
		_ = t.m.node.Network().ClosePeer(t.peerID)
		return fmt.Errorf("%w: peer %s rejected handshake", errWhitelistMismatch, t.peerID)
	}

	// 4. Record peer's whitelist info regardless of match.
	ws := pbToWhitelistState(peerState)
	t.m.updatePeerWhitelistState(t.peerID, ws, peerState.Timestamp)

	// 5. Check match.
	matched := flexibleWhitelistMatch(currentWl, proposedWl, peerState.PeerIDs, peerState.ProposedPeerIDs)

	if matched {
		t.m.node.ConnManager().Protect(t.peerID, "tatanka-node")
		return nil
	}

	_ = t.m.node.Network().ClosePeer(t.peerID)
	return fmt.Errorf("%w for peer %s", errWhitelistMismatch, t.peerID)
}

// meshConnectionManager manages the connections to the whitelist peers.
type meshConnectionManager struct {
	log            slog.Logger
	node           host.Host
	ctx            context.Context
	cancelCtx      context.CancelFunc
	retryBaseDelay time.Duration

	trackersMtx  sync.RWMutex
	peerTrackers map[peer.ID]*peerTracker

	initialCh   chan struct{}
	initialOnce sync.Once

	peerStateUpdated func(admin.PeerInfo)

	// Whitelist handshake callbacks
	getLocalWhitelistState   func() *types.WhitelistState
	updatePeerWhitelistState func(peerID peer.ID, ws *types.WhitelistState, timestamp int64) bool
	getPeerWhitelistState    func(peerID peer.ID) *types.WhitelistState

	// Oracle quota handshake callbacks
	getLocalQuotas   func() map[string]*pb.QuotaStatus
	handlePeerQuotas func(peerID peer.ID, quotas map[string]*pb.QuotaStatus)
}

// meshConnectionManagerConfig holds configuration for creating a meshConnectionManager.
type meshConnectionManagerConfig struct {
	log                      slog.Logger
	node                     host.Host
	peerStateUpdated         func(admin.PeerInfo)
	getLocalQuotas           func() map[string]*pb.QuotaStatus
	handlePeerQuotas         func(peerID peer.ID, quotas map[string]*pb.QuotaStatus)
	getLocalWhitelistState   func() *types.WhitelistState
	updatePeerWhitelistState func(peerID peer.ID, ws *types.WhitelistState, timestamp int64) bool
	getPeerWhitelistState    func(peerID peer.ID) *types.WhitelistState
	// retryBaseDelay overrides the default baseRetryDelay for testing.
	// When zero, baseRetryDelay is used.
	retryBaseDelay time.Duration
}

func newMeshConnectionManager(cfg *meshConnectionManagerConfig) *meshConnectionManager {
	retryDelay := cfg.retryBaseDelay
	if retryDelay == 0 {
		retryDelay = baseRetryDelay
	}
	m := &meshConnectionManager{
		log:                      cfg.log,
		node:                     cfg.node,
		retryBaseDelay:           retryDelay,
		peerTrackers:             make(map[peer.ID]*peerTracker),
		initialCh:                make(chan struct{}),
		peerStateUpdated:         cfg.peerStateUpdated,
		getLocalQuotas:           cfg.getLocalQuotas,
		handlePeerQuotas:         cfg.handlePeerQuotas,
		getLocalWhitelistState:   cfg.getLocalWhitelistState,
		updatePeerWhitelistState: cfg.updatePeerWhitelistState,
		getPeerWhitelistState:    cfg.getPeerWhitelistState,
	}

	m.ctx, m.cancelCtx = context.WithCancel(context.Background())

	// Register for network events to react instantly to connects and disconnects.
	cfg.node.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(_ network.Network, conn network.Conn) {
			m.triggerReconnect(conn.RemotePeer())
		},
		DisconnectedF: func(_ network.Network, conn network.Conn) {
			m.triggerReconnect(conn.RemotePeer())
		},
	})

	return m
}

// triggerReconnect signals the specific peer tracker to wake up and retry immediately.
func (m *meshConnectionManager) triggerReconnect(pid peer.ID) {
	m.trackersMtx.RLock()
	defer m.trackersMtx.RUnlock()
	if t, ok := m.peerTrackers[pid]; ok {
		select {
		case t.signalReconnect <- struct{}{}:
		default:
		}
	}
}

// triggerReconnectAll triggers a reconnect for all trackers.
func (m *meshConnectionManager) triggerReconnectAll() {
	m.trackersMtx.RLock()
	defer m.trackersMtx.RUnlock()
	for _, t := range m.peerTrackers {
		select {
		case t.signalReconnect <- struct{}{}:
		default:
		}
	}
}

// startTracker initializes and runs a new tracker loop for a peer.
func (m *meshConnectionManager) startTracker(ctx context.Context, pid peer.ID) *peerTracker {
	m.trackersMtx.Lock()
	defer m.trackersMtx.Unlock()

	if tracker, exists := m.peerTrackers[pid]; exists {
		return tracker
	}

	ctx, cancel := context.WithCancel(ctx)
	t := &peerTracker{
		peerID:          pid,
		m:               m,
		ctx:             ctx,
		cancel:          cancel,
		signalReconnect: make(chan struct{}, 1),
		initialCh:       make(chan struct{}),
	}
	m.peerTrackers[pid] = t
	go t.run()

	return t
}

// stopTracker cancels and removes a tracker (used when whitelist changes).
func (m *meshConnectionManager) stopTracker(pid peer.ID) {
	m.trackersMtx.Lock()
	defer m.trackersMtx.Unlock()

	if t, exists := m.peerTrackers[pid]; exists {
		t.cancel()
		// If we stop tracking a peer (e.g. whitelist update), make sure the connection
		// isn't artificially kept alive.
		m.node.ConnManager().Unprotect(pid, "tatanka-node")
		delete(m.peerTrackers, pid)
	}
}

// sendDiscoveryRequest sends a discovery request to reqPeerID for the addresses of targetPeerID.
func (t *peerTracker) sendDiscoveryRequest(reqPeerID, targetPeerID peer.ID) ([]ma.Multiaddr, error) {
	ctx, cancel := context.WithTimeout(t.ctx, connectToPeerTimeout)
	defer cancel()

	s, err := t.m.node.NewStream(ctx, reqPeerID, discoveryProtocol)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()

	request := &pb.DiscoveryRequest{Id: []byte(targetPeerID)}
	if err := codec.WriteLengthPrefixedMessage(s, request); err != nil {
		_ = s.Reset()
		return nil, err
	}

	response := &pb.DiscoveryResponse{}
	if err := codec.ReadLengthPrefixedMessage(s, response); err != nil {
		_ = s.Reset()
		return nil, err
	}

	if response.GetSuccess() != nil {
		addrs := make([]ma.Multiaddr, 0, len(response.GetSuccess().GetAddrs()))
		for _, addr := range response.GetSuccess().GetAddrs() {
			addr, err := ma.NewMultiaddrBytes(addr)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, addr)
		}
		return addrs, nil
	}

	return nil, errDiscoveryNotFound
}

// reconcileTrackers should be called when the whitelist changes. It stops
// trackers for peers not in the new whitelist and starts trackers for peers
// in the new whitelist that aren't already tracked.
func (m *meshConnectionManager) reconcileTrackers() {
	whitelistState := m.getLocalWhitelistState()
	currentPeers := whitelistState.Current.PeerIDs

	// Stop trackers for peers not in the new whitelist.
	m.trackersMtx.RLock()
	var toStop []peer.ID
	for pid := range m.peerTrackers {
		if _, ok := currentPeers[pid]; !ok {
			toStop = append(toStop, pid)
		}
	}
	m.trackersMtx.RUnlock()

	for _, pid := range toStop {
		m.stopTracker(pid)
		_ = m.node.Network().ClosePeer(pid)
	}

	// Start trackers for new peers.
	m.trackersMtx.RLock()
	var toStart []peer.ID
	for pid := range currentPeers {
		if pid == m.node.ID() {
			continue
		}
		if _, ok := m.peerTrackers[pid]; ok {
			continue // already tracked
		}
		toStart = append(toStart, pid)
	}
	m.trackersMtx.RUnlock()

	for _, pid := range toStart {
		m.startTracker(m.ctx, pid)
	}

	// Clear stale mismatch flags on existing trackers and signal them to
	// reconnect immediately. After a whitelist change, prior mismatches may
	// no longer apply.
	m.trackersMtx.RLock()
	for _, t := range m.peerTrackers {
		t.whitelistMismatch.Store(false)
	}
	m.trackersMtx.RUnlock()
	m.triggerReconnectAll()
}

// getPeerInfo builds an admin.PeerInfo for a single peer.
func (m *meshConnectionManager) getPeerInfo(pid peer.ID) admin.PeerInfo {
	state := admin.StateDisconnected

	m.trackersMtx.RLock()
	t := m.peerTrackers[pid]
	m.trackersMtx.RUnlock()

	if t != nil && t.whitelistMismatch.Load() {
		state = admin.StateWhitelistMismatch
	} else if m.node.Network().Connectedness(pid) == network.Connected {
		state = admin.StateConnected
	}

	addrs := m.node.Peerstore().Addrs(pid)
	addrStrs := make([]string, len(addrs))
	for i, addr := range addrs {
		addrStrs[i] = addr.String()
	}

	return admin.PeerInfo{
		PeerID:         pid.String(),
		State:          state,
		Addresses:      addrStrs,
		WhitelistState: m.getPeerWhitelistState(pid),
	}
}

// peerInfoSnapshot returns a snapshot of all tracked peers' connection info.
func (m *meshConnectionManager) peerInfoSnapshot() map[peer.ID]admin.PeerInfo {
	m.trackersMtx.RLock()
	pids := make([]peer.ID, 0, len(m.peerTrackers))
	for pid := range m.peerTrackers {
		pids = append(pids, pid)
	}
	m.trackersMtx.RUnlock()

	result := make(map[peer.ID]admin.PeerInfo, len(pids))
	for _, pid := range pids {
		result[pid] = m.getPeerInfo(pid)
	}
	return result
}

// waitInitial blocks until the initial connectivity pass is marked complete.
func (m *meshConnectionManager) waitInitial(ctx context.Context) {
	select {
	case <-m.initialCh:
	case <-ctx.Done():
	}
}

// markInitial marks the initial connectivity pass as complete.
func (m *meshConnectionManager) markInitial() {
	m.initialOnce.Do(func() {
		close(m.initialCh)
	})
}

// run starts the connection manager.
func (m *meshConnectionManager) run(ctx context.Context) {
	// Link the lifecycle of our internal context to the run context
	go func() {
		<-ctx.Done()
		m.cancelCtx()
	}()

	whitelist := m.getLocalWhitelistState().Current
	allTrackers := make(map[peer.ID]*peerTracker)

	waitForAllTrackers := func() {
		for _, t := range allTrackers {
			select {
			case <-t.initialCh:
			case <-ctx.Done():
				return
			}
		}
	}

	// PASS 1: Start trackers for peers with addresses. Wait for
	// them all to finish their initial connection loop before
	// starting trackers for peers without addresses.
	for pid := range whitelist.PeerIDs {
		if pid == m.node.ID() {
			continue
		}
		hasAddrs := len(m.node.Peerstore().Addrs(pid)) > 0
		if hasAddrs {
			allTrackers[pid] = m.startTracker(ctx, pid)
		}
	}

	waitForAllTrackers()

	// PASS 2: Start peers WITHOUT addresses as well.
	for pid := range whitelist.PeerIDs {
		if pid == m.node.ID() {
			continue
		}
		// no-op if already started.
		t := m.startTracker(ctx, pid)
		allTrackers[pid] = t
	}

	waitForAllTrackers()

	m.markInitial()

	recoveryCh := notifyOnConnectivityRecovery(ctx)
	for {
		select {
		case <-ctx.Done():
			// Cleanup
			m.trackersMtx.Lock()
			for _, t := range m.peerTrackers {
				t.cancel()
			}
			m.trackersMtx.Unlock()
			return
		case <-recoveryCh:
			m.log.Infof("Internet connectivity recovered...")
			m.triggerReconnectAll()
		}
	}
}

// notifyOnConnectivityRecovery returns a channel that signals when the system
// transitions from "Offline" to "Online" using a TCP handshake check.
func notifyOnConnectivityRecovery(ctx context.Context) <-chan struct{} {
	notifyCh := make(chan struct{}, 1)

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		wasOnline := checkConnectivity()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				isOnline := checkConnectivity()
				if !wasOnline && isOnline {
					select {
					case notifyCh <- struct{}{}:
					default:
					}
				}
				wasOnline = isOnline
			}
		}
	}()

	return notifyCh
}

// checkConnectivity attempts to open a TCP connection to a public DNS.
// We use TCP to ensure a full round-trip handshake occurs.
func checkConnectivity() bool {
	// Try Cloudflare
	if isReachable("1.1.1.1:53") {
		return true
	}
	// Fallback to Google
	if isReachable("8.8.8.8:53") {
		return true
	}
	return false
}

func isReachable(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		conn.Close()
		return true
	}
	return false
}
