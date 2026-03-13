package tatanka

import (
	"context"
	"os"
	"sync"
	"testing"
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
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// testMCMWLUpdate records a single updatePeerWhitelistState invocation.
type testMCMWLUpdate struct {
	peerID    peer.ID
	ws        *types.WhitelistState
	timestamp int64
}

// testMCMQuotaUpdate records a single handlePeerQuotas invocation.
type testMCMQuotaUpdate struct {
	peerID peer.ID
	quotas map[string]*pb.QuotaStatus
}

// testMCM is a lightweight harness for unit-testing meshConnectionManager
// without spinning up a full TatankaNode.
type testMCM struct {
	t      *testing.T
	mnet   mocknet.Mocknet
	local  host.Host
	remote host.Host
	mcm    *meshConnectionManager

	// Callback recording channels.
	peerStates   chan admin.PeerInfo
	wlUpdates    chan testMCMWLUpdate
	quotaUpdates chan testMCMQuotaUpdate

	// In-memory peer whitelist state store.
	peerWLMu     sync.Mutex
	peerWLStates map[peer.ID]*types.WhitelistState

	localWLState     *types.WhitelistState
	peerReplyWLState *types.WhitelistState
}

// newTestMCM builds a meshConnectionManager wired to a two-host mocknet.
// If peerReplyWLState is nil the remote host replies with a matching whitelist.
func newTestMCM(t *testing.T, peerReplyWLState *types.WhitelistState) *testMCM {
	t.Helper()

	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatal(err)
	}

	peers := mnet.Peers()
	local := mnet.Host(peers[0])
	remote := mnet.Host(peers[1])

	// LinkPeers creates a virtual link so the hosts can dial each other.
	// AddAddrs seeds the peerstores so the connection manager can find
	// the peer's address. This mirrors linkNodeWithMesh in tatanka_test.go.
	if _, err := mnet.LinkPeers(local.ID(), remote.ID()); err != nil {
		t.Fatal(err)
	}
	local.Peerstore().AddAddrs(remote.ID(), remote.Addrs(), peerstore.PermanentAddrTTL)
	remote.Peerstore().AddAddrs(local.ID(), local.Addrs(), peerstore.PermanentAddrTTL)

	sharedWL := types.NewWhitelist([]peer.ID{local.ID(), remote.ID()})
	localWLState := &types.WhitelistState{Current: sharedWL}

	if peerReplyWLState == nil {
		peerReplyWLState = &types.WhitelistState{Current: sharedWL}
	}

	tm := &testMCM{
		t:                t,
		mnet:             mnet,
		local:            local,
		remote:           remote,
		peerStates:       make(chan admin.PeerInfo, 10),
		wlUpdates:        make(chan testMCMWLUpdate, 10),
		quotaUpdates:     make(chan testMCMQuotaUpdate, 10),
		peerWLStates:     make(map[peer.ID]*types.WhitelistState),
		localWLState:     localWLState,
		peerReplyWLState: peerReplyWLState,
	}

	// Register protocol handlers on the remote host.
	remote.SetStreamHandler(whitelistProtocol, tm.handleWhitelist)
	remote.SetStreamHandler(quotaHandshakeProtocol, tm.handleQuotaHandshake)

	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger("test-mcm")
	log.SetLevel(slog.LevelWarn)

	tm.mcm = newMeshConnectionManager(&meshConnectionManagerConfig{
		log:  log,
		node: local,
		peerStateUpdated: func(pi admin.PeerInfo) {
			tm.peerStates <- pi
		},
		getLocalQuotas: func() map[string]*pb.QuotaStatus {
			return nil
		},
		handlePeerQuotas: func(peerID peer.ID, quotas map[string]*pb.QuotaStatus) {
			tm.quotaUpdates <- testMCMQuotaUpdate{peerID: peerID, quotas: quotas}
		},
		getLocalWhitelistState: func() *types.WhitelistState {
			return tm.localWLState
		},
		updatePeerWhitelistState: func(peerID peer.ID, ws *types.WhitelistState, timestamp int64) bool {
			tm.peerWLMu.Lock()
			tm.peerWLStates[peerID] = ws
			tm.peerWLMu.Unlock()
			tm.wlUpdates <- testMCMWLUpdate{peerID: peerID, ws: ws, timestamp: timestamp}
			return true
		},
		getPeerWhitelistState: func(peerID peer.ID) *types.WhitelistState {
			tm.peerWLMu.Lock()
			defer tm.peerWLMu.Unlock()
			return tm.peerWLStates[peerID]
		},
	})

	return tm
}

// handleWhitelist is the whitelist protocol handler registered on the remote
// host. It reads the initiator's state and replies with peerReplyWLState.
func (tm *testMCM) handleWhitelist(s network.Stream) {
	defer s.Close()

	req := &pb.WhitelistState{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		return
	}

	reply := whitelistStateToPb(tm.peerReplyWLState)
	if err := codec.WriteLengthPrefixedMessage(s, reply); err != nil {
		_ = s.Reset()
		return
	}
}

// handleQuotaHandshake is the quota protocol handler registered on the remote
// host. It reads the request and replies with an empty QuotaHandshake.
func (tm *testMCM) handleQuotaHandshake(s network.Stream) {
	defer s.Close()

	req := &pb.QuotaHandshake{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		return
	}

	reply := &pb.QuotaHandshake{}
	if err := codec.WriteLengthPrefixedMessage(s, reply); err != nil {
		_ = s.Reset()
		return
	}
}

// waitForPeerState drains the peerStates channel until an update with the
// expected state is received or the timeout elapses.
func waitForPeerState(t *testing.T, ch <-chan admin.PeerInfo, state admin.NodeConnectionState, timeout time.Duration) admin.PeerInfo {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case pi := <-ch:
			if pi.State == state {
				return pi
			}
		case <-deadline:
			t.Fatalf("timeout waiting for peer state %s", state)
			return admin.PeerInfo{}
		}
	}
}

func TestMeshConnectionManager_PeerConnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tm := newTestMCM(t, nil)
	go tm.mcm.run(ctx)

	// Wait for connected state.
	pi := waitForPeerState(t, tm.peerStates, admin.StateConnected, 10*time.Second)
	if pi.PeerID != tm.remote.ID().String() {
		t.Fatalf("connected peer: got %s, want %s", pi.PeerID, tm.remote.ID())
	}

	// updatePeerWhitelistState is called before peerStateUpdated, so the
	// update should already be in the channel (or stored in the map).
	select {
	case wu := <-tm.wlUpdates:
		if wu.peerID != tm.remote.ID() {
			t.Fatalf("whitelist update peer: got %s, want %s", wu.peerID, tm.remote.ID())
		}
		if wu.ws == nil {
			t.Fatal("whitelist state is nil")
		}
	case <-time.After(time.Second):
		tm.peerWLMu.Lock()
		ws := tm.peerWLStates[tm.remote.ID()]
		tm.peerWLMu.Unlock()
		if ws == nil {
			t.Fatal("updatePeerWhitelistState was not called")
		}
	}

	// handlePeerQuotas runs asynchronously; give it a moment.
	select {
	case qu := <-tm.quotaUpdates:
		if qu.peerID != tm.remote.ID() {
			t.Fatalf("quota update peer: got %s, want %s", qu.peerID, tm.remote.ID())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handlePeerQuotas")
	}

	// getPeerInfo should return StateConnected.
	info := tm.mcm.getPeerInfo(tm.remote.ID())
	if info.State != admin.StateConnected {
		t.Fatalf("getPeerInfo state: got %s, want %s", info.State, admin.StateConnected)
	}

	// peerInfoSnapshot should contain the peer with StateConnected.
	snapshot := tm.mcm.peerInfoSnapshot()
	snap, ok := snapshot[tm.remote.ID()]
	if !ok {
		t.Fatal("peer missing from peerInfoSnapshot")
	}
	if snap.State != admin.StateConnected {
		t.Fatalf("snapshot state: got %s, want %s", snap.State, admin.StateConnected)
	}
}

func TestMeshConnectionManager_WhitelistMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Remote replies with a completely different whitelist.
	mismatchWL := &types.WhitelistState{
		Current: types.NewWhitelist([]peer.ID{randomPeerID(t), randomPeerID(t)}),
	}
	tm := newTestMCM(t, mismatchWL)
	go tm.mcm.run(ctx)

	// Wait for mismatch state.
	pi := waitForPeerState(t, tm.peerStates, admin.StateWhitelistMismatch, 10*time.Second)
	if pi.PeerID != tm.remote.ID().String() {
		t.Fatalf("mismatch peer: got %s, want %s", pi.PeerID, tm.remote.ID())
	}

	// getPeerInfo should reflect the mismatch.
	info := tm.mcm.getPeerInfo(tm.remote.ID())
	if info.State != admin.StateWhitelistMismatch {
		t.Fatalf("getPeerInfo state: got %s, want %s", info.State, admin.StateWhitelistMismatch)
	}
}

func TestMeshConnectionManager_Reconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tm := newTestMCM(t, nil)
	go tm.mcm.run(ctx)

	// 1. Wait for initial connection.
	waitForPeerState(t, tm.peerStates, admin.StateConnected, 10*time.Second)

	// Drain extra state events from the connect/notify cycle.
	for len(tm.peerStates) > 0 {
		<-tm.peerStates
	}

	// 2. Disconnect: unlink prevents new connections, ClosePeer tears down
	//    the existing one.
	if err := tm.mnet.UnlinkPeers(tm.local.ID(), tm.remote.ID()); err != nil {
		t.Fatalf("UnlinkPeers: %v", err)
	}
	if err := tm.local.Network().ClosePeer(tm.remote.ID()); err != nil {
		t.Fatalf("ClosePeer: %v", err)
	}

	// 3. Wait for disconnected state.
	waitForPeerState(t, tm.peerStates, admin.StateDisconnected, 5*time.Second)

	// 4. Re-link to allow the tracker to reconnect.
	if _, err := tm.mnet.LinkPeers(tm.local.ID(), tm.remote.ID()); err != nil {
		t.Fatalf("LinkPeers: %v", err)
	}

	// 5. Wait for reconnection (tracker retries on backoff ~2-4 s).
	waitForPeerState(t, tm.peerStates, admin.StateConnected, 10*time.Second)

	// Verify final state.
	info := tm.mcm.getPeerInfo(tm.remote.ID())
	if info.State != admin.StateConnected {
		t.Fatalf("getPeerInfo after reconnect: got %s, want %s", info.State, admin.StateConnected)
	}
}
