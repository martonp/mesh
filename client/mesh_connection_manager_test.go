package client

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshConnectionManagerFailover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("mesh-conn-manager-test")

	node1ID := randomPeerID(t)
	node2ID := randomPeerID(t)
	node1Addr := ma.StringCast("/ip4/127.0.0.1/tcp/10001")
	node2Addr := ma.StringCast("/ip4/127.0.0.1/tcp/10002")
	node1Info := peer.AddrInfo{ID: node1ID, Addrs: []ma.Multiaddr{node1Addr}}
	node2Info := peer.AddrInfo{ID: node2ID, Addrs: []ma.Multiaddr{node2Addr}}

	conn1 := newTMeshConnection(node1ID)
	conn1.fetchNodes = []peer.AddrInfo{node1Info, node2Info}
	conn2 := newTMeshConnection(node2ID)

	node1Available := true
	connFactory := func(peerID peer.ID) meshConn {
		switch peerID {
		case node1ID:
			if node1Available {
				return conn1
			}
			conn := newTMeshConnection(node1ID)
			conn.fail(errors.New("node-1 unavailable"))
			return conn
		case node2ID:
			return conn2
		default:
			t.Fatalf("unexpected peer ID %s", peerID)
			return nil
		}
	}

	h, err := libp2p.New()
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer func() { _ = h.Close() }()

	h.Peerstore().AddAddrs(node1ID, node1Info.Addrs, peerstore.PermanentAddrTTL)
	m := newMeshConnectionManager(&meshConnectionManagerConfig{
		host:        h,
		log:         logger,
		connFactory: connFactory,
	})

	done := make(chan struct{})
	go func() {
		m.run(ctx)
		close(done)
	}()

	// wait for the primary connection to be set to the expected peer ID
	waitForPrimary := func(expected peer.ID) {
		t.Helper()
		requireEventually(t, func() bool {
			c, err := m.primaryConnection()
			if err != nil {
				return false
			}
			return c.remotePeerID() == expected
		}, 2*time.Second, 10*time.Millisecond, "primary connection not set to %s", expected)
	}

	waitForPrimary(node1ID)

	requireEventually(t, func() bool {
		addrs := h.Peerstore().Addrs(node2ID)
		return len(addrs) > 0
	}, 2*time.Second, 10*time.Millisecond, "peerstore missing addresses for node 2")

	node1Available = false
	conn1.fail(errors.New("node-1 down"))

	waitForPrimary(node2ID)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("mesh connection manager did not stop")
	}
}
