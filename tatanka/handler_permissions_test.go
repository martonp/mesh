package tatanka

import (
	"context"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
)

// TestPushPermissions tests the permissions for the push protocol.
func TestPushPermissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a mock network with 2 peers (1 mesh node, 1 client)
	mnet, err := mocknet.WithNPeers(2)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mnet.Peers()
	meshHost := mnet.Host(allPeers[0])
	clientHost := mnet.Host(allPeers[1])

	// Create a whitelist with just the mesh node
	mockWhitelist := types.NewWhitelist([]peer.ID{meshHost.ID()})

	// Create the test node with bondStorage initially set to score 0
	dir := t.TempDir()
	node := newTestNode(t, ctx, meshHost, dir, mockWhitelist)
	testBondStorage := &testBondStorage{score: 0}
	node.bondStorage = testBondStorage

	// Link and connect the client to the mesh node
	if _, err := mnet.LinkPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to link client to node: %v", err)
	}
	if _, err := mnet.ConnectPeers(clientHost.ID(), meshHost.ID()); err != nil {
		t.Fatalf("Failed to connect client to node: %v", err)
	}

	requireEventually(t, func() bool {
		return mnet.Net(clientHost.ID()).Connectedness(meshHost.ID()) == network.Connected
	}, time.Second, 5*time.Millisecond, "client failed to connect to mesh node")

	// With bond score 0, push protocol should return unauthorized error
	stream, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Read the response
	response1 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, response1); err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Verify it's an unauthorized error
	if errResp, ok := response1.Response.(*protocolsPb.Response_Error); ok {
		if _, isUnauthorized := errResp.Error.Error.(*protocolsPb.Error_Unauthorized); !isUnauthorized {
			t.Fatalf("Expected unauthorized error, got different error type")
		}
	} else {
		t.Fatalf("Expected error response, got: %T", response1.Response)
	}

	_ = stream.Close()

	// Update bond score to 1, push protocol should now succeed
	testBondStorage.score = 1

	stream2, err := clientHost.NewStream(ctx, meshHost.ID(), protocols.ClientPushProtocol)
	if err != nil {
		t.Fatalf("Failed to create second stream: %v", err)
	}
	defer func() { _ = stream2.Close() }()

	initialSubs := &protocolsPb.InitialSubscriptions{Topics: []string{"test-topic"}}
	if err := codec.WriteLengthPrefixedMessage(stream2, initialSubs); err != nil {
		t.Fatalf("Failed to send initial subscriptions: %v", err)
	}

	// Read the response
	response2 := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream2, response2); err != nil {
		t.Fatalf("Failed to read second response: %v", err)
	}

	// Verify it's a success response
	if _, ok := response2.Response.(*protocolsPb.Response_Success); !ok {
		t.Fatalf("Expected success response, got: %T", response2.Response)
	}
}
