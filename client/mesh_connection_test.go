package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

// createHost creates a libp2p host listening on a dynamic TCP port.
func createHost(t *testing.T) (host.Host, string) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("crypto.GenerateEd25519Key: %v", err)
	}

	// Use port 0 for dynamic allocation.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // Close listener after getting port.

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}

	return h, fmt.Sprintf("%s/p2p/%s", addr, h.ID().String())
}

func receiveWithTimeout[T any](t *testing.T, ch <-chan T, timeout time.Duration) T {
	select {
	case msg := <-ch:
		return msg
	case <-time.After(timeout):
		t.Fatalf("Timed out waiting for message after %v", timeout)
	}
	return *new(T) // unreachable
}

func randomPeerID(t *testing.T) peer.ID {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to create peer ID: %v", err)
	}
	return id
}

// requireEventually asserts that the given condition function returns true within
// the specified timeout. It polls the condition at the given tick interval.
func requireEventually(t *testing.T, condition func() bool, timeout, tick time.Duration, msg string, args ...any) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(tick)
	}

	if condition() {
		return
	}

	t.Fatalf("Condition failed after %v: %s", timeout, fmt.Sprintf(msg, args...))
}

type meshConnHarness struct {
	clientHost           host.Host
	tatankaHost          host.Host
	tatankaAddr          string
	ctx                  context.Context
	cancel               context.CancelFunc
	logger               slog.Logger
	clientReceived       chan *protocolsPb.PushMessage
	publishReceived      chan *protocolsPb.PublishRequest
	subscribeReceived    chan *protocolsPb.SubscribeRequest
	postBondReceived     chan *protocolsPb.PostBondRequest
	meshNodesResp        *protocolsPb.AvailableMeshNodesResponse
	initialSubscriptions chan *protocolsPb.InitialSubscriptions
	postBondResponses    []proto.Message
	tatankaPushStream    atomic.Pointer[network.Stream]
	meshConn             *meshConnection
	runDone              chan error
}

func newMeshConnHarness(t *testing.T, topics []string) *meshConnHarness {
	clientHost, _ := createHost(t)
	tatankaHost, tatankaAddr := createHost(t)

	// Add tatanka address to client's peerstore
	tatankaMultiaddr, err := ma.NewMultiaddr(tatankaAddr)
	if err != nil {
		t.Fatalf("Failed to parse tatanka multiaddr: %v", err)
	}
	tatankaInfo, err := peer.AddrInfoFromP2pAddr(tatankaMultiaddr)
	if err != nil {
		t.Fatalf("Failed to parse tatanka peer info: %v", err)
	}
	clientHost.Peerstore().AddAddrs(tatankaInfo.ID, tatankaInfo.Addrs, 10*time.Minute)

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("meshconn-test")
	logger.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())

	h := &meshConnHarness{
		clientHost:           clientHost,
		tatankaHost:          tatankaHost,
		tatankaAddr:          tatankaAddr,
		ctx:                  ctx,
		cancel:               cancel,
		logger:               logger,
		clientReceived:       make(chan *protocolsPb.PushMessage, 10),
		publishReceived:      make(chan *protocolsPb.PublishRequest, 10),
		subscribeReceived:    make(chan *protocolsPb.SubscribeRequest, 10),
		postBondReceived:     make(chan *protocolsPb.PostBondRequest, 10),
		initialSubscriptions: make(chan *protocolsPb.InitialSubscriptions, 10),
		runDone:              make(chan error, 1),
	}

	h.setupDefaultHandlers(t)

	bondInfo := bond.NewBondInfo()
	bondInfo.AddBonds([]*bond.BondParams{{
		ID:       "test-bond",
		Expiry:   time.Now().Add(6 * time.Hour),
		Strength: bond.MinRequiredBondStrength,
	}}, time.Now())

	h.meshConn = newMeshConnection(
		h.clientHost,
		h.tatankaHost.ID(),
		h.logger,
		bondInfo,
		func() []string { return topics },
		func(msg *protocolsPb.PushMessage) {
			h.clientReceived <- msg
		},
	)

	go func() {
		h.runDone <- h.meshConn.run(h.ctx)
	}()

	receiveWithTimeout(t, h.meshConn.readyCh, 2*time.Second)

	return h
}

func (h *meshConnHarness) setupDefaultHandlers(t *testing.T) {
	h.clientHost.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {})

	h.tatankaHost.SetStreamHandler(protocols.ClientPushProtocol, func(s network.Stream) {
		initialSubs := &protocolsPb.InitialSubscriptions{}
		if err := codec.ReadLengthPrefixedMessage(s, initialSubs); err != nil {
			t.Fatalf("Failed to read initial subscriptions: %v", err)
		}
		h.initialSubscriptions <- initialSubs

		resp := &protocolsPb.Response{
			Response: &protocolsPb.Response_Success{Success: &protocolsPb.Success{}},
		}
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.Fatalf("Failed to send push stream ack: %v", err)
		}
		h.tatankaPushStream.Store(&s)
	})

	h.tatankaHost.SetStreamHandler(protocols.ClientPublishProtocol, func(s network.Stream) {
		defer s.Close()
		var msg protocolsPb.PublishRequest
		if err := codec.ReadLengthPrefixedMessage(s, &msg); err != nil {
			t.Fatalf("Publish read error: %v", err)
		}
		h.publishReceived <- &msg
	})

	h.tatankaHost.SetStreamHandler(protocols.ClientSubscribeProtocol, func(s network.Stream) {
		defer s.Close()
		var msg protocolsPb.SubscribeRequest
		if err := codec.ReadLengthPrefixedMessage(s, &msg); err != nil {
			t.Fatalf("Subscribe read error: %v", err)
		}
		h.subscribeReceived <- &msg
		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.Response{Response: &protocolsPb.Response_Success{Success: &protocolsPb.Success{}}}); err != nil {
			t.Fatalf("Subscribe write error: %v", err)
		}
	})

	h.tatankaHost.SetStreamHandler(protocols.ClientUnsubscribeProtocol, func(s network.Stream) {
		defer s.Close()
		var msg protocolsPb.SubscribeRequest
		if err := codec.ReadLengthPrefixedMessage(s, &msg); err != nil {
			t.Fatalf("Unsubscribe read error: %v", err)
		}
		h.subscribeReceived <- &msg
		if err := codec.WriteLengthPrefixedMessage(s, &protocolsPb.Response{Response: &protocolsPb.Response_Success{Success: &protocolsPb.Success{}}}); err != nil {
			t.Fatalf("Unsubscribe write error: %v", err)
		}
	})

	h.tatankaHost.SetStreamHandler(protocols.PostBondsProtocol, func(s network.Stream) {
		defer s.Close()
		var msg protocolsPb.PostBondRequest
		if err := codec.ReadLengthPrefixedMessage(s, &msg); err != nil {
			t.Fatalf("PostBond read error: %v", err)
		}
		h.postBondReceived <- &msg

		var resp proto.Message
		if len(h.postBondResponses) > 0 {
			resp = h.postBondResponses[0]
			h.postBondResponses = h.postBondResponses[1:]
		} else {
			resp = &protocolsPb.Response{
				Response: &protocolsPb.Response_PostBondResponse{
					PostBondResponse: &protocolsPb.PostBondResponse{
						BondStrength: bond.MinRequiredBondStrength,
					},
				},
			}
		}
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.Fatalf("Failed to send post-bond response: %v", err)
		}
	})

	h.tatankaHost.SetStreamHandler(protocols.AvailableMeshNodesProtocol, func(s network.Stream) {
		defer s.Close()

		resp := &protocolsPb.Response{
			Response: &protocolsPb.Response_AvailableMeshNodesResponse{
				AvailableMeshNodesResponse: h.meshNodesResp,
			},
		}
		if err := codec.WriteLengthPrefixedMessage(s, resp); err != nil {
			t.Fatalf("Failed to send available mesh nodes response: %v", err)
		}
	})
}

func (h *meshConnHarness) getTatankaPushStream() network.Stream {
	ptr := h.tatankaPushStream.Load()
	if ptr != nil {
		return *ptr
	}
	return nil
}

func (h *meshConnHarness) cleanup() {
	h.cancel()
	_ = h.clientHost.Close()
	_ = h.tatankaHost.Close()
}

func TestMeshConnection_PushMessages(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	// Send a push message from tatanka → client
	pushMsg := &protocolsPb.PushMessage{
		Topic: "test-topic",
		Data:  []byte("hello from tatanka"),
	}

	s := h.getTatankaPushStream()
	if s == nil {
		t.Fatal("tatanka push stream not established")
	}

	if err := codec.WriteLengthPrefixedMessage(s, pushMsg); err != nil {
		t.Fatalf("Failed to send push message: %v", err)
	}

	received := receiveWithTimeout(t, h.clientReceived, 2*time.Second)

	if received.Topic != pushMsg.Topic {
		t.Errorf("expected topic %q, got %q", pushMsg.Topic, received.Topic)
	}
	if !bytes.Equal(received.Data, pushMsg.Data) {
		t.Errorf("expected data %q, got %q", pushMsg.Data, received.Data)
	}
}
func TestMeshConnection_Subscribe(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	topic := "test-topic"
	if err := h.meshConn.subscribe(h.ctx, []string{topic}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	sub := receiveWithTimeout(t, h.subscribeReceived, 2*time.Second)

	if sub.Topics[0] != topic {
		t.Errorf("unexpected subscribe request: %+v", sub)
	}
}

func TestMeshConnection_Unsubscribe(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	topic := "test-topic"
	if err := h.meshConn.unsubscribe(h.ctx, []string{topic}); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}

	sub := receiveWithTimeout(t, h.subscribeReceived, 2*time.Second)

	if sub.Topics[0] != topic {
		t.Errorf("unexpected unsubscribe request: %+v", sub)
	}
}

func TestMeshConnection_FetchAvailableMeshNodes(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	peer1ID := randomPeerID(t)
	peer2ID := randomPeerID(t)
	addr1, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4457")
	addr2, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4458")
	addr3, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4459")
	addr4, _ := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4460")

	h.meshNodesResp = &protocolsPb.AvailableMeshNodesResponse{
		Peers: []*protocolsPb.PeerInfo{
			{
				Id:    []byte(peer1ID),
				Addrs: [][]byte{addr1.Bytes(), addr2.Bytes()},
			},
			{
				Id:    []byte(peer2ID),
				Addrs: [][]byte{addr3.Bytes(), addr4.Bytes()},
			},
		},
	}

	expectedPeers := []peer.AddrInfo{
		{ID: peer1ID, Addrs: []ma.Multiaddr{addr1, addr2}},
		{ID: peer2ID, Addrs: []ma.Multiaddr{addr3, addr4}},
	}

	peers, err := h.meshConn.fetchAvailableMeshNodes(h.ctx)
	if err != nil {
		t.Fatalf("fetchAvailableMeshNodes failed: %v", err)
	}

	if len(peers) != len(expectedPeers) {
		t.Fatalf("expected %d peers, got %d", len(expectedPeers), len(peers))
	}

	for i, expectedPeer := range expectedPeers {
		actualPeer := peers[i]

		if actualPeer.ID != expectedPeer.ID {
			t.Errorf("peer %d: expected ID %s, got %s", i, expectedPeer.ID, actualPeer.ID)
		}

		if len(actualPeer.Addrs) != len(expectedPeer.Addrs) {
			t.Errorf("peer %d: expected %d addresses, got %d", i, len(expectedPeer.Addrs), len(actualPeer.Addrs))
			continue
		}

		for j, expectedAddr := range expectedPeer.Addrs {
			actualAddr := actualPeer.Addrs[j]
			if expectedAddr.String() != actualAddr.String() {
				t.Errorf("peer %d, addr %d: expected %s, got %s", i, j, expectedAddr, actualAddr)
			}
		}
	}
}

func TestMeshConnection_InitialSubscriptions(t *testing.T) {
	expectedTopics := []string{"topic1", "topic2", "topic3"}
	h := newMeshConnHarness(t, expectedTopics)
	defer h.cleanup()

	initialSubs := receiveWithTimeout(t, h.initialSubscriptions, 2*time.Second)

	if len(initialSubs.Topics) != len(expectedTopics) {
		t.Fatalf("expected %d topics, got %d", len(expectedTopics), len(initialSubs.Topics))
	}

	for i, expectedTopic := range expectedTopics {
		if initialSubs.Topics[i] != expectedTopic {
			t.Errorf("topic %d: expected %s, got %s", i, expectedTopic, initialSubs.Topics[i])
		}
	}
}

func TestMeshConnection_Broadcast(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	topic := "test-topic"
	data := []byte("testing")
	if err := h.meshConn.broadcast(h.ctx, topic, data); err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}

	pub := receiveWithTimeout(t, h.publishReceived, 2*time.Second)

	if pub.Topic != topic || !bytes.Equal(pub.Data, data) {
		t.Errorf("expected topic %q data %q, got topic %q data %q",
			topic, data, pub.Topic, pub.Data)
	}
}

func TestMeshConnection_Reconnection(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	// First test that if the push stream is closed, but the tatanka node is still
	// connected, a new push stream is established.
	s := h.getTatankaPushStream()
	if s == nil {
		t.Fatal("push stream not established")
	}
	s.Close()

	newPushMsg := &protocolsPb.PushMessage{
		Topic: "reconnect-test",
		Data:  []byte("after reconnect"),
	}

	requireEventually(t, func() bool {
		return h.getTatankaPushStream() != nil && s != h.getTatankaPushStream()
	}, 10*time.Second, 100*time.Millisecond, "push stream not established after reconnect")

	s = h.getTatankaPushStream()
	if s == nil {
		t.Fatal("no push stream after reconnect")
	}
	if err := codec.WriteLengthPrefixedMessage(s, newPushMsg); err != nil {
		t.Fatalf("Failed to send push message after reconnect: %v", err)
	}
	received := receiveWithTimeout(t, h.clientReceived, 2*time.Second)
	if received.Topic != newPushMsg.Topic || !bytes.Equal(received.Data, newPushMsg.Data) {
		t.Errorf("reconnect test message mismatch")
	}

	// If the tatanka node disconnects, run should exit
	err := h.tatankaHost.Network().ClosePeer(h.clientHost.ID())
	if err != nil {
		t.Fatalf("failed to close peer: %v", err)
	}
	requireEventually(t, func() bool {
		select {
		case err := <-h.runDone:
			return err != nil && strings.Contains(err.Error(), "disconnected")
		default:
			return false
		}
	}, 10*time.Second, 100*time.Millisecond, "meshConnection.run did not exit after peer disconnect")
}

func TestMeshConnection_PostBondWithRetries(t *testing.T) {
	h := newMeshConnHarness(t, nil)
	defer h.cleanup()

	// Set up responses for retry attempts
	h.postBondResponses = []proto.Message{
		// First attempt: fail with error message
		&protocolsPb.Response{
			Response: &protocolsPb.Response_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_Message{
						Message: "failed to post bonds",
					},
				},
			},
		},
		// Second attempt: fail with invalid bond index
		&protocolsPb.Response{
			Response: &protocolsPb.Response_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_PostBondError{
						PostBondError: &protocolsPb.PostBondError{
							InvalidBondIndex: 0,
						},
					},
				},
			},
		},
		// Third attempt: succeed
		&protocolsPb.Response{
			Response: &protocolsPb.Response_PostBondResponse{
				PostBondResponse: &protocolsPb.PostBondResponse{
					BondStrength: bond.MinRequiredBondStrength,
				},
			},
		},
	}

	bondReq, err := bond.PostBondReqFromBondInfo(h.meshConn.bondInfo)
	if err != nil {
		t.Fatalf("Unexpected error creating post bond request: %v", err)
	}

	// First attempt should fail with error message
	err = h.meshConn.postBondsInternal(h.ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}

	bondMsg := receiveWithTimeout(t, h.postBondReceived, 2*time.Second)
	if len(bondMsg.Bonds) != 1 {
		t.Fatalf("Expected %d bond, got %d", 1, len(bondMsg.Bonds))
	}

	// Second attempt should fail with invalid bond index
	err = h.meshConn.postBondsInternal(h.ctx, bondReq)
	if err == nil {
		t.Fatal("Expected a post bond error")
	}
	if !errors.Is(err, errInvalidBondIndex) {
		t.Fatalf("Expected an invalid bond index error, got %v", err)
	}

	_ = receiveWithTimeout(t, h.postBondReceived, 2*time.Second)

	// Third attempt should succeed
	err = h.meshConn.postBondsInternal(h.ctx, bondReq)
	if err != nil {
		t.Fatalf("Expected a successful post bond attempt, got %v", err)
	}

	_ = receiveWithTimeout(t, h.postBondReceived, 2*time.Second)
}
