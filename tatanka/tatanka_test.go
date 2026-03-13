package tatanka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/bisoncraft/mesh/codec"
	"github.com/bisoncraft/mesh/oracle"
	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/protocols"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/bisoncraft/mesh/tatanka/types"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"google.golang.org/protobuf/proto"
)

var (
	errRelayRejected = errors.New("relay rejected")
	errRelayNotFound = errors.New("relay counterparty not found")
	errRelayOther    = errors.New("relay error")

	oraclePricesTopic   = protocols.PriceTopicPrefix + "BTC"
	oracleFeeRatesTopic = protocols.FeeRateTopicPrefix + "BTC"
)

type testBondStorage struct {
	score uint32
}

var _ bondStorage = (*testBondStorage)(nil)

func (tbs *testBondStorage) addBonds(peerID peer.ID, bonds []*bond.BondParams) uint32 {
	return tbs.score
}

func (tbs *testBondStorage) bondStrength(peerID peer.ID) uint32 {
	return tbs.score
}

// tOracle is a test oracle that tracks merged updates.
type tOracle struct {
	mtx      sync.Mutex
	merged   []*oracle.OracleUpdate
	prices   map[oracle.Ticker]float64
	feeRates map[oracle.Network]*big.Int
}

var _ oracleService = (*tOracle)(nil)

func newTOracle() *tOracle {
	return &tOracle{
		merged:   make([]*oracle.OracleUpdate, 0),
		prices:   make(map[oracle.Ticker]float64),
		feeRates: make(map[oracle.Network]*big.Int),
	}
}

func (t *tOracle) Run(ctx context.Context) {
	<-ctx.Done()
}

func (t *tOracle) Merge(update *oracle.OracleUpdate, senderID string) *oracle.MergeResult {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.merged = append(t.merged, update)

	result := &oracle.MergeResult{}

	if len(update.Prices) > 0 {
		result.Prices = make(map[oracle.Ticker]float64, len(update.Prices))
		for ticker, price := range update.Prices {
			result.Prices[ticker] = price
			t.prices[ticker] = price
		}
	}

	if len(update.FeeRates) > 0 {
		result.FeeRates = make(map[oracle.Network]*big.Int, len(update.FeeRates))
		for network, feeRate := range update.FeeRates {
			result.FeeRates[network] = feeRate
			t.feeRates[network] = feeRate
		}
	}

	return result
}

func (t *tOracle) Price(ticker oracle.Ticker) (float64, bool) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	price, found := t.prices[ticker]
	return price, found
}

func (t *tOracle) FeeRate(network oracle.Network) (*big.Int, bool) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	value, found := t.feeRates[network]
	if !found {
		return nil, false
	}
	return new(big.Int).Set(value), true
}

func (t *tOracle) GetLocalQuotas() map[string]*sources.QuotaStatus { return nil }

func (t *tOracle) UpdatePeerSourceQuota(string, *oracle.TimestampedQuotaStatus, string) {}

func (t *tOracle) OracleSnapshot() *oracle.OracleSnapshot { return nil }

func (t *tOracle) SetPrices(prices map[oracle.Ticker]float64) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.prices = prices
}

func (t *tOracle) SetFeeRates(feeRates map[oracle.Network]*big.Int) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.feeRates = feeRates
}

func newTestNode(t *testing.T, ctx context.Context, h host.Host, dataDir string, wl *types.Whitelist) *TatankaNode {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelWarn)

	// Write the whitelist to the data directory.
	if err := saveWhitelist(filepath.Join(dataDir, "whitelist.json"), wl); err != nil {
		t.Fatalf("Failed to write whitelist: %v", err)
	}

	n, err := NewTatankaNode(&Config{
		Logger:  log,
		DataDir: dataDir,
	}, WithHost(h))
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}

	n.bondStorage = &testBondStorage{score: 1}
	n.oracle = newTOracle()

	go func() {
		if err := n.Run(ctx); err != nil {
			t.Errorf("Failed to run test node: %v", err)
		}
	}()

	if err := n.WaitReady(ctx); err != nil {
		t.Fatalf("Failed to start test node: %v", err)
	}

	return n
}

// newTestNodeWithOracle creates a test node with a custom oracle implementation.
func newTestNodeWithOracle(t *testing.T, ctx context.Context, h host.Host, dataDir string, wl *types.Whitelist, testOracle oracleService) *TatankaNode {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelWarn)

	// Write the whitelist to the data directory.
	if err := saveWhitelist(filepath.Join(dataDir, "whitelist.json"), wl); err != nil {
		t.Fatalf("Failed to write whitelist: %v", err)
	}

	n, err := NewTatankaNode(&Config{
		Logger:  log,
		DataDir: dataDir,
	}, WithHost(h))
	if err != nil {
		t.Fatalf("Failed to create test node: %v", err)
	}

	n.bondStorage = &testBondStorage{score: 1}
	n.oracle = testOracle

	go func() {
		if err := n.Run(ctx); err != nil {
			t.Errorf("Failed to run test node: %v", err)
		}
	}()

	if err := n.WaitReady(ctx); err != nil {
		t.Fatalf("Failed to start test node: %v", err)
	}

	return n
}

// testClient simulates a client that connects to a TatankaNode.
type testClient struct {
	log        slog.Logger
	host       host.Host
	nodeID     peer.ID
	pushStream network.Stream
	channels   map[string]chan *protocolsPb.PushMessage
	relays     chan relayRequest
	mtx        sync.RWMutex
}

type relayRequest struct {
	stream network.Stream
	req    *protocolsPb.TatankaRelayMessageRequest
}

// newTestClient creates a new test client connected to a mesh node.
// It establishes the long-running push stream and starts listening for
// messages.
func newTestClient(ctx context.Context, h host.Host, nodeID peer.ID) (*testClient, error) {
	logBackend := slog.NewBackend(os.Stdout)
	log := logBackend.Logger(h.ID().ShortString())
	log.SetLevel(slog.LevelWarn)

	stream, err := h.NewStream(ctx, nodeID, protocols.ClientPushProtocol)
	if err != nil {
		return nil, err
	}

	// Send initial subscriptions (empty list).
	initialSubs := &protocolsPb.InitialSubscriptions{Topics: nil}
	if err := codec.WriteLengthPrefixedMessage(stream, initialSubs); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to send initial subscriptions: %w", err)
	}

	// Read the success response.
	resp := &protocolsPb.Response{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("failed to read push stream response: %w", err)
	}
	if _, ok := resp.Response.(*protocolsPb.Response_Success); !ok {
		_ = stream.Close()
		return nil, fmt.Errorf("unexpected push stream response: %T", resp.Response)
	}

	tc := &testClient{
		log:        log,
		host:       h,
		nodeID:     nodeID,
		pushStream: stream,
		channels:   make(map[string]chan *protocolsPb.PushMessage),
		relays:     make(chan relayRequest, 2),
	}

	h.SetStreamHandler(protocols.TatankaRelayMessageProtocol, tc.handleIncomingRelay)

	// Start goroutine to read incoming push messages
	go tc.readPushMessages()

	return tc, nil
}

// readPushMessages reads and decodes messages from the push stream.
// Messages are length-prefixed: 4 bytes (big-endian) length, then protobuf data.
func (tc *testClient) readPushMessages() {
	lengthBuf := make([]byte, 4)
	for {
		// Read 4-byte length prefix
		if _, err := io.ReadFull(tc.pushStream, lengthBuf); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
				tc.log.Errorf("Error reading message length from client %s: %v", tc.nodeID.ShortString(), err)
			}

			return
		}

		// Decode length (big-endian)
		msgLen := uint32(lengthBuf[0])<<24 | uint32(lengthBuf[1])<<16 | uint32(lengthBuf[2])<<8 | uint32(lengthBuf[3])
		if msgLen == 0 || msgLen > 10*1024*1024 { // Sanity check: max 10MB
			tc.log.Errorf("Invalid message length %d", msgLen)
			return
		}

		// Read the protobuf message
		data := make([]byte, msgLen)
		if _, err := io.ReadFull(tc.pushStream, data); err != nil {
			tc.log.Errorf("Error reading message data from client %s: %v", tc.nodeID.ShortString(), err)
			return
		}

		msg := &protocolsPb.PushMessage{}
		if err := proto.Unmarshal(data, msg); err != nil {
			tc.log.Errorf("Error unmarshaling push message from client %s: %v", tc.nodeID.ShortString(), err)
			continue
		}

		tc.mtx.Lock()
		ch, exists := tc.channels[msg.Topic]
		if !exists {
			ch = make(chan *protocolsPb.PushMessage, 100)
			tc.channels[msg.Topic] = ch
		}
		tc.mtx.Unlock()

		// Send message to channel (non-blocking with buffer)
		select {
		case ch <- msg:
		default:
			tc.log.Errorf("Warning: message buffer full for topic %s, dropping message", msg.Topic)
		}
	}
}

// Subscribe subscribes the client to a topic.
func (tc *testClient) Subscribe(ctx context.Context, topic string) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	subMsg := &protocolsPb.SubscribeRequest{Topics: []string{topic}}
	if err := codec.WriteLengthPrefixedMessage(stream, subMsg); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	resp := &protocolsPb.Success{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	return nil
}

// Unsubscribe unsubscribes the client from a topic.
func (tc *testClient) Unsubscribe(ctx context.Context, topic string) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientSubscribeProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	subMsg := &protocolsPb.UnsubscribeRequest{Topics: []string{topic}}
	if err := codec.WriteLengthPrefixedMessage(stream, subMsg); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	resp := &protocolsPb.Success{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	return nil
}

// Publish publishes a message to a topic.
func (tc *testClient) Publish(ctx context.Context, topic string, data []byte) error {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientPublishProtocol)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	pubMsg := &protocolsPb.PublishRequest{Topic: topic, Data: data}
	if err := codec.WriteLengthPrefixedMessage(stream, pubMsg); err != nil {
		if !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
	}

	return nil
}

// Close terminates the client.
func (tc *testClient) Close() {
	conns := tc.host.Network().Conns()
	for _, conn := range conns {
		streams := conn.GetStreams()
		for _, stream := range streams {
			_ = stream.Close()
		}

		_ = conn.Close()
	}

	_ = tc.host.Close()
}

// Next blocks until a message is received for the given topic and returns it.
// Returns an error if the context is cancelled before a message arrives.
func (tc *testClient) Next(ctx context.Context, topic string) (*protocolsPb.PushMessage, error) {
	tc.mtx.Lock()
	ch, exists := tc.channels[topic]
	if !exists {
		ch = make(chan *protocolsPb.PushMessage, 100)
		tc.channels[topic] = ch
	}
	tc.mtx.Unlock()

	select {
	case msg := <-ch:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NextData blocks until a DATA message (not a subscription event) is received
// for the given topic and returns it.
func (tc *testClient) NextData(ctx context.Context, topic string) (*protocolsPb.PushMessage, error) {
	for {
		msg, err := tc.Next(ctx, topic)
		if err != nil {
			return nil, err
		}
		if msg.MessageType == protocolsPb.PushMessage_BROADCAST {
			return msg, nil
		}
	}
}

// relayMessage asks the connected tatanka node to relay a message to the
// given counterparty client and returns the response payload.
func (tc *testClient) relayMessage(ctx context.Context, counterparty peer.ID, message []byte) ([]byte, error) {
	stream, err := tc.host.NewStream(ctx, tc.nodeID, protocols.ClientRelayMessageProtocol)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()

	req := &protocolsPb.ClientRelayMessageRequest{
		PeerID:  []byte(counterparty),
		Message: message,
	}
	if err := codec.WriteLengthPrefixedMessage(stream, req); err != nil {
		return nil, err
	}

	resp := &protocolsPb.ClientRelayMessageResponse{}
	if err := codec.ReadLengthPrefixedMessage(stream, resp); err != nil {
		return nil, err
	}

	if resp.GetError() != nil {
		errObj := resp.GetError()
		switch {
		case errObj.GetCpNotFoundError() != nil:
			return nil, errRelayNotFound
		case errObj.GetCpRejectedError() != nil:
			return nil, errRelayRejected
		default:
			return nil, fmt.Errorf("%w: %v", errRelayOther, errObj)
		}
	}

	return resp.GetMessage(), nil
}

// acceptRelay waits for an incoming TatankaRelayMessageRequest and responds
// with the provided response payload.
func (tc *testClient) acceptRelay(ctx context.Context, response []byte) ([]byte, error) {
	select {
	case rr := <-tc.relays:
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Message{
				Message: response,
			},
		}
		if err := codec.WriteLengthPrefixedMessage(rr.stream, resp); err != nil {
			_ = rr.stream.Close()
			return nil, err
		}
		_ = rr.stream.Close()
		return rr.req.Message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// rejectRelay responds to a TatankaRelayMessageRequest with a rejection.
func (tc *testClient) rejectRelay(ctx context.Context) error {
	select {
	case rr := <-tc.relays:
		resp := &protocolsPb.TatankaRelayMessageResponse{
			Response: &protocolsPb.TatankaRelayMessageResponse_Error{
				Error: &protocolsPb.Error{
					Error: &protocolsPb.Error_CpRejectedError{
						CpRejectedError: &protocolsPb.CounterpartyRejectedError{},
					},
				},
			},
		}
		if err := codec.WriteLengthPrefixedMessage(rr.stream, resp); err != nil {
			_ = rr.stream.Close()
			return err
		}
		_ = rr.stream.Close()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (tc *testClient) handleIncomingRelay(s network.Stream) {
	req := &protocolsPb.TatankaRelayMessageRequest{}
	if err := codec.ReadLengthPrefixedMessage(s, req); err != nil {
		_ = s.Close()
		return
	}

	select {
	case tc.relays <- relayRequest{stream: s, req: req}:
	default:
		tc.log.Infof("relay channel full, closing stream")
		_ = s.Close()
	}
}

func fullyConnectedMeshWithClients(ctx context.Context, t *testing.T, numMeshNodes, numClients int, clientToNode func(int) int) (
	meshNodes []*TatankaNode, wl *types.Whitelist, clients []*testClient) {
	mnet, err := mocknet.WithNPeers(numMeshNodes + numClients)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mnet.Peers()
	meshHosts := make([]host.Host, numMeshNodes)
	for i := range meshHosts {
		meshHosts[i] = mnet.Host(allPeers[i])
	}

	clientHosts := make([]host.Host, numClients)
	for i := range clientHosts {
		clientHosts[i] = mnet.Host(allPeers[numMeshNodes+i])
	}

	whitelistPeerIDs := make([]peer.ID, numMeshNodes)
	for i, h := range meshHosts {
		whitelistPeerIDs[i] = h.ID()
	}
	mockWhitelist := types.NewWhitelist(whitelistPeerIDs)

	runningNodes := make([]*TatankaNode, 0, numMeshNodes)
	for i, h := range meshHosts {
		dir := t.TempDir()
		if err := linkNodeWithMesh(mnet, h, runningNodes, true); err != nil {
			t.Fatalf("Failed to link node %d: %v", i, err)
		}
		node := newTestNode(t, ctx, h, dir, mockWhitelist)
		runningNodes = append(runningNodes, node)
	}

	// Make sure the mesh is fully connected
	requireEventually(t, func() bool {
		return checkFullyConnected(t, runningNodes)
	}, time.Second, 5*time.Millisecond, "failed to fully connect mesh")

	clients = make([]*testClient, numClients)
	for i, clientHost := range clientHosts {
		nodeIdx := clientToNode(i)
		if _, err := mnet.LinkPeers(clientHosts[i].ID(), meshHosts[nodeIdx].ID()); err != nil {
			t.Fatalf("Failed to link client %d to node %d: %v", i, nodeIdx, err)
		}
		if _, err := mnet.ConnectPeers(clientHosts[i].ID(), meshHosts[nodeIdx].ID()); err != nil {
			t.Fatalf("Failed to connect client %d to node %d: %v", i, nodeIdx, err)
		}
		clients[i], err = newTestClient(ctx, clientHost, meshHosts[nodeIdx].ID())
		if err != nil {
			t.Fatalf("Failed to create client %d: %v", i, err)
		}
	}

	for i, clientHost := range clientHosts {
		nodeIdx := clientToNode(i)
		requireEventually(t, func() bool {
			return mnet.Net(clientHost.ID()).Connectedness(meshHosts[nodeIdx].ID()) == network.Connected
		}, time.Second, 5*time.Millisecond, "failed to connect client %d to node %d", i, nodeIdx)
	}

	return runningNodes, mockWhitelist, clients
}

// checkFullyConnected verifies that all provided nodes are connected to each other.
// Returns true if fully connected, false otherwise.
func checkFullyConnected(t *testing.T, nodes []*TatankaNode) bool {
	t.Helper()
	if len(nodes) < 2 {
		return true
	}

	fullyConnected := true
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			n1 := nodes[i]
			n2 := nodes[j]

			// Check n1 -> n2
			connStatus1 := n1.node.Network().Connectedness(n2.node.ID())
			if connStatus1 != network.Connected {
				t.Logf("Node %s is not connected to %s (status: %s)",
					n1.node.ID().ShortString(), n2.node.ID().ShortString(), connStatus1)
				fullyConnected = false
			}

			// Check n2 -> n1
			connStatus2 := n2.node.Network().Connectedness(n1.node.ID())
			if connStatus2 != network.Connected {
				t.Logf("Node %s is not connected to %s (status: %s)",
					n2.node.ID().ShortString(), n1.node.ID().ShortString(), connStatus2)
				fullyConnected = false
			}
		}
	}

	return fullyConnected
}

// linkNodeWithMesh links a node to the other running nodes. Linking mocks
// the ability for a node to be reached from another node over the network.
func linkNodeWithMesh(mesh mocknet.Mocknet, host host.Host, runningNodes []*TatankaNode, link bool) error {
	if len(runningNodes) == 0 {
		return nil
	}

	for _, otherNode := range runningNodes {
		if link {
			if _, err := mesh.LinkPeers(host.ID(), otherNode.node.ID()); err != nil {
				return err
			}
			// Add addresses to each other's peerstores so the connection
			// manager can dial. LinkPeers only creates a virtual link.
			host.Peerstore().AddAddrs(otherNode.node.ID(), otherNode.node.Addrs(), peerstore.PermanentAddrTTL)
			otherNode.node.Peerstore().AddAddrs(host.ID(), host.Addrs(), peerstore.PermanentAddrTTL)
		} else {
			if err := mesh.DisconnectPeers(host.ID(), otherNode.node.ID()); err != nil {
				return err
			}
			if err := mesh.UnlinkPeers(host.ID(), otherNode.node.ID()); err != nil {
				return err
			}
		}
	}

	return nil
}

// TestProgressiveMeshStartup simulates the staggered startup of a 5-node mesh.
// Makes sure that each node fully connects to the mesh as it comes online.
func TestProgressiveMeshStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a mock network with 5 nodes
	const numPeers = 5
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}

	// Define nodes and whitelist
	peerIDs := mesh.Peers()
	h1 := mesh.Host(peerIDs[0])
	h2 := mesh.Host(peerIDs[1])
	h3 := mesh.Host(peerIDs[2])
	h4 := mesh.Host(peerIDs[3])
	h5 := mesh.Host(peerIDs[4])
	mockWhitelist := types.NewWhitelist([]peer.ID{
		h1.ID(),
		h2.ID(),
		h3.ID(),
		h4.ID(),
		h5.ID(),
	})

	// runningNodes will hold all running nodes that have been connected
	// to the mesh.
	runningNodes := make([]*TatankaNode, 0, numPeers)

	// startNode links a node to the rest of the mesh, runs the connection logic,
	// then makes sure that the mesh is still fully connected.
	startNode := func(nodeNum int, host host.Host, nodeType string) {
		t.Helper()

		dir := t.TempDir()
		t.Logf("--- Starting Node %d (%s) ---", nodeNum, nodeType)
		err := linkNodeWithMesh(mesh, host, runningNodes, true)
		if err != nil {
			t.Fatal(err)
		}
		node := newTestNode(t, ctx, host, dir, mockWhitelist)
		runningNodes = append(runningNodes, node)

		// First node starts alone, others should be fully connected
		if len(runningNodes) == 1 {
			if len(node.node.Network().Peers()) != 0 {
				t.Errorf("node %d should have 0 peers, but has %d", nodeNum, len(node.node.Network().Peers()))
			} else {
				t.Logf("Node %d is up. Connected to 0 peers.", nodeNum)
			}
		} else {
			if checkFullyConnected(t, runningNodes) {
				t.Logf("Node %d is up. Mesh size: %d. Fully connected.", nodeNum, len(runningNodes))
			} else {
				t.Errorf("Node %d is up. Mesh size: %d. Not fully connected.", nodeNum, len(runningNodes))
			}
		}
	}

	// Bring up nodes one by one
	startNode(1, h1, "Bootstrap")
	startNode(2, h2, "Peer")
	startNode(3, h3, "Bootstrap")
	startNode(4, h4, "Peer")
	startNode(5, h5, "Peer")
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

// newPriceUpdate creates an OracleUpdate with only prices for testing.
func newPriceUpdate(source string, stamp time.Time, prices map[oracle.Ticker]float64) *oracle.OracleUpdate {
	return &oracle.OracleUpdate{
		Source: source,
		Stamp:  stamp,
		Prices: prices,
	}
}

// newFeeRateUpdate creates an OracleUpdate with only fee rates for testing.
func newFeeRateUpdate(source string, stamp time.Time, feeRates map[oracle.Network]*big.Int) *oracle.OracleUpdate {
	return &oracle.OracleUpdate{
		Source:   source,
		Stamp:    stamp,
		FeeRates: feeRates,
	}
}

func TestMeshRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Setup a standard 3-node mesh
	const numPeers = 3
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}

	// Fully connected whitelist (no discovery required)
	hosts := mesh.Hosts()
	var whitelistPeerIDs []peer.ID
	for _, h := range hosts {
		whitelistPeerIDs = append(whitelistPeerIDs, h.ID())
	}
	mockWhitelist := types.NewWhitelist(whitelistPeerIDs)

	// Start the nodes
	var nodes []*TatankaNode
	for _, h := range hosts {
		err := linkNodeWithMesh(mesh, h, nodes, true)
		if err != nil {
			t.Fatal(err)
		}
		node := newTestNode(t, ctx, h, t.TempDir(), mockWhitelist)
		nodes = append(nodes, node)
	}

	// 2. Verify the mesh is fully connected
	if !checkFullyConnected(t, nodes) {
		t.Fatal("Initial mesh failed to connect")
	}

	// 3. Crash node 1
	victim := nodes[1]
	t.Logf("--- Simulating crash of Node 1 (%s) ---", victim.node.ID())
	mesh.UnlinkPeers(victim.node.ID(), nodes[0].node.ID())
	mesh.UnlinkPeers(victim.node.ID(), nodes[2].node.ID())
	victim.node.Network().ClosePeer(nodes[0].node.ID())
	victim.node.Network().ClosePeer(nodes[2].node.ID())

	// 4. Verify the mesh is broken
	requireEventually(t, func() bool {
		return nodes[0].node.Network().Connectedness(victim.node.ID()) == network.NotConnected
	}, 2*time.Second, 100*time.Millisecond, "Node 0 failed to detect Node 1 disconnect")

	// 5. "Restart" Node 1 (Restore the links)
	t.Log("--- Recovering Node 1 ---")
	mesh.LinkPeers(victim.node.ID(), nodes[0].node.ID())
	mesh.LinkPeers(victim.node.ID(), nodes[2].node.ID())

	// 6. Verify Self-Healing
	t.Log("Waiting for mesh self-healing...")
	requireEventually(t, func() bool {
		return checkFullyConnected(t, nodes)
	}, 10*time.Second, 100*time.Millisecond, "Mesh failed to auto-heal after node recovery")
}

func TestWhitelistMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start a 3 node mesh
	const numPeers = 3
	mesh, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}
	hosts := mesh.Hosts()
	h1, h2, h3 := hosts[0], hosts[1], hosts[2]

	goodWhitelist := types.NewWhitelist([]peer.ID{
		h1.ID(),
		h2.ID(),
		h3.ID(),
	})

	badWhitelist := types.NewWhitelist([]peer.ID{
		h1.ID(),
		h2.ID(),
		h3.ID(),
		randomPeerID(t),
	})

	// Start Nodes
	// Node 1 & 2 get the Good Whitelist
	// Node 3 gets the Bad Whitelist
	var nodes []*TatankaNode
	startNode := func(h host.Host, whitelist *types.Whitelist) (*TatankaNode, context.CancelFunc) {
		err = linkNodeWithMesh(mesh, h, nodes, true)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(ctx)
		node := newTestNode(t, ctx, h, t.TempDir(), whitelist)
		nodes = append(nodes, node)
		return node, cancel
	}
	n1, _ := startNode(h1, goodWhitelist)
	n2, _ := startNode(h2, goodWhitelist)
	_, cancel3 := startNode(h3, badWhitelist)

	// Check that node 1 and node2 are connected, but node 3 is not.
	checkConnected := func(h1, h2 host.Host, expected network.Connectedness) bool {
		return h1.Network().Connectedness(h2.ID()) == expected &&
			h2.Network().Connectedness(h1.ID()) == expected
	}
	checkConnected(h1, h2, network.Connected)
	checkConnected(h1, h3, network.NotConnected)
	checkConnected(h2, h3, network.NotConnected)

	// Shut down node 3, restart with the correct whitelist.
	cancel3()
	n3, _ := startNode(h3, goodWhitelist)

	// Check that the mesh is fully connected.
	if !checkFullyConnected(t, []*TatankaNode{n1, n2, n3}) {
		t.Fatal("Mesh failed to connect after node 3 restart")
	}
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

func TestClientRelay(t *testing.T) {
	t.Run("across_nodes", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		nodes, _, clients := fullyConnectedMeshWithClients(ctx, t, 2, 2, func(i int) int { return i })
		requireEventually(t, func() bool {
			return len(nodes[0].clientConnectionManager.getTatankaPeersForClient(clients[1].host.ID())) > 0 &&
				len(nodes[1].clientConnectionManager.getTatankaPeersForClient(clients[0].host.ID())) > 0
		}, 5*time.Second, 10*time.Millisecond, "client discovery didn't propagate")
		checkRelayHappyPath(ctx, t, clients[0], clients[1])
	})

	t.Run("same_node", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 2, func(i int) int { return 0 })
		checkRelayHappyPath(ctx, t, clients[0], clients[1])
	})

	t.Run("counterparty_not_found", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 1, func(i int) int { return 0 })
		initiator := clients[0]

		_, err := initiator.relayMessage(ctx, randomPeerID(t), []byte("hi"))
		if !errors.Is(err, errRelayNotFound) {
			t.Fatalf("expected counterparty not found error, got %v", err)
		}
	})

	t.Run("direct_reject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _, clients := fullyConnectedMeshWithClients(ctx, t, 1, 2, func(i int) int { return 0 })
		initiator := clients[0]
		counterparty := clients[1]

		// Make counterparty reject.
		go func() {
			_ = counterparty.rejectRelay(ctx)
		}()

		if _, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("hi")); !errors.Is(err, errRelayRejected) {
			t.Fatalf("expected rejection error, got %v", err)
		}
	})

	t.Run("forward_reject", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		nodes, _, clients := fullyConnectedMeshWithClients(ctx, t, 2, 2, func(i int) int { return i })
		requireEventually(t, func() bool {
			return len(nodes[0].clientConnectionManager.getTatankaPeersForClient(clients[1].host.ID())) > 0 &&
				len(nodes[1].clientConnectionManager.getTatankaPeersForClient(clients[0].host.ID())) > 0
		}, 5*time.Second, 10*time.Millisecond, "client discovery didn't propagate")
		initiator := clients[0]
		counterparty := clients[1]

		// Make counterparty reject.
		go func() {
			_ = counterparty.rejectRelay(ctx)
		}()

		if _, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("hi")); !errors.Is(err, errRelayRejected) {
			t.Fatalf("expected rejection error, got %v", err)
		}
	})
}

// checkRelayHappyPath checks that sending a message between two clients works.
func checkRelayHappyPath(ctx context.Context, t *testing.T, initiator, counterparty *testClient) {
	t.Helper()
	errCh := make(chan error, 1)
	respCh := make(chan []byte, 1)
	go func() {
		reqMsg, err := counterparty.acceptRelay(ctx, []byte("pong from counterparty"))
		if err != nil {
			errCh <- err
			return
		}
		if string(reqMsg) != "ping from initiator" {
			errCh <- fmt.Errorf("unexpected request message: %q", string(reqMsg))
			return
		}
		respCh <- []byte("pong from counterparty")
	}()

	resp, err := initiator.relayMessage(ctx, counterparty.host.ID(), []byte("ping from initiator"))
	if err != nil {
		t.Fatalf("initiator failed to relay message: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case err := <-errCh:
		t.Fatalf("counterparty relay error: %v", err)
	case want := <-respCh:
		if string(resp) != string(want) {
			t.Fatalf("unexpected response: got %q want %q", string(resp), string(want))
		}
	case <-ctx.Done():
		t.Fatalf("timeout waiting for relay: %v", ctx.Err())
	}
}

func TestClientSubscriptionAndBroadcast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numMeshNodes = 3
	const numClients = 6

	_, _, clients := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, numClients, func(i int) int {
		return i / 2
	})

	topic1 := "topic_1"
	topic2 := "topic_2"

	// Subscribe clients to topics:
	// Clients 0, 1 -> topic1
	// Clients 2, 3 -> topic2
	// Clients 4, 5 -> both topics
	topic1Subscribers := []*testClient{clients[0], clients[1], clients[4], clients[5]}
	topic2Subscribers := []*testClient{clients[2], clients[3], clients[4], clients[5]}

	for _, client := range topic1Subscribers {
		if err := client.Subscribe(ctx, topic1); err != nil {
			t.Fatalf("Failed to subscribe client %s to topic %s: %v", client.host.ID().ShortString(), topic1, err)
		}
	}

	for _, client := range topic2Subscribers {
		if err := client.Subscribe(ctx, topic2); err != nil {
			t.Fatalf("Failed to subscribe client %s to topic %s: %v", client.host.ID().ShortString(), topic2, err)
		}
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Run 100 iterations of random publish/receive
	for iteration := 0; iteration < 100; iteration++ {
		// Randomly select a topic
		var topic string
		var subscribers []*testClient
		if rng.Intn(2) == 0 {
			topic = topic1
			subscribers = topic1Subscribers
		} else {
			topic = topic2
			subscribers = topic2Subscribers
		}

		// Randomly select a publisher from the subscribers
		publisherIdx := rng.Intn(len(subscribers))
		publisher := subscribers[publisherIdx]

		// Create a unique message
		msgData := []byte(fmt.Sprintf("message_%d_from_%s_on_%s", iteration, publisher.host.ID().ShortString(), topic))

		// Publish the message
		if err := publisher.Publish(ctx, topic, msgData); err != nil {
			t.Fatalf("Iteration %d: Failed to publish message to topic %s: %v", iteration, topic, err)
		}

		// All subscribers (except the publisher) should receive the message
		for _, subscriber := range subscribers {
			// Skip the publisher - they should not receive their own message
			if subscriber == publisher {
				continue
			}

			msg, err := subscriber.NextData(ctx, topic)
			if err != nil {
				t.Fatalf("Iteration %d: Client %s failed to receive message on topic %s: %v",
					iteration, subscriber.host.ID().ShortString(), topic, err)
			}

			if msg.Topic != topic {
				t.Fatalf("Iteration %d: Client %s received message with wrong topic. Expected %s, got %s",
					iteration, subscriber.host.ID().ShortString(), topic, msg.Topic)
			}

			if string(msg.Data) != string(msgData) {
				t.Fatalf("Iteration %d: Client %s received message with wrong data. Expected %s, got %s",
					iteration, subscriber.host.ID().ShortString(), string(msgData), string(msg.Data))
			}

			t.Logf("Iteration %d: Client %s successfully received message on topic %s",
				iteration, subscriber.host.ID().ShortString(), topic)
		}
	}

	// Terminate clients.
	for idx := range clients {
		clients[idx].Close()
	}
}

func TestGossipSubOracleUpdates_PriceUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMeshNodes = 3

	nodes, _, _ := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, 0, func(int) int { return 0 })

	// Create nodes with custom oracles that track merges
	oracles := make([]*tOracle, numMeshNodes)
	for i, node := range nodes {
		oracles[i] = node.oracle.(*tOracle)
	}

	// Node 0 publishes price updates
	now := time.Now()
	update := newPriceUpdate("test-source", now, map[oracle.Ticker]float64{
		"BTC": 50000.0,
		"ETH": 3000.0,
	})
	if err := nodes[0].gossipSub.publishOracleUpdate(ctx, update); err != nil {
		t.Fatalf("Failed to publish oracle update: %v", err)
	}

	// Verify that all nodes received and merged the updates
	for i := 0; i < numMeshNodes; i++ {
		requireEventually(t, func() bool {
			oracles[i].mtx.Lock()
			mergedCount := len(oracles[i].merged)
			oracles[i].mtx.Unlock()

			if mergedCount != 1 {
				return false
			}

			oracles[i].mtx.Lock()
			merged := oracles[i].merged[0]
			oracles[i].mtx.Unlock()

			if merged.Source != "test-source" {
				t.Fatalf("Node %d: expected source 'test-source', got %s", i, merged.Source)
			}
			if len(merged.Prices) != 2 {
				t.Fatalf("Node %d: expected 2 prices, got %d", i, len(merged.Prices))
			}
			if merged.Prices["BTC"] != 50000.0 {
				t.Fatalf("Node %d: BTC price incorrect: %v", i, merged.Prices["BTC"])
			}
			if merged.Prices["ETH"] != 3000.0 {
				t.Fatalf("Node %d: ETH price incorrect: %v", i, merged.Prices["ETH"])
			}
			return true
		}, 2*time.Second, 5*time.Millisecond, "update never received")
	}
}

func TestGossipSubOracleUpdates_FeeRateUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMeshNodes = 3
	nodes, _, _ := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, 0, func(int) int { return 0 })
	// Node 1 publishes fee rate updates
	now := time.Now()
	update := newFeeRateUpdate("test-source", now, map[oracle.Network]*big.Int{
		"Bitcoin":  big.NewInt(100),
		"Ethereum": big.NewInt(50),
	})
	if err := nodes[1].gossipSub.publishOracleUpdate(ctx, update); err != nil {
		t.Fatalf("Failed to publish oracle update: %v", err)
	}

	// Verify that all nodes received and merged the updates
	for i := 0; i < numMeshNodes; i++ {
		requireEventually(t, func() bool {

			oracle := nodes[i].oracle.(*tOracle)
			oracle.mtx.Lock()
			mergedCount := len(oracle.merged)
			oracle.mtx.Unlock()

			if mergedCount != 1 {
				return false
			}

			oracle.mtx.Lock()
			merged := oracle.merged[0]
			oracle.mtx.Unlock()

			if merged.Source != "test-source" {
				t.Fatalf("Node %d: expected source 'test-source', got %s", i, merged.Source)
			}
			if len(merged.FeeRates) != 2 {
				t.Fatalf("Node %d: expected 2 fee rates, got %d", i, len(merged.FeeRates))
			}
			if merged.FeeRates["Bitcoin"].Cmp(big.NewInt(100)) != 0 {
				t.Fatalf("Node %d: Bitcoin fee rate incorrect: %v", i, merged.FeeRates["Bitcoin"])
			}
			if merged.FeeRates["Ethereum"].Cmp(big.NewInt(50)) != 0 {
				t.Fatalf("Node %d: Ethereum fee rate incorrect: %v", i, merged.FeeRates["Ethereum"])
			}
			return true
		}, 2*time.Second, 5*time.Millisecond, "update never received")
	}
}

func TestGossipSubOracleUpdates_MultipleNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMeshNodes = 4
	nodes, _, _ := fullyConnectedMeshWithClients(ctx, t, numMeshNodes, 0, func(int) int { return 0 })

	now := time.Now()

	// Node 0 publishes price updates
	if err := nodes[0].gossipSub.publishOracleUpdate(ctx, newPriceUpdate("node-0", now, map[oracle.Ticker]float64{
		"BTC": 50000.0,
	})); err != nil {
		t.Fatalf("Failed to publish price update from node 0: %v", err)
	}

	// Node 1 publishes fee rate updates
	if err := nodes[1].gossipSub.publishOracleUpdate(ctx, newFeeRateUpdate("node-1", now, map[oracle.Network]*big.Int{
		"Bitcoin": big.NewInt(100),
	})); err != nil {
		t.Fatalf("Failed to publish fee rate update from node 1: %v", err)
	}

	// Node 2 publishes price updates
	if err := nodes[2].gossipSub.publishOracleUpdate(ctx, newPriceUpdate("node-2", now, map[oracle.Ticker]float64{
		"ETH": 3000.0,
	})); err != nil {
		t.Fatalf("Failed to publish price update from node 2: %v", err)
	}

	// Verify all nodes received all 3 updates (2 price + 1 fee rate)
	for i := range nodes {
		orc := nodes[i].oracle.(*tOracle)
		requireEventually(t, func() bool {
			orc.mtx.Lock()
			mergedCount := len(orc.merged)
			orc.mtx.Unlock()

			if mergedCount == 3 {
				return true
			}
			return false
		}, 2*time.Second, 5*time.Millisecond, "update never received")
	}
}

func TestGossipSubOracleUpdates_ClientDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMeshNodes = 2
	const numClients = 2

	mesh, err := mocknet.WithNPeers(numMeshNodes + numClients)
	if err != nil {
		t.Fatal(err)
	}

	allPeers := mesh.Peers()
	meshHosts := make([]host.Host, numMeshNodes)
	for i := range meshHosts {
		meshHosts[i] = mesh.Host(allPeers[i])
	}

	clientHosts := make([]host.Host, numClients)
	for i := range clientHosts {
		clientHosts[i] = mesh.Host(allPeers[numMeshNodes+i])
	}

	// Create whitelist
	whitelistPeerIDs := make([]peer.ID, numMeshNodes)
	for i, h := range meshHosts {
		whitelistPeerIDs[i] = h.ID()
	}
	mockWhitelist := types.NewWhitelist(whitelistPeerIDs)

	// Create nodes with custom oracles that return updated prices/fee rates
	nodes := make([]*TatankaNode, numMeshNodes)
	oracles := make([]*tOracle, numMeshNodes)
	for i := range nodes {
		dir := t.TempDir()
		testOracle := newTOracle()
		// Set up the oracle to return updates when MergePrices is called
		testOracle.SetPrices(map[oracle.Ticker]float64{
			"BTC": 50001.0,
		})
		testOracle.SetFeeRates(map[oracle.Network]*big.Int{
			"BTC": big.NewInt(101),
		})
		oracles[i] = testOracle
		nodes[i] = newTestNodeWithOracle(t, ctx, meshHosts[i], dir, mockWhitelist, testOracle)
	}

	connectPeers := func(peerA, peerB peer.ID) {
		t.Helper()
		if _, err := mesh.LinkPeers(peerA, peerB); err != nil {
			t.Fatalf("Failed to link mesh peers: %v", err)
		}
		if _, err := mesh.ConnectPeers(peerA, peerB); err != nil {
			t.Fatalf("Failed to connect mesh peers: %v", err)
		}
		requireEventually(t, func() bool {
			return mesh.Net(peerA).Connectedness(peerB) == network.Connected
		}, time.Second, 5*time.Millisecond, "failed to connect mesh peers")
	}

	connectPeers(meshHosts[0].ID(), meshHosts[1].ID())

	// Create clients and connect them to nodes
	clients := make([]*testClient, numClients)
	for i := range clients {
		nodeIdx := i % numMeshNodes
		connectPeers(clientHosts[i].ID(), meshHosts[nodeIdx].ID())
		clients[i], err = newTestClient(ctx, clientHosts[i], meshHosts[nodeIdx].ID())
		if err != nil {
			t.Fatalf("Failed to create client %d: %v", i, err)
		}
	}

	subscribeToTopic := func(peerIdx int, topic string) {
		t.Helper()
		if err := clients[peerIdx].Subscribe(ctx, topic); err != nil {
			t.Fatalf("Failed to subscribe client %d to topic %s: %v", peerIdx, topic, err)
		}
		peerID := clients[peerIdx].host.ID()
		meshNode := nodes[peerIdx%numMeshNodes]
		requireEventually(t, func() bool {
			subs := meshNode.subTrie.Subscribers([]string{topic})
			return slices.Contains(subs[topic], peerID)
		}, time.Second, 5*time.Millisecond, "failed to subscribe client %d to topic %s", peerIdx, topic)
	}

	// Subscribe clients to oracle topics
	subscribeToTopic(0, oraclePricesTopic)
	subscribeToTopic(1, oracleFeeRatesTopic)

	// Node 0 publishes price updates via gossipsub
	now := time.Now()
	if err := nodes[0].gossipSub.publishOracleUpdate(ctx, newPriceUpdate("test-source", now, map[oracle.Ticker]float64{
		"BTC": 50000.0,
	})); err != nil {
		t.Fatalf("Failed to publish price update: %v", err)
	}

	requireEventually(t, func() bool {
		p, found := nodes[1].oracle.Price("BTC")
		if !found || p != 50000.0 {
			return false
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "Failed to publish price update")

	// Node 1 publishes fee rate updates via gossipsub
	if err := nodes[1].gossipSub.publishOracleUpdate(ctx, newFeeRateUpdate("test-source", now, map[oracle.Network]*big.Int{
		"BTC": big.NewInt(100),
	})); err != nil {
		t.Fatalf("Failed to publish fee rate update: %v", err)
	}

	requireEventually(t, func() bool {
		p, found := nodes[0].oracle.FeeRate("BTC")
		if !found || p.Cmp(big.NewInt(100)) != 0 {
			return false
		}
		return true
	}, 2*time.Second, 5*time.Millisecond, "Failed to publish price update")

	// Terminate clients
	for idx := range clients {
		clients[idx].Close()
	}
}

// proposeOnAll proposes the same whitelist on every node.
func proposeOnAll(t *testing.T, nodes []*TatankaNode, proposed *types.Whitelist) {
	t.Helper()
	for i, n := range nodes {
		if err := n.whitelistManager.proposeWhitelist(proposed); err != nil {
			t.Fatalf("node %d: proposeWhitelist: %v", i, err)
		}
	}
}

// waitTransitionComplete waits until every node's proposal is cleared,
// indicating the transition committed.
func waitTransitionComplete(t *testing.T, nodes []*TatankaNode) {
	t.Helper()
	for i, n := range nodes {
		requireEventually(t, func() bool {
			return n.whitelistManager.getLocalWhitelistState().Proposed == nil
		}, 30*time.Second, 5*time.Millisecond,
			"node %d did not complete whitelist transition", i)
	}
}

// TestWhitelistTransition_AllAgree verifies the end-to-end wiring: all nodes
// propose the same whitelist, the proposals propagate via gossipsub, the
// whitelist manager reaches consensus, and the new whitelist is committed.
func TestWhitelistTransition_AllAgree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes, wl, _ := fullyConnectedMeshWithClients(ctx, t, 3, 0, func(int) int { return 0 })

	proposeOnAll(t, nodes, wl)
	waitTransitionComplete(t, nodes)

	// After commit, every node's current whitelist should match the proposal.
	for i, n := range nodes {
		cur := n.whitelistManager.getLocalWhitelistState().Current
		if !cur.Equals(wl) {
			t.Fatalf("node %d: current whitelist doesn't match proposed after transition", i)
		}
	}
}

// TestWhitelistTransition_AddNode verifies that adding a new node via whitelist
// transition works end-to-end: existing nodes propose a whitelist that includes
// a new node, the transition completes, and the connection manager reconciles
// trackers so the new node becomes connected.
func TestWhitelistTransition_AddNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numPeers = 3
	mnet, err := mocknet.WithNPeers(numPeers)
	if err != nil {
		t.Fatal(err)
	}

	hosts := mnet.Hosts()
	whitelistPeerIDs := make([]peer.ID, numPeers)
	for i, h := range hosts {
		whitelistPeerIDs[i] = h.ID()
	}
	initialWL := types.NewWhitelist(whitelistPeerIDs)

	existingNodes := make([]*TatankaNode, 0, numPeers)
	for i, h := range hosts {
		if err := linkNodeWithMesh(mnet, h, existingNodes, true); err != nil {
			t.Fatalf("Failed to link node %d: %v", i, err)
		}
		existingNodes = append(existingNodes, newTestNode(t, ctx, h, t.TempDir(), initialWL))
	}
	checkFullyConnected(t, existingNodes)

	// Create the 4th host and link it to the existing mesh.
	newHost, err := mnet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range existingNodes {
		if _, err := mnet.LinkPeers(newHost.ID(), n.node.ID()); err != nil {
			t.Fatalf("LinkPeers: %v", err)
		}
		newHost.Peerstore().AddAddrs(n.node.ID(), n.node.Addrs(), peerstore.PermanentAddrTTL)
		n.node.Peerstore().AddAddrs(newHost.ID(), newHost.Addrs(), peerstore.PermanentAddrTTL)
	}

	// Proposed whitelist: all 3 existing + new node.
	proposedIDs := make([]peer.ID, 0, 4)
	for _, n := range existingNodes {
		proposedIDs = append(proposedIDs, n.node.ID())
	}
	proposedIDs = append(proposedIDs, newHost.ID())
	proposedWL := types.NewWhitelist(proposedIDs)

	// Start the new node with the proposed whitelist as its initial whitelist.
	_ = newTestNode(t, ctx, newHost, t.TempDir(), proposedWL)

	// All existing nodes propose the new whitelist.
	proposeOnAll(t, existingNodes, proposedWL)
	waitTransitionComplete(t, existingNodes)

	// After transition, the existing nodes should have reconciled trackers
	// and connected to the new node.
	for i, n := range existingNodes {
		requireEventually(t, func() bool {
			return n.node.Network().Connectedness(newHost.ID()) == network.Connected
		}, 15*time.Second, 200*time.Millisecond,
			"node %d not connected to new node after transition", i)
	}
}

// TestWhitelistTransition_RemoveNode verifies that removing a node via
// whitelist transition works: overlap nodes agree, the transition completes,
// and the connection manager stops tracking the removed node.
func TestWhitelistTransition_RemoveNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes, _, _ := fullyConnectedMeshWithClients(ctx, t, 3, 0, func(int) int { return 0 })

	// Proposed whitelist: only nodes 0 and 1 (remove node 2).
	proposed := types.NewWhitelist([]peer.ID{nodes[0].node.ID(), nodes[1].node.ID()})

	// Both overlap nodes propose.
	for i := 0; i < 2; i++ {
		if err := nodes[i].whitelistManager.proposeWhitelist(proposed); err != nil {
			t.Fatalf("node %d: proposeWhitelist: %v", i, err)
		}
	}

	// Transition should complete on the overlap nodes.
	waitTransitionComplete(t, nodes[:2])

	// After transition, the removed node's tracker should be stopped.
	removedID := nodes[2].node.ID()
	for i := 0; i < 2; i++ {
		requireEventually(t, func() bool {
			nodes[i].connectionManager.trackersMtx.RLock()
			_, tracked := nodes[i].connectionManager.peerTrackers[removedID]
			nodes[i].connectionManager.trackersMtx.RUnlock()
			return !tracked
		}, 10*time.Second, 5*time.Millisecond,
			"node %d still tracking removed node", i)
	}
}
