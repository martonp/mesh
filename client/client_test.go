package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bisoncraft/mesh/bond"
	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/peer"
)

type relayMessageParams struct {
	PeerID  peer.ID
	Message []byte
}

type broadcastParams struct {
	Topic string
	Data  []byte
}

type relayMessageReturnValue struct {
	resp []byte
	err  error
}

type tMeshConnection struct {
	mu sync.Mutex

	remotePeer peer.ID

	broadcastCalls []broadcastParams
	broadcastErr   error

	subscribeCalls [][]string
	subscribeErr   error

	unsubscribeCalls [][]string
	unsubscribeErr   error

	postBondCalls []struct{}
	postBondErr   error

	fetchNodes []peer.AddrInfo

	runErrCh chan error

	KillCalls int
}

var _ meshConn = (*tMeshConnection)(nil)

func newTMeshConnection(remotePeer peer.ID) *tMeshConnection {
	return &tMeshConnection{
		remotePeer: remotePeer,
		runErrCh:   make(chan error, 1),
		fetchNodes: []peer.AddrInfo{},
	}
}

func (m *tMeshConnection) remotePeerID() peer.ID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.remotePeer
}

func (m *tMeshConnection) broadcast(ctx context.Context, topic string, data []byte) error {
	m.mu.Lock()
	call := broadcastParams{
		Topic: topic,
		Data:  append([]byte(nil), data...),
	}
	m.broadcastCalls = append(m.broadcastCalls, call)
	m.mu.Unlock()

	return m.broadcastErr
}

func (m *tMeshConnection) subscribe(ctx context.Context, topics []string) error {
	m.mu.Lock()
	m.subscribeCalls = append(m.subscribeCalls, topics)
	m.mu.Unlock()

	return m.subscribeErr
}

func (m *tMeshConnection) unsubscribe(ctx context.Context, topics []string) error {
	m.mu.Lock()
	m.unsubscribeCalls = append(m.unsubscribeCalls, topics)
	m.mu.Unlock()

	return m.unsubscribeErr
}

func (m *tMeshConnection) postAllBonds(ctx context.Context) error {
	m.mu.Lock()
	m.postBondCalls = append(m.postBondCalls, struct{}{})
	m.mu.Unlock()

	return m.postBondErr
}

func (m *tMeshConnection) addBonds(ctx context.Context, bonds []*bond.BondParams) error {
	return nil
}

func (m *tMeshConnection) searchTopics(ctx context.Context, filters []string) ([]string, error) {
	return filters, nil
}

func (m *tMeshConnection) kill() {}

func (m *tMeshConnection) run(ctx context.Context) error {
	if m.runErrCh != nil {
		select {
		case err := <-m.runErrCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func (m *tMeshConnection) fetchAvailableMeshNodes(ctx context.Context) ([]peer.AddrInfo, error) {
	m.mu.Lock()
	nodes := append([]peer.AddrInfo(nil), m.fetchNodes...)
	m.mu.Unlock()
	return nodes, nil
}

func (m *tMeshConnection) waitReady(ctx context.Context) {
}

func (m *tMeshConnection) fail(err error) {
	m.runErrCh <- err
}

// setTestMeshConnection sets up a mock mesh connection for testing.
func (c *Client) setTestMeshConnection(mc meshConn) {
	if c.connManager == nil {
		c.connManager = &meshConnectionManager{}
	}
	c.connManager.setPrimaryConnection(mc)
}

func TestSubscribe(t *testing.T) {
	ctx := context.Background()

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{},
		topicRegistry: newTopicRegistry(),
	}
	c.setTestMeshConnection(mc)

	// Subscribe to a topic successfully.
	err := c.Subscribe(ctx, []string{"topic-1"}, func(topic string, ev TopicEvent) {})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", got)
	}
	if mc.subscribeCalls[0][0] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.subscribeCalls[0])
	}

	// Subscribe to the same topic again should return ErrRedundantSubscription and not call mesh conn.
	err = c.Subscribe(ctx, []string{"topic-1"}, func(topic string, ev TopicEvent) {})
	if !errors.Is(err, ErrRedundantSubscription) {
		t.Fatalf("expected ErrRedundantSubscription, got %v", err)
	}
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected subscribe call count to remain 1, got %d", got)
	}

	// Subscribe to another topic when meshConn returns an error.
	wantErr := errors.New("subscribe failed")
	mc.subscribeErr = wantErr
	err = c.Subscribe(ctx, []string{"topic-2"}, func(topic string, ev TopicEvent) {})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected error %v, got %v", wantErr, err)
	}
	if got := len(mc.subscribeCalls); got != 2 {
		t.Fatalf("expected 2 subscribe calls, got %d", got)
	}
	if mc.subscribeCalls[1][0] != "topic-2" {
		t.Fatalf("expected topic %q, got %q", "topic-2", mc.subscribeCalls[1])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-2"); err == nil {
		t.Fatalf("topic-2 should not be registered when subscribe returns an error")
	}

	// Unsubscribe from topic-1 with an error from meshConn.
	wantUnsubErr := errors.New("unsubscribe failed")
	mc.unsubscribeErr = wantUnsubErr
	err = c.Unsubscribe(ctx, "topic-1")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, wantUnsubErr) {
		t.Fatalf("expected error %v, got %v", wantUnsubErr, err)
	}
	if got := len(mc.unsubscribeCalls); got != 1 {
		t.Fatalf("expected 1 unsubscribe call, got %d", got)
	}
	if mc.unsubscribeCalls[0][0] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.unsubscribeCalls[0])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-1"); err != nil {
		t.Fatalf("topic-1 should remain registered when unsubscribe returns an error")
	}

	// Unsubscribe without error.
	mc.unsubscribeErr = nil
	err = c.Unsubscribe(ctx, "topic-1")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if got := len(mc.unsubscribeCalls); got != 2 {
		t.Fatalf("expected 2 unsubscribe calls, got %d", got)
	}
	if mc.unsubscribeCalls[1][0] != "topic-1" {
		t.Fatalf("expected topic %q, got %q", "topic-1", mc.unsubscribeCalls[1])
	}
	if _, err := c.topicRegistry.fetchHandler("topic-1"); err == nil {
		t.Fatalf("topic-1 should be unregistered after successful unsubscribe")
	}

	// Unsubscribe again should return ErrRedundantUnsubscription and not call mesh connection.
	err = c.Unsubscribe(ctx, "topic-1")
	if !errors.Is(err, ErrRedundantUnsubscription) {
		t.Fatalf("expected ErrRedundantUnsubscription, got %v", err)
	}
	if got := len(mc.unsubscribeCalls); got != 2 {
		t.Fatalf("expected unsubscribe call count to remain 2, got %d", got)
	}
}

func TestPushMessage(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	type handled struct {
		topic string
		ev    TopicEvent
	}
	handledCh := make(chan handled, 10)

	handler1 := func(topic string, ev TopicEvent) { handledCh <- handled{topic: "topic-1", ev: ev} }
	handler2 := func(topic string, ev TopicEvent) { handledCh <- handled{topic: "topic-2", ev: ev} }

	if err := c.Subscribe(ctx, []string{"topic-1"}, handler1); err != nil {
		t.Fatalf("subscribe topic-1: %v", err)
	}
	if err := c.Subscribe(ctx, []string{"topic-2"}, handler2); err != nil {
		t.Fatalf("subscribe topic-2: %v", err)
	}

	sender := peer.ID("sender-peer")

	// BROADCAST on topic-1 should call handler1 with TopicEventData and data payload.
	broadcastData := []byte("hello")
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_BROADCAST,
		Topic:       "topic-1",
		Data:        broadcastData,
		Sender:      []byte(sender),
	})

	// SUBSCRIBE on topic-2 should call handler2 with TopicEventPeerSubscribed.
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_SUBSCRIBE,
		Topic:       "topic-2",
		Sender:      []byte(sender),
	})

	// UNSUBSCRIBE on topic-1 should call handler1 with TopicEventPeerUnsubscribed.
	c.handlePushMessage(&protocolsPb.PushMessage{
		MessageType: protocolsPb.PushMessage_UNSUBSCRIBE,
		Topic:       "topic-1",
		Sender:      []byte(sender),
	})

	// Validate exactly 3 handler invocations and that the correct handler was used per topic.
	var got []handled
	for range 3 {
		select {
		case h := <-handledCh:
			got = append(got, h)
		default:
			t.Fatalf("expected 3 handled events, got %d", len(got))
		}
	}

	// Ensure no additional handler invocations occurred.
	select {
	case extra := <-handledCh:
		t.Fatalf("unexpected extra handled event for topic %q: %+v", extra.topic, extra.ev)
	default:
	}

	// Helper to find the next event for a topic.
	nextFor := func(topic string) (TopicEvent, bool) {
		for i, h := range got {
			if h.topic == topic {
				ev := h.ev
				got = append(got[:i], got[i+1:]...)
				return ev, true
			}
		}
		return TopicEvent{}, false
	}

	ev, ok := nextFor("topic-1")
	if !ok {
		t.Fatalf("expected an event for topic-1")
	}
	if ev.Type != TopicEventData {
		t.Fatalf("topic-1 first event: expected TopicEventData, got %v", ev.Type)
	}
	if string(ev.Data) != string(broadcastData) {
		t.Fatalf("topic-1 first event: expected data %q, got %q", string(broadcastData), string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-1 first event: expected peer %q, got %q", sender, ev.Peer)
	}

	ev, ok = nextFor("topic-2")
	if !ok {
		t.Fatalf("expected an event for topic-2")
	}
	if ev.Type != TopicEventPeerSubscribed {
		t.Fatalf("topic-2 event: expected TopicEventPeerSubscribed, got %v", ev.Type)
	}
	if len(ev.Data) != 0 {
		t.Fatalf("topic-2 event: expected empty data, got %q", string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-2 event: expected peer %q, got %q", sender, ev.Peer)
	}

	ev, ok = nextFor("topic-1")
	if !ok {
		t.Fatalf("expected a second event for topic-1")
	}
	if ev.Type != TopicEventPeerUnsubscribed {
		t.Fatalf("topic-1 second event: expected TopicEventPeerUnsubscribed, got %v", ev.Type)
	}
	if len(ev.Data) != 0 {
		t.Fatalf("topic-1 second event: expected empty data, got %q", string(ev.Data))
	}
	if ev.Peer != sender {
		t.Fatalf("topic-1 second event: expected peer %q, got %q", sender, ev.Peer)
	}

	if len(got) != 0 {
		t.Fatalf("unexpected extra handled events: %d", len(got))
	}
}

func TestConcurrentBroadcasts(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	testTopic := "concurrent-broadcast-test"

	// Subscribe to the test topic.
	err := c.Subscribe(ctx, []string{testTopic}, func(topic string, ev TopicEvent) {})
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic %s: %v", testTopic, err)
	}

	// Verify subscribe was called.
	if got := len(mc.subscribeCalls); got != 1 {
		t.Fatalf("expected 1 subscribe call, got %d", got)
	}
	if mc.subscribeCalls[0][0] != testTopic {
		t.Fatalf("expected topic %q, got %q", testTopic, mc.subscribeCalls[0])
	}

	// Launch concurrent broadcasts to the same topic.
	workers := 20
	var wg sync.WaitGroup
	wg.Add(workers)

	errs := make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			data := fmt.Appendf(nil, "message-%d", idx)
			err := c.Broadcast(ctx, testTopic, data)
			if err != nil {
				errs <- fmt.Errorf("broadcast %d failed: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Verify no errors occurred.
	for err := range errs {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify all broadcast messages were received by the mock.
	mc.mu.Lock()
	receivedCount := len(mc.broadcastCalls)
	broadcastCalls := make([]broadcastParams, len(mc.broadcastCalls))
	copy(broadcastCalls, mc.broadcastCalls)
	mc.mu.Unlock()

	if receivedCount != workers {
		t.Fatalf("Expected %d broadcast calls, got %d", workers, receivedCount)
	}

	// Verify all messages are present.
	receivedMessages := make(map[string]bool)
	for _, call := range broadcastCalls {
		if call.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, call.Topic)
		}
		receivedMessages[string(call.Data)] = true
	}

	// Verify we received all unique messages.
	for i := range workers {
		expectedData := fmt.Sprintf("message-%d", i)
		if !receivedMessages[expectedData] {
			t.Fatalf("Expected to receive message %s", expectedData)
		}
	}
}

func TestConcurrentSubscribeUnsubscribe(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	testTopic := "concurrent-test"
	workers := 10

	// Track which handlers were registered.
	var handlerCalls atomic.Int32
	handlers := make([]func(string, TopicEvent), workers)
	for i := range workers {
		idx := i
		handlers[idx] = func(_ string, ev TopicEvent) {
			handlerCalls.Add(int32(idx + 1))
		}
	}

	// Launch concurrent subscribe attempts.
	var wg sync.WaitGroup
	wg.Add(workers)

	errs := make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			err := c.Subscribe(ctx, []string{testTopic}, handlers[idx])
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Count errors - we expect workers-1 redundant subscription errors.
	var redundantSubErrors int
	for err := range errs {
		if errors.Is(err, ErrRedundantSubscription) {
			redundantSubErrors++
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if redundantSubErrors != workers-1 {
		t.Fatalf("Expected %d redundant subscription errors, got %d", workers-1, redundantSubErrors)
	}

	// Verify only one topic is registered.
	topics := c.topicRegistry.fetchTopics()
	if len(topics) != 1 {
		t.Fatalf("Expected 1 registered topic, got %d", len(topics))
	}

	if topics[0] != testTopic {
		t.Fatalf("Expected topic %s, got %s", testTopic, topics[0])
	}

	// Launch concurrent unsubscribe attempts.
	wg.Add(workers)

	errs = make(chan error, workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			err := c.Unsubscribe(ctx, testTopic)
			if err != nil {
				errs <- err
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	// Count errors - we expect workers-1 redundant unsubscription errors.
	var redundantUnsubErrors int
	for err := range errs {
		if errors.Is(err, ErrRedundantUnsubscription) {
			redundantUnsubErrors++
		} else {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	if redundantUnsubErrors != workers-1 {
		t.Fatalf("Expected %d redundant unsubscription errors, got %d", workers-1, redundantUnsubErrors)
	}

	// Verify no topics are registered.
	topics = c.topicRegistry.fetchTopics()
	if len(topics) != 0 {
		t.Fatalf("Expected 0 registered topics, got %d", len(topics))
	}
}

func TestMalformedInput(t *testing.T) {
	ctx := context.Background()

	logBackend := slog.NewBackend(os.Stdout)
	logger := logBackend.Logger("client_test")

	mc := newTMeshConnection(randomPeerID(t))

	c := &Client{
		cfg:           &Config{Logger: logger},
		topicRegistry: newTopicRegistry(),
		log:           logger,
	}
	c.setTestMeshConnection(mc)

	emptyTopic := ""

	// Test 1: Subscribe with empty topic name should be rejected.
	t.Run("Subscribe with empty topic", func(t *testing.T) {
		err := c.Subscribe(ctx, []string{emptyTopic}, func(topic string, ev TopicEvent) {})
		if err == nil {
			t.Fatal("Expected error when subscribing with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		subCallCount := len(mc.subscribeCalls)
		mc.mu.Unlock()

		if subCallCount != 0 {
			t.Fatalf("Expected 0 subscribe calls, got %d", subCallCount)
		}

		// Verify empty topic was not registered.
		topics := c.topicRegistry.fetchTopics()
		for _, topic := range topics {
			if topic == emptyTopic {
				t.Fatal("Empty topic should not be registered")
			}
		}
	})

	// Test 2: Unsubscribe from empty topic should be rejected.
	t.Run("Unsubscribe from empty topic", func(t *testing.T) {
		err := c.Unsubscribe(ctx, emptyTopic)
		if err == nil {
			t.Fatal("Expected error when unsubscribing with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		unsubCallCount := len(mc.unsubscribeCalls)
		mc.mu.Unlock()

		if unsubCallCount != 0 {
			t.Fatalf("Expected 0 unsubscribe calls, got %d", unsubCallCount)
		}
	})

	// Test 3: Broadcast with empty topic name should be rejected.
	t.Run("Broadcast with empty topic", func(t *testing.T) {
		err := c.Broadcast(ctx, emptyTopic, []byte("test-data"))
		if err == nil {
			t.Fatal("Expected error when broadcasting with empty topic, got nil")
		}
		if !errors.Is(err, errEmptyTopic) {
			t.Fatalf("Expected ErrEmptyTopic, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		broadcastCallCount := len(mc.broadcastCalls)
		mc.mu.Unlock()

		if broadcastCallCount != 0 {
			t.Fatalf("Expected 0 broadcast calls, got %d", broadcastCallCount)
		}
	})

	// Test 4: Subscribe with nil handler should be rejected.
	t.Run("Subscribe with nil handler", func(t *testing.T) {
		validTopic := "valid-topic"
		err := c.Subscribe(ctx, []string{validTopic}, nil)
		if err == nil {
			t.Fatal("Expected error when subscribing with nil handler, got nil")
		}
		if !strings.Contains(err.Error(), "handler function cannot be nil") {
			t.Fatalf("Expected a nil handler error, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		subCallCount := len(mc.subscribeCalls)
		mc.mu.Unlock()

		if subCallCount != 0 {
			t.Fatalf("Expected 0 subscribe calls, got %d", subCallCount)
		}

		// Verify the topic was not registered.
		topics := c.topicRegistry.fetchTopics()
		for _, topic := range topics {
			if topic == validTopic {
				t.Fatal("Topic should not be registered after nil handler error")
			}
		}
	})

	// Test 5: Broadcast with nil data should be rejected.
	t.Run("Broadcast with nil data", func(t *testing.T) {
		validTopic := "valid-topic-for-broadcast"
		err := c.Broadcast(ctx, validTopic, nil)
		if err == nil {
			t.Fatal("Expected error when broadcasting with nil data, got nil")
		}
		if !strings.Contains(err.Error(), "data cannot be nil") {
			t.Fatalf("Expected a nil data error, got %v", err)
		}

		// Verify no calls were made to the mesh connection.
		mc.mu.Lock()
		broadcastCallCount := len(mc.broadcastCalls)
		mc.mu.Unlock()

		if broadcastCallCount != 0 {
			t.Fatalf("Expected 0 broadcast calls, got %d", broadcastCallCount)
		}
	})
}
