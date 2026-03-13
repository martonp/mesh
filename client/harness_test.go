//go:build harness

package client

// Client tests expect the tatanka test harness to be running a node.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"sync"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/decred/slog"
	"github.com/libp2p/go-libp2p/core/crypto"
)

func TestMain(m *testing.M) {
	m.Run()
}

// fetchNodeAddressAtIndex fetches the the address of the test harness node running at the provided index.
// The node address details are fetched from the saved test harness whitelist. This should be called after
// running the tatanka test harness.
func fetchNodeAddressAtIndex(index int) (string, error) {
	curUser, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("failed to resolve current user: %w", err)
	}

	whitelistPath := fmt.Sprintf("%s/%s", curUser.HomeDir, ".tatanka-test/whitelist.json")
	whitelist := struct {
		Peers []struct {
			ID      string `json:"id"`
			Address string `json:"address"`
		} `json:"peers"`
	}{}

	data, err := os.ReadFile(whitelistPath)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(data, &whitelist); err != nil {
		return "", err
	}

	if index < 0 || index >= len(whitelist.Peers) {
		return "", fmt.Errorf("no node found at provided index: %d", index)
	}

	node := whitelist.Peers[index]
	if node.Address == "" {
		return "", fmt.Errorf("peer at index %d missing address", index)
	}
	if node.ID == "" {
		return "", fmt.Errorf("peer at index %d missing peer ID", index)
	}

	addr := fmt.Sprintf("%s/p2p/%s", node.Address, node.ID)

	return addr, nil
}

// cd tatanka-mesh
// go test -v -tags=harness -run=TestClientIntegration ./client
func TestClientIntegration(t *testing.T) {
	logBackend := slog.NewBackend(os.Stdout)
	loggerC1 := logBackend.Logger("c1")
	loggerC1.SetLevel(slog.LevelDebug)
	loggerC2 := logBackend.Logger("c2")
	loggerC2.SetLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c1Port := 12366
	c2Port := 12367

	nodeAddr, err := fetchNodeAddressAtIndex(0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("node address is: %s", nodeAddr)

	c1Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c2Priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c1Cfg := Config{
		RemotePeerAddrs: []string{nodeAddr},
		Port:            c1Port,
		PrivateKey:      c1Priv,
		Logger:          loggerC1,
	}

	c2Cfg := Config{
		RemotePeerAddrs: []string{nodeAddr},
		Port:            c2Port,
		PrivateKey:      c2Priv,
		Logger:          loggerC2,
	}

	// Create two clients (c1 & c2) and connect to the node.
	c1, err := NewClient(&c1Cfg)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := NewClient(&c2Cfg)
	if err != nil {
		t.Fatal(err)
	}

	bonds := []*bond.BondParams{}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := c1.Run(ctx, bonds)
		if err != nil {
			c1.cfg.Logger.Errorf("unexpected error running c1: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		err := c2.Run(ctx, bonds)
		if err != nil {
			c1.cfg.Logger.Errorf("unexpected error running c2: %v", err)
		}
	}()

	// Wait briefly for initialization.
	time.Sleep(time.Second)

	// Ensure both clients can subscribe to a topic.
	pingTopic := "ping"
	makePingHandler := func(c *Client, received chan TopicEvent) func(TopicEvent) {
		return func(evt TopicEvent) {
			senderID := evt.Peer
			switch evt.Type {
			case TopicEventPeerSubscribed:
				c.cfg.Logger.Infof("%s: received ping topic subscription event, triggered by %s", c.host.ID(), senderID)
			case TopicEventPeerUnsubscribed:
				c.cfg.Logger.Infof("%s: received ping topic unsubscribe event, triggered by %s", c.host.ID(), senderID)
			case TopicEventData:
				c.cfg.Logger.Infof("%s: received broadcast from %s", c.host.ID(), senderID)
			}
			received <- evt
		}
	}

	c1ReceivedEvts := make(chan TopicEvent, 10)
	c1PingHandler := makePingHandler(c1, c1ReceivedEvts)

	c2ReceivedEvts := make(chan TopicEvent, 10)
	c2PingHandler := makePingHandler(c2, c2ReceivedEvts)

	receiveWithTimeout := func(ch <-chan TopicEvent) TopicEvent {
		select {
		case evt := <-ch:
			return evt
		case <-time.After(time.Second * 2):
			t.Fatal("Timed out waiting for push message")
			return TopicEvent{}
		}
	}

	// Subscribe to the ping topic for both c1 and c2.
	err = c1.Subscribe(ctx, pingTopic, c1PingHandler)
	if err != nil {
		t.Fatalf("Unexpected error subscribing to topic for c1: %v", err)
	}

	// Wait briefly before subscribing c2.
	time.Sleep(time.Millisecond * 100)

	err = c2.Subscribe(ctx, pingTopic, c2PingHandler)
	if err != nil {
		t.Fatalf("unexpected error subscribing to topic for c2: %v", err)
	}

	// Ensure c1 gets the topic subscription event for c2.
	evt := receiveWithTimeout(c1ReceivedEvts)
	if evt.Type != TopicEventPeerSubscribed {
		t.Fatalf("Expected a topic subscription event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c2.host.ID())) {
		t.Fatalf("Expected subscription event peer to be %s, got %s", c2.host.ID().String(), evt.Peer.String())
	}

	// Ensure there are no outstanding messages to be processed for c1 and c2.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding events for c1, got %d", len(c1ReceivedEvts))
	}

	if len(c2ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c2, got %d", len(c2ReceivedEvts))
	}

	pongData := []byte("pong")

	// Ensure c2 receives a broadcast from c1.
	err = c1.Broadcast(ctx, pingTopic, []byte(pongData))
	if err != nil {
		t.Fatalf("Unexpected error broadcasting from c1, %v", err)
	}

	evt = receiveWithTimeout(c2ReceivedEvts)
	if evt.Type != TopicEventData {
		t.Fatalf("Expected a broadcast event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c1.host.ID())) {
		t.Fatalf("Expected broadcast event peer to be %s, got %s", c1.host.ID().String(), evt.Peer.String())
	}

	if string(evt.Data) != string(pongData) {
		t.Fatalf("Expected broadcasted data to be %s, got %s", pongData, string(evt.Data))
	}

	// Ensure c2 does not receive its own broadcast.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c1, got %d", len(c1ReceivedEvts))
	}

	// Ensure c1 receives c2s unsubscription notification.
	err = c2.Unsubscribe(ctx, pingTopic)
	if err != nil {
		t.Fatalf("Unexpected error unsubscribing from ping topic by c2 %v", err)
	}

	evt = receiveWithTimeout(c1ReceivedEvts)
	if evt.Type != TopicEventPeerUnsubscribed {
		t.Fatalf("Expected a topic unsubscribe event , got %v", evt.Type)
	}

	if !bytes.Equal([]byte(evt.Peer), []byte(c2.host.ID())) {
		t.Fatalf("Expected unsubscribe event peer to be %s, got %s", c2.host.ID().String(), evt.Peer.String())
	}

	// Ensure there are no outstanding messages to be processed for c1 and c2.
	if len(c1ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c1, got %d", len(c1ReceivedEvts))
	}

	if len(c2ReceivedEvts) != 0 {
		t.Fatalf("Expected no outstanding messages for c2, got %d", len(c2ReceivedEvts))
	}

	// Ensure the c1 and c2 can post bonds.
	err = c1.PostAllBonds(ctx)
	if err != nil {
		t.Fatalf("Unexpected error posting bonds for c1: %v", err)
	}

	err = c2.PostAllBonds(ctx)
	if err != nil {
		t.Fatalf("Unexpected error posting bonds for c2: %v", err)
	}

	// TODO: add relay test between c1 and c2.

	cancel()
	wg.Wait()
}
