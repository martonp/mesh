package codec

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/protocols"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	ma "github.com/multiformats/go-multiaddr"
)

func createHost(t *testing.T, port int) (host.Host, string) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	addr := fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	host, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(addr),
	)
	if err != nil {
		t.Fatal(err)
	}

	return host, fmt.Sprintf("%s/p2p/%s", addr, host.ID().String())
}

func TestReadWriteHelpers(t *testing.T) {
	// Create host A.
	hostAPort := 5577
	hostA, hostAAddr := createHost(t, hostAPort)
	defer func() { _ = hostA.Close() }()

	hostAReceivedMsgs := make(chan proto.Message, 10)
	hostASendHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		// Ensure reading messages succeeds.
		id := s.Conn().RemotePeer()
		msg := &protocolsPb.PublishRequest{}
		if err := ReadLengthPrefixedMessage(s, msg); err != nil {
			t.Fatalf("Failed to read message %s: %v", id, err)
			return
		}

		hostAReceivedMsgs <- msg
	}

	hostA.SetStreamHandler(protocols.ClientPublishProtocol, hostASendHandler)

	// Create host B.
	hostBPort := 5588
	hostB, _ := createHost(t, hostBPort)
	defer func() { _ = hostB.Close() }()

	hostBReceivedMsgs := make(chan proto.Message, 10)
	hostBSendHandler := func(s network.Stream) {
		defer func() { _ = s.Close() }()

		// Ensure reading messages with a timeout succeeds.
		id := s.Conn().RemotePeer()
		msg := &protocolsPb.PublishRequest{}
		if err := ReadLengthPrefixedMessage(s, msg, time.Second*5); err != nil {
			t.Fatalf("Failed to read message %s: %v", id, err)
			return
		}

		hostBReceivedMsgs <- msg
	}

	hostB.SetStreamHandler(protocols.ClientPublishProtocol, hostBSendHandler)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	aAddr, err := ma.NewMultiaddr(hostAAddr)
	if err != nil {
		t.Fatalf("Failed to parse host address: %v", err)
	}

	// Connect B to A.
	err = hostB.Connect(ctx, peer.AddrInfo{
		ID:    hostA.ID(),
		Addrs: []ma.Multiaddr{aAddr},
	})
	if err != nil {
		t.Fatalf("Failed to connect B to A: %v", err)
	}

	// Ensure writing messages succeeds.
	s, err := hostB.NewStream(ctx, hostA.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to create a stream to A: %v", err)
	}

	defer func() { _ = s.Close() }()

	testTopic := "test"
	data := []byte("hello")
	msg := &protocolsPb.PublishRequest{
		Topic: testTopic,
		Data:  data,
	}

	err = WriteLengthPrefixedMessage(s, msg)
	if err != nil {
		t.Fatalf("Failed to write message to host B: %v", err)
	}

	select {
	case rcvMsg := <-hostAReceivedMsgs:
		pubMsg, ok := rcvMsg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a PublishRequest")
		}

		if pubMsg.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, pubMsg.Topic)
		}

		if !bytes.Equal(pubMsg.Data, data) {
			t.Fatalf("Expected data %s, got %s", string(data), string(pubMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive message")
	}

	// Ensure writing messages with a timeout succeeds.
	s, err = hostA.NewStream(ctx, hostB.ID(), protocols.ClientPublishProtocol)
	if err != nil {
		t.Fatalf("Failed to create a stream to A: %v", err)
	}

	defer func() { _ = s.Close() }()

	err = WriteLengthPrefixedMessage(s, msg, time.Second*5)
	if err != nil {
		t.Fatalf("Failed to write message to host B: %v", err)
	}

	select {
	case rcvMsg := <-hostBReceivedMsgs:
		pubMsg, ok := rcvMsg.(*protocolsPb.PublishRequest)
		if !ok {
			t.Fatal("Received message is not a PublishRequest")
		}

		if pubMsg.Topic != testTopic {
			t.Fatalf("Expected topic %s, got %s", testTopic, pubMsg.Topic)
		}

		if !bytes.Equal(pubMsg.Data, data) {
			t.Fatalf("Expected data %s, got %s", string(data), string(pubMsg.Data))
		}

	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting to receive message")
	}
}
