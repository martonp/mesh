package tatanka

import (
	"testing"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestPeerInfoRoundTrip(t *testing.T) {
	// Create sample multiaddrs
	addr1, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	if err != nil {
		t.Fatalf("Failed to create multiaddr1: %v", err)
	}
	addr2, err := ma.NewMultiaddr("/ip6/::1/tcp/5678")
	if err != nil {
		t.Fatalf("Failed to create multiaddr2: %v", err)
	}

	// Create sample peer.AddrInfo
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	originalID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to get peer ID from public key: %v", err)
	}
	original := peer.AddrInfo{
		ID:    originalID,
		Addrs: []ma.Multiaddr{addr1, addr2},
	}

	// Test round-trip: libp2p to PB to libp2p
	pbPeer := libp2pPeerInfoToPb(original)
	roundTrip, err := pbPeerInfoToLibp2p(pbPeer)
	if err != nil {
		t.Fatalf("Failed to convert back from PB: %v", err)
	}

	// Assert IDs match
	if roundTrip.ID != original.ID {
		t.Errorf("Expected ID %s, got %s", original.ID, roundTrip.ID)
	}

	// Assert addrs match (order may not preserve, so check lengths and contents)
	if len(roundTrip.Addrs) != len(original.Addrs) {
		t.Errorf("Expected %d addrs, got %d", len(original.Addrs), len(roundTrip.Addrs))
	}
	addrMap := make(map[string]bool)
	for _, addr := range original.Addrs {
		addrMap[addr.String()] = true
	}
	for _, addr := range roundTrip.Addrs {
		if !addrMap[addr.String()] {
			t.Errorf("Unexpected addr: %s", addr)
		}
	}

	// Test empty addrs
	originalEmpty := peer.AddrInfo{ID: originalID}
	pbEmpty := libp2pPeerInfoToPb(originalEmpty)
	roundTripEmpty, err := pbPeerInfoToLibp2p(pbEmpty)
	if err != nil {
		t.Fatalf("Failed empty round-trip: %v", err)
	}
	if roundTripEmpty.ID != originalEmpty.ID || len(roundTripEmpty.Addrs) != 0 {
		t.Error("Empty addrs round-trip failed")
	}

	// Test invalid PB (e.g., bad ID bytes)
	badPb := &protocolsPb.PeerInfo{Id: []byte("invalid")}
	_, err = pbPeerInfoToLibp2p(badPb)
	if err == nil {
		t.Error("Expected error for invalid peer ID")
	}

	// Test invalid addr bytes
	badAddrPb := &protocolsPb.PeerInfo{
		Id:    []byte(originalID),
		Addrs: [][]byte{[]byte("invalid addr")},
	}
	_, err = pbPeerInfoToLibp2p(badAddrPb)
	if err == nil {
		t.Error("Expected error for invalid multiaddr")
	}
}
