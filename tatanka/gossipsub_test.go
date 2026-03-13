package tatanka

import (
	"reflect"
	"testing"
	"time"

	pb "github.com/bisoncraft/mesh/tatanka/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestClientConnectionUpdateRoundTrip(t *testing.T) {
	// Create valid peer IDs from key pairs
	_, clientPub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate client key pair: %v", err)
	}
	clientID, err := peer.IDFromPublicKey(clientPub)
	if err != nil {
		t.Fatalf("Failed to get client ID from public key: %v", err)
	}

	_, reporterPub, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		t.Fatalf("Failed to generate reporter key pair: %v", err)
	}
	reporterID, err := peer.IDFromPublicKey(reporterPub)
	if err != nil {
		t.Fatalf("Failed to get reporter ID from public key: %v", err)
	}

	// Create sample clientConnectionUpdate
	original := &clientConnectionUpdate{
		clientID:   clientID,
		reporterID: reporterID,
		timestamp:  time.Now().UnixMilli(),
		connected:  true,
	}

	// Test round-trip: clientConnectionUpdate -> PB -> clientConnectionUpdate
	pbMsg := original.toPb()
	roundTrip, err := newClientConnectionUpdateFromPb(pbMsg)
	if err != nil {
		t.Fatalf("Failed to convert back from PB: %v", err)
	}

	// Assert structs are equal using DeepEqual
	if !reflect.DeepEqual(original, roundTrip) {
		t.Errorf("Round-trip structs not equal: original=%+v, roundTrip=%+v", original, roundTrip)
	}

	// Test empty addrs
	originalEmpty := &clientConnectionUpdate{
		clientID:   clientID,
		reporterID: reporterID,
		timestamp:  time.Now().UnixMilli(),
		connected:  false,
	}
	pbEmpty := originalEmpty.toPb()
	roundTripEmpty, err := newClientConnectionUpdateFromPb(pbEmpty)
	if err != nil {
		t.Fatalf("Failed empty round-trip: %v", err)
	}
	if !reflect.DeepEqual(originalEmpty, roundTripEmpty) {
		t.Errorf("Empty addrs round-trip failed: original=%+v, roundTrip=%+v", originalEmpty, roundTripEmpty)
	}

	// Test invalid client ID
	badPb := &pb.ClientConnectionMsg{
		Id:         []byte("invalid"),
		ReporterId: []byte(reporterID),
	}
	_, err = newClientConnectionUpdateFromPb(badPb)
	if err == nil {
		t.Error("Expected error for invalid client ID")
	}

	// Test invalid reporter ID
	badPb = &pb.ClientConnectionMsg{
		Id:         []byte(clientID),
		ReporterId: []byte("invalid"),
	}
	_, err = newClientConnectionUpdateFromPb(badPb)
	if err == nil {
		t.Error("Expected error for invalid reporter ID")
	}
}
