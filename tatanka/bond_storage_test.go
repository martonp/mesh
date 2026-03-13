package tatanka

import (
	"testing"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestMemoryBondStorage(t *testing.T) {
	// Mock time for testing expiry
	mockTime := time.Now()
	timeNow := func() time.Time { return mockTime }

	storage := newMemoryBondStorage(timeNow)
	peerID1 := peer.ID("peer1")
	peerID2 := peer.ID("peer2")

	// Unknown peer returns 0
	if strength := storage.bondStrength(peerID1); strength != 0 {
		t.Errorf("Expected 0 for unknown peer, got %d", strength)
	}

	// Add bonds for peer1
	bonds := []*bond.BondParams{
		{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Hour)},
		{ID: "bond2", Strength: 50, Expiry: mockTime.Add(2 * time.Hour)},
	}
	if strength := storage.addBonds(peerID1, bonds); strength != 150 {
		t.Errorf("Expected strength 150, got %d", strength)
	}
	if strength := storage.bondStrength(peerID1); strength != 150 {
		t.Errorf("Expected strength 150, got %d", strength)
	}

	// Duplicate bonds not added
	duplicates := []*bond.BondParams{{ID: "bond1", Strength: 100, Expiry: mockTime.Add(time.Hour)}}
	if strength := storage.addBonds(peerID1, duplicates); strength != 150 {
		t.Errorf("Expected strength 150 (no change), got %d", strength)
	}

	// Already expired bonds not added
	expired := []*bond.BondParams{{ID: "bond3", Strength: 200, Expiry: mockTime.Add(-time.Hour)}}
	if strength := storage.addBonds(peerID1, expired); strength != 150 {
		t.Errorf("Expected strength 150 (expired not added), got %d", strength)
	}

	// Different peer has independent storage
	bonds2 := []*bond.BondParams{{ID: "bond4", Strength: 75, Expiry: mockTime.Add(time.Hour)}}
	if strength := storage.addBonds(peerID2, bonds2); strength != 75 {
		t.Errorf("Expected strength 75 for peer2, got %d", strength)
	}

	// Advance time - bond1 expires
	bonds[0].Expiry = mockTime.Add(-time.Hour)
	if strength := storage.bondStrength(peerID1); strength != 50 {
		t.Errorf("Expected strength 50 after expiry, got %d", strength)
	}

	// Both bonds expired
	bonds[1].Expiry = mockTime.Add(-time.Hour)
	if strength := storage.bondStrength(peerID1); strength != 0 {
		t.Errorf("Expected strength 0 after all expire, got %d", strength)
	}
}
