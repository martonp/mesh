package bond

import (
	"testing"
	"time"
)

func TestBondInfo(t *testing.T) {
	mockTime := time.Now()
	bond1 := &BondParams{ID: "bond1", Strength: 20, Expiry: mockTime.Add(time.Hour)}
	bond2 := &BondParams{ID: "bond2", Strength: 20, Expiry: mockTime.Add(time.Hour * 3)}

	// Ensure a bond info can add bonds.
	bInfo := NewBondInfo()
	bInfo.AddBonds([]*BondParams{bond1}, mockTime)
	initialStrength := bInfo.BondStrength()

	// Ensure adding bonds updates the cumulative bond strength.
	bInfo.AddBonds([]*BondParams{bond2}, mockTime)
	updatedStrength := bInfo.BondStrength()

	if updatedStrength-initialStrength != bond2.Strength {
		t.Fatalf("Expected updated bond strength of %d, got %d", bond1.Strength+bond2.Strength, updatedStrength)
	}

	// Expire bond 1
	bond1.Expiry = time.Now()
	afterExpiryStrength := bInfo.BondStrength()

	if afterExpiryStrength != bond2.Strength {
		t.Fatalf("Expected after expiry strength to be %d, got %d", 20, afterExpiryStrength)
	}

	bondReq, err := PostBondReqFromBondInfo(bInfo)
	if err != nil {
		t.Fatalf("Unexpected error creating a post bond request from bond info")
	}

	if string(bondReq.Bonds[0].BondID) != bond2.ID {
		t.Fatalf("Expected bond id %s in request, got %s", bond2.ID, string(bondReq.Bonds[0].BondID))
	}
}
