package bond

import (
	"fmt"
	"sort"
	"sync"
	"time"

	protocolsPb "github.com/bisoncraft/mesh/protocols/pb"
)

const (
	// MinRequiredBondStrength is the minimum required bond strength for a client.
	MinRequiredBondStrength = 1
)

// BondParams contains the parameters of a bond.
type BondParams struct {
	ID       string
	Expiry   time.Time
	Strength uint32
}

// BondInfo stores the bond information for a client.
type BondInfo struct {
	mtx sync.RWMutex
	// bondParams are sorted in order of increasing expiry time to enable
	// efficient removal of expired bonds.
	bondParams []*BondParams
}

// NewBondInfo initializes bond information for a client.
func NewBondInfo() *BondInfo {
	return &BondInfo{
		bondParams: make([]*BondParams, 0),
	}
}

// AddBond adds a bond to the client bond info, maintaining sorted order.
func (bi *BondInfo) AddBonds(bonds []*BondParams, now time.Time) {
	bi.mtx.Lock()
	defer bi.mtx.Unlock()

	existing := make(map[string]struct{}, len(bi.bondParams))
	for _, bond := range bi.bondParams {
		existing[bond.ID] = struct{}{}
	}

	// Make sure none of bonds are already stored, and that none of the new
	// bonds are duplicates.
	for _, bond := range bonds {
		if bond.Expiry.Before(now) {
			continue
		}
		if _, ok := existing[bond.ID]; ok {
			continue
		}
		bi.bondParams = append(bi.bondParams, bond)
	}

	// Sort highest expiry time to lowest expiry time.
	sort.Slice(bi.bondParams, func(i, j int) bool {
		return bi.bondParams[i].Expiry.After(bi.bondParams[j].Expiry)
	})

	// Remove expired bonds.
	var expiredIdx int = -1
	for i, bond := range bi.bondParams {
		if bond.Expiry.Before(now) {
			expiredIdx = i
			break
		}
	}

	if expiredIdx != -1 {
		bi.bondParams = bi.bondParams[:expiredIdx]
	}
}

// BondStrength returns the total bond strength of the the provided client bond info.
func (bi *BondInfo) BondStrength() (s uint32) {
	now := time.Now()
	bi.mtx.RLock()
	defer bi.mtx.RUnlock()

	for _, bond := range bi.bondParams {
		if bond.Expiry.Before(now) {
			break
		}
		s += bond.Strength
	}
	return s
}

// PostBondReqFromBondInfo converts the provided bond info into a post bond request.
func PostBondReqFromBondInfo(bi *BondInfo) (*protocolsPb.PostBondRequest, error) {
	bi.mtx.RLock()
	defer bi.mtx.RUnlock()

	now := time.Now()

	req := &protocolsPb.PostBondRequest{}
	for _, bp := range bi.bondParams {
		if bp.Expiry.Before(now) {
			break
		}
		req.Bonds = append(req.Bonds, &protocolsPb.Bond{BondID: []byte(bp.ID)})
	}

	if len(req.Bonds) == 0 {
		return nil, fmt.Errorf("no bonds to post")
	}

	return req, nil
}
