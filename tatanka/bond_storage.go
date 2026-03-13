package tatanka

import (
	"sync"
	"time"

	"github.com/bisoncraft/mesh/bond"
	"github.com/libp2p/go-libp2p/core/peer"
)

type bondStorage interface {
	addBonds(peerID peer.ID, bonds []*bond.BondParams) uint32
	bondStrength(peerID peer.ID) uint32
}

// memoryBondStorage stores the bond information for all clients in memory.
// TODO: we should only store currently connected clients in this map. Other
// previously verified bonds should be stored in a database to avoid having to
// re-verify them.
type memoryBondStorage struct {
	mtx     sync.RWMutex
	clients map[peer.ID]*bond.BondInfo
	timeNow func() time.Time
}

// newMemoryBondStorage creates a new memory bond storage with the given function
// to get the current time so that it can be mocked in tests.
func newMemoryBondStorage(timeNow func() time.Time) *memoryBondStorage {
	return &memoryBondStorage{
		clients: make(map[peer.ID]*bond.BondInfo),
		timeNow: timeNow,
	}
}

// addBonds adds bonds to the bond storage for a given client.
func (bs *memoryBondStorage) addBonds(peerID peer.ID, bonds []*bond.BondParams) uint32 {
	bs.mtx.Lock()
	clientInfo, ok := bs.clients[peerID]
	if !ok {
		clientInfo = bond.NewBondInfo()
		bs.clients[peerID] = clientInfo
	}
	bs.mtx.Unlock()

	now := bs.timeNow()
	clientInfo.AddBonds(bonds, now)

	return clientInfo.BondStrength()
}

// bondStrength returns the total strength of all bonds for a given client.
func (bs *memoryBondStorage) bondStrength(peerID peer.ID) uint32 {
	bs.mtx.RLock()
	clientInfo, ok := bs.clients[peerID]
	if !ok {
		bs.mtx.RUnlock()
		return 0
	}
	bs.mtx.RUnlock()

	return clientInfo.BondStrength()
}
