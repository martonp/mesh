package oracle

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

// TimestampedQuotaStatus wraps a QuotaStatus with the time it was received.
type TimestampedQuotaStatus struct {
	*sources.QuotaStatus
	ReceivedAt time.Time
}

// networkSchedule contains the coordinated fetch schedule for a source.
type networkSchedule struct {
	NextFetchTime            time.Time
	NetworkSustainableRate   float64
	MinPeriod                time.Duration
	NetworkSustainablePeriod time.Duration
	NetworkNextFetchTime     time.Time
	OrderedNodes             []string
}

const (
	// maxPeriod is the maximum period between fetches for a source.
	maxPeriod = 1 * time.Hour
	// quotaPeerActiveThreshold is the threshold for a peer to be considered "active".
	// If there have been no quota updates from a peer within this period, they will
	// not be considered as participating in the fetching for this source.
	quotaPeerActiveThreshold = 6 * time.Minute
	// quotaHeartbeatInterval is the interval at which the quota manager will broadcast
	// the quotas for all sources to the network.
	quotaHeartbeatInterval = 5 * time.Minute
	// networkSafetyMargin is the buffer for network rate calculations. We do not account
	// for this proportion of the quota when calculating the sustainable rate.
	networkSafetyMargin = 0.1
	// propagationDelay is the amount of time we wait to receive results from the previous
	// node in the fetch order before the next node attempts to fetch.
	propagationDelay = 3 * time.Second
)

// quotaManager coordinates quota tracking and network-wide quota sharing for
// oracle sources. It supports network-coordinated fetch scheduling where nodes
// deterministically order themselves to avoid redundant fetches.
type quotaManager struct {
	log              slog.Logger
	nodeID           string
	onStateUpdate    func(*OracleSnapshot)
	publishHeartbeat func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error

	srcsMtx sync.RWMutex
	srcs    map[string]sources.Source

	peerQuotasMtx sync.RWMutex
	peerQuotas    map[string]map[string]*TimestampedQuotaStatus
}

// quotaManagerConfig contains configuration for the quota manager.
type quotaManagerConfig struct {
	log                   slog.Logger
	nodeID                string
	publishQuotaHeartbeat func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error
	onStateUpdate         func(*OracleSnapshot)
	sources               []sources.Source
}

// newQuotaManager creates a new quota manager.
func newQuotaManager(cfg *quotaManagerConfig) *quotaManager {
	srcs := make(map[string]sources.Source, len(cfg.sources))
	for _, src := range cfg.sources {
		srcs[src.Name()] = src
	}

	return &quotaManager{
		log:              cfg.log,
		nodeID:           cfg.nodeID,
		srcs:             srcs,
		peerQuotas:       make(map[string]map[string]*TimestampedQuotaStatus),
		publishHeartbeat: cfg.publishQuotaHeartbeat,
		onStateUpdate:    cfg.onStateUpdate,
	}
}

// Run starts the quota manager's background tasks.
func (qm *quotaManager) run(ctx context.Context) {
	heartbeatTicker := time.NewTicker(quotaHeartbeatInterval)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			if err := qm.publishHeartbeat(ctx, qm.getLocalQuotas()); err != nil {
				qm.log.Warnf("Failed to publish quota heartbeat: %v", err)
			}
			qm.expireStalePeerQuotas()
		}
	}
}

// HandlePeerSourceQuota processes an update to a peer's quota for a given source.
func (qm *quotaManager) handlePeerSourceQuota(peerID string, quota *TimestampedQuotaStatus, source string) {
	qm.peerQuotasMtx.Lock()
	if qm.peerQuotas[peerID] == nil {
		qm.peerQuotas[peerID] = make(map[string]*TimestampedQuotaStatus)
	}
	qm.peerQuotas[peerID][source] = quota
	qm.peerQuotasMtx.Unlock()

	qm.onStateUpdate(&OracleSnapshot{
		Sources: map[string]*SourceStatus{
			source: {
				Quotas: map[string]*Quota{
					peerID: {
						FetchesRemaining: quota.FetchesRemaining,
						FetchesLimit:     quota.FetchesLimit,
						ResetTime:        quota.ResetTime,
					},
				},
			},
		},
	})
}

// expireStalePeerQuotas expires peer quotas that have not been updated within
// the active threshold and will no longer be used in fetch scheduling.
func (qm *quotaManager) expireStalePeerQuotas() {
	qm.peerQuotasMtx.Lock()
	defer qm.peerQuotasMtx.Unlock()

	now := time.Now()
	for peerID, srcs := range qm.peerQuotas {
		for source, quota := range srcs {
			if now.Sub(quota.ReceivedAt) > quotaPeerActiveThreshold {
				delete(srcs, source)
			}
		}
		if len(srcs) == 0 {
			delete(qm.peerQuotas, peerID)
		}
	}
}

// getLocalQuotas returns this node's quota status for all sources.
func (qm *quotaManager) getLocalQuotas() map[string]*sources.QuotaStatus {
	qm.srcsMtx.RLock()
	defer qm.srcsMtx.RUnlock()

	result := make(map[string]*sources.QuotaStatus)
	for name, src := range qm.srcs {
		result[name] = src.QuotaStatus()
	}
	return result
}

// getNetworkQuotas returns all nodes' quotas for all sources.
func (qm *quotaManager) getNetworkQuotas() map[string]map[string]*TimestampedQuotaStatus {
	qm.peerQuotasMtx.RLock()
	defer qm.peerQuotasMtx.RUnlock()

	// Copy map structure so callers can modify the map without affecting the original.
	result := make(map[string]map[string]*TimestampedQuotaStatus)
	for peerID, srcs := range qm.peerQuotas {
		result[peerID] = make(map[string]*TimestampedQuotaStatus)
		for source, quota := range srcs {
			q := *quota
			result[peerID][source] = &q
		}
	}
	return result
}

// getActivePeersForSource returns quotas for peers that shared their quota within
// the active threshold.
func (qm *quotaManager) getActivePeersForSource(source string, now time.Time) map[string]*TimestampedQuotaStatus {
	result := make(map[string]*TimestampedQuotaStatus)

	// Add local node's quota
	qm.srcsMtx.RLock()
	if src, ok := qm.srcs[source]; ok {
		result[qm.nodeID] = &TimestampedQuotaStatus{
			QuotaStatus: src.QuotaStatus(),
			ReceivedAt:  now,
		}
	}
	qm.srcsMtx.RUnlock()

	// Add active peer quotas
	qm.peerQuotasMtx.RLock()
	defer qm.peerQuotasMtx.RUnlock()

	for peerID, srcs := range qm.peerQuotas {
		if quota, ok := srcs[source]; ok {
			if now.Sub(quota.ReceivedAt) <= quotaPeerActiveThreshold {
				q := *quota
				result[peerID] = &q
			}
		}
	}

	return result
}

func (qm *quotaManager) getNetworkSchedule(source string, minPeriod time.Duration) networkSchedule {
	now := time.Now()
	activePeers := qm.getActivePeersForSource(source, now)
	return computeNetworkSchedule(activePeers, qm.nodeID, minPeriod, now)
}

// computeNetworkSchedule computes a coordinated fetch schedule for a source
// across all active peers. The algorithm works in three steps:
//
//  1. Sustainable rate: Each peer's quota yields a rate (fetches/sec) after
//     applying a safety margin. The network rate is the sum of all peer rates,
//     and its reciprocal gives the sustainable period — clamped between
//     minPeriod and maxPeriod.
//
//  2. Deterministic ordering: Peers are ranked by score = SHA256(timeWindow,
//     nodeID) / rate. The time window rotates every minPeriod seconds so the
//     ordering reshuffles periodically, while dividing by rate biases nodes
//     with more remaining quota toward the front. Every node computes the
//     same ordering independently.
//
//  3. Fetch timing: The first node in the order fetches after the clamped
//     period. Each subsequent node adds a propagation delay, giving the
//     earlier node time to share results before the next one attempts a
//     redundant fetch.
func computeNetworkSchedule(activePeers map[string]*TimestampedQuotaStatus, nodeID string, minPeriod time.Duration, now time.Time) networkSchedule {
	// Pre-compute sustainable rate for each active peer.
	peerRates := make(map[string]float64, len(activePeers))
	var networkRate float64
	for id, quota := range activePeers {
		rate := sustainableRate(quota, now)
		peerRates[id] = rate
		networkRate += rate
	}

	// Raw sustainable period = 1 / network_rate (with maxPeriod fallback).
	var sustainablePeriod time.Duration
	if networkRate <= 0 {
		sustainablePeriod = maxPeriod
	} else {
		sustainablePeriod = time.Duration(float64(time.Second) / networkRate)
	}
	clampedPeriod := clamp(sustainablePeriod, minPeriod, maxPeriod)

	// For a deterministic consistent changing value across the network,
	// we use a time window based on the minimum period of the source.
	windowSecs := int64(minPeriod.Seconds())
	if windowSecs <= 0 {
		windowSecs = 1
	}
	timeWindow := now.Unix() / windowSecs

	// Next we calculate a randomized score weighted by the sustainable rate of the peer
	// to create an ordering of peers for their next fetch time.
	type nodeScore struct {
		id    string
		score *big.Int
	}
	scores := make([]nodeScore, 0, len(activePeers))
	for id := range activePeers {
		rate := peerRates[id]
		if rate <= 0 {
			rate = 0.00001 // avoid division by zero
		}

		// hash = SHA256(timeWindow || nodeID)
		h := sha256.Sum256(fmt.Appendf(nil, "%d:%s", timeWindow, id))
		hashInt := new(big.Int).SetBytes(h[:])

		// score = hash / rate, scaled to 9 decimal places to avoid floating
		// point precision issues
		scaledHash := new(big.Int).Mul(hashInt, big.NewInt(1e9))
		if rate > 1e9 {
			rate = 1e9 // Cap to prevent int64 overflow when scaling.
		}
		rateInt := big.NewInt(int64(rate * 1e9))
		if rateInt.Cmp(big.NewInt(0)) <= 0 {
			rateInt = big.NewInt(1)
		}
		score := new(big.Int).Div(scaledHash, rateInt)

		scores = append(scores, nodeScore{id, score})
	}

	// Sort by score ascending (lower = fetches first)
	sort.Slice(scores, func(i, j int) bool {
		c := scores[i].score.Cmp(scores[j].score)
		if c != 0 {
			return c < 0
		}
		return scores[i].id < scores[j].id
	})

	// Extract ordered node IDs and find local node's position
	orderedNodes := make([]string, len(scores))
	order := len(scores)
	for i, s := range scores {
		orderedNodes[i] = s.id
		if s.id == nodeID {
			order = i
		}
	}

	// Calculate next fetch time: clamped period + (order * delay)
	nextFetchAfter := clampedPeriod + time.Duration(order)*propagationDelay

	return networkSchedule{
		NextFetchTime:            now.Add(nextFetchAfter),
		NetworkSustainableRate:   networkRate,
		MinPeriod:                minPeriod,
		NetworkSustainablePeriod: sustainablePeriod,
		NetworkNextFetchTime:     now.Add(clampedPeriod),
		OrderedNodes:             orderedNodes,
	}
}

// sustainableRate returns the sustainable fetch rate (fetches/second) for a peer.
// Applies safety margin to prevent quota exhaustion.
func sustainableRate(quota *TimestampedQuotaStatus, now time.Time) float64 {
	// Unlimited quota
	if quota.FetchesRemaining >= 1<<62 {
		return 1.0 // Cap at 1 fetch/second for unlimited sources
	}

	// Exhausted quota
	if quota.FetchesRemaining <= 0 {
		return 0
	}

	timeRemaining := quota.ResetTime.Sub(now)
	if timeRemaining <= 0 {
		return 1.0 // Quota should have reset, assume fresh
	}

	// Apply safety margin: effective_remaining = remaining * (1 - margin)
	effectiveRemaining := float64(quota.FetchesRemaining) * (1 - networkSafetyMargin)
	if effectiveRemaining <= 0 {
		return 0
	}

	// Rate = effective_remaining / time_remaining
	return effectiveRemaining / timeRemaining.Seconds()
}

func clamp(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}
