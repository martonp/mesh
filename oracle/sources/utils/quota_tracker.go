package utils

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

const (
	defaultReconcileInterval = time.Hour
	reconcileTimeout         = 10 * time.Second
)

// QuotaTracker tracks quota for one or more sources that share a single API
// key/credit pool. It divides the available quota evenly among registered
// sources. It tracks consumption locally and periodically reconciles with an
// API endpoint.
type QuotaTracker struct {
	mtx              sync.RWMutex
	creditsRemaining int64
	creditsLimit     int64
	resetTime        time.Time
	sourceCount      int
	initialized      bool

	name              string
	fetchQuota        func(ctx context.Context) (*sources.QuotaStatus, error)
	reconcileInterval time.Duration
	lastReconcile     time.Time
	reconciling       atomic.Bool
	initOnce          sync.Once
	log               slog.Logger
}

// QuotaTrackerConfig configures a QuotaTracker.
type QuotaTrackerConfig struct {
	// Name identifies this quota tracker in log messages.
	Name string

	// FetchQuota fetches quota status from the server.
	FetchQuota func(ctx context.Context) (*sources.QuotaStatus, error)

	// ReconcileInterval is how often to reconcile quota with the API.
	ReconcileInterval time.Duration

	Log slog.Logger
}

// verify validates that all fields are set. Panics if any field is missing.
func (cfg *QuotaTrackerConfig) verify() {
	if cfg == nil {
		panic("quota tracker config is nil")
	}
	if cfg.FetchQuota == nil {
		panic("fetch quota function is required")
	}
	if cfg.Log == nil {
		panic("logger is required")
	}
}

// NewQuotaTracker creates a new quota tracker.
func NewQuotaTracker(cfg *QuotaTrackerConfig) *QuotaTracker {
	cfg.verify()

	reconcileInterval := cfg.ReconcileInterval
	if reconcileInterval == 0 {
		reconcileInterval = defaultReconcileInterval
	}

	return &QuotaTracker{
		name:              cfg.Name,
		fetchQuota:        cfg.FetchQuota,
		reconcileInterval: reconcileInterval,
		log:               cfg.Log,
	}
}

// ConsumeCredits decrements the tracker's credit counter.
func (p *QuotaTracker) ConsumeCredits(n int64) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.creditsRemaining <= n {
		p.creditsRemaining = 0
	} else {
		p.creditsRemaining -= n
	}
}

// AddSource increments the source count.
func (p *QuotaTracker) AddSource() {
	p.mtx.Lock()
	p.sourceCount++
	p.mtx.Unlock()
}

// QuotaStatus returns the quota divided by source count.
// Each source gets an equal share of the available credits.
// The first call blocks until reconciliation completes so that callers
// always receive accurate quota data. Subsequent reconciliations are
// triggered in the background when the interval elapses.
// Returns a zero-valued status if the tracker has not been initialized via
// reconciliation.
func (p *QuotaTracker) QuotaStatus() *sources.QuotaStatus {
	// Block on first reconciliation to ensure accurate initial quota data.
	p.initOnce.Do(func() {
		p.reconciling.Store(true)
		p.reconcile()
	})

	p.mtx.RLock()
	defer p.mtx.RUnlock()

	// Trigger async reconciliation if stale.
	now := time.Now().UTC()
	if now.Sub(p.lastReconcile) > p.reconcileInterval {
		if p.reconciling.CompareAndSwap(false, true) {
			go p.reconcile()
		}
	}

	sourceCount := p.sourceCount
	if sourceCount == 0 {
		sourceCount = 1
	}

	return &sources.QuotaStatus{
		FetchesRemaining: p.creditsRemaining / int64(sourceCount),
		FetchesLimit:     p.creditsLimit / int64(sourceCount),
		ResetTime:        p.resetTime,
	}
}

// reconcile fetches the current quota from the server and merges it with
// local state. On the first successful sync it adopts the server's values
// unconditionally. On subsequent syncs it conservatively keeps the lower
// of the two remaining counts to avoid over-fetching when another consumer
// shares the same API key.
func (p *QuotaTracker) reconcile() {
	defer p.reconciling.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), reconcileTimeout)
	defer cancel()

	serverQuota, err := p.fetchQuota(ctx)
	now := time.Now().UTC()

	if err != nil {
		p.log.Errorf("[%s] Failed to reconcile quota: %v", p.name, err)
		// Update lastReconcile to avoid hammering the endpoint.
		p.mtx.Lock()
		p.lastReconcile = now
		p.mtx.Unlock()
		return
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()

	if serverQuota == nil {
		p.log.Warnf("[%s] Quota reconcile: server returned nil quota", p.name)
		p.lastReconcile = now
		return
	}

	firstSync := !p.initialized

	p.creditsLimit = serverQuota.FetchesLimit
	p.resetTime = serverQuota.ResetTime
	p.initialized = true

	// For some sources, the API's counter is eventually consistent and can lag
	// behind our local consumption tracking. Only adopt the API's remaining
	// count when it reports more usage than we've tracked locally (e.g.
	// another consumer sharing the same key) or on the first sync.
	if firstSync {
		p.log.Infof("[%s] Quota initial sync: server remaining = %d, limit = %d",
			p.name, serverQuota.FetchesRemaining, serverQuota.FetchesLimit)
		p.creditsRemaining = serverQuota.FetchesRemaining
	} else if serverQuota.FetchesRemaining < p.creditsRemaining {
		// Server reports more usage than we tracked — another consumer
		// may be sharing this key.
		p.log.Warnf("[%s] Quota reconcile: server remaining (%d) < local estimate (%d), syncing down",
			p.name, serverQuota.FetchesRemaining, p.creditsRemaining)
		p.creditsRemaining = serverQuota.FetchesRemaining
	} else if serverQuota.FetchesRemaining > p.creditsRemaining {
		// Server hasn't caught up with our local consumption yet.
		p.log.Infof("[%s] Quota reconcile: server remaining (%d) > local estimate (%d), keeping local",
			p.name, serverQuota.FetchesRemaining, p.creditsRemaining)
	}

	p.lastReconcile = now
}

// TrackedSourceConfig configures a TrackedSource.
type TrackedSourceConfig struct {
	Name              string
	Weight            float64
	MinPeriod         time.Duration
	FetchRates        FetchRatesFunc
	Tracker           *QuotaTracker
	CreditsPerRequest int64
}

// TrackedSource is a source whose quota is managed by a shared QuotaTracker.
type TrackedSource struct {
	name              string
	weight            float64
	minPeriod         time.Duration
	fetchRates        FetchRatesFunc
	tracker           *QuotaTracker
	creditsPerRequest int64
}

// NewTrackedSource creates a new tracked source. It validates config, applies
// defaults for Weight and MinPeriod, and registers itself with the tracker.
func NewTrackedSource(cfg TrackedSourceConfig) *TrackedSource {
	if cfg.Name == "" {
		panic("tracked source: name is required")
	}
	if cfg.FetchRates == nil {
		panic("tracked source: FetchRates is required")
	}
	if cfg.Tracker == nil {
		panic("tracked source: Tracker is required")
	}

	weight := cfg.Weight
	if weight == 0 {
		weight = defaultWeight
	}
	minPeriod := cfg.MinPeriod
	if minPeriod == 0 {
		minPeriod = defaultMinPeriod
	}

	if cfg.CreditsPerRequest <= 0 {
		cfg.CreditsPerRequest = 1
	}

	cfg.Tracker.AddSource()

	return &TrackedSource{
		name:              cfg.Name,
		weight:            weight,
		minPeriod:         minPeriod,
		fetchRates:        cfg.FetchRates,
		tracker:           cfg.Tracker,
		creditsPerRequest: cfg.CreditsPerRequest,
	}
}

func (s *TrackedSource) Name() string             { return s.name }
func (s *TrackedSource) Weight() float64          { return s.weight }
func (s *TrackedSource) MinPeriod() time.Duration { return s.minPeriod }

func (s *TrackedSource) FetchRates(ctx context.Context) (*sources.RateInfo, error) {
	rates, err := s.fetchRates(ctx)
	if err != nil {
		return nil, err
	}

	s.tracker.ConsumeCredits(s.creditsPerRequest)
	return rates, nil
}

func (s *TrackedSource) QuotaStatus() *sources.QuotaStatus {
	status := s.tracker.QuotaStatus()
	if s.creditsPerRequest > 1 {
		status.FetchesRemaining /= s.creditsPerRequest
		status.FetchesLimit /= s.creditsPerRequest
	}
	return status
}
