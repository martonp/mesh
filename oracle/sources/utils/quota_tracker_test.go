package utils

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

// newTestPool creates a QuotaTracker whose FetchQuota always errors.
// Useful for tests that don't need initialized quota (e.g. panic validation,
// field accessor tests, the "zero before initialized" test).
func newTestPool(t *testing.T) *QuotaTracker {
	t.Helper()
	return NewQuotaTracker(&QuotaTrackerConfig{
		Name: "test",
		FetchQuota: func(ctx context.Context) (*sources.QuotaStatus, error) {
			return nil, fmt.Errorf("test pool: no server")
		},
		Log: slog.Disabled,
	})
}

// newTestPoolWithQuota creates a QuotaTracker whose FetchQuota returns the
// given values. The first QuotaStatus() call triggers reconciliation and
// seeds the tracker.
func newTestPoolWithQuota(t *testing.T, remaining, limit int64) *QuotaTracker {
	t.Helper()
	return NewQuotaTracker(&QuotaTrackerConfig{
		Name: "test",
		FetchQuota: func(ctx context.Context) (*sources.QuotaStatus, error) {
			return &sources.QuotaStatus{
				FetchesRemaining: remaining,
				FetchesLimit:     limit,
			}, nil
		},
		Log: slog.Disabled,
	})
}

func TestNewQuotaTracker_NilConfigPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil config")
		}
	}()
	NewQuotaTracker(nil)
}

func TestNewQuotaTracker_MissingFetchQuotaPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for missing FetchQuota")
		}
	}()
	NewQuotaTracker(&QuotaTrackerConfig{
		Log: slog.Disabled,
	})
}

func TestNewQuotaTracker_MissingLogPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for missing Log")
		}
	}()
	NewQuotaTracker(&QuotaTrackerConfig{
		FetchQuota: func(ctx context.Context) (*sources.QuotaStatus, error) {
			return nil, nil
		},
	})
}

func TestNewQuotaTracker_Defaults(t *testing.T) {
	p := NewQuotaTracker(&QuotaTrackerConfig{
		FetchQuota: func(ctx context.Context) (*sources.QuotaStatus, error) {
			return nil, fmt.Errorf("test")
		},
		Log: slog.Disabled,
	})
	if p.reconcileInterval != defaultReconcileInterval {
		t.Errorf("expected default reconcile interval %v, got %v",
			defaultReconcileInterval, p.reconcileInterval)
	}
}

func TestQuotaTracker_ConsumeCredits(t *testing.T) {
	p := newTestPoolWithQuota(t, 100, 100)
	p.AddSource()
	// Trigger initial reconciliation to seed values.
	_ = p.QuotaStatus()

	p.ConsumeCredits(30)
	status := p.QuotaStatus()
	if status.FetchesRemaining != 70 {
		t.Errorf("expected 70 remaining, got %d", status.FetchesRemaining)
	}

	p.ConsumeCredits(50)
	status = p.QuotaStatus()
	if status.FetchesRemaining != 20 {
		t.Errorf("expected 20 remaining, got %d", status.FetchesRemaining)
	}
}

func TestQuotaTracker_ConsumeCreditsFloorAtZero(t *testing.T) {
	p := newTestPoolWithQuota(t, 10, 100)
	p.AddSource()
	_ = p.QuotaStatus()

	p.ConsumeCredits(50) // exceeds remaining
	status := p.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0 remaining (floor), got %d", status.FetchesRemaining)
	}
}

func TestQuotaTracker_DividesAmongSources(t *testing.T) {
	p := newTestPoolWithQuota(t, 300, 900)
	p.AddSource()
	p.AddSource()
	p.AddSource()

	status := p.QuotaStatus()
	if status.FetchesRemaining != 100 {
		t.Errorf("expected 100 remaining per source (300/3), got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 300 {
		t.Errorf("expected 300 limit per source (900/3), got %d", status.FetchesLimit)
	}
}

func TestQuotaTracker_ZeroSourcesDefaultsToOne(t *testing.T) {
	p := newTestPoolWithQuota(t, 200, 500)
	// No AddSource calls.

	status := p.QuotaStatus()
	if status.FetchesRemaining != 200 {
		t.Errorf("expected 200 remaining (no division), got %d", status.FetchesRemaining)
	}
}

func TestQuotaTracker_ZeroBeforeInitialized(t *testing.T) {
	p := newTestPool(t)
	p.AddSource()
	// Pool's FetchQuota returns error, so reconciliation fails and the
	// pool stays uninitialized.
	status := p.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0 fetches for uninitialized pool, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 0 {
		t.Errorf("expected 0 limit for uninitialized pool, got %d", status.FetchesLimit)
	}
}

func TestQuotaTracker_Reconcile(t *testing.T) {
	fetchQuota := func(ctx context.Context) (*sources.QuotaStatus, error) {
		return &sources.QuotaStatus{
			FetchesRemaining: 800,
			FetchesLimit:     1000,
			ResetTime:        time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
		}, nil
	}

	p := NewQuotaTracker(&QuotaTrackerConfig{
		FetchQuota:        fetchQuota,
		ReconcileInterval: time.Millisecond,
		Log:               slog.Disabled,
	})
	p.AddSource()

	// First call blocks until reconciliation completes.
	status := p.QuotaStatus()
	if status.FetchesRemaining != 800 {
		t.Errorf("expected 800 remaining after reconcile, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 1000 {
		t.Errorf("expected 1000 limit after reconcile, got %d", status.FetchesLimit)
	}
}

func TestQuotaTracker_ReconcileError(t *testing.T) {
	fetchQuota := func(ctx context.Context) (*sources.QuotaStatus, error) {
		return nil, fmt.Errorf("network error")
	}

	p := NewQuotaTracker(&QuotaTrackerConfig{
		FetchQuota:        fetchQuota,
		ReconcileInterval: time.Millisecond,
		Log:               slog.Disabled,
	})
	p.AddSource()

	// First call blocks on reconciliation which fails.
	// Pool remains uninitialized, so returns zero quota.
	status := p.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0 after failed reconcile, got %d", status.FetchesRemaining)
	}
}

func TestQuotaTracker_ReconcileSyncsToServer(t *testing.T) {
	t.Run("server shows more usage than local", func(t *testing.T) {
		// Server reports fewer remaining than our local estimate,
		// meaning another consumer used credits. We should sync down.
		var call int
		fetchQuota := func(ctx context.Context) (*sources.QuotaStatus, error) {
			call++
			if call == 1 {
				// Initial sync: seed with 1000/1000.
				return &sources.QuotaStatus{
					FetchesRemaining: 1000,
					FetchesLimit:     1000,
				}, nil
			}
			// Subsequent: server says 700 remaining.
			return &sources.QuotaStatus{
				FetchesRemaining: 700,
				FetchesLimit:     1000,
			}, nil
		}

		p := NewQuotaTracker(&QuotaTrackerConfig{
			FetchQuota:        fetchQuota,
			ReconcileInterval: time.Hour, // prevent auto-reconcile
			Log:               slog.Disabled,
		})
		p.AddSource()

		// Trigger initial sync.
		_ = p.QuotaStatus() // call=1, remaining=1000

		p.ConsumeCredits(200) // local: 800

		p.reconcile() // call=2, server=700 < local=800, adopt 700

		status := p.QuotaStatus()
		// Server says 700, local says 800. Adopt server (lower = more usage).
		if status.FetchesRemaining != 700 {
			t.Errorf("expected 700 remaining after reconcile sync, got %d", status.FetchesRemaining)
		}
	})

	t.Run("server lags behind local consumption", func(t *testing.T) {
		// Server's hits counter is eventually consistent and hasn't
		// caught up with our local consumption. Keep local estimate.
		var call int
		fetchQuota := func(ctx context.Context) (*sources.QuotaStatus, error) {
			call++
			if call == 1 {
				return &sources.QuotaStatus{
					FetchesRemaining: 1000,
					FetchesLimit:     1000,
				}, nil
			}
			return &sources.QuotaStatus{
				FetchesRemaining: 900,
				FetchesLimit:     1000,
			}, nil
		}

		p := NewQuotaTracker(&QuotaTrackerConfig{
			FetchQuota:        fetchQuota,
			ReconcileInterval: time.Hour,
			Log:               slog.Disabled,
		})
		p.AddSource()

		_ = p.QuotaStatus() // call=1, remaining=1000

		p.ConsumeCredits(200) // local: 800

		p.reconcile() // call=2, server=900 > local=800, keep local

		status := p.QuotaStatus()
		// Server says 900, local says 800. Keep local (more conservative).
		if status.FetchesRemaining != 800 {
			t.Errorf("expected 800 remaining (local estimate preserved), got %d", status.FetchesRemaining)
		}
	})
}

// --- TrackedSource tests ---

func TestNewTrackedSource_RegistersWithTracker(t *testing.T) {
	p := newTestPool(t)
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{}, nil
	}

	_ = NewTrackedSource(TrackedSourceConfig{
		Name: "test1", FetchRates: fetchRates, Tracker: p, CreditsPerRequest: 1,
	})
	if p.sourceCount != 1 {
		t.Errorf("expected sourceCount 1, got %d", p.sourceCount)
	}

	_ = NewTrackedSource(TrackedSourceConfig{
		Name: "test2", FetchRates: fetchRates, Tracker: p, CreditsPerRequest: 1,
	})
	if p.sourceCount != 2 {
		t.Errorf("expected sourceCount 2, got %d", p.sourceCount)
	}
}

func TestNewTrackedSource_PanicsOnMissingName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for missing name")
		}
	}()
	NewTrackedSource(TrackedSourceConfig{
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) { return nil, nil },
		Tracker:    newTestPool(t),
	})
}

func TestNewTrackedSource_PanicsOnMissingFetchRates(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for missing FetchRates")
		}
	}()
	NewTrackedSource(TrackedSourceConfig{
		Name:    "test",
		Tracker: newTestPool(t),
	})
}

func TestNewTrackedSource_PanicsOnMissingTracker(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for missing Tracker")
		}
	}()
	NewTrackedSource(TrackedSourceConfig{
		Name:       "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) { return nil, nil },
	})
}

func TestTrackedSource_ConsumesCreditsOnFetch(t *testing.T) {
	p := newTestPoolWithQuota(t, 100, 100)

	pooled := NewTrackedSource(TrackedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
		Tracker:           p,
		CreditsPerRequest: 10,
	})

	// Trigger initial reconciliation to seed values.
	_ = pooled.QuotaStatus()

	_, err := pooled.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Pool: 100 - 10 = 90 raw credits, divided by creditsPerRequest=10 = 9 fetches.
	status := pooled.QuotaStatus()
	if status.FetchesRemaining != 9 {
		t.Errorf("expected 9 fetches remaining, got %d", status.FetchesRemaining)
	}
}

func TestTrackedSource_NoConsumeOnError(t *testing.T) {
	p := newTestPoolWithQuota(t, 100, 100)

	pooled := NewTrackedSource(TrackedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return nil, fmt.Errorf("fetch error")
		},
		Tracker:           p,
		CreditsPerRequest: 10,
	})

	// Trigger initial reconciliation to seed values.
	_ = pooled.QuotaStatus()

	_, err := pooled.FetchRates(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	// Credits should not have been consumed.
	// Pool: 100 raw credits, divided by creditsPerRequest=10 = 10 fetches.
	status := pooled.QuotaStatus()
	if status.FetchesRemaining != 10 {
		t.Errorf("expected 10 fetches remaining after failed fetch, got %d", status.FetchesRemaining)
	}
}

func TestTrackedSource_FieldAccessors(t *testing.T) {
	p := newTestPool(t)

	pooled := NewTrackedSource(TrackedSourceConfig{
		Name:      "inner-source",
		Weight:    0.75,
		MinPeriod: 42 * time.Second,
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
		Tracker:           p,
		CreditsPerRequest: 1,
	})
	if pooled.Name() != "inner-source" {
		t.Errorf("expected Name() = inner-source, got %s", pooled.Name())
	}
	if pooled.Weight() != 0.75 {
		t.Errorf("expected Weight() = 0.75, got %f", pooled.Weight())
	}
	if pooled.MinPeriod() != 42*time.Second {
		t.Errorf("expected MinPeriod() = 42s, got %v", pooled.MinPeriod())
	}
}

func TestTrackedSource_QuotaStatusFromTracker(t *testing.T) {
	p := newTestPoolWithQuota(t, 600, 1200)

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		return &sources.RateInfo{}, nil
	}

	p1 := NewTrackedSource(TrackedSourceConfig{
		Name: "test1", FetchRates: fetchRates, Tracker: p, CreditsPerRequest: 1,
	})
	p2 := NewTrackedSource(TrackedSourceConfig{
		Name: "test2", FetchRates: fetchRates, Tracker: p, CreditsPerRequest: 1,
	})

	// 2 sources registered, so each gets 600/2=300 remaining, 1200/2=600 limit.
	s1 := p1.QuotaStatus()
	s2 := p2.QuotaStatus()

	if s1.FetchesRemaining != 300 {
		t.Errorf("p1: expected 300 remaining, got %d", s1.FetchesRemaining)
	}
	if s2.FetchesLimit != 600 {
		t.Errorf("p2: expected 600 limit, got %d", s2.FetchesLimit)
	}
}

func TestTrackedSource_ConcurrentFetches(t *testing.T) {
	p := newTestPoolWithQuota(t, 1000, 1000)

	pooled := NewTrackedSource(TrackedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
		Tracker:           p,
		CreditsPerRequest: 1,
	})

	// Trigger initial reconciliation to seed values.
	_ = pooled.QuotaStatus()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = pooled.FetchRates(context.Background())
		}()
	}
	wg.Wait()

	status := pooled.QuotaStatus()
	if status.FetchesRemaining != 900 {
		t.Errorf("expected 900 remaining after 100 fetches, got %d", status.FetchesRemaining)
	}
}
