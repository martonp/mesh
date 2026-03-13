package utils

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
)

func TestNewUnlimitedSource_FullConfig(t *testing.T) {
	called := false
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name:      "test",
		Weight:    0.5,
		MinPeriod: 10 * time.Second,
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			called = true
			return &sources.RateInfo{}, nil
		},
	})

	if s.Name() != "test" {
		t.Errorf("expected Name() = test, got %s", s.Name())
	}
	if s.Weight() != 0.5 {
		t.Errorf("expected Weight() = 0.5, got %f", s.Weight())
	}
	if s.MinPeriod() != 10*time.Second {
		t.Errorf("expected MinPeriod() = 10s, got %v", s.MinPeriod())
	}

	_, err := s.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("FetchRates did not call the underlying function")
	}
}

func TestNewUnlimitedSource_DefaultWeightAndMinPeriod(t *testing.T) {
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "defaults",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
	})

	if s.Weight() != defaultWeight {
		t.Errorf("expected default weight %f, got %f", defaultWeight, s.Weight())
	}
	if s.MinPeriod() != defaultMinPeriod {
		t.Errorf("expected default min period %v, got %v", defaultMinPeriod, s.MinPeriod())
	}
}

func TestNewUnlimitedSource_EmptyNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty name")
		}
	}()
	NewUnlimitedSource(UnlimitedSourceConfig{
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
	})
}

func TestNewUnlimitedSource_NilFetchRatesPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil FetchRates")
		}
	}()
	NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "test",
	})
}

func TestUnlimitedSource_QuotaStatusIsUnlimited(t *testing.T) {
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
	})

	status := s.QuotaStatus()
	if status == nil {
		t.Fatal("expected non-nil QuotaStatus")
	}
	if status.FetchesRemaining != math.MaxInt64 {
		t.Errorf("expected unlimited fetches, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != math.MaxInt64 {
		t.Errorf("expected unlimited fetches limit, got %d", status.FetchesLimit)
	}
	if status.ResetTime.IsZero() {
		t.Error("expected non-zero reset time")
	}
}

func TestUnlimitedSource_FetchRatesPropagatesError(t *testing.T) {
	fetchErr := fmt.Errorf("upstream failure")
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return nil, fetchErr
		},
	})

	_, err := s.FetchRates(context.Background())
	if err != fetchErr {
		t.Errorf("expected fetchErr, got %v", err)
	}
}

func TestUnlimitedSource_FetchRatesReturnsPrices(t *testing.T) {
	expected := &sources.RateInfo{
		Prices: []*sources.PriceUpdate{
			{Ticker: "BTC", Price: 50000},
			{Ticker: "ETH", Price: 3000},
		},
	}
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return expected, nil
		},
	})

	result, err := s.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Prices) != 2 {
		t.Fatalf("expected 2 prices, got %d", len(result.Prices))
	}
	if result.Prices[0].Ticker != "BTC" {
		t.Errorf("expected BTC, got %s", result.Prices[0].Ticker)
	}
}

func TestUnlimitedSource_FetchRatesRespectsContext(t *testing.T) {
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return &sources.RateInfo{}, nil
			}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := s.FetchRates(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestUnlimitedSource_ImplementsSourceInterface(t *testing.T) {
	s := NewUnlimitedSource(UnlimitedSourceConfig{
		Name: "iface-test",
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{}, nil
		},
	})

	// Compile-time check: *UnlimitedSource must satisfy sources.Source.
	var _ sources.Source = s
}
