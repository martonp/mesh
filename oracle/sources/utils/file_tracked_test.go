package utils

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
)

func newTestFileTrackedSource(t *testing.T, quotaFile string, creditsLimit int64) *FileTrackedSource {
	t.Helper()
	return NewFileTrackedSource(FileTrackedSourceConfig{
		Name:              "test-file-tracked",
		CreditsLimit:      creditsLimit,
		CreditsPerRequest: 1,
		QuotaFile:         quotaFile,
		MinPeriod:         30 * time.Second,
		Log:               slog.Disabled,
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return &sources.RateInfo{
				Prices: []*sources.PriceUpdate{
					{Ticker: "BTC", Price: 50000},
				},
			}, nil
		},
	})
}

func TestFileTrackedSource_DefaultsWhenNoFile(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	s := newTestFileTrackedSource(t, quotaFile, 9500)

	status := s.QuotaStatus()
	if status.FetchesRemaining != 9500 {
		t.Errorf("expected 9500 remaining, got %d", status.FetchesRemaining)
	}
	if status.FetchesLimit != 9500 {
		t.Errorf("expected 9500 limit, got %d", status.FetchesLimit)
	}
	if status.ResetTime.IsZero() {
		t.Error("expected non-zero reset time")
	}
	// Reset time should be the first of next month.
	now := time.Now().UTC()
	expectedReset := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	if !status.ResetTime.Equal(expectedReset) {
		t.Errorf("expected reset time %v, got %v", expectedReset, status.ResetTime)
	}
}

func TestFileTrackedSource_LoadsFromFile(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")

	// Write a valid quota file with a future reset time.
	futureReset := time.Now().UTC().Add(30 * 24 * time.Hour)
	state := fileQuotaState{
		CreditsRemaining: 5000,
		CreditsLimit:     9500,
		ResetTime:        futureReset,
	}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(quotaFile, data, 0600); err != nil {
		t.Fatal(err)
	}

	s := newTestFileTrackedSource(t, quotaFile, 9500)
	status := s.QuotaStatus()
	if status.FetchesRemaining != 5000 {
		t.Errorf("expected 5000 remaining, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_ResetsWhenExpired(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")

	// Write a quota file with a past reset time.
	pastReset := time.Now().UTC().Add(-24 * time.Hour)
	state := fileQuotaState{
		CreditsRemaining: 100,
		CreditsLimit:     9500,
		ResetTime:        pastReset,
	}
	data, _ := json.Marshal(state)
	if err := os.WriteFile(quotaFile, data, 0600); err != nil {
		t.Fatal(err)
	}

	s := newTestFileTrackedSource(t, quotaFile, 9500)
	status := s.QuotaStatus()
	if status.FetchesRemaining != 9500 {
		t.Errorf("expected full reset to 9500, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_DecrementsOnFetch(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	s := newTestFileTrackedSource(t, quotaFile, 9500)

	_, err := s.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := s.QuotaStatus()
	if status.FetchesRemaining != 9499 {
		t.Errorf("expected 9499 remaining after 1 fetch, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_PersistsAfterFetch(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	s := newTestFileTrackedSource(t, quotaFile, 9500)

	_, err := s.FetchRates(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait briefly for async persist.
	time.Sleep(50 * time.Millisecond)

	data, err := os.ReadFile(quotaFile)
	if err != nil {
		t.Fatalf("failed to read quota file: %v", err)
	}

	var state fileQuotaState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("failed to unmarshal quota file: %v", err)
	}
	if state.CreditsRemaining != 9499 {
		t.Errorf("expected 9499 in file, got %d", state.CreditsRemaining)
	}
}

func TestFileTrackedSource_DoesNotDecrementOnError(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")

	fetchErr := context.DeadlineExceeded
	s := NewFileTrackedSource(FileTrackedSourceConfig{
		Name:         "test-err",
		CreditsLimit: 9500,
		QuotaFile:    quotaFile,
		Log:          slog.Disabled,
		FetchRates: func(ctx context.Context) (*sources.RateInfo, error) {
			return nil, fetchErr
		},
	})

	_, err := s.FetchRates(context.Background())
	if err != fetchErr {
		t.Fatalf("expected fetchErr, got %v", err)
	}

	status := s.QuotaStatus()
	if status.FetchesRemaining != 9500 {
		t.Errorf("expected no decrement on error, got %d remaining", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_CorruptFileResetsToFull(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	if err := os.WriteFile(quotaFile, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}

	s := newTestFileTrackedSource(t, quotaFile, 9500)
	status := s.QuotaStatus()
	if status.FetchesRemaining != 9500 {
		t.Errorf("expected full reset on corrupt file, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_BottomsOutAtZero(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")

	// Pre-seed with 1 remaining.
	futureReset := time.Now().UTC().Add(30 * 24 * time.Hour)
	state := fileQuotaState{
		CreditsRemaining: 1,
		CreditsLimit:     9500,
		ResetTime:        futureReset,
	}
	data, _ := json.Marshal(state)
	os.WriteFile(quotaFile, data, 0600)

	s := newTestFileTrackedSource(t, quotaFile, 9500)

	// First fetch: 1 -> 0
	s.FetchRates(context.Background())
	status := s.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0 after depleting, got %d", status.FetchesRemaining)
	}

	// Second fetch: stays at 0
	s.FetchRates(context.Background())
	status = s.QuotaStatus()
	if status.FetchesRemaining != 0 {
		t.Errorf("expected 0, got %d", status.FetchesRemaining)
	}
}

func TestFileTrackedSource_ImplementsSourceInterface(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	s := newTestFileTrackedSource(t, quotaFile, 9500)

	var _ sources.Source = s
}

func TestFileTrackedSource_InterfaceMethods(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")
	s := newTestFileTrackedSource(t, quotaFile, 9500)

	if s.Name() != "test-file-tracked" {
		t.Errorf("expected name test-file-tracked, got %s", s.Name())
	}
	if s.Weight() != defaultWeight {
		t.Errorf("expected default weight, got %f", s.Weight())
	}
	if s.MinPeriod() != 30*time.Second {
		t.Errorf("expected 30s min period, got %v", s.MinPeriod())
	}
}

func TestFileTrackedSource_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	quotaFile := filepath.Join(dir, "quota.json")

	// Create first instance and fetch a few times.
	s1 := newTestFileTrackedSource(t, quotaFile, 9500)
	for range 5 {
		s1.FetchRates(context.Background())
	}
	// Wait for async persist.
	time.Sleep(50 * time.Millisecond)

	// Create second instance (simulating restart) — should load from file.
	s2 := newTestFileTrackedSource(t, quotaFile, 9500)
	status := s2.QuotaStatus()
	if status.FetchesRemaining != 9495 {
		t.Errorf("expected 9495 after restart, got %d", status.FetchesRemaining)
	}
}
