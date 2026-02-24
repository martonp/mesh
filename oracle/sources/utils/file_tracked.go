package utils

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
)

// fileQuotaState is the JSON-serializable quota state persisted to disk.
type fileQuotaState struct {
	CreditsRemaining int64     `json:"credits_remaining"`
	CreditsLimit     int64     `json:"credits_limit"`
	ResetTime        time.Time `json:"reset_time"`
}

// FileTrackedSourceConfig configures a FileTrackedSource.
type FileTrackedSourceConfig struct {
	Name              string
	Weight            float64
	MinPeriod         time.Duration
	FetchRates        FetchRatesFunc
	CreditsPerRequest int64
	CreditsLimit      int64
	QuotaFile         string
	Log               slog.Logger
}

// FileTrackedSource is a source that tracks quota by persisting usage to a
// JSON file. Designed for APIs with known fixed limits where the quota
// endpoint is unavailable (e.g. CoinGecko demo tier).
type FileTrackedSource struct {
	name              string
	weight            float64
	minPeriod         time.Duration
	fetchRates        FetchRatesFunc
	creditsPerRequest int64
	creditsLimit      int64
	quotaFile         string
	log               slog.Logger

	mtx              sync.Mutex
	creditsRemaining int64
	resetTime        time.Time

	writeMtx sync.Mutex
}

func (cfg *FileTrackedSourceConfig) verify() {
	if cfg.Name == "" {
		panic("file tracked source: name is required")
	}
	if cfg.FetchRates == nil {
		panic("file tracked source: FetchRates is required")
	}
	if cfg.QuotaFile == "" {
		panic("file tracked source: QuotaFile is required")
	}
	if cfg.CreditsLimit <= 0 {
		panic("file tracked source: CreditsLimit must be positive")
	}
	if cfg.CreditsPerRequest <= 0 {
		panic("file tracked source: CreditsPerRequest must be positive")
	}
	if cfg.Log == nil {
		panic("file tracked source: Log is required")
	}
}

// NewFileTrackedSource creates a new file-tracked source. It loads quota from
// the file (or defaults if missing/expired) and registers itself.
func NewFileTrackedSource(cfg FileTrackedSourceConfig) *FileTrackedSource {
	cfg.verify()

	weight := cfg.Weight
	if weight == 0 {
		weight = defaultWeight
	}
	minPeriod := cfg.MinPeriod
	if minPeriod == 0 {
		minPeriod = defaultMinPeriod
	}

	s := &FileTrackedSource{
		name:              cfg.Name,
		weight:            weight,
		minPeriod:         minPeriod,
		fetchRates:        cfg.FetchRates,
		creditsPerRequest: cfg.CreditsPerRequest,
		creditsLimit:      cfg.CreditsLimit,
		quotaFile:         cfg.QuotaFile,
		log:               cfg.Log,
	}

	s.loadQuota()

	return s
}

func (s *FileTrackedSource) Name() string             { return s.name }
func (s *FileTrackedSource) Weight() float64          { return s.weight }
func (s *FileTrackedSource) MinPeriod() time.Duration { return s.minPeriod }

// FetchRates calls the underlying fetch function and, on success, decrements
// the remaining quota and persists the updated state to disk.
func (s *FileTrackedSource) FetchRates(ctx context.Context) (*sources.RateInfo, error) {
	rates, err := s.fetchRates(ctx)
	if err != nil {
		return nil, err
	}

	s.mtx.Lock()
	if s.creditsRemaining > s.creditsPerRequest {
		s.creditsRemaining -= s.creditsPerRequest
	} else {
		s.creditsRemaining = 0
	}
	s.mtx.Unlock()

	go s.persistQuota()
	return rates, nil
}

// QuotaStatus returns the current in-memory quota state.
func (s *FileTrackedSource) QuotaStatus() *sources.QuotaStatus {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return &sources.QuotaStatus{
		FetchesRemaining: s.creditsRemaining,
		FetchesLimit:     s.creditsLimit,
		ResetTime:        s.resetTime,
	}
}

// loadQuota reads quota state from the file. If the file is missing, corrupt,
// or the reset time has passed, it resets to the full limit with the next
// month's reset time.
func (s *FileTrackedSource) loadQuota() {
	data, err := os.ReadFile(s.quotaFile)
	if err != nil {
		s.log.Infof("[%s] No quota file found, starting with full limit (%d)", s.name, s.creditsLimit)
		s.resetToFull()
		return
	}

	var state fileQuotaState
	if err := json.Unmarshal(data, &state); err != nil {
		s.log.Warnf("[%s] Corrupt quota file, resetting to full limit: %v", s.name, err)
		s.resetToFull()
		return
	}

	if time.Now().UTC().After(state.ResetTime) {
		s.log.Infof("[%s] Quota period expired, resetting to full limit (%d)", s.name, s.creditsLimit)
		s.resetToFull()
		return
	}

	s.creditsRemaining = state.CreditsRemaining
	s.creditsLimit = state.CreditsLimit
	s.resetTime = state.ResetTime
	s.log.Infof("[%s] Loaded quota from file: %d/%d remaining, resets at %s",
		s.name, s.creditsRemaining, s.creditsLimit, s.resetTime.Format(time.RFC3339))
}

// resetToFull sets the quota to the full configured limit with reset at the
// start of next month UTC.
func (s *FileTrackedSource) resetToFull() {
	now := time.Now().UTC()
	s.creditsRemaining = s.creditsLimit
	s.resetTime = time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

// persistQuota writes the current quota state to the file. Serializes writes
// with a dedicated mutex to avoid contention with the main state mutex.
func (s *FileTrackedSource) persistQuota() {
	s.mtx.Lock()
	state := fileQuotaState{
		CreditsRemaining: s.creditsRemaining,
		CreditsLimit:     s.creditsLimit,
		ResetTime:        s.resetTime,
	}
	s.mtx.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		s.log.Errorf("[%s] Failed to marshal quota state: %v", s.name, err)
		return
	}

	s.writeMtx.Lock()
	defer s.writeMtx.Unlock()

	if err := os.WriteFile(s.quotaFile, data, 0600); err != nil {
		s.log.Errorf("[%s] Failed to write quota file: %v", s.name, err)
	}
}
