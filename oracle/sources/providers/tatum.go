package providers

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
	"github.com/decred/slog"
)

const (
	tatumCreditsPerRequest = 10 // Each fee estimation call costs 10 credits.
	tatumReconcileInterval = 10 * time.Minute
	tatumMinPeriod         = 10 * time.Second // Real-time fee data, 3 req/sec rate limit.
)

// TatumConfig configures the Tatum source group.
type TatumConfig struct {
	HTTPClient utils.HTTPClient
	Log        slog.Logger
	APIKey     string
}

// TatumSources holds all Tatum-powered fee rate sources that share
// a single API key and quota tracker.
type TatumSources struct {
	Bitcoin  sources.Source
	Litecoin sources.Source
	Dogecoin sources.Source
	pool     *utils.QuotaTracker
}

// All returns all Tatum sources.
func (ts *TatumSources) All() []sources.Source {
	return []sources.Source{ts.Bitcoin, ts.Litecoin, ts.Dogecoin}
}

// NewTatumSources creates a Tatum source group with a shared quota tracker.
func NewTatumSources(cfg TatumConfig) *TatumSources {
	tracker := utils.NewQuotaTracker(&utils.QuotaTrackerConfig{
		Name:              "tatum",
		FetchQuota:        tatumQuotaFetcher(cfg.HTTPClient, cfg.APIKey),
		ReconcileInterval: tatumReconcileInterval,
		Log:               cfg.Log,
	})

	headers := []http.Header{{"x-api-key": []string{cfg.APIKey}}}

	mkSource := func(coin string, network sources.Network, name string) sources.Source {
		url := fmt.Sprintf("https://api.tatum.io/v3/blockchain/fee/%s", coin)
		parse := tatumParserForNetwork(network)
		fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
			resp, err := utils.DoGet(ctx, cfg.HTTPClient, url, headers)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			return parse(resp.Body)
		}
		return utils.NewTrackedSource(utils.TrackedSourceConfig{
			Name:              name,
			MinPeriod:         tatumMinPeriod,
			FetchRates:        fetchRates,
			Tracker:           tracker,
			CreditsPerRequest: tatumCreditsPerRequest,
		})
	}

	return &TatumSources{
		Bitcoin:  mkSource("BTC", "BTC", "tatum.btc"),
		Litecoin: mkSource("LTC", "LTC", "tatum.ltc"),
		Dogecoin: mkSource("DOGE", "DOGE", "tatum.doge"),
		pool:     tracker,
	}
}

// tatumQuotaFetcher returns a function that fetches quota from Tatum's usage endpoint.
func tatumQuotaFetcher(client utils.HTTPClient, apiKey string) func(ctx context.Context) (*sources.QuotaStatus, error) {
	return func(ctx context.Context) (*sources.QuotaStatus, error) {
		url := "https://api.tatum.io/v3/tatum/usage"
		resp, err := utils.DoGet(ctx, client, url, []http.Header{{"x-api-key": []string{apiKey}}})
		if err != nil {
			return nil, fmt.Errorf("error fetching tatum quota: %v", err)
		}
		defer resp.Body.Close()

		var result struct {
			Used  int64 `json:"used"`
			Limit int64 `json:"limit"`
		}

		if err := utils.StreamDecodeJSON(resp.Body, &result); err != nil {
			return nil, fmt.Errorf("error parsing tatum quota response: %v", err)
		}

		// Reset at first of next month.
		now := time.Now().UTC()
		nextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)

		return &sources.QuotaStatus{
			FetchesRemaining: max(result.Limit-result.Used, 0),
			FetchesLimit:     result.Limit,
			ResetTime:        nextMonth,
		}, nil
	}
}

func tatumParserForNetwork(network sources.Network) func(io.Reader) (*sources.RateInfo, error) {
	return func(r io.Reader) (*sources.RateInfo, error) {
		var resp struct {
			Fast float64 `json:"fast"`
		}
		if err := utils.StreamDecodeJSON(r, &resp); err != nil {
			return nil, err
		}
		if resp.Fast <= 0 {
			return nil, fmt.Errorf("fee rate cannot be negative or zero")
		}
		return &sources.RateInfo{
			FeeRates: []*sources.FeeRateUpdate{{
				Network: network,
				FeeRate: uint64ToBigInt(uint64(math.Round(resp.Fast))),
			}},
		}, nil
	}
}
