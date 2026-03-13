package providers

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
	"github.com/decred/slog"
)

// NewBlockcypherLitecoinSource creates a BlockCypher Litecoin fee rate source.
// BlockCypher has a free quota endpoint at /v1/tokens/$TOKEN that resets hourly.
// Free tier: 100 requests/hour = ~36 second interval minimum.
func NewBlockcypherLitecoinSource(httpClient utils.HTTPClient, log slog.Logger, token string) sources.Source {
	dataURL := "https://api.blockcypher.com/v1/ltc/main"
	if token != "" {
		dataURL += "?token=" + url.QueryEscape(token)
	}
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, httpClient, dataURL, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return blockcypherLitecoinParser(resp.Body)
	}

	tracker := utils.NewQuotaTracker(&utils.QuotaTrackerConfig{
		Name:              "blockcypher",
		FetchQuota:        blockcypherQuotaFetcher(httpClient, token),
		ReconcileInterval: 30 * time.Second,
		Log:               log,
	})
	return utils.NewTrackedSource(utils.TrackedSourceConfig{
		Name:              "ltc.blockcypher",
		Weight:            0.25,
		MinPeriod:         36 * time.Second,
		FetchRates:        fetchRates,
		Tracker:           tracker,
		CreditsPerRequest: 1,
	})
}

func blockcypherQuotaFetcher(client utils.HTTPClient, token string) func(ctx context.Context) (*sources.QuotaStatus, error) {
	return func(ctx context.Context) (*sources.QuotaStatus, error) {
		if token == "" {
			return utils.UnlimitedQuotaStatus(), nil
		}

		url := fmt.Sprintf("https://api.blockcypher.com/v1/tokens/%s", token)
		resp, err := utils.DoGet(ctx, client, url, nil)
		if err != nil {
			return nil, fmt.Errorf("error fetching quota: %v", err)
		}
		defer resp.Body.Close()

		var result struct {
			Limits struct {
				APIHour int64 `json:"api/hour"`
			} `json:"limits"`
			Hits struct {
				APIHour int64 `json:"api/hour"`
			} `json:"hits"`
		}

		if err := utils.StreamDecodeJSON(resp.Body, &result); err != nil {
			return nil, fmt.Errorf("error parsing quota response: %v", err)
		}

		// Calculate remaining from limit and current hour's usage
		limit := result.Limits.APIHour
		used := result.Hits.APIHour

		// Reset at top of next hour
		now := time.Now().UTC()
		resetTime := now.Truncate(time.Hour).Add(time.Hour)

		return &sources.QuotaStatus{
			FetchesRemaining: max(limit-used, 0),
			FetchesLimit:     limit,
			ResetTime:        resetTime,
		}, nil
	}
}

func blockcypherLitecoinParser(r io.Reader) (*sources.RateInfo, error) {
	var resp struct {
		Medium float64 `json:"medium_fee_per_kb"`
	}
	if err := utils.StreamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	if resp.Medium <= 0 {
		return nil, fmt.Errorf("fee rate cannot be negative or zero")
	}
	return &sources.RateInfo{
		FeeRates: []*sources.FeeRateUpdate{{
			Network: "LTC",
			// medium_fee_per_kb is in litoshis/kB. Divide by 1000 to get litoshis/byte.
			FeeRate: uint64ToBigInt(uint64(math.Round(resp.Medium / 1000))),
		}},
	}, nil
}
