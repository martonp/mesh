package providers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
)

const (
	// coingeckoDemoMonthlyLimit is the known monthly call limit for the
	// CoinGecko demo plan. Used for local quota tracking since the demo
	// tier does not expose a /key endpoint.
	coingeckoDemoMonthlyLimit = 9_500
)

// NewCoinGeckoSource creates a CoinGecko price source. An API key is required.
// If pro is true, the pro-tier base URL and header are used with API-based
// quota tracking via the /key endpoint. Otherwise, the demo-tier base URL is
// used with file-based quota tracking since the /key endpoint is pro-only.
// quotaFilePath is used only for demo tier and should be the path to a
// persistent JSON file for tracking quota across restarts.
func NewCoinGeckoSource(httpClient utils.HTTPClient, log slog.Logger, apiKey string, pro bool, quotaFilePath string) sources.Source {
	var baseURL, headerName string
	if pro {
		baseURL = "https://pro-api.coingecko.com/api/v3"
		headerName = "x-cg-pro-api-key"
	} else {
		baseURL = "https://api.coingecko.com/api/v3"
		headerName = "x-cg-demo-api-key"
	}

	marketsURL := baseURL + "/coins/markets?vs_currency=usd&per_page=250&page=1"
	headers := []http.Header{{headerName: []string{apiKey}}}

	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, httpClient, marketsURL, headers)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return coingeckoParser(resp.Body)
	}

	if pro {
		fetchQuota := coingeckoProQuotaFetcher(httpClient, baseURL, headerName, apiKey)
		tracker := utils.NewQuotaTracker(&utils.QuotaTrackerConfig{
			Name:              "coingecko",
			FetchQuota:        fetchQuota,
			ReconcileInterval: 30 * time.Second,
			Log:               log,
		})
		return utils.NewTrackedSource(utils.TrackedSourceConfig{
			Name:              "coingecko",
			MinPeriod:         30 * time.Second,
			FetchRates:        fetchRates,
			Tracker:           tracker,
			CreditsPerRequest: 1,
		})
	}

	return utils.NewFileTrackedSource(utils.FileTrackedSourceConfig{
		Name:              "coingecko",
		MinPeriod:         30 * time.Second,
		FetchRates:        fetchRates,
		CreditsPerRequest: 1,
		CreditsLimit:      coingeckoDemoMonthlyLimit,
		QuotaFile:         quotaFilePath,
		Log:               log,
	})
}

func coingeckoProQuotaFetcher(client utils.HTTPClient, baseURL, headerName, apiKey string) func(ctx context.Context) (*sources.QuotaStatus, error) {
	return func(ctx context.Context) (*sources.QuotaStatus, error) {
		url := baseURL + "/key"
		resp, err := utils.DoGet(ctx, client, url, []http.Header{{headerName: []string{apiKey}}})
		if err != nil {
			return nil, fmt.Errorf("error fetching quota: %v", err)
		}
		defer resp.Body.Close()

		var result struct {
			CurrentRemainingMonthlyCalls int64 `json:"current_remaining_monthly_calls"`
			ApiKeyMonthlyCallCredit      int64 `json:"api_key_monthly_call_credit"`
		}

		if err := utils.StreamDecodeJSON(resp.Body, &result); err != nil {
			return nil, fmt.Errorf("error parsing quota response: %v", err)
		}

		resetTime := time.Now().UTC()
		resetTime = time.Date(resetTime.Year(), resetTime.Month()+1, 1, 0, 0, 0, 0, time.UTC)

		return &sources.QuotaStatus{
			FetchesRemaining: result.CurrentRemainingMonthlyCalls,
			FetchesLimit:     result.ApiKeyMonthlyCallCredit,
			ResetTime:        resetTime,
		}, nil
	}
}

func coingeckoParser(r io.Reader) (*sources.RateInfo, error) {
	var prices []*struct {
		Symbol       string  `json:"symbol"`
		CurrentPrice float64 `json:"current_price"`
	}
	if err := utils.StreamDecodeJSON(r, &prices); err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(prices))
	us := make([]*sources.PriceUpdate, 0, len(prices))
	for _, p := range prices {
		ticker := strings.ToUpper(p.Symbol)
		if seen[ticker] {
			continue
		}
		seen[ticker] = true
		us = append(us, &sources.PriceUpdate{
			Ticker: sources.Ticker(ticker),
			Price:  p.CurrentPrice,
		})
	}
	return &sources.RateInfo{Prices: us}, nil
}
