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

// NewCoinGeckoSource creates a CoinGecko price source.
func NewCoinGeckoSource(httpClient utils.HTTPClient, log slog.Logger, apiKey string) sources.Source {
	if apiKey == "" {
		return newCoinGeckoFreeSource(httpClient)
	}
	return newCoinGeckoProSource(httpClient, log, apiKey)
}

func newCoinGeckoFreeSource(httpClient utils.HTTPClient) *utils.UnlimitedSource {
	url := "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page=1"
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, httpClient, url, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return coingeckoParser(resp.Body)
	}
	return utils.NewUnlimitedSource(utils.UnlimitedSourceConfig{
		Name:       "coingecko",
		MinPeriod:  60 * time.Second,
		FetchRates: fetchRates,
	})
}

func newCoinGeckoProSource(httpClient utils.HTTPClient, log slog.Logger, apiKey string) *utils.TrackedSource {
	url := "https://pro-api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page=1"
	headers := []http.Header{{"x-cg-pro-api-key": []string{apiKey}}}
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, httpClient, url, headers)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return coingeckoParser(resp.Body)
	}

	tracker := utils.NewQuotaTracker(&utils.QuotaTrackerConfig{
		Name:              "coingecko",
		FetchQuota:        coingeckoQuotaFetcher(httpClient, apiKey),
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

func coingeckoQuotaFetcher(client utils.HTTPClient, apiKey string) func(ctx context.Context) (*sources.QuotaStatus, error) {
	return func(ctx context.Context) (*sources.QuotaStatus, error) {
		url := "https://pro-api.coingecko.com/api/v3/key"
		resp, err := utils.DoGet(ctx, client, url, []http.Header{{"x-cg-pro-api-key": []string{apiKey}}})
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
