package providers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/utils"
	"github.com/decred/slog"
)

// NewCoinMarketCapSource creates a CoinMarketCap price source.
// Free plan gives 10,000 credits per month. The listings endpoint uses 1 credit
// per call per 200 assets. With 400 assets, we can call ~5,000 times per month,
// which is about 1 call per 8.9 minutes. We call every 10 minutes to be safe.
// MinPeriod is 60s because CoinMarketCap data only updates every minute.
func NewCoinMarketCapSource(httpClient utils.HTTPClient, log slog.Logger, apiKey string) sources.Source {
	url := "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=400"
	headers := []http.Header{{"X-CMC_PRO_API_KEY": []string{apiKey}}}
	fetchRates := func(ctx context.Context) (*sources.RateInfo, error) {
		resp, err := utils.DoGet(ctx, httpClient, url, headers)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return coinmarketcapParser(resp.Body)
	}

	tracker := utils.NewQuotaTracker(&utils.QuotaTrackerConfig{
		Name:              "coinmarketcap",
		FetchQuota:        cmcQuotaFetcher(httpClient, apiKey),
		ReconcileInterval: 30 * time.Second,
		Log:               log,
	})
	return utils.NewTrackedSource(utils.TrackedSourceConfig{
		Name:              "coinmarketcap",
		MinPeriod:         60 * time.Second,
		FetchRates:        fetchRates,
		Tracker:           tracker,
		CreditsPerRequest: 2, // 1 per 200 assets, fetching 400
	})
}

func cmcQuotaFetcher(client utils.HTTPClient, apiKey string) func(ctx context.Context) (*sources.QuotaStatus, error) {
	return func(ctx context.Context) (*sources.QuotaStatus, error) {
		url := "https://pro-api.coinmarketcap.com/v1/key/info"
		resp, err := utils.DoGet(ctx, client, url, []http.Header{{"X-CMC_PRO_API_KEY": []string{apiKey}}})
		if err != nil {
			return nil, fmt.Errorf("error fetching quota: %v", err)
		}
		defer resp.Body.Close()

		var result struct {
			Data struct {
				Plan struct {
					CreditLimitMonthly        int64  `json:"credit_limit_monthly"`
					CreditLimitMonthlyResetTS string `json:"credit_limit_monthly_reset_timestamp"`
				} `json:"plan"`
				Usage struct {
					CurrentMonth struct {
						CreditsUsed int64 `json:"credits_used"`
					} `json:"current_month"`
				} `json:"usage"`
			} `json:"data"`
		}

		if err := utils.StreamDecodeJSON(resp.Body, &result); err != nil {
			return nil, fmt.Errorf("error parsing quota response: %v", err)
		}

		// Parse reset timestamp from API response
		resetTime, err := time.Parse(time.RFC3339, result.Data.Plan.CreditLimitMonthlyResetTS)
		if err != nil {
			// Fallback to first of next month if parsing fails
			now := time.Now().UTC()
			resetTime = time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		}

		return &sources.QuotaStatus{
			FetchesRemaining: max(result.Data.Plan.CreditLimitMonthly-result.Data.Usage.CurrentMonth.CreditsUsed, 0),
			FetchesLimit:     result.Data.Plan.CreditLimitMonthly,
			ResetTime:        resetTime,
		}, nil
	}
}

func coinmarketcapParser(r io.Reader) (*sources.RateInfo, error) {
	var resp struct {
		Data []*struct {
			Symbol string `json:"symbol"`
			Quote  struct {
				USD struct {
					Price float64 `json:"price"`
				} `json:"USD"`
			} `json:"quote"`
		} `json:"data"`
	}
	if err := utils.StreamDecodeJSON(r, &resp); err != nil {
		return nil, err
	}
	prices := resp.Data
	seen := make(map[string]bool, len(prices))
	us := make([]*sources.PriceUpdate, 0, len(prices))
	for _, p := range prices {
		if seen[p.Symbol] {
			continue
		}
		seen[p.Symbol] = true
		us = append(us, &sources.PriceUpdate{
			Ticker: sources.Ticker(p.Symbol),
			Price:  p.Quote.USD.Price,
		})
	}
	return &sources.RateInfo{Prices: us}, nil
}
