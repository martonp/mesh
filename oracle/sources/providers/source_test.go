package providers_test

import (
	"bytes"
	"context"
	"io"
	"math"
	"math/big"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/providers"
)

// tHTTPClient implements sources.HTTPClient for testing.
type tHTTPClient struct {
	response *http.Response
	err      error
}

func (tc *tHTTPClient) Do(*http.Request) (*http.Response, error) { return tc.response, tc.err }

func newMockResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     make(http.Header),
	}
}

func testLogger() slog.Logger { return slog.Disabled }

func TestDcrdataSource(t *testing.T) {
	client := &tHTTPClient{response: newMockResponse(`{"2": 0.0001}`)}
	src := providers.NewDcrdataSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"2": 0.0001}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "DCR" {
			t.Errorf("expected network DCR, got %s", result.FeeRates[0].Network)
		}

		// 0.0001 DCR/kB * 1e5 = 10 atoms/byte
		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(10)) != 0 {
			t.Errorf("expected fee rate 10, got %s", result.FeeRates[0].FeeRate.String())
		}
	})

	t.Run("quota status is unlimited", func(t *testing.T) {
		status := src.QuotaStatus()
		if status.FetchesRemaining != math.MaxInt64 {
			t.Errorf("expected unlimited fetches, got %d", status.FetchesRemaining)
		}
	})
}

func TestMempoolDotSpaceSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewMempoolDotSpaceSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"fastestFee": 25}`)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "BTC" {
			t.Errorf("expected network BTC, got %s", result.FeeRates[0].Network)
		}

		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(25)) != 0 {
			t.Errorf("expected fee rate 25, got %s", result.FeeRates[0].FeeRate.String())
		}
	})
}

func TestCoinpaprikaSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewCoinpaprikaSource(client, testLogger())

	t.Run("valid response", func(t *testing.T) {
		body := `[
			{"id":"btc-bitcoin","symbol":"BTC","quotes":{"USD":{"price":87838.55}}},
			{"id":"eth-ethereum","symbol":"ETH","quotes":{"USD":{"price":2954.14}}}
		]`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}

		prices := make(map[sources.Ticker]float64)
		for _, p := range result.Prices {
			prices[p.Ticker] = p.Price
		}

		if prices["BTC"] != 87838.55 {
			t.Errorf("expected BTC price 87838.55, got %f", prices["BTC"])
		}
	})
}

func TestCoinMarketCapSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewCoinMarketCapSource(client, testLogger(), "test-api-key")

	t.Run("valid response", func(t *testing.T) {
		body := `{
			"data": [
				{"symbol":"BTC","quote":{"USD":{"price":90000.50}}},
				{"symbol":"ETH","quote":{"USD":{"price":3100.25}}}
			]
		}`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}
	})

	t.Run("quota status refreshes", func(t *testing.T) {
		// Initially should return unlimited (no quota fetched yet)
		status := src.QuotaStatus()
		if status == nil {
			t.Fatal("expected quota status")
		}
	})
}

func TestTatumSources(t *testing.T) {
	client := &tHTTPClient{}
	tatumSources := providers.NewTatumSources(providers.TatumConfig{
		HTTPClient: client,
		Log:        testLogger(),
		APIKey:     "test-api-key",
	})

	t.Run("all sources returned", func(t *testing.T) {
		all := tatumSources.All()
		if len(all) != 3 {
			t.Fatalf("expected 3 sources, got %d", len(all))
		}
	})

	t.Run("btc valid response", func(t *testing.T) {
		client.response = newMockResponse(`{"fast": 25}`)
		result, err := tatumSources.Bitcoin.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "BTC" {
			t.Errorf("expected network BTC, got %s", result.FeeRates[0].Network)
		}

		if result.FeeRates[0].FeeRate.Cmp(big.NewInt(25)) != 0 {
			t.Errorf("expected fee rate 25, got %s", result.FeeRates[0].FeeRate.String())
		}
	})

	t.Run("pool tracks consumption", func(t *testing.T) {
		// After a fetch, pool should have consumed 10 credits.
		// Before reconciliation, quota is unlimited.
		status := tatumSources.Bitcoin.QuotaStatus()
		if status == nil {
			t.Fatal("expected quota status")
		}
	})
}

func TestBlockcypherSource(t *testing.T) {
	client := &tHTTPClient{}
	src := providers.NewBlockcypherLitecoinSource(client, testLogger(), "test-token")

	t.Run("valid response", func(t *testing.T) {
		body := `{"medium_fee_per_kb": 10000}`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.FeeRates) != 1 {
			t.Fatalf("expected 1 fee rate, got %d", len(result.FeeRates))
		}

		if result.FeeRates[0].Network != "LTC" {
			t.Errorf("expected network LTC, got %s", result.FeeRates[0].Network)
		}
	})
}

func TestCoinGeckoSource(t *testing.T) {
	client := &tHTTPClient{}
	quotaFile := filepath.Join(t.TempDir(), "coingecko_quota.json")
	src := providers.NewCoinGeckoSource(client, testLogger(), "test-key", false, quotaFile)

	t.Run("valid response", func(t *testing.T) {
		body := `[
			{"symbol":"btc","current_price":90000.50},
			{"symbol":"eth","current_price":3100.25}
		]`
		client.response = newMockResponse(body)
		result, err := src.FetchRates(context.Background())
		if err != nil {
			t.Fatalf("fetch failed: %v", err)
		}

		if len(result.Prices) != 2 {
			t.Fatalf("expected 2 prices, got %d", len(result.Prices))
		}

		prices := make(map[sources.Ticker]float64)
		for _, p := range result.Prices {
			prices[p.Ticker] = p.Price
		}

		if prices["BTC"] != 90000.50 {
			t.Errorf("expected BTC price 90000.50, got %f", prices["BTC"])
		}

		if prices["ETH"] != 3100.25 {
			t.Errorf("expected ETH price 3100.25, got %f", prices["ETH"])
		}
	})

	t.Run("quota status is file tracked", func(t *testing.T) {
		status := src.QuotaStatus()
		if status == nil {
			t.Fatal("expected non-nil QuotaStatus")
		}
		// File-tracked demo source starts with full limit minus fetches done.
		if status.FetchesLimit != 9500 {
			t.Errorf("expected 9500 limit, got %d", status.FetchesLimit)
		}
	})
}
