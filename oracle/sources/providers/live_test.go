//go:build live

package providers_test

import (
	"context"
	"math"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/decred/slog"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/providers"
)

// These tests make real HTTP requests to external APIs.
// Run with: go test -tags=live -v ./oracle/sources/providers

// Supply API keys before running authenticated source tests.
const (
	coinmarketcapAPIKey = "b2def99762ca4df2b5d557ae6bf1a4a5"
	tatumAPIKey         = ""
	blockcypherToken    = "91ded84bd49348688d319245a62388af"
	coingeckoAPIKey     = ""
)

func liveTestLogger() slog.Logger { return slog.Disabled }

func httpClient() *http.Client { return &http.Client{Timeout: 30 * time.Second} }

// === Unlimited Sources (no API key required) ===

func TestLiveDcrdataSource(t *testing.T) {
	src := providers.NewDcrdataSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "DCR")
	testMinPeriod(t, src, 30*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveMempoolDotSpaceSource(t *testing.T) {
	src := providers.NewMempoolDotSpaceSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "BTC")
	testMinPeriod(t, src, 10*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveCoinpaprikaSource(t *testing.T) {
	src := providers.NewCoinpaprikaSource(httpClient(), liveTestLogger())
	testPriceSource(t, src)
	testMinPeriod(t, src, 60*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveCoinGeckoSourceDemo(t *testing.T) {
	if coingeckoAPIKey == "" {
		t.Skip("coingecko API key not provided")
	}
	quotaFile := filepath.Join(t.TempDir(), "coingecko_quota.json")
	src := providers.NewCoinGeckoSource(httpClient(), liveTestLogger(), coingeckoAPIKey, false, quotaFile)
	testPriceSource(t, src)
	testMinPeriod(t, src, 30*time.Second)
	testPooledQuota(t, src)
}

func TestLiveBitcoreBitcoinCashSource(t *testing.T) {
	src := providers.NewBitcoreBitcoinCashSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "BCH")
	testMinPeriod(t, src, 30*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveBitcoreDogecoinSource(t *testing.T) {
	src := providers.NewBitcoreDogecoinSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "DOGE")
	testMinPeriod(t, src, 30*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveBitcoreLitecoinSource(t *testing.T) {
	src := providers.NewBitcoreLitecoinSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "LTC")
	testMinPeriod(t, src, 30*time.Second)
	testUnlimitedQuota(t, src)
}

func TestLiveFiroOrgSource(t *testing.T) {
	src := providers.NewFiroOrgSource(httpClient(), liveTestLogger())
	testFeeRateSource(t, src, "FIRO")
	testMinPeriod(t, src, 30*time.Second)
	testUnlimitedQuota(t, src)
}

// === Authenticated Sources (API key required) ===

func TestLiveBlockcypherLitecoinSource(t *testing.T) {
	src := providers.NewBlockcypherLitecoinSource(httpClient(), liveTestLogger(), blockcypherToken)
	testFeeRateSource(t, src, "LTC")
	testMinPeriod(t, src, 36*time.Second)

	if blockcypherToken != "" {
		testPooledQuota(t, src)
	} else {
		t.Log("Skipping quota test: blockcypher token not provided")
		testUnlimitedQuota(t, src)
	}
}

func TestLiveCoinMarketCapSource(t *testing.T) {
	if coinmarketcapAPIKey == "" {
		t.Skip("coinmarketcap API key not provided")
	}
	src := providers.NewCoinMarketCapSource(httpClient(), liveTestLogger(), coinmarketcapAPIKey)
	testPriceSource(t, src)
	testMinPeriod(t, src, 60*time.Second)
	testPooledQuota(t, src)
}

func TestLiveCoinGeckoSourcePro(t *testing.T) {
	if coingeckoAPIKey == "" {
		t.Skip("coingecko API key not provided")
	}
	src := providers.NewCoinGeckoSource(httpClient(), liveTestLogger(), coingeckoAPIKey, true, "")
	testPriceSource(t, src)
	testMinPeriod(t, src, 30*time.Second)
	testPooledQuota(t, src)
}

func TestLiveTatumSources(t *testing.T) {
	if tatumAPIKey == "" {
		t.Skip("tatum API key not provided")
	}
	tatumSources := providers.NewTatumSources(providers.TatumConfig{
		HTTPClient: httpClient(),
		Log:        liveTestLogger(),
		APIKey:     tatumAPIKey,
	})

	t.Run("bitcoin", func(t *testing.T) {
		testFeeRateSource(t, tatumSources.Bitcoin, "BTC")
		testMinPeriod(t, tatumSources.Bitcoin, 10*time.Second)
	})
	t.Run("litecoin", func(t *testing.T) {
		testFeeRateSource(t, tatumSources.Litecoin, "LTC")
		testMinPeriod(t, tatumSources.Litecoin, 10*time.Second)
	})
	t.Run("dogecoin", func(t *testing.T) {
		testFeeRateSource(t, tatumSources.Dogecoin, "DOGE")
		testMinPeriod(t, tatumSources.Dogecoin, 10*time.Second)
	})
	t.Run("shared quota", func(t *testing.T) {
		// Give reconciliation time to complete.
		time.Sleep(2 * time.Second)
		for _, src := range tatumSources.All() {
			testPooledQuota(t, src)
		}
	})
}

// === Helper Functions ===

func testFeeRateSource(t *testing.T, src sources.Source, expectedNetwork sources.Network) {
	t.Helper()

	testSourceInterface(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := src.FetchRates(ctx)
	if err != nil {
		t.Fatalf("FetchRates failed: %v", err)
	}
	if len(result.FeeRates) == 0 {
		t.Fatal("no fee rates returned")
	}

	found := false
	for _, fr := range result.FeeRates {
		if fr.Network == expectedNetwork {
			found = true
			if fr.FeeRate == nil || fr.FeeRate.Sign() <= 0 {
				t.Errorf("fee rate for %s is nil or non-positive", expectedNetwork)
			}
			t.Logf("[%s] %s fee rate: %s", src.Name(), expectedNetwork, fr.FeeRate.String())
		}
	}
	if !found {
		t.Errorf("expected network %s not found in fee rates", expectedNetwork)
	}
}

func testPriceSource(t *testing.T, src sources.Source) {
	t.Helper()

	testSourceInterface(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := src.FetchRates(ctx)
	if err != nil {
		t.Fatalf("FetchRates failed: %v", err)
	}
	if len(result.Prices) == 0 {
		t.Fatal("no prices returned")
	}

	t.Logf("[%s] total prices returned: %d", src.Name(), len(result.Prices))

	// Log some common tickers
	commonTickers := []sources.Ticker{"BTC", "ETH", "LTC", "DCR", "DOGE"}
	for _, ticker := range commonTickers {
		for _, p := range result.Prices {
			if p.Ticker == ticker {
				if p.Price <= 0 {
					t.Errorf("price for %s is <= 0: %f", ticker, p.Price)
				}
				t.Logf("[%s] %s price: $%.2f", src.Name(), ticker, p.Price)
				break
			}
		}
	}
}

func testSourceInterface(t *testing.T, src sources.Source) {
	t.Helper()

	name := src.Name()
	if name == "" {
		t.Error("Name() returned empty string")
	}

	weight := src.Weight()
	if weight <= 0 || weight > 1 {
		t.Errorf("Weight() returned %f, expected (0, 1]", weight)
	}

	minPeriod := src.MinPeriod()
	if minPeriod <= 0 {
		t.Errorf("MinPeriod() returned %v, expected > 0", minPeriod)
	}

	quota := src.QuotaStatus()
	if quota == nil {
		t.Error("QuotaStatus() returned nil")
	}

	t.Logf("[%s] interface: weight=%.2f, minPeriod=%v", name, weight, minPeriod)
}

func testMinPeriod(t *testing.T, src sources.Source, expected time.Duration) {
	t.Helper()
	actual := src.MinPeriod()
	if actual != expected {
		t.Errorf("[%s] MinPeriod() = %v, expected %v", src.Name(), actual, expected)
	}
}

func testUnlimitedQuota(t *testing.T, src sources.Source) {
	t.Helper()
	status := src.QuotaStatus()
	if status == nil {
		t.Fatal("QuotaStatus() returned nil")
	}
	if status.FetchesRemaining != math.MaxInt64 {
		t.Errorf("[%s] expected unlimited fetches (MaxInt64), got %d", src.Name(), status.FetchesRemaining)
	}
	t.Logf("[%s] quota: unlimited (fetches=%d)", src.Name(), status.FetchesRemaining)
}

func testPooledQuota(t *testing.T, src sources.Source) {
	t.Helper()
	status := src.QuotaStatus()
	if status == nil {
		t.Fatal("QuotaStatus() returned nil")
	}
	if status.FetchesLimit <= 0 {
		t.Errorf("[%s] expected positive FetchesLimit, got %d", src.Name(), status.FetchesLimit)
	}
	if status.FetchesRemaining < 0 {
		t.Errorf("[%s] expected non-negative FetchesRemaining, got %d", src.Name(), status.FetchesRemaining)
	}
	if status.ResetTime.IsZero() {
		t.Errorf("[%s] expected ResetTime to be set", src.Name())
	}
	t.Logf("[%s] pooled quota: %d/%d fetches remaining (per source), resets at %v",
		src.Name(), status.FetchesRemaining, status.FetchesLimit, status.ResetTime.Format(time.RFC3339))
}
