package oracle

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/bisoncraft/mesh/oracle/sources/providers"
	"github.com/decred/slog"
)

// Ticker is the upper-case symbol used to indicate an asset.
type Ticker string

// Network is the network symbol of a Blockchain.
type Network string

// OracleUpdate is the payload published to the mesh for oracle data.
// At least one of Prices or FeeRates should be populated.
type OracleUpdate struct {
	Source   string
	Stamp    time.Time
	Prices   map[Ticker]float64
	FeeRates map[Network]*big.Int
	Quota    *sources.QuotaStatus
}

// MergeResult contains the aggregated rates that changed after a merge.
type MergeResult struct {
	Prices   map[Ticker]float64
	FeeRates map[Network]*big.Int
}

// HTTPClient defines the requirements for implementing an http client.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Config contains configuration for the Oracle.
type Config struct {
	// NodeID is the ID of the local node running the oracle.
	NodeID string

	// PublishUpdate is called when the oracle has fetched new data from a
	// source.
	PublishUpdate func(ctx context.Context, update *OracleUpdate) error

	// OnStateUpdate is called when some state in the oracle has changed.
	// Only the updated fields are populated. The full snapshot can be fetched
	// using OracleSnapshot, and then updates received on this function can be
	// combined with the full snapshot to get the current state.
	OnStateUpdate func(*OracleSnapshot)

	// PublishQuotaHeartbeat is called periodically to update other nodes with
	// the current quota status for all sources.
	PublishQuotaHeartbeat func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error

	// Log is the logger used to log messages.
	Log slog.Logger

	// CMCKey is the token used to fetch data from the CoinMarketCap API.
	CMCKey string

	// TatumKey is the token used to fetch data from the Tatum API.
	TatumKey string

	// BlockcypherToken is the token used to fetch data from the Blockcypher API.
	BlockcypherToken string

	// HTTPClient is the HTTP client used to fetch data from the sources.
	// If nil, http.DefaultClient is used.
	HTTPClient HTTPClient
}

// verify validates the Oracle configuration.
func (cfg *Config) verify() error {
	if cfg == nil {
		return fmt.Errorf("oracle config is nil")
	}
	if cfg.PublishUpdate == nil {
		return fmt.Errorf("publish update callback is required")
	}
	if cfg.OnStateUpdate == nil {
		return fmt.Errorf("state update callback is required")
	}
	if cfg.PublishQuotaHeartbeat == nil {
		return fmt.Errorf("publish quota heartbeat callback is required")
	}
	if cfg.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	return nil
}

// Oracle manages price and fee rate data from multiple sources.
type Oracle struct {
	log        slog.Logger
	httpClient HTTPClient
	srcs       []sources.Source

	feeRatesMtx sync.RWMutex
	feeRates    map[Network]*feeRateBucket

	pricesMtx sync.RWMutex
	prices    map[Ticker]*priceBucket

	divinersMtx sync.RWMutex
	diviners    map[string]*diviner

	publishUpdate func(ctx context.Context, update *OracleUpdate) error
	onStateUpdate func(*OracleSnapshot)
	quotaManager  *quotaManager
	fetchTracker  *fetchTracker
	nodeID        string
}

// New creates a new Oracle with the given configuration.
func New(cfg *Config) (*Oracle, error) {
	if err := cfg.verify(); err != nil {
		return nil, err
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	// Add all sources that don't require an API key.
	unlimitedSources := []sources.Source{
		providers.NewDcrdataSource(httpClient, cfg.Log),
		providers.NewMempoolDotSpaceSource(httpClient, cfg.Log),
		providers.NewCoinpaprikaSource(httpClient, cfg.Log),
		providers.NewBitcoreBitcoinCashSource(httpClient, cfg.Log),
		providers.NewBitcoreDogecoinSource(httpClient, cfg.Log),
		providers.NewBitcoreLitecoinSource(httpClient, cfg.Log),
		providers.NewFiroOrgSource(httpClient, cfg.Log),
	}
	allSources := make([]sources.Source, 0, len(unlimitedSources))
	allSources = append(allSources, unlimitedSources...)

	if cfg.BlockcypherToken != "" {
		blockcypherSource := providers.NewBlockcypherLitecoinSource(httpClient, cfg.Log, cfg.BlockcypherToken)
		allSources = append(allSources, blockcypherSource)
	}

	if cfg.CMCKey != "" {
		cmcSource := providers.NewCoinMarketCapSource(httpClient, cfg.Log, cfg.CMCKey)
		allSources = append(allSources, cmcSource)
	}

	if cfg.TatumKey != "" {
		tatumSources := providers.NewTatumSources(providers.TatumConfig{
			HTTPClient: httpClient,
			Log:        cfg.Log,
			APIKey:     cfg.TatumKey,
		})
		allSources = append(allSources, tatumSources.All()...)
	}

	quotaManager := newQuotaManager(&quotaManagerConfig{
		log:                   cfg.Log,
		nodeID:                cfg.NodeID,
		publishQuotaHeartbeat: cfg.PublishQuotaHeartbeat,
		onStateUpdate:         cfg.OnStateUpdate,
		sources:               allSources,
	})

	oracle := &Oracle{
		log:           cfg.Log,
		httpClient:    httpClient,
		srcs:          allSources,
		feeRates:      make(map[Network]*feeRateBucket),
		prices:        make(map[Ticker]*priceBucket),
		diviners:      make(map[string]*diviner),
		publishUpdate: cfg.PublishUpdate,
		onStateUpdate: cfg.OnStateUpdate,
		quotaManager:  quotaManager,
		fetchTracker:  newFetchTracker(),
		nodeID:        cfg.NodeID,
	}

	// Create diviners for each source
	for _, src := range oracle.srcs {
		getNetworkSchedule := func(s sources.Source) func() networkSchedule {
			return func() networkSchedule {
				return quotaManager.getNetworkSchedule(s.Name(), s.MinPeriod())
			}
		}(src)
		div := newDiviner(src, oracle.publishUpdate, oracle.log, getNetworkSchedule, cfg.OnStateUpdate)
		oracle.diviners[src.Name()] = div
	}

	return oracle, nil
}

// allFeeRates returns the aggregated tx fee rates for all known networks.
func (o *Oracle) allFeeRates() map[Network]*big.Int {
	o.feeRatesMtx.RLock()
	defer o.feeRatesMtx.RUnlock()

	feeRates := make(map[Network]*big.Int, len(o.feeRates))
	for net, bucket := range o.feeRates {
		if rate := bucket.aggregatedRate(); rate != nil && rate.Sign() > 0 {
			feeRates[net] = rate
		}
	}
	return feeRates
}

// Merge merges an oracle update from another node into this oracle.
// Returns the aggregated rates that changed.
func (o *Oracle) Merge(update *OracleUpdate, senderID string) *MergeResult {
	if update == nil || (len(update.Prices) == 0 && len(update.FeeRates) == 0) {
		return nil
	}

	weight := o.sourceWeight(update.Source)
	result := &MergeResult{}

	if len(update.FeeRates) > 0 {
		result.FeeRates = o.mergeFeeRates(update, weight)
	}
	if len(update.Prices) > 0 {
		result.Prices = o.mergePrices(update, weight)
	}

	o.fetchTracker.recordFetch(update.Source, senderID, update.Stamp)
	o.rescheduleDiviner(update.Source, senderID)

	return result
}

func (o *Oracle) mergeFeeRates(update *OracleUpdate, weight float64) map[Network]*big.Int {
	if len(update.FeeRates) == 0 {
		return nil
	}

	updatedFeeRates := make(map[Network]*big.Int)
	snapshotFeeRates := make(map[string]*SnapshotRate)
	var latestFeeRates map[string]string

	for network, feeRate := range update.FeeRates {
		proposedUpdate := &feeRateUpdate{
			network: network,
			feeRate: feeRate,
			stamp:   update.Stamp,
			weight:  weight,
		}

		bucket := o.getOrCreateFeeRateBucket(network)
		updated, agg := bucket.mergeAndUpdateAggregate(update.Source, proposedUpdate)
		if updated && agg.Sign() > 0 {
			updatedFeeRates[network] = agg
			snapshotFeeRates[string(network)] = &SnapshotRate{
				Value: agg.String(),
				Contributions: map[string]*SourceContribution{
					update.Source: {
						Value:  feeRate.String(),
						Stamp:  update.Stamp,
						Weight: weight,
					},
				},
			}
			if latestFeeRates == nil {
				latestFeeRates = make(map[string]string)
			}
			latestFeeRates[string(network)] = feeRate.String()
		}
	}

	if len(snapshotFeeRates) > 0 {
		fetchCounts := o.fetchTracker.sourceFetchCounts(update.Source)
		stamp := update.Stamp
		o.onStateUpdate(&OracleSnapshot{
			Sources: map[string]*SourceStatus{
				update.Source: {
					LastFetch:  &stamp,
					Fetches24h: fetchCounts,
					LatestData: map[string]map[string]string{
						FeeRateData: latestFeeRates,
					},
				},
			},
			FeeRates: snapshotFeeRates,
		})
	}

	return updatedFeeRates
}

// allPrices returns the aggregated prices for all known tickers.
func (o *Oracle) allPrices() map[Ticker]float64 {
	o.pricesMtx.RLock()
	defer o.pricesMtx.RUnlock()

	prices := make(map[Ticker]float64, len(o.prices))
	for ticker, bucket := range o.prices {
		if price := bucket.aggregatedPrice(); price > 0 {
			prices[ticker] = price
		}
	}
	return prices
}

// Price returns the cached aggregated price for a single ticker.
func (o *Oracle) Price(ticker Ticker) (float64, bool) {
	bucket := o.getPriceBucket(ticker)
	if bucket == nil {
		return 0, false
	}
	if price := bucket.aggregatedPrice(); price > 0 {
		return price, true
	}
	return 0, false
}

// FeeRate returns the cached aggregated fee rate for a single network.
func (o *Oracle) FeeRate(network Network) (*big.Int, bool) {
	bucket := o.getFeeRateBucket(network)
	if bucket == nil {
		return nil, false
	}
	if rate := bucket.aggregatedRate(); rate != nil && rate.Sign() > 0 {
		return rate, true
	}
	return nil, false
}

func (o *Oracle) rescheduleDiviner(name string, lastFetchNodeID string) {
	// diviner reschedules itself after a fetch.
	if lastFetchNodeID == o.nodeID {
		return
	}

	o.divinersMtx.RLock()
	div, found := o.diviners[name]
	o.divinersMtx.RUnlock()
	if !found {
		return
	}

	div.reschedule()
}

// sourceWeight returns the configured weight for a source by name.
// If the source is not found, returns 1.0 as a default weight.
func (o *Oracle) sourceWeight(sourceName string) float64 {
	o.divinersMtx.RLock()
	div, found := o.diviners[sourceName]
	o.divinersMtx.RUnlock()
	if !found {
		return 1.0
	}
	return div.source.Weight()
}

func (o *Oracle) mergePrices(update *OracleUpdate, weight float64) map[Ticker]float64 {
	if len(update.Prices) == 0 {
		return nil
	}

	updatedPrices := make(map[Ticker]float64)
	snapshotPrices := make(map[string]*SnapshotRate)
	var latestPrices map[string]string

	for ticker, price := range update.Prices {
		proposedUpdate := &priceUpdate{
			ticker: ticker,
			price:  price,
			stamp:  update.Stamp,
			weight: weight,
		}

		bucket := o.getOrCreatePriceBucket(ticker)
		updated, agg := bucket.mergeAndUpdateAggregate(update.Source, proposedUpdate)
		if updated && agg > 0 {
			updatedPrices[ticker] = agg
			snapshotPrices[string(ticker)] = &SnapshotRate{
				Value: fmt.Sprintf("%f", agg),
				Contributions: map[string]*SourceContribution{
					update.Source: {
						Value:  fmt.Sprintf("%f", price),
						Stamp:  update.Stamp,
						Weight: weight,
					},
				},
			}
			if latestPrices == nil {
				latestPrices = make(map[string]string)
			}
			latestPrices[string(ticker)] = fmt.Sprintf("%f", price)
		}
	}

	if len(snapshotPrices) > 0 {
		fetchCounts := o.fetchTracker.sourceFetchCounts(update.Source)
		stamp := update.Stamp
		o.onStateUpdate(&OracleSnapshot{
			Sources: map[string]*SourceStatus{
				update.Source: {
					LastFetch:  &stamp,
					Fetches24h: fetchCounts,
					LatestData: map[string]map[string]string{
						PriceData: latestPrices,
					},
				},
			},
			Prices: snapshotPrices,
		})
	}

	return updatedPrices
}

// Run starts the oracle and blocks until the context is done.
func (o *Oracle) Run(ctx context.Context) {
	var wg sync.WaitGroup

	// Run quota manager.
	wg.Add(1)
	go func() {
		defer wg.Done()
		o.quotaManager.run(ctx)
	}()

	// Run all diviners
	o.divinersMtx.RLock()
	for _, div := range o.diviners {
		wg.Add(1)
		go func(d *diviner) {
			defer wg.Done()
			d.run(ctx)
		}(div)
	}
	o.divinersMtx.RUnlock()

	wg.Wait()
}

// GetLocalQuotas returns all local source quotas for handshake/heartbeat.
func (o *Oracle) GetLocalQuotas() map[string]*sources.QuotaStatus {
	return o.quotaManager.getLocalQuotas()
}

// UpdatePeerSourceQuota processes a single source's quota from a peer node.
func (o *Oracle) UpdatePeerSourceQuota(peerID string, quota *TimestampedQuotaStatus, source string) {
	o.quotaManager.handlePeerSourceQuota(peerID, quota, source)
}
