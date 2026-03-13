package oracle

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/bisoncraft/mesh/oracle/sources"
	"github.com/decred/slog"
)

// makePriceBuckets converts a test-friendly format to the Oracle's bucket format.
func makePriceBuckets(m map[Ticker]map[string]*priceUpdate) map[Ticker]*priceBucket {
	result := make(map[Ticker]*priceBucket, len(m))
	for ticker, sources := range m {
		bucket := newPriceBucket()
		for source, update := range sources {
			bucket.mergeAndUpdateAggregate(source, update)
		}
		result[ticker] = bucket
	}
	return result
}

// makeFeeRateBuckets converts a test-friendly format to the Oracle's bucket format.
func makeFeeRateBuckets(m map[Network]map[string]*feeRateUpdate) map[Network]*feeRateBucket {
	result := make(map[Network]*feeRateBucket, len(m))
	for network, sources := range m {
		bucket := newFeeRateBucket()
		for source, update := range sources {
			bucket.mergeAndUpdateAggregate(source, update)
		}
		result[network] = bucket
	}
	return result
}

func newTestOracle(log slog.Logger) *Oracle {
	return &Oracle{
		log:           log,
		prices:        make(map[Ticker]*priceBucket),
		feeRates:      make(map[Network]*feeRateBucket),
		diviners:      make(map[string]*diviner),
		fetchTracker:  newFetchTracker(),
		onStateUpdate: func(*OracleSnapshot) {},
	}
}

func setSourceWeights(oracle *Oracle, weights map[string]float64) {
	for name, weight := range weights {
		oracle.diviners[name] = &diviner{source: &mockSource{name: name, weight: weight}}
	}
}

func TestMergePrices(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name           string
		existingPrices map[Ticker]map[string]*priceUpdate
		update         *OracleUpdate
		sourceWeights  map[string]float64
		expectedPrices map[Ticker]map[string]*priceUpdate
		expectedResult map[Ticker]float64
	}{
		{
			name:           "new ticker from external source",
			existingPrices: map[Ticker]map[string]*priceUpdate{},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 50000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with newer timestamp should update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  48000.0,
						stamp:  oldStamp,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				Prices: map[Ticker]float64{
					"BTC": 50000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50000.0,
			},
		},
		{
			name: "existing ticker with older timestamp should ignore",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				Prices: map[Ticker]float64{
					"BTC": 48000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"external-oracle": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  newerStamp,
						weight: 1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple prices in single update",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 51000.0,
					"ETH": 3000.0,
				},
			},
			sourceWeights: map[string]float64{
				"source2": 0.8,
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
				"ETH": {
					"source2": {
						ticker: "ETH",
						price:  3000.0,
						stamp:  now,
						weight: 0.8,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50444.444444444445253, // (50000*1.0 + 51000*0.8) / 1.8
				"ETH": 3000.0,
			},
		},
		{
			name: "new source for existing ticker",
			existingPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				Prices: map[Ticker]float64{
					"BTC": 51000.0,
				},
			},
			expectedPrices: map[Ticker]map[string]*priceUpdate{
				"BTC": {
					"source1": {
						ticker: "BTC",
						price:  50000.0,
						stamp:  now,
						weight: 1.0,
					},
					"source2": {
						ticker: "BTC",
						price:  51000.0,
						stamp:  now,
						weight: 1.0,
					},
				},
			},
			expectedResult: map[Ticker]float64{
				"BTC": 50500.0, // (50000*1.0 + 51000*1.0) / 2.0
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := newTestOracle(log)
			oracle.prices = makePriceBuckets(tt.existingPrices)
			if len(tt.sourceWeights) > 0 {
				setSourceWeights(oracle, tt.sourceWeights)
			}

			mergeResult := oracle.Merge(tt.update, "test-sender")

			// Extract price results
			var result map[Ticker]float64
			if mergeResult != nil {
				result = mergeResult.Prices
			}

			// Verify the merged prices match expected
			if len(oracle.prices) != len(tt.expectedPrices) {
				t.Errorf("Expected %d tickers, got %d", len(tt.expectedPrices), len(oracle.prices))
			}

			for ticker, expectedSources := range tt.expectedPrices {
				actualBucket, found := oracle.prices[ticker]
				if !found {
					t.Errorf("Expected ticker %s to be in oracle.prices", ticker)
					continue
				}

				if len(actualBucket.sources) != len(expectedSources) {
					t.Errorf("For ticker %s, expected %d sources, got %d",
						ticker, len(expectedSources), len(actualBucket.sources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualBucket.sources[source]
					if !found {
						t.Errorf("Expected source %s for ticker %s", source, ticker)
						continue
					}

					if actualUpdate.price != expectedUpdate.price {
						t.Errorf("For ticker %s source %s, expected price %.2f, got %.2f",
							ticker, source, expectedUpdate.price, actualUpdate.price)
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For ticker %s source %s, expected weight %.2f, got %.2f",
							ticker, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For ticker %s source %s, expected stamp %v, got %v",
							ticker, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d tickers in result, got %d", len(tt.expectedResult), len(result))
					}
					for ticker, expectedPrice := range tt.expectedResult {
						actualPrice, found := result[ticker]
						if !found {
							t.Errorf("Expected ticker %s in result", ticker)
							continue
						}
						if actualPrice != expectedPrice {
							t.Errorf("For ticker %s, expected aggregated price %.15f, got %.15f",
								ticker, expectedPrice, actualPrice)
						}
					}
					for ticker := range result {
						if _, expected := tt.expectedResult[ticker]; !expected {
							t.Errorf("Unexpected ticker %s in result", ticker)
						}
					}
				}
			}
		})
	}
}

func TestMergeFeeRates(t *testing.T) {
	now := time.Now()
	oldStamp := now.Add(-time.Hour)
	newerStamp := now.Add(time.Hour)

	tests := []struct {
		name             string
		existingFeeRates map[Network]map[string]*feeRateUpdate
		update           *OracleUpdate
		sourceWeights    map[string]float64
		expectedFeeRates map[Network]map[string]*feeRateUpdate
		expectedResult   map[Network]*big.Int
	}{
		{
			name:             "new network from external source",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(100),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with newer timestamp should update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(80),
						stamp:   oldStamp,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  newerStamp,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(100),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(100),
			},
		},
		{
			name: "existing network with older timestamp should ignore",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "external-oracle",
				Stamp:  oldStamp,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(80),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"external-oracle": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   newerStamp,
						weight:  1.0,
					},
				},
			},
			expectedResult: nil, // No update occurred
		},
		{
			name: "multiple fee rates in single update",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(120),
					"ETH": big.NewInt(50),
				},
			},
			sourceWeights: map[string]float64{
				"source2": 0.8,
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  0.8,
					},
				},
				"ETH": {
					"source2": {
						network: "ETH",
						feeRate: big.NewInt(50),
						stamp:   now,
						weight:  0.8,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(109), // round((100*1.0 + 120*0.8) / 1.8) = round(108.888...) = 109
				"ETH": big.NewInt(50),
			},
		},
		{
			name: "new source for existing network",
			existingFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			update: &OracleUpdate{
				Source: "source2",
				Stamp:  now,
				FeeRates: map[Network]*big.Int{
					"BTC": big.NewInt(120),
				},
			},
			expectedFeeRates: map[Network]map[string]*feeRateUpdate{
				"BTC": {
					"source1": {
						network: "BTC",
						feeRate: big.NewInt(100),
						stamp:   now,
						weight:  1.0,
					},
					"source2": {
						network: "BTC",
						feeRate: big.NewInt(120),
						stamp:   now,
						weight:  1.0,
					},
				},
			},
			expectedResult: map[Network]*big.Int{
				"BTC": big.NewInt(110), // round((100*1.0 + 120*1.0) / 2.0) = round(110.0) = 110
			},
		},
	}

	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oracle := newTestOracle(log)
			oracle.feeRates = makeFeeRateBuckets(tt.existingFeeRates)
			if len(tt.sourceWeights) > 0 {
				setSourceWeights(oracle, tt.sourceWeights)
			}

			mergeResult := oracle.Merge(tt.update, "test-sender")

			// Extract fee rate results
			var result map[Network]*big.Int
			if mergeResult != nil {
				result = mergeResult.FeeRates
			}

			// Verify the merged fee rates match expected
			if len(oracle.feeRates) != len(tt.expectedFeeRates) {
				t.Errorf("Expected %d networks, got %d", len(tt.expectedFeeRates), len(oracle.feeRates))
			}

			for network, expectedSources := range tt.expectedFeeRates {
				actualBucket, found := oracle.feeRates[network]
				if !found {
					t.Errorf("Expected network %s to be in oracle.feeRates", network)
					continue
				}

				if len(actualBucket.sources) != len(expectedSources) {
					t.Errorf("For network %s, expected %d sources, got %d",
						network, len(expectedSources), len(actualBucket.sources))
				}

				for source, expectedUpdate := range expectedSources {
					actualUpdate, found := actualBucket.sources[source]
					if !found {
						t.Errorf("Expected source %s for network %s", source, network)
						continue
					}

					if actualUpdate.feeRate.Cmp(expectedUpdate.feeRate) != 0 {
						t.Errorf("For network %s source %s, expected fee rate %s, got %s",
							network, source, expectedUpdate.feeRate.String(), actualUpdate.feeRate.String())
					}

					if actualUpdate.weight != expectedUpdate.weight {
						t.Errorf("For network %s source %s, expected weight %.2f, got %.2f",
							network, source, expectedUpdate.weight, actualUpdate.weight)
					}

					if !actualUpdate.stamp.Equal(expectedUpdate.stamp) {
						t.Errorf("For network %s source %s, expected stamp %v, got %v",
							network, source, expectedUpdate.stamp, actualUpdate.stamp)
					}
				}
			}

			// Verify the return value matches expected result
			if tt.expectedResult == nil {
				if len(result) > 0 {
					t.Errorf("Expected nil/empty result, got %v", result)
				}
			} else {
				if result == nil {
					t.Error("Expected non-nil result")
				} else {
					if len(result) != len(tt.expectedResult) {
						t.Errorf("Expected %d networks in result, got %d", len(tt.expectedResult), len(result))
					}
					for network, expectedRate := range tt.expectedResult {
						actualRate, found := result[network]
						if !found {
							t.Errorf("Expected network %s in result", network)
							continue
						}
						if actualRate.Cmp(expectedRate) != 0 {
							t.Errorf("For network %s, expected aggregated fee rate %s, got %s",
								network, expectedRate.String(), actualRate.String())
						}
					}
					for network := range result {
						if _, expected := tt.expectedResult[network]; !expected {
							t.Errorf("Unexpected network %s in result", network)
						}
					}
				}
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("multiple goroutines reading prices simultaneously", func(t *testing.T) {
		now := time.Now()
		oracle := newTestOracle(log)
		oracle.prices = makePriceBuckets(map[Ticker]map[string]*priceUpdate{
			"BTC": {
				"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
				"source2": {ticker: "BTC", price: 51000.0, stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
			},
		})

		// Launch multiple readers concurrently
		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					prices := oracle.allPrices()
					if len(prices) > 0 {
						// Verify data integrity
						if btcPrice, found := prices["BTC"]; found {
							if btcPrice < 0 {
								t.Errorf("Invalid BTC price: %.2f", btcPrice)
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("multiple goroutines reading fee rates simultaneously", func(t *testing.T) {
		now := time.Now()
		oracle := newTestOracle(log)
		oracle.feeRates = makeFeeRateBuckets(map[Network]map[string]*feeRateUpdate{
			"BTC": {
				"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
				"source2": {network: "BTC", feeRate: big.NewInt(120), stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
			},
		})

		const numReaders = 50
		done := make(chan bool, numReaders)

		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					feeRates := oracle.allFeeRates()
					if len(feeRates) > 0 {
						// Verify data integrity
						if btcRate, found := feeRates["BTC"]; found {
							if btcRate.Sign() == 0 {
								t.Error("Invalid BTC fee rate: 0")
							}
						}
					}
				}
				done <- true
			}()
		}

		// Wait for all readers to complete
		for i := 0; i < numReaders; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of prices", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allPrices()
					_, _ = oracle.Price("BTC")
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Prices: map[Ticker]float64{
							"BTC": float64(50000 + j),
							"ETH": float64(3000 + j),
						},
					}
					oracle.Merge(update, fmt.Sprintf("writer-%d", writerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent reads and writes of fee rates", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numWriters = 5
		done := make(chan bool, numReaders+numWriters)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allFeeRates()
					_, _ = oracle.FeeRate("BTC")
				}
				done <- true
			}()
		}

		// Launch writers
		for i := 0; i < numWriters; i++ {
			writerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("writer-%d", writerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						FeeRates: map[Network]*big.Int{
							"BTC": big.NewInt(int64(100 + j)),
							"ETH": big.NewInt(int64(50 + j)),
						},
					}
					oracle.Merge(update, fmt.Sprintf("writer-%d", writerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numWriters; i++ {
			<-done
		}
	})

	t.Run("concurrent merge and read operations", func(t *testing.T) {
		oracle := newTestOracle(log)

		const numReaders = 20
		const numMergers = 10
		done := make(chan bool, numReaders+numMergers)

		now := time.Now()

		// Launch readers
		for i := 0; i < numReaders; i++ {
			go func() {
				for j := 0; j < 50; j++ {
					_ = oracle.allPrices()
					_ = oracle.allFeeRates()
				}
				done <- true
			}()
		}

		// Launch mergers
		for i := 0; i < numMergers; i++ {
			mergerID := i
			go func() {
				for j := 0; j < 10; j++ {
					update := &OracleUpdate{
						Source: fmt.Sprintf("merger-%d", mergerID),
						Stamp:  now.Add(time.Duration(j) * time.Millisecond),
						Prices: map[Ticker]float64{
							"BTC": float64(50000 + j),
						},
						FeeRates: map[Network]*big.Int{
							"BTC": big.NewInt(int64(100 + j)),
						},
					}
					oracle.Merge(update, fmt.Sprintf("merger-%d", mergerID))
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numReaders+numMergers; i++ {
			<-done
		}
	})
}

func TestPublicPrices(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all prices", func(t *testing.T) {
		oracle := newTestOracle(log)
		oracle.prices = makePriceBuckets(map[Ticker]map[string]*priceUpdate{
			"BTC": {
				"source1": {ticker: "BTC", price: 50000.0, stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {ticker: "ETH", price: 3000.0, stamp: now, weight: 1.0},
			},
		})

		result := oracle.allPrices()

		if len(result) != 2 {
			t.Errorf("Expected 2 prices, got %d", len(result))
		}

		if result["BTC"] != 50000.0 {
			t.Errorf("Expected BTC price 50000.0, got %.2f", result["BTC"])
		}

		if result["ETH"] != 3000.0 {
			t.Errorf("Expected ETH price 3000.0, got %.2f", result["ETH"])
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.allPrices()

		if len(result) != 0 {
			t.Errorf("Expected 0 prices, got %d", len(result))
		}
	})
}

func TestPublicFeeRates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")
	now := time.Now()

	t.Run("returns all fee rates", func(t *testing.T) {
		oracle := newTestOracle(log)
		oracle.feeRates = makeFeeRateBuckets(map[Network]map[string]*feeRateUpdate{
			"BTC": {
				"source1": {network: "BTC", feeRate: big.NewInt(100), stamp: now, weight: 1.0},
			},
			"ETH": {
				"source1": {network: "ETH", feeRate: big.NewInt(50), stamp: now, weight: 1.0},
			},
		})

		result := oracle.allFeeRates()

		if len(result) != 2 {
			t.Errorf("Expected 2 fee rates, got %d", len(result))
		}

		if result["BTC"].Cmp(big.NewInt(100)) != 0 {
			t.Errorf("Expected BTC fee rate 100, got %s", result["BTC"].String())
		}

		if result["ETH"].Cmp(big.NewInt(50)) != 0 {
			t.Errorf("Expected ETH fee rate 50, got %s", result["ETH"].String())
		}
	})

	t.Run("returns empty map for empty oracle", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.allFeeRates()

		if len(result) != 0 {
			t.Errorf("Expected 0 fee rates, got %d", len(result))
		}
	})
}

func TestMergeWithEmptyUpdates(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("Merge with nil", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		result := oracle.Merge(nil, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}

		if len(oracle.prices) != 0 {
			t.Errorf("Expected no prices, got %d", len(oracle.prices))
		}
	})

	t.Run("Merge with empty prices map", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.Merge(&OracleUpdate{
			Source: "test",
			Prices: map[Ticker]float64{},
		}, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})

	t.Run("Merge with empty fee rates map", func(t *testing.T) {
		oracle := newTestOracle(log)

		result := oracle.Merge(&OracleUpdate{
			Source:   "test",
			FeeRates: map[Network]*big.Int{},
		}, "test-sender")

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})
}

// TestAgedWeightBoundaries tests edge cases in weight calculation.
func TestAgedWeightBoundaries(t *testing.T) {
	const defaultWeight = 1.0

	t.Run("exactly at full validity period boundary", func(t *testing.T) {
		stamp := time.Now().Add(-fullValidityPeriod)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly fullValidityPeriod, should still be full weight
		if weight < 0.99 || weight > 1.0 {
			t.Errorf("Expected weight ~1.0 at fullValidityPeriod boundary, got %.4f", weight)
		}
	})

	t.Run("exactly at expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration)
		weight := agedWeight(defaultWeight, stamp)

		// At exactly validityExpiration, should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 at expiration boundary, got %.4f", weight)
		}
	})

	t.Run("just before expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration + time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// Just before expiration should be very small but not zero
		if weight <= 0 || weight > 0.01 {
			t.Errorf("Expected small positive weight just before expiration, got %.4f", weight)
		}
	})

	t.Run("just after expiration boundary", func(t *testing.T) {
		stamp := time.Now().Add(-validityExpiration - time.Millisecond)
		weight := agedWeight(defaultWeight, stamp)

		// After expiration should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 after expiration, got %.4f", weight)
		}
	})

	t.Run("future timestamp", func(t *testing.T) {
		futureStamp := time.Now().Add(time.Hour)
		weight := agedWeight(defaultWeight, futureStamp)

		// Future timestamps should get full weight (age is negative)
		if weight != defaultWeight {
			t.Errorf("Expected full weight for future timestamp, got %.4f", weight)
		}
	})

	t.Run("very old timestamp", func(t *testing.T) {
		veryOld := time.Now().Add(-24 * time.Hour)
		weight := agedWeight(defaultWeight, veryOld)

		// Very old should be 0
		if weight != 0 {
			t.Errorf("Expected weight 0 for very old timestamp, got %.4f", weight)
		}
	})

	t.Run("zero default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0, stamp)

		// Zero weight should always be zero
		if weight != 0 {
			t.Errorf("Expected weight 0 for zero default weight, got %.4f", weight)
		}
	})

	t.Run("fractional default weight", func(t *testing.T) {
		stamp := time.Now()
		weight := agedWeight(0.5, stamp)

		// Fresh timestamp with 0.5 weight should return 0.5
		if weight != 0.5 {
			t.Errorf("Expected weight 0.5 for fresh timestamp with 0.5 default, got %.4f", weight)
		}
	})

	t.Run("decay progression is linear", func(t *testing.T) {
		// Test that decay is linear through the decay period
		quarterDecay := time.Now().Add(-fullValidityPeriod - decayPeriod/4)
		halfDecay := time.Now().Add(-fullValidityPeriod - decayPeriod/2)
		threeQuarterDecay := time.Now().Add(-fullValidityPeriod - 3*decayPeriod/4)

		weightQuarter := agedWeight(defaultWeight, quarterDecay)
		weightHalf := agedWeight(defaultWeight, halfDecay)
		weightThreeQuarter := agedWeight(defaultWeight, threeQuarterDecay)

		// Should be approximately: 0.75, 0.5, 0.25
		if weightQuarter < 0.70 || weightQuarter > 0.80 {
			t.Errorf("Expected weight ~0.75 at quarter decay, got %.4f", weightQuarter)
		}
		if weightHalf < 0.45 || weightHalf > 0.55 {
			t.Errorf("Expected weight ~0.5 at half decay, got %.4f", weightHalf)
		}
		if weightThreeQuarter < 0.20 || weightThreeQuarter > 0.30 {
			t.Errorf("Expected weight ~0.25 at three-quarter decay, got %.4f", weightThreeQuarter)
		}

		// Verify progression is decreasing
		if weightQuarter <= weightHalf || weightHalf <= weightThreeQuarter {
			t.Errorf("Weight should decrease linearly: %.4f > %.4f > %.4f",
				weightQuarter, weightHalf, weightThreeQuarter)
		}
	})

	t.Run("within full validity period", func(t *testing.T) {
		// Test various points within full validity period
		fresh := time.Now()
		oneMinute := time.Now().Add(-time.Minute)
		twoMinutes := time.Now().Add(-2 * time.Minute)
		fourMinutes := time.Now().Add(-4 * time.Minute)

		weights := []float64{
			agedWeight(defaultWeight, fresh),
			agedWeight(defaultWeight, oneMinute),
			agedWeight(defaultWeight, twoMinutes),
			agedWeight(defaultWeight, fourMinutes),
		}

		// All should be full weight since fullValidityPeriod is 5 minutes
		for i, w := range weights {
			if w != defaultWeight {
				t.Errorf("Expected full weight at %d minutes old, got %.4f", i, w)
			}
		}
	})
}

func TestRescheduleDiviner(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("reschedules existing diviner", func(t *testing.T) {
		mockDiv := &diviner{
			source:     &mockSource{name: "test-source"},
			resetTimer: make(chan struct{}, 1),
		}

		oracle := newTestOracle(log)
		oracle.diviners = map[string]*diviner{
			"test-source": mockDiv,
		}

		oracle.rescheduleDiviner("test-source", "other-node")

		// Verify the reschedule signal was sent
		select {
		case <-mockDiv.resetTimer:
			// Success - signal was sent
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected reschedule signal to be sent")
		}
	})

	t.Run("does nothing for non-existent diviner", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		oracle.rescheduleDiviner("non-existent", "other-node")
	})

	t.Run("does nothing when diviners is empty", func(t *testing.T) {
		oracle := newTestOracle(log)

		// Should not panic
		oracle.rescheduleDiviner("any-source", "other-node")
	})
}

func TestRun(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("Run completes with no diviners", func(t *testing.T) {
		qm := newQuotaManager(&quotaManagerConfig{
			log:    log,
			nodeID: "test-node",
		})
		oracle := newTestOracle(log)
		oracle.diviners = make(map[string]*diviner)
		oracle.quotaManager = qm

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		// Cancel immediately since there are no diviners
		cancel()

		select {
		case <-done:
			// Success - Run exited after cancel
		case <-time.After(time.Second):
			t.Error("Run did not complete after context cancellation")
		}
	})

	t.Run("Run waits for diviners and exits on context cancellation", func(t *testing.T) {
		qm := newQuotaManager(&quotaManagerConfig{
			log:    log,
			nodeID: "test-node",
		})

		// Create mock diviners that wait for context
		mockDiviners := make(map[string]*diviner)
		for i := 0; i < 2; i++ {
			name := fmt.Sprintf("source%d", i)
			localName := name
			mockDiviners[name] = &diviner{
				source: &mockSource{
					name:      name,
					minPeriod: time.Hour, // Long period to avoid immediate fetch
					fetchFunc: func(ctx context.Context) (*sources.RateInfo, error) {
						<-ctx.Done() // Block until context cancelled
						return nil, ctx.Err()
					},
				},
				resetTimer: make(chan struct{}),
				log:        log,
				getNetworkSchedule: func() networkSchedule {
					now := time.Now()
					activePeers := qm.getActivePeersForSource(localName, now)
					return computeNetworkSchedule(activePeers, "local", time.Hour, now)
				},
				onScheduleChanged: func(*OracleSnapshot) {},
			}
		}

		oracle := newTestOracle(log)
		oracle.diviners = mockDiviners
		oracle.quotaManager = qm

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			oracle.Run(ctx)
			close(done)
		}()

		// Give Run time to start goroutines
		time.Sleep(50 * time.Millisecond)

		// Cancel and wait for completion
		cancel()

		select {
		case <-done:
			// Success - Run exited
		case <-time.After(time.Second):
			t.Error("Run did not exit after context cancellation")
		}
	})
}

func TestNewOracle(t *testing.T) {
	backend := slog.NewBackend(os.Stdout)
	log := backend.Logger("test")

	t.Run("creates oracle with default sources", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle == nil {
			t.Fatal("Expected non-nil oracle")
		}

		if oracle.log != log {
			t.Error("Expected logger to be set")
		}

		if len(oracle.diviners) == 0 {
			t.Error("Expected diviners to be initialized with default sources")
		}

		if oracle.prices == nil || oracle.feeRates == nil {
			t.Error("Expected prices and fee rates maps to be initialized")
		}
	})

	t.Run("initializes with unauthed sources", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify some default unauthed sources exist
		if len(oracle.diviners) == 0 {
			t.Error("Expected at least some diviners from unauthed sources")
		}
	})

	t.Run("nil http client uses default client", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			HTTPClient:            nil,
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient == nil {
			t.Error("Expected httpClient to be set to default")
		}
	})

	t.Run("custom http client is used", func(t *testing.T) {
		customClient := &mockHTTPClient{}
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			HTTPClient:            customClient,
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if oracle.httpClient != customClient {
			t.Error("Expected custom HTTP client to be used")
		}
	})

	t.Run("initializes empty price and fee rate maps", func(t *testing.T) {
		cfg := &Config{
			Log:                   log,
			NodeID:                "test-node",
			PublishUpdate:         func(ctx context.Context, update *OracleUpdate) error { return nil },
			OnStateUpdate:         func(*OracleSnapshot) {},
			PublishQuotaHeartbeat: func(ctx context.Context, quotas map[string]*sources.QuotaStatus) error { return nil },
		}

		oracle, err := New(cfg)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(oracle.prices) != 0 {
			t.Error("Expected empty prices map")
		}

		if len(oracle.feeRates) != 0 {
			t.Error("Expected empty fee rates map")
		}
	})
}

// mockHTTPClient is a mock implementation of HTTPClient for testing
type mockHTTPClient struct{}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return nil, nil
}
