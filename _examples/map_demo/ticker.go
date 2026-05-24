package main

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/centrifugal/centrifuge"
)

// Ticker sector assignments.
var tickerSectors = map[string]string{
	"AAPL": "tech", "GOOG": "tech", "MSFT": "tech", "META": "tech", "NVDA": "tech",
	"AMZN": "ecommerce",
	"TSLA": "auto",
	"NFLX": "media",
	"ORCL": "enterprise", "CRM": "enterprise",
}

func roundToTwoDecimals(f float64) float64 {
	return math.Round(f*100) / 100
}

// publishTickerData publishes random stock prices to the "tickers" map channel.
// Each tick updates 2-4 randomly chosen tickers (not all at once), which is more
// realistic and produces visually distinct highlights on the client.
func publishTickerData(node *centrifuge.Node) {
	tc := time.NewTicker(500 * time.Millisecond)
	defer tc.Stop()

	tickers := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ORCL", "CRM"}
	for range tc.C {
		// Pick 2-4 random tickers to update this tick.
		count := 2 + rand.Intn(3)
		perm := rand.Perm(len(tickers))
		for i := range count {
			ticker := tickers[perm[i]]
			basePrice := 100.0 + float64(len(ticker))*10.0
			bid := roundToTwoDecimals(basePrice + (rand.Float64()-0.5)*10.0)
			ask := roundToTwoDecimals(bid + rand.Float64()*2.0)

			data := map[string]any{
				"ticker": ticker,
				"bid":    bid,
				"ask":    ask,
				"time":   time.Now().UnixMilli(),
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				continue
			}
			_, err = node.MapPublish(context.Background(), "tickers", ticker, centrifuge.MapPublishOptions{
				Data: jsonData,
				Tags: map[string]string{"sector": tickerSectors[ticker]},
			})
			if err != nil {
				log.Printf("Failed to publish ticker %s: %v", ticker, err)
			}
		}
	}
}
