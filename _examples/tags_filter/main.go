package main

import (
	"context"
	"encoding/json"
	"html/template"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
			Info:   []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	useProtobuf := r.URL.Query().Get("protobuf") == "true"

	tmpl, err := template.ParseFiles("index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := struct {
		UseProtobuf bool
	}{
		UseProtobuf: useProtobuf,
	}

	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func waitExitSignal(n *centrifuge.Node) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery:  true,
					EmitPresence:    true,
					EmitJoinLeave:   true,
					PushJoinLeave:   true,
					AllowTagsFilter: true, // Enable tags filtering.
					RecoveryMode:    centrifuge.RecoveryModeCache,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			result, err := node.Publish(
				e.Channel, e.Data,
				centrifuge.WithHistory(300, time.Minute),
				centrifuge.WithClientInfo(e.ClientInfo),
			)
			cb(centrifuge.PublishReply{
				Result: &result,
			}, err)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", handleIndex)

	// Start publishing ticker data
	go publishTickerData(node)

	log.Print("Starting server, visit http://localhost:8000")
	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func roundToTwoDecimals(f float64) float64 {
	return math.Round(f*100) / 100
}

func publishTickerData(node *centrifuge.Node) {
	tc := time.NewTicker(time.Second)
	defer tc.Stop()

	tickers := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ORCL", "CRM"}
	for {
		select {
		case <-tc.C:
			for _, ticker := range tickers {
				// Generate random bid/ask prices.
				basePrice := 100.0 + float64(len(ticker))*10.0
				bid := roundToTwoDecimals(basePrice + (rand.Float64()-0.5)*10.0)
				ask := roundToTwoDecimals(bid + rand.Float64()*2.0)

				data := map[string]interface{}{
					"ticker": ticker, // Maybe be excluded BTW since sent in tags.
					"bid":    bid,
					"ask":    ask,
					"time":   time.Now().UnixMilli(),
				}

				jsonData, err := json.Marshal(data)
				if err != nil {
					log.Printf("Failed to marshal ticker data: %v", err)
					continue
				}

				_, err = node.Publish(
					"tickers",
					jsonData,
					centrifuge.WithHistory(300, time.Minute),
					centrifuge.WithTags(map[string]string{
						"ticker": ticker,
					}),
				)
				if err != nil {
					log.Printf("Failed to publish ticker data: %v", err)
				}
			}
		}
	}
}
