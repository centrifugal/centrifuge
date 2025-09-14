package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/maypok86/otter"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

// Global CEL filter cache
var celFilterCache *otter.Cache[string, *CELPublicationFilterer]

func initCELFilterCache() {
	cache, err := otter.MustBuilder[string, *CELPublicationFilterer](10000).
		Cost(func(key string, value *CELPublicationFilterer) uint32 {
			return uint32(len(key))
		}).
		WithTTL(10 * time.Minute).
		Build()
	if err != nil {
		log.Fatal("Failed to create CEL filter cache:", err)
	}
	celFilterCache = &cache
}

func getOrCreateCELFilter(env *cel.Env, expression string) (*CELPublicationFilterer, error) {
	if celFilterCache != nil {
		if filter, ok := celFilterCache.Get(expression); ok {
			log.Printf("filter expression found in cache: %s", expression)
			return filter, nil
		}
	}

	filter, err := NewCELPublicationFilterer(env, expression)
	if err != nil {
		return nil, err
	}

	if celFilterCache != nil {
		celFilterCache.Set(expression, filter)
	}

	return filter, nil
}

type CELPublicationFilterer struct {
	program cel.Program
}

func NewCELPublicationFilterer(env *cel.Env, expression string) (*CELPublicationFilterer, error) {
	ast, iss := env.Parse(expression)
	if iss.Err() != nil {
		return nil, iss.Err()
	}

	checked, iss := env.Check(ast)
	if iss.Err() != nil {
		return nil, iss.Err()
	}

	if checked.OutputType() != cel.BoolType {
		return nil, errors.New("expected bool output type")
	}

	globalVars := map[string]any{}

	program, err := env.Program(checked, cel.Globals(globalVars))
	if err != nil {
		return nil, err
	}

	return &CELPublicationFilterer{
		program: program,
	}, nil
}

func (f *CELPublicationFilterer) FilterPublication(variables any) bool {
	out, _, err := f.program.Eval(variables)
	if err != nil {
		return false
	}
	return out == types.True
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
	// Initialize CEL filter cache
	initCELFilterCache()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
		}, nil
	})

	celEnv, err := cel.NewEnv(
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
		cel.Variable("meta", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		log.Fatal(err)
	}

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)

			var filter centrifuge.PublicationFilterer
			if e.Filter != "" {
				var err error
				filter, err = getOrCreateCELFilter(celEnv, e.Filter)
				if err != nil {
					log.Printf("failed to create CEL filter: %v", err)
					cb(centrifuge.SubscribeReply{}, centrifuge.DisconnectBadRequest)
					return
				}
				log.Printf("created CEL filter for channel %s with expression: %s", e.Channel, e.Filter)
			}

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery:      true,
					EmitPresence:        true,
					EmitJoinLeave:       true,
					PushJoinLeave:       true,
					PublicationFilterer: filter,
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
	http.Handle("/", http.FileServer(http.Dir("./")))

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
					"ticker": ticker,
					"bid":    bid,
					"ask":    ask,
					"time":   time.Now().UnixMilli(),
				}

				jsonData, err := json.Marshal(data)
				if err != nil {
					log.Printf("Failed to marshal ticker data: %v", err)
					continue
				}

				metaData, _ := json.Marshal(map[string]any{
					"bid": bid,
					"ask": ask,
				})

				_, err = node.Publish(
					"tickers",
					jsonData,
					centrifuge.WithHistory(300, time.Minute),
					centrifuge.WithTags(map[string]string{
						"ticker": ticker,
					}),
					centrifuge.WithMeta(metaData),
				)
				if err != nil {
					log.Printf("Failed to publish ticker data: %v", err)
				}
			}
		}
	}
}
