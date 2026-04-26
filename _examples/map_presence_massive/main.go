// Massive map_clients presence demo with two scales:
//   - 100k members, channel clients:massive_100k, viewer at /100k.html
//   - 1M  members, channel clients:massive_1m,   viewer at /1m.html
//
// Both populations are populated and churned directly via the MapBroker
// (no real WebSocket clients), so a single browser tab can subscribe and
// see the entire population synchronized over the protocol.
//
// This example lives in its own folder because the farm produces
// non-trivial CPU and network load — the regular map_demo should not
// pay that cost.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
)

const (
	channel100k = "clients:massive_100k"
	channel1m   = "clients:massive_1m"

	pool100k = 320 * 320   // 102,400
	pool1m   = 1024 * 1024 // 1,048,576

	count100k = 100000
	count1m   = 1000000

	churn100k = 200
	churn1m   = 1000
)

var (
	port      = flag.String("port", "3010", "HTTP server port")
	redisAddr = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory map broker.")
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

func main() {
	flag.Parse()

	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelInfo,
		LogHandler: handleLog,
		Map: centrifuge.MapConfig{
			GetMapChannelOptions: func(channel string) centrifuge.MapChannelOptions {
				switch channel {
				case channel100k, channel1m:
					// Recoverable mode so the SDK's stream catch-up phase
					// can replay events that occurred during the long
					// state pagination — ephemeral has no stream and
					// loses convergence at this scale, so two viewer
					// tabs would end up with different counts.
					// SubscribeCatchUpTimeout disabled (-1) because
					// paginating up to 1M entries exceeds the 5s default.
					streamSize := 50000
					streamTTL := 5 * time.Minute
					if channel == channel1m {
						streamSize = 200000
						streamTTL = 10 * time.Minute
					}
					return centrifuge.MapChannelOptions{
						Mode:                    centrifuge.MapModeRecoverable,
						KeyTTL:                  24 * time.Hour,
						StreamSize:              streamSize,
						StreamTTL:               streamTTL,
						DefaultPageSize:         1000,
						MinPageSize:             1,
						MaxPageSize:             10000,
						SubscribeCatchUpTimeout: -1,
					}
				}
				return centrifuge.MapChannelOptions{
					Mode:   centrifuge.MapModeEphemeral,
					KeyTTL: 60 * time.Second,
				}
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	mapBroker, err := setupMapBroker(node, *redisAddr)
	if err != nil {
		log.Fatal(err)
	}
	node.SetMapBroker(mapBroker)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		userID := e.Name
		if userID == "" {
			userID = "viewer"
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: userID},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if e.Channel != channel100k && e.Channel != channel1m {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			if e.Type != centrifuge.SubscriptionTypeMapClients {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{Options: centrifuge.SubscribeOptions{Type: e.Type}}, nil)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	farmCtx, cancelFarm := context.WithCancel(context.Background())
	defer cancelFarm()

	go runPresenceFarm(farmCtx, node, presenceFarmConfig{
		Channel:      channel100k,
		PoolSize:     pool100k,
		InitialCount: count100k,
		ChurnPerSec:  churn100k,
	})
	go runPresenceFarm(farmCtx, node, presenceFarmConfig{
		Channel:      channel1m,
		PoolSize:     pool1m,
		InitialCount: count1m,
		ChurnPerSec:  churn1m,
	})

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))

	server := &http.Server{Addr: ":" + *port}

	go func() {
		log.Printf("Massive presence demo: http://localhost:%s/", *port)
		log.Printf("  100k viewer: http://localhost:%s/100k.html  (channel %s, pool %d)", *port, channel100k, pool100k)
		log.Printf("  1M   viewer: http://localhost:%s/1m.html    (channel %s, pool %d)", *port, channel1m, pool1m)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cancelFarm()
	_ = server.Shutdown(ctx)
	_ = node.Shutdown(ctx)
}
