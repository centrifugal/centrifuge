// Massive map_clients presence demo — populates a single map_clients
// presence channel with up to 100k synthetic entries directly via the
// MapBroker (no real WebSocket clients) and serves a viewer page that
// subscribes from a single browser tab. The viewer renders entries on
// a canvas grid for very lightweight DOM cost.
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

const presenceChannel = "clients:massive"

var (
	port         = flag.String("port", "3010", "HTTP server port")
	redisAddr    = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory map broker.")
	initialCount = flag.Int("count", 100000, "Initial number of synthetic presence entries")
	churnRate    = flag.Int("churn-rate", 200, "Replace events per second (each event = 1 leave + 1 join, count stays constant)")
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
				if channel == presenceChannel {
					// Synthetic entries are managed by presence_farm.go,
					// not by real client connections. Long TTL so synthetic
					// entries don't expire on their own. Default page size
					// 1000 keeps initial state pagination cheap at 100k+
					// entries; the viewer can override via SDK pageSize.
					return centrifuge.MapChannelOptions{
						Mode:            centrifuge.MapModeEphemeral,
						KeyTTL:          24 * time.Hour,
						DefaultPageSize: 1000,
						MinPageSize:     1,
						MaxPageSize:     10000,
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
			if e.Channel != presenceChannel {
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
		Channel:      presenceChannel,
		InitialCount: *initialCount,
		ChurnPerSec:  *churnRate,
	})

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))

	server := &http.Server{Addr: ":" + *port}

	go func() {
		log.Printf("Massive presence demo: http://localhost:%s/", *port)
		log.Printf("Channel: %s   count=%d   churn=%d/s", presenceChannel, *initialCount, *churnRate)
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
