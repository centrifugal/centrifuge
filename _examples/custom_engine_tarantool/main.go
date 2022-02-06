package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifuge/_examples/custom_engine_tarantool/tntengine"
)

var (
	port      = flag.Int("port", 8000, "Port to bind app to")
	sharded   = flag.Bool("sharded", false, "Start sharded example")
	ha        = flag.Bool("ha", false, "Start high availability example")
	raft      = flag.Bool("raft", false, "Using Raft-based replication")
	user      = flag.String("user", "guest", "Connection user")
	password  = flag.String("password", "", "Connection password")
	addresses = flag.String("addresses", "", "Configure Tarantool addresses (by default we use hardcoded here)")
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
			Info:   []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(ctx)
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
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					Presence:  true,
					JoinLeave: true,
					Recover:   true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			cb(centrifuge.PublishReply{
				Options: centrifuge.PublishOptions{
					HistorySize: 10,
					HistoryTTL:  10 * time.Minute,
				},
			}, nil)
		})

		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			log.Printf("user %s calls presence on %s", client.UserID(), e.Channel)
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PresenceReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.PresenceReply{}, nil)
		})

		client.OnPresenceStats(func(e centrifuge.PresenceStatsEvent, cb centrifuge.PresenceStatsCallback) {
			log.Printf("user %s calls presence stats on %s", client.UserID(), e.Channel)
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PresenceStatsReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.PresenceStatsReply{}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	// Single Tarantool.
	mode := tntengine.ConnectionModeSingleInstance
	shardAddresses := [][]string{
		{"127.0.0.1:3301"},
	}

	if *ha {
		if *raft {
			// Single Tarantool RS with automatic leader election with Raft (Tarantool >= 2.7.0).
			shardAddresses = [][]string{
				{"127.0.0.1:3301", "127.0.0.1:3302", "127.0.0.1:3303"},
			}
			mode = tntengine.ConnectionModeLeaderFollowerRaft
		} else {
			// Single Tarantool RS with automatic leader election (ex. in Cartridge).
			shardAddresses = [][]string{
				{"127.0.0.1:3301", "127.0.0.1:3302"},
			}
			mode = tntengine.ConnectionModeLeaderFollower
		}
	} else if *sharded {
		// Client-side sharding between two Tarantool instances (without HA).
		shardAddresses = [][]string{
			{"127.0.0.1:3301"},
			{"127.0.0.1:3302"},
		}
	}

	if *addresses != "" {
		var customShardAddresses [][]string
		shardParts := strings.Split(*addresses, " ")
		for _, shardPart := range shardParts {
			customShardAddresses = append(customShardAddresses, strings.Split(shardPart, ","))
		}
		shardAddresses = customShardAddresses
	}

	var shards []*tntengine.Shard
	for _, addresses := range shardAddresses {
		shard, err := tntengine.NewShard(tntengine.ShardConfig{
			Addresses:      addresses,
			User:           *user,
			Password:       *password,
			ConnectionMode: mode,
		})
		if err != nil {
			log.Fatal(err)
		}
		shards = append(shards, shard)
	}

	broker, err := tntengine.NewBroker(node, tntengine.BrokerConfig{
		UsePolling: false,
		Shards:     shards,
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetBroker(broker)

	presenceManager, err := tntengine.NewPresenceManager(node, tntengine.PresenceManagerConfig{
		Shards: shards,
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetPresenceManager(presenceManager)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
