package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

var (
	port = flag.Int("port", 8000, "Port to bind app to")
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Our middleware logic goes here...
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
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	flag.Parse()

	cfg := centrifuge.DefaultConfig
	cfg.Namespaces = []centrifuge.ChannelNamespace{
		centrifuge.ChannelNamespace{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				Publish:         true,
				JoinLeave:       true,
				Presence:        true,
				HistoryLifetime: 60,
				HistorySize:     1000,
				HistoryRecover:  true,
			},
		},
	}

	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			return centrifuge.SubscribeReply{}
		})

		client.On().Unsubscribe(func(e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
			return centrifuge.UnsubscribeReply{}
		})

		client.On().Publish(func(e centrifuge.PublishEvent) centrifuge.PublishReply {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			return centrifuge.PublishReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected, disconnect: %#v", client.UserID(), e.Disconnect)
			return centrifuge.DisconnectReply{}
		})

		transport := client.Transport()
		log.Printf("user %s connected via %s with encoding: %s", client.UserID(), transport.Name(), transport.Encoding())
		return centrifuge.ConnectReply{}
	})

	node.SetLogHandler(centrifuge.LogLevelDebug, handleLog)

	engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
		PublishOnHistoryAdd: false,
		Shards: []centrifuge.RedisShardConfig{
			centrifuge.RedisShardConfig{
				Host: "localhost",
				Port: 6379,
			},
			centrifuge.RedisShardConfig{
				Host: "localhost",
				Port: 6380,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetEngine(engine)

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
