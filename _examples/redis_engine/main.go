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
	"time"

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
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	flag.Parse()

	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog
	cfg.ChannelOptionsFunc = func(channel string) (centrifuge.ChannelOptions, bool, error) {
		return centrifuge.ChannelOptions{
			Presence:        true,
			JoinLeave:       true,
			HistorySize:     100,
			HistoryLifetime: 300,
			HistoryRecover:  true,
		}, true, nil
	}

	node, _ := centrifuge.New(cfg)

	node.OnConnect(func(c *centrifuge.Client) {
		transport := c.Transport()
		log.Printf("user %s connected via %s with protocol: %s", c.UserID(), transport.Name(), transport.Protocol())
	})

	node.OnSubscribe(func(c *centrifuge.Client, e centrifuge.SubscribeEvent) (centrifuge.SubscribeReply, error) {
		log.Printf("user %s subscribes on %s", c.UserID(), e.Channel)
		return centrifuge.SubscribeReply{}, nil
	})

	node.OnUnsubscribe(func(c *centrifuge.Client, e centrifuge.UnsubscribeEvent) {
		log.Printf("user %s unsubscribed from %s", c.UserID(), e.Channel)
	})

	node.OnPublish(func(c *centrifuge.Client, e centrifuge.PublishEvent) (centrifuge.PublishReply, error) {
		log.Printf("user %s publishes into channel %s: %s", c.UserID(), e.Channel, string(e.Data))
		return centrifuge.PublishReply{}, nil
	})

	node.OnDisconnect(func(c *centrifuge.Client, e centrifuge.DisconnectEvent) {
		log.Printf("user %s disconnected, disconnect: %s", c.UserID(), e.Disconnect)
	})

	engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
		// This allows to publish into Redis channel when adding Publication
		// to history instead of making separate Publish call thus saving one RTT.
		PublishOnHistoryAdd: true,

		// We are using Redis streams here for history.
		UseStreams: true,

		// Use reasonably large expiration interval for stream meta key,
		// much bigger than maximum HistoryLifetime value in Node config.
		// This way stream meta data will expire, in some cases you may want
		// to prevent its expiration setting this to zero value.
		HistoryMetaTTL: 7 * 24 * time.Hour,

		// And configure a couple of shards to use.
		Shards: []centrifuge.RedisShardConfig{
			{
				Host: "localhost",
				Port: 6379,
			},
			{
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

	go func() {
		// Publish channel notifications from server periodically.
		i := 1
		for {
			_, err := node.Publish("chat:index", []byte(`{"input": "`+strconv.Itoa(i)+`"}`))
			if err != nil {
				log.Printf("error publishing to personal channel: %s", err)
			}
			i++
			time.Sleep(5000 * time.Millisecond)
		}
	}()

	// Simulate some work inside Redis.
	for i := 0; i < 10; i++ {
		go func(i int) {
			for {
				time.Sleep(time.Second)
				_, err := node.Publish("chat:"+strconv.Itoa(i), []byte("hello"))
				if err != nil {
					log.Println(err.Error())
				}
				_, err = node.History("chat:" + strconv.Itoa(i))
				if err != nil {
					log.Println(err.Error())
				}
			}
		}(i)
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
