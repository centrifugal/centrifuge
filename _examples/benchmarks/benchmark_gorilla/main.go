package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %+v", e.Message, e.Fields)
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
	log.Printf("NumCPU: %d", runtime.NumCPU())

	cfg := centrifuge.DefaultConfig
	cfg.Publish = true
	cfg.LogLevel = centrifuge.LogLevelError
	cfg.LogHandler = handleLog
	cfg.ClientInsecure = true

	node, _ := centrifuge.New(cfg)

	if os.Getenv("CENTRIFUGE_ENGINE") == "redis" {
		engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
			PublishOnHistoryAdd: true,
			HistoryMetaTTL:      7 * 24 * time.Hour,
			Shards: []centrifuge.RedisShardConfig{
				centrifuge.RedisShardConfig{
					Host: "localhost",
					Port: 6379,
				},
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetEngine(engine)
	}

	node.On().ClientConnected(func(ctx context.Context, client *centrifuge.Client) {

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			return centrifuge.SubscribeReply{}
		})

		client.On().Unsubscribe(func(e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
			// log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
			return centrifuge.UnsubscribeReply{}
		})

		client.On().Publish(func(e centrifuge.PublishEvent) centrifuge.PublishReply {
			// Do not log here - lots of publications expected.
			return centrifuge.PublishReply{}
		})

		client.On().Message(func(e centrifuge.MessageEvent) centrifuge.MessageReply {
			// Do not log here - lots of messages expected.
			err := client.Send(e.Data)
			if err != nil {
				if err != io.EOF {
					log.Fatalln("error senfing to client:", err.Error())
				}
			}
			return centrifuge.MessageReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected", client.UserID())
			return centrifuge.DisconnectReply{}
		})

		log.Printf("user %s connected via %s with encoding: %s", client.UserID(), client.Transport().Name(), client.Transport().Encoding())
	})

	if err := node.Run(); err != nil {
		panic(err)
	}

	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		WriteTimeout: time.Second,
	}))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			panic(err)
		}
	}()

	waitExitSignal(node)
	fmt.Println("exiting")
}
