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
	log.Printf("NumCPU: %d", runtime.NumCPU())

	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelError
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	if os.Getenv("CENTRIFUGE_ENGINE") == "redis" {
		engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
			PublishOnHistoryAdd: true,
			HistoryMetaTTL:      7 * 24 * time.Hour,
			Shards: []centrifuge.RedisShardConfig{
				{
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

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "bench",
			},
		}, nil
	})

	node.OnConnect(func(c *centrifuge.Client) {})

	node.OnSubscribe(func(c *centrifuge.Client, e centrifuge.SubscribeEvent) (centrifuge.SubscribeReply, error) {
		return centrifuge.SubscribeReply{}, nil
	})

	node.OnPublish(func(c *centrifuge.Client, e centrifuge.PublishEvent) (centrifuge.PublishReply, error) {
		return centrifuge.PublishReply{}, nil
	})

	node.OnMessage(func(c *centrifuge.Client, e centrifuge.MessageEvent) {
		err := c.Send(e.Data)
		if err != nil {
			if err != io.EOF {
				log.Fatalln("error sending to client:", err.Error())
			}
		}
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
