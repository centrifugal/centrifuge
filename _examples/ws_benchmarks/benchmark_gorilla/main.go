package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
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
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	var queueInitialCap int
	if os.Getenv("QUEUE_INITIAL_CAP") != "" {
		v, _ := strconv.Atoi(os.Getenv("QUEUE_INITIAL_CAP"))
		queueInitialCap = v
	}
	var writeDelay time.Duration
	if os.Getenv("WRITE_DELAY") != "" {
		v, _ := strconv.Atoi(os.Getenv("WRITE_DELAY"))
		writeDelay = time.Duration(v) * time.Millisecond
	}
	var maxMessagesInFrame int
	if os.Getenv("MAX_FRAME_MESSAGES") != "" {
		v, _ := strconv.Atoi(os.Getenv("MAX_FRAME_MESSAGES"))
		maxMessagesInFrame = v
	}
	log.Printf("NumCPU: %d, Write Delay: %s, Max messages in frame: %d, Queue init cap: %d\n",
		runtime.NumCPU(), writeDelay, maxMessagesInFrame, queueInitialCap)

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:           centrifuge.LogLevelError,
		LogHandler:         handleLog,
		ClientQueueMaxSize: 10 * 1024 * 1024,
	})

	if os.Getenv("CENTRIFUGE_BROKER") == "redis" {
		redisShardConfigs := []centrifuge.RedisShardConfig{
			{Address: "localhost:6379"},
		}
		var redisShards []*centrifuge.RedisShard
		for _, redisConf := range redisShardConfigs {
			redisShard, err := centrifuge.NewRedisShard(node, redisConf)
			if err != nil {
				log.Fatal(err)
			}
			redisShards = append(redisShards, redisShard)
		}
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetBroker(broker)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			WriteDelay:         writeDelay,
			MaxMessagesInFrame: maxMessagesInFrame,
			ReplyWithoutQueue:  true,
			QueueInitialCap:    queueInitialCap,
			Credentials: &centrifuge.Credentials{
				UserID: "bench",
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			cb(centrifuge.PublishReply{}, nil)
		})
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
