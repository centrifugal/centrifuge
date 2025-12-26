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
	var writeWithTimer bool
	if os.Getenv("WRITE_WITH_TIMER") == "1" || os.Getenv("WRITE_WITH_TIMER") == "true" {
		writeWithTimer = true
	}
	if writeWithTimer && writeDelay == 0 {
		log.Fatal("WRITE_DELAY must be set when WRITE_WITH_TIMER is on")
	}
	var shrinkDelay time.Duration
	if os.Getenv("SHRINK_DELAY") != "" {
		v, _ := strconv.Atoi(os.Getenv("SHRINK_DELAY"))
		shrinkDelay = time.Duration(v) * time.Millisecond
	}
	var maxMessagesInFrame int
	if os.Getenv("MAX_FRAME_MESSAGES") != "" {
		v, _ := strconv.Atoi(os.Getenv("MAX_FRAME_MESSAGES"))
		maxMessagesInFrame = v
	}
	var useWriteBufferPool bool
	if os.Getenv("USE_WRITE_BUFFER_POOL") == "1" || os.Getenv("USE_WRITE_BUFFER_POOL") == "true" {
		useWriteBufferPool = true
	}
	var readBufferSize int
	if os.Getenv("READ_BUFFER_SIZE") != "" {
		v, _ := strconv.Atoi(os.Getenv("READ_BUFFER_SIZE"))
		readBufferSize = v
	}

	// Display configuration in a beautiful compact way with colors
	const (
		colorReset = "\033[0m"
		colorCyan  = "\033[36m"
		colorGreen = "\033[32m"
	)
	fmt.Printf("\n%s┌─ Benchmark Server Configuration ─────────────────────────────────┐%s\n", colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "NumCPU:", colorGreen, runtime.NumCPU(), colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "WRITE_DELAY:", colorGreen, writeDelay, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "WRITE_WITH_TIMER:", colorGreen, writeWithTimer, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "SHRINK_DELAY:", colorGreen, shrinkDelay, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "MAX_FRAME_MESSAGES:", colorGreen, maxMessagesInFrame, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "QUEUE_INITIAL_CAP:", colorGreen, queueInitialCap, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "USE_WRITE_BUFFER_POOL:", colorGreen, useWriteBufferPool, colorReset, colorCyan, colorReset)
	fmt.Printf("%s│%s %-25s %s%-38v%s %s│%s\n", colorCyan, colorReset, "READ_BUFFER_SIZE:", colorGreen, readBufferSize, colorReset, colorCyan, colorReset)
	fmt.Printf("%s└───────────────────────────────────────────────────────────────────┘%s\n\n", colorCyan, colorReset)

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
			QueueShrinkDelay:   shrinkDelay,
			WriteWithTimer:     writeWithTimer,
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

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			// Echo back the request data for benchmark RPC mode
			cb(centrifuge.RPCReply{Data: e.Data}, nil)
		})
	})

	if err := node.Run(); err != nil {
		panic(err)
	}

	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		WriteTimeout:       time.Second,
		UseWriteBufferPool: useWriteBufferPool,
		ReadBufferSize:     readBufferSize,
	}))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			panic(err)
		}
	}()

	waitExitSignal(node)
	fmt.Println("exiting")
}
