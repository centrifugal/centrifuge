package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
		ctx := r.Context()
		ctx = centrifuge.SetCredentials(ctx, &centrifuge.Credentials{UserID: ""})
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

const (
	surveyInternalError  uint32 = 1
	surveyMethodNotFound uint32 = 2
)

func surveyChannels(node *centrifuge.Node) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, err := node.Survey(ctx, "channels", nil, "")
	if err != nil {
		return nil, err
	}
	channels := map[string]int{}
	for nodeID, result := range results {
		if result.Code > 0 {
			return nil, fmt.Errorf("non-zero code from node %s: %d", nodeID, result.Code)
		}
		var nodeChannels map[string]int
		err := json.Unmarshal(result.Data, &nodeChannels)
		if err != nil {
			return nil, fmt.Errorf("error unmarshaling data from node %s: %v", nodeID, err)
		}
		for ch, numSubscribers := range nodeChannels {
			channels[ch] += numSubscribers
		}
	}
	return channels, nil
}

func respondChannelsSurvey(node *centrifuge.Node) centrifuge.SurveyReply {
	channels := node.Hub().Channels()
	channelsMap := make(map[string]int, len(channels))
	for _, ch := range channels {
		if numSubscribers := node.Hub().NumSubscribers(ch); numSubscribers > 0 {
			channelsMap[ch] = numSubscribers
		}
	}
	data, err := json.Marshal(channelsMap)
	if err != nil {
		return centrifuge.SurveyReply{Code: surveyInternalError}
	}
	return centrifuge.SurveyReply{Data: data}
}

func main() {
	flag.Parse()

	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	node.OnSurvey(func(event centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		switch event.Op {
		case "channels":
			cb(respondChannelsSurvey(node))
		default:
			cb(centrifuge.SurveyReply{Code: surveyMethodNotFound})
		}
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(event centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})

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

	// Using Redis Broker here to scale nodes.
	broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
		Shards: redisShards,
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetBroker(broker)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(time.Second)
			channels, err := surveyChannels(node)
			if err != nil {
				log.Printf("error channels survey: %v", err)
				continue
			}
			log.Printf("Channels on all nodes: %v", channels)
		}
	}()

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
