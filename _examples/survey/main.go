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

const surveyInternalError uint32 = 1
const surveyMethodNotFound uint32 = 2

func surveyChannels(node *centrifuge.Node) (map[string]bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	results, err := node.Survey(ctx, "channels", nil)
	if err != nil {
		return nil, err
	}
	channels := map[string]bool{}
	for nodeID, result := range results {
		if result.Code > 0 {
			return nil, fmt.Errorf("non-zero code from node %s: %d", nodeID, result.Code)
		}
		var nodeChannels []string
		err := json.Unmarshal(result.Data, &nodeChannels)
		if err != nil {
			return nil, err
		}
		for _, channel := range nodeChannels {
			channels[channel] = true
		}
	}
	return channels, nil
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
			channels := node.Hub().Channels()
			data, err := json.Marshal(channels)
			if err != nil {
				cb(centrifuge.SurveyReply{Code: surveyInternalError})
				return
			}
			cb(centrifuge.SurveyReply{
				Data: data,
			})
		default:
			cb(centrifuge.SurveyReply{
				Code: surveyMethodNotFound,
			})
		}
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(event centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})

	// Using Redis engine here to scale nodes.
	engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
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
