package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type nonceType int

const nonceContextKey nonceType = 0

func main() {
	rand.Seed(time.Now().UnixNano())

	cfg := centrifuge.DefaultConfig

	cfg.Namespaces = []centrifuge.ChannelNamespace{
		centrifuge.ChannelNamespace{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				Publish:         true,
				JoinLeave:       true,
				HistoryLifetime: 60,
				HistorySize:     1000,
				HistoryRecover:  true,
			},
		},
	}

	node, _ := centrifuge.New(cfg)

	node.On().Session(func(ctx context.Context, t centrifuge.Transport, e centrifuge.SessionEvent) centrifuge.SessionReply {
		nonce := randString(10)
		log.Printf("sending nonce to client: %s", nonce)
		ctx = context.WithValue(ctx, nonceContextKey, nonce)
		return centrifuge.SessionReply{
			Context: ctx,
			Data:    centrifuge.Raw(`{"nounce": "` + nonce + `"}`),
		}
	})

	node.On().Auth(func(ctx context.Context, t centrifuge.Transport, e centrifuge.AuthEvent) centrifuge.AuthReply {
		log.Printf("authenticating client connection over %s", t.Name())

		var data struct {
			Digest string `json:"digest"`
		}
		json.Unmarshal(e.Data, &data)

		if nonceSent, ok := ctx.Value(nonceContextKey).(string); ok && strings.ToUpper(nonceSent) == data.Digest {
			return centrifuge.AuthReply{
				Credentials: &centrifuge.Credentials{
					UserID: "97",
				},
			}
		}
		return centrifuge.AuthReply{
			Disconnect: centrifuge.DisconnectBadRequest,
		}
	})

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client) {

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
	})

	node.SetLogHandler(centrifuge.LogLevelDebug, handleLog)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
