package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %+v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
		})
		r = r.WithContext(newCtx)
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

var dataBytes []byte

func init() {
	data := map[string]interface{}{
		"_id":        "5adece493c1a23736b037c52",
		"isActive":   false,
		"balance":    "$2,199.02",
		"picture":    "http://placehold.it/32x32",
		"age":        25,
		"eyeColor":   "blue",
		"name":       "Swanson Walker",
		"gender":     "male",
		"company":    "SHADEASE",
		"email":      "swansonwalker@shadease.com",
		"phone":      "+1 (885) 410-3991",
		"address":    "768 Paerdegat Avenue, Gouglersville, Oklahoma, 5380",
		"registered": "2016-01-24T07:40:09 -03:00",
		"latitude":   -71.336378,
		"longitude":  -28.155956,
		"tags": []string{
			"magna",
			"nostrud",
			"irure",
			"aliquip",
			"culpa",
			"sint",
		},
		"greeting":      "Hello, Swanson Walker! You have 9 unread messages.",
		"favoriteFruit": "apple",
	}

	var err error
	dataBytes, err = json.Marshal(data)
	if err != nil {
		panic(err.Error())
	}
}

func main() {
	cfg := centrifuge.DefaultConfig
	cfg.Publish = true

	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {

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
			err := client.Send(dataBytes)
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
		return centrifuge.ConnectReply{}
	})

	node.SetLogHandler(centrifuge.LogLevelError, handleLog)

	if err := node.Run(); err != nil {
		panic(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			panic(err)
		}
	}()

	waitExitSignal(node)
	fmt.Println("exiting")
}
