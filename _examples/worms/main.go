package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/centrifugal/centrifuge"
)

type event struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge %s] %s: %v", centrifuge.LogLevelToString(e.Level), e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "",
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
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectResult, error) {
		return centrifuge.ConnectResult{
			Credentials: &centrifuge.Credentials{
				UserID: "",
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		log.Printf("worm connected via %s", client.Transport().Name())

		client.OnMessage(func(e centrifuge.MessageEvent) {
			var ev event
			_ = json.Unmarshal(e.Data, &ev)
			_, _ = node.Publish("moving", ev.Payload)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("worm subscribed on %s", e.Channel)
			cb(centrifuge.SubscribeResult{}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("worm unsubscribed from %s", e.Channel)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("worm disconnected, disconnect: %s", e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		panic(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			panic(err)
		}
	}()

	waitExitSignal(node)
	fmt.Println("exiting")
}
