package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID:   "42",
			ExpireAt: time.Now().Unix() + 10,
			Info:     []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(newCtx)
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
	cfg := centrifuge.DefaultConfig

	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	node.On().Connecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
		}
	})

	node.On().Connected(func(ctx context.Context, client *centrifuge.Client) centrifuge.Event {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		go func() {
			err := client.Send([]byte("hello"))
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Fatalln(err.Error())
			}
		}()

		return centrifuge.EventAll
	})

	node.On().Refresh(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RefreshEvent) centrifuge.RefreshReply {
		log.Printf("user %s connection is going to expire, refreshing", client.UserID())
		return centrifuge.RefreshReply{
			ExpireAt: time.Now().Unix() + 10,
		}
	})

	node.On().Subscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
		log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
		return centrifuge.SubscribeReply{}
	})

	node.On().Unsubscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
		log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		return centrifuge.UnsubscribeReply{}
	})

	node.On().Publish(func(ctx context.Context, client *centrifuge.Client, e centrifuge.PublishEvent) centrifuge.PublishReply {
		log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
		return centrifuge.PublishReply{}
	})

	node.On().RPC(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RPCEvent) centrifuge.RPCReply {
		log.Printf("RPC from user: %s, data: %s", client.UserID(), string(e.Data))
		return centrifuge.RPCReply{
			Data: []byte(`{"year": "2020"}`),
		}
	})

	node.On().Message(func(ctx context.Context, client *centrifuge.Client, e centrifuge.MessageEvent) centrifuge.MessageReply {
		log.Printf("message from user: %s, data: %s", client.UserID(), string(e.Data))
		return centrifuge.MessageReply{}
	})

	node.On().Disconnect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
		log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		return centrifuge.DisconnectReply{}
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
