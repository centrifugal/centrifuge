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

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
		}, nil
	})

	node.OnConnect(func(c *centrifuge.Client) {
		transport := c.Transport()
		log.Printf("user %s connected via %s with protocol: %s", c.UserID(), transport.Name(), transport.Protocol())

		go func() {
			err := c.Send([]byte("hello"))
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Fatalln(err.Error())
			}
		}()
	})

	node.OnRefresh(func(c *centrifuge.Client, e centrifuge.RefreshEvent) (centrifuge.RefreshReply, error) {
		log.Printf("user %s connection is going to expire, refreshing", c.UserID())
		return centrifuge.RefreshReply{
			ExpireAt: time.Now().Unix() + 10,
		}, nil
	})

	node.OnSubscribe(func(c *centrifuge.Client, e centrifuge.SubscribeEvent) (centrifuge.SubscribeReply, error) {
		log.Printf("user %s subscribes on %s", c.UserID(), e.Channel)
		return centrifuge.SubscribeReply{}, nil
	})

	node.OnUnsubscribe(func(c *centrifuge.Client, e centrifuge.UnsubscribeEvent) {
		log.Printf("user %s unsubscribed from %s", c.UserID(), e.Channel)
	})

	node.OnPublish(func(c *centrifuge.Client, e centrifuge.PublishEvent) (centrifuge.PublishReply, error) {
		log.Printf("user %s publishes into channel %s: %s", c.UserID(), e.Channel, string(e.Data))
		return centrifuge.PublishReply{}, nil
	})

	node.OnRPC(func(c *centrifuge.Client, e centrifuge.RPCEvent) (centrifuge.RPCReply, error) {
		log.Printf("RPC from user: %s, data: %s", c.UserID(), string(e.Data))
		return centrifuge.RPCReply{
			Data: []byte(`{"year": "2020"}`),
		}, nil
	})

	node.OnMessage(func(c *centrifuge.Client, e centrifuge.MessageEvent) {
		log.Printf("message from user: %s, data: %s", c.UserID(), string(e.Data))
	})

	node.OnDisconnect(func(c *centrifuge.Client, e centrifuge.DisconnectEvent) {
		log.Printf("user %s disconnected, disconnect: %s", c.UserID(), e.Disconnect)
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
