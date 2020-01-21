package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
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

func main() {
	cfg := centrifuge.DefaultConfig

	// Set HMAC secret to handle requests with JWT auth too. This is
	// not required if you don't use token authentication and
	// private subscriptions verified by token.
	cfg.TokenHMACSecretKey = "secret"
	cfg.Publish = true

	cfg.Namespaces = []centrifuge.ChannelNamespace{
		centrifuge.ChannelNamespace{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				Publish:         true,
				Presence:        true,
				JoinLeave:       true,
				HistoryLifetime: 60,
				HistorySize:     1000,
				HistoryRecover:  true,
			},
		},
	}

	node, _ := centrifuge.New(cfg, nil)

	node.On().ClientConnecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		log.Printf("authenticating client connection with id: %s dialed via %s (%s proto)", e.ClientID, t.Name(), t.Encoding())
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "72",
			},
		}
	})

	node.On().ClientConnected(func(ctx context.Context, client *centrifuge.Client) {

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

		client.On().RPC(func(e centrifuge.RPCEvent) centrifuge.RPCReply {
			log.Printf("RPC from user: %s, data: %s", client.UserID(), string(e.Data))
			return centrifuge.RPCReply{
				Data: []byte(`{"year": "2018"}`),
			}
		})

		client.On().Message(func(e centrifuge.MessageEvent) centrifuge.MessageReply {
			log.Printf("Message from user: %s, data: %s", client.UserID(), string(e.Data))
			return centrifuge.MessageReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected, disconnect: %#v", client.UserID(), e.Disconnect)
			return centrifuge.DisconnectReply{}
		})

		log.Printf("client with id %s successfully connected as user %s", client.ID(), client.UserID())
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
