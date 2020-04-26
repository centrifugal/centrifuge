package main

import (
	"context"
	"io"
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
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	cfg.JoinLeave = true
	cfg.HistoryLifetime = 300
	cfg.HistorySize = 1000
	cfg.HistoryRecover = true

	cfg.UserSubscribeToPersonal = true

	cfg.Namespaces = []centrifuge.ChannelNamespace{
		{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				Publish:   true,
				Presence:  true,
				JoinLeave: true,
			},
		},
	}

	node, _ := centrifuge.New(cfg)

	node.On().ClientConnecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
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
			log.Printf("message from user: %s, data: %s", client.UserID(), string(e.Data))
			return centrifuge.MessageReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected, disconnect: %#v", client.UserID(), e.Disconnect)
			return centrifuge.DisconnectReply{}
		})

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
	})

	node.On().ClientRefresh(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RefreshEvent) centrifuge.RefreshReply {
		log.Printf("user %s connection is going to expire, refreshing", client.UserID())
		return centrifuge.RefreshReply{
			ExpireAt: time.Now().Unix() + 10,
		}
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		i := 0
		for {
			_, err := node.Publish(node.PersonalChannel("42"), []byte(`{"message": "personal `+strconv.Itoa(i)+`"}`))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5000 * time.Millisecond)
			i++
		}
	}()

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
