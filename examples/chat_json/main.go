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
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	// Set secret to handle requests with JWT auth too. This is
	// not required if you don't use token authentication and
	// private subscriptions verified by token.
	cfg.Secret = "secret"
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

	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {

		client.On().Refresh(func(e centrifuge.RefreshEvent) centrifuge.RefreshReply {
			log.Printf("user %s connection is going to expire, refreshing", client.UserID())
			return centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 10,
			}
		})

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			return centrifuge.SubscribeReply{
				ExpireAt: time.Now().Unix() + 10,
			}
		})

		client.On().SubRefresh(func(e centrifuge.SubRefreshEvent) centrifuge.SubRefreshReply {
			log.Printf("user %s subscription on channel %s is going to expire, refreshing", client.UserID(), e.Channel)
			return centrifuge.SubRefreshReply{
				ExpireAt: time.Now().Unix() + 10,
			}
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

		transport := client.Transport()
		log.Printf("user %s connected via %s with encoding: %s", client.UserID(), transport.Name(), transport.Encoding())

		go func() {
			messageData, _ := json.Marshal("hello client " + client.ID())
			err := client.Send(messageData)
			if err != nil {
				if err == io.EOF {
					return
				}
				log.Fatalln(err.Error())
			}
		}()

		return centrifuge.ConnectReply{
			Data: []byte(`{"timezone": "Moscow/Europe"}`),
		}
	})

	node.SetLogHandler(centrifuge.LogLevelDebug, handleLog)

	// engine, err := centrifuge.NewRedisEngine(node, centrifuge.RedisEngineConfig{
	// 	Shards: []centrifuge.RedisShardConfig{
	// 		centrifuge.RedisShardConfig{
	// 			Host: "localhost",
	// 			Port: 6379,
	// 		},
	// 	},
	// })
	// if err != nil {
	// 	panic(err)
	// }
	// node.SetEngine(engine)

	if err := node.Run(); err != nil {
		panic(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/connection/sockjs/", authMiddleware(centrifuge.NewSockjsHandler(node, centrifuge.SockjsConfig{
		URL:           "https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js",
		HandlerPrefix: "/connection/sockjs",
	})))
	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			panic(err)
		}
	}()

	waitExitSignal(node)
	fmt.Println("exiting")
}
