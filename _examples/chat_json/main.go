package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var port = flag.Int("port", 8000, "Port to bind app to")

type clientMessage struct {
	Timestamp int64  `json:"timestamp"`
	Input     string `json:"input"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID:   "42",
			ExpireAt: time.Now().Unix() + 60,
			Info:     []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

func waitExitSignal(n *centrifuge.Node, s *http.Server) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = n.Shutdown(ctx)
		_ = s.Shutdown(ctx)
		done <- true
	}()
	<-done
}

const exampleChannel = "chat:index"

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool {
	return channel == exampleChannel
}

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:       centrifuge.LogLevelInfo,
		LogHandler:     handleLog,
		HistoryMetaTTL: 24 * time.Hour,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
			// Subscribe to a personal server-side channel.
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"#" + cred.UserID: {
					EnableRecovery: true,
					EmitPresence:   true,
					EmitJoinLeave:  true,
					PushJoinLeave:  true,
				},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("[user %s] connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		// Event handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				select {
				case <-client.Context().Done():
					return
				case <-time.After(5 * time.Second):
					err := client.Send([]byte(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
					if err != nil {
						if err == io.EOF {
							return
						}
						log.Printf("error sending message: %s", err)
					}
				}
			}
		}()

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("[user %s] connection is going to expire, refreshing", client.UserID())

			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 60,
			}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("[user %s] subscribes on %s", client.UserID(), e.Channel)

			if !channelSubscribeAllowed(e.Channel) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery: true,
					EmitPresence:   true,
					EmitJoinLeave:  true,
					PushJoinLeave:  true,
					Data:           []byte(`{"msg": "welcome"}`),
				},
			}, nil)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("[user %s] publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))

			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			var msg clientMessage
			err := json.Unmarshal(e.Data, &msg)
			if err != nil {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorBadRequest)
				return
			}
			msg.Timestamp = time.Now().Unix()
			data, _ := json.Marshal(msg)

			result, err := node.Publish(
				e.Channel, data,
				centrifuge.WithHistory(300, time.Minute),
				centrifuge.WithClientInfo(e.ClientInfo),
			)

			cb(centrifuge.PublishReply{Result: &result}, err)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			log.Printf("[user %s] sent RPC, data: %s, method: %s", client.UserID(), string(e.Data), e.Method)
			switch e.Method {
			case "getCurrentYear":
				cb(centrifuge.RPCReply{Data: []byte(`{"year": "2020"}`)}, nil)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			log.Printf("[user %s] calls presence on %s", client.UserID(), e.Channel)

			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PresenceReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.PresenceReply{}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("[user %s] unsubscribed from %s: %s", client.UserID(), e.Channel, e.Reason)
		})

		client.OnAlive(func() {
			log.Printf("[user %s] connection is still active", client.UserID())
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("[user %s] disconnected: %s", client.UserID(), e.Reason)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		// Publish personal notifications for user 42 periodically.
		i := 1
		for {
			_, err := node.Publish(
				"#42",
				[]byte(`{"personal": "`+strconv.Itoa(i)+`"}`),
				centrifuge.WithHistory(300, time.Minute),
			)
			if err != nil {
				log.Printf("error publishing to personal channel: %s", err)
			}
			i++
			time.Sleep(5000 * time.Millisecond)
		}
	}()

	go func() {
		// Publish to channel periodically.
		i := 1
		for {
			_, err := node.Publish(
				"chat:index",
				[]byte(`{"input": "Publish from server `+strconv.Itoa(i)+`"}`),
				centrifuge.WithHistory(300, time.Minute),
			)
			if err != nil {
				log.Printf("error publishing to channel: %s", err)
			}
			i++
			time.Sleep(10000 * time.Millisecond)
		}
	}()

	mux := http.DefaultServeMux

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	mux.Handle("/connection/websocket", authMiddleware(websocketHandler))

	sockjsHandler := centrifuge.NewSockjsHandler(node, centrifuge.SockjsConfig{
		URL:                      "https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js",
		HandlerPrefix:            "/connection/sockjs",
		WebsocketReadBufferSize:  1024,
		WebsocketWriteBufferSize: 1024,
	})
	mux.Handle("/connection/sockjs/", authMiddleware(sockjsHandler))

	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/", http.FileServer(http.Dir("./")))

	server := &http.Server{
		Handler:      mux,
		Addr:         ":" + strconv.Itoa(*port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node, server)
	log.Println("bye!")
}
