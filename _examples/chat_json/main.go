package main

import (
	"context"
	"encoding/json"
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

const exampleChannel = "chat:index"

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool {
	return channel == exampleChannel
}

func main() {
	cfg := centrifuge.DefaultConfig

	cfg.LogLevel = centrifuge.LogLevelInfo
	cfg.LogHandler = handleLog

	// Turn on all
	cfg.ChannelOptionsFunc = func(channel string) (centrifuge.ChannelOptions, bool, error) {
		if channel == exampleChannel {
			// For exampleChannel turn on all features. You should only
			// enable channel options where really needed to consume less
			// resources on server.
			return centrifuge.ChannelOptions{
				Presence:        true,
				JoinLeave:       true,
				HistorySize:     100,
				HistoryLifetime: 300,
				HistoryRecover:  true,
			}, true, nil
		}
		return centrifuge.ChannelOptions{}, true, nil
	}

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	node, _ := centrifuge.New(cfg)

	engine, _ := centrifuge.NewMemoryEngine(node, centrifuge.MemoryEngineConfig{
		HistoryMetaTTL: 120 * time.Second,
	})
	node.SetEngine(engine)

	node.On().Connecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		cred, _ := centrifuge.GetCredentials(ctx)
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
			// Subscribe to personal several server-side channel.
			Channels: []string{"#" + cred.UserID},
		}
	})

	node.On().Connected(func(ctx context.Context, client *centrifuge.Client) centrifuge.Event {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		// Event handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				select {
				case <-ctx.Done():
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

		return centrifuge.EventAll
	})

	node.On().Alive(func(ctx context.Context, client *centrifuge.Client, e centrifuge.AliveEvent) centrifuge.AliveReply {
		log.Printf("user %s connection is still active", client.UserID())
		return centrifuge.AliveReply{}
	})

	node.On().Refresh(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RefreshEvent) centrifuge.RefreshReply {
		log.Printf("user %s connection is going to expire, refreshing", client.UserID())
		return centrifuge.RefreshReply{ExpireAt: time.Now().Unix() + 60}
	})

	node.On().Subscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
		log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
		if !channelSubscribeAllowed(e.Channel) {
			return centrifuge.SubscribeReply{Error: centrifuge.ErrorPermissionDenied}
		}
		return centrifuge.SubscribeReply{}
	})

	node.On().Unsubscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
		log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		return centrifuge.UnsubscribeReply{}
	})

	node.On().Publish(func(ctx context.Context, client *centrifuge.Client, e centrifuge.PublishEvent) centrifuge.PublishReply {
		log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
		if _, ok := client.Channels()[e.Channel]; !ok {
			return centrifuge.PublishReply{Error: centrifuge.ErrorPermissionDenied}
		}
		var msg clientMessage
		err := json.Unmarshal(e.Data, &msg)
		if err != nil {
			return centrifuge.PublishReply{Error: centrifuge.ErrorBadRequest}
		}
		msg.Timestamp = time.Now().Unix()
		data, _ := json.Marshal(msg)
		return centrifuge.PublishReply{
			Data: data,
		}
	})

	node.On().RPC(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RPCEvent) centrifuge.RPCReply {
		log.Printf("RPC from user: %s, data: %s, method: %s", client.UserID(), string(e.Data), e.Method)
		return centrifuge.RPCReply{Data: []byte(`{"year": "2020"}`)}
	})

	node.On().Presence(func(ctx context.Context, client *centrifuge.Client, e centrifuge.PresenceEvent) centrifuge.PresenceReply {
		log.Printf("user %s calls presence on %s", client.UserID(), e.Channel)
		if _, ok := client.Channels()[e.Channel]; !ok {
			return centrifuge.PresenceReply{Error: centrifuge.ErrorPermissionDenied}
		}
		return centrifuge.PresenceReply{}
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

	go func() {
		// Publish personal notifications for user 42 periodically.
		i := 1
		for {
			_, err := node.Publish("#42", []byte(`{"message": "personal `+strconv.Itoa(i)+`"}`))
			if err != nil {
				log.Printf("error publishing to personal channel: %s", err)
			}
			i++
			time.Sleep(5000 * time.Millisecond)
		}
	}()

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", authMiddleware(websocketHandler))

	sockjsHandler := centrifuge.NewSockjsHandler(node, centrifuge.SockjsConfig{
		URL:                      "https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js",
		HandlerPrefix:            "/connection/sockjs",
		WebsocketReadBufferSize:  1024,
		WebsocketWriteBufferSize: 1024,
	})
	http.Handle("/connection/sockjs/", authMiddleware(sockjsHandler))

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
