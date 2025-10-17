package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	port         = flag.Int("port", 8443, "Port to bind app to")
	cert         = flag.String("cert", "certs/localhost.pem", "TLS certificate file")
	key          = flag.String("key", "certs/localhost-key.pem", "TLS key file")
	instanceName = flag.String("instance", "", "Instance name for identification")
)

type clientMessage struct {
	Timestamp    int64  `json:"timestamp"`
	Input        string `json:"input"`
	InstanceName string `json:"instance,omitempty"`
}

func handleLog(e centrifuge.LogEntry) {
	instance := os.Getenv("INSTANCE_NAME")
	if instance == "" {
		instance = *instanceName
	}
	if instance != "" {
		log.Printf("[%s] %s: %v", instance, e.Message, e.Fields)
	} else {
		log.Printf("%s: %v", e.Message, e.Fields)
	}
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
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

var exampleChannels = []string{
	"chat:index",
	"tokenized",
}

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool {
	for _, ch := range exampleChannels {
		if ch == channel {
			return true
		}
	}
	return false
}

func getInstanceName() string {
	instance := os.Getenv("INSTANCE_NAME")
	if instance == "" {
		instance = *instanceName
	}
	if instance == "" {
		instance = "unknown"
	}
	return instance
}

func main() {
	flag.Parse()

	// Override port from environment if set (for Docker)
	if envPort := os.Getenv("PORT"); envPort != "" {
		if p, err := strconv.Atoi(envPort); err == nil {
			*port = p
		}
	}

	instance := getInstanceName()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		return centrifuge.ConnectReply{
			Data: []byte(`{"instance": "` + instance + `"}`),
			// Subscribe to personal several server-side channel.
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"#" + cred.UserID: {EnableRecovery: true, EmitPresence: true, EmitJoinLeave: true, PushJoinLeave: true},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("[%s] user %s connected via %s with protocol: %s", instance, client.UserID(), transport.Name(), transport.Protocol())

		client.OnAlive(func() {
			log.Printf("[%s] user %s connection is still active", instance, client.UserID())
		})

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("[%s] user %s connection is going to expire, refreshing", instance, client.UserID())
			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 60,
			}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("[%s] user %s subscribes on %s", instance, client.UserID(), e.Channel)
			if !channelSubscribeAllowed(e.Channel) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnablePositioning: e.Positioned,
					EnableRecovery:    e.Recoverable,
					EmitPresence:      true,
					EmitJoinLeave:     true,
					PushJoinLeave:     true,
					Data:              []byte(`{"msg": "welcome from ` + instance + `"}`),
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("[%s] user %s unsubscribed from %s (%s)", instance, client.UserID(), e.Channel, e.Reason)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("[%s] user %s publishes into channel %s: %s", instance, client.UserID(), e.Channel, string(e.Data))
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
			msg.InstanceName = instance
			data, _ := json.Marshal(msg)

			result, err := node.Publish(
				e.Channel, data,
				centrifuge.WithHistory(300, time.Minute),
				centrifuge.WithClientInfo(e.ClientInfo),
			)

			cb(centrifuge.PublishReply{Result: &result}, err)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			log.Printf("[%s] [user %s] sent RPC, data: %s, method: %s", instance, client.UserID(), string(e.Data), e.Method)
			switch e.Method {
			case "getCurrentYear":
				cb(centrifuge.RPCReply{Data: []byte(`{"year": "2025", "instance": "` + instance + `"}`)}, nil)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			log.Printf("[%s] user %s calls presence on %s", instance, client.UserID(), e.Channel)
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PresenceReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.PresenceReply{}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("[%s] user %s disconnected (%s)", instance, client.UserID(), e.Reason)
		})
	})

	go func() {
		// Publish to channel periodically.
		i := 1
		for {
			_, err := node.Publish(
				"chat:index",
				[]byte(`{"input": "Publish from server `+instance+` #`+strconv.Itoa(i)+`", "instance": "`+instance+`"}`),
				centrifuge.WithHistory(300, time.Minute),
			)
			if err != nil {
				log.Printf("[%s] error publishing to channel: %s", instance, err)
			}
			i++
			time.Sleep(10000 * time.Millisecond)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	mux.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	})))
	mux.Handle("/connection/http_stream", authMiddleware(centrifuge.NewHTTPStreamHandler(node, centrifuge.HTTPStreamConfig{})))
	mux.Handle("/connection/sse", authMiddleware(centrifuge.NewSSEHandler(node, centrifuge.SSEConfig{})))
	mux.Handle("/emulation", centrifuge.NewEmulationHandler(node, centrifuge.EmulationConfig{}))

	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/", http.FileServer(http.Dir("./")))

	addr := "0.0.0.0:" + strconv.Itoa(*port)
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	log.Printf("[%s] running with TLS+HTTP/2 on %s", instance, addr)
	go func() {
		if err := srv.ListenAndServeTLS(*cert, *key); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Printf("[%s] bye!", instance)
}
