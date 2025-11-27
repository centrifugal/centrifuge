package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	port = flag.Int("port", 8080, "Port to bind app to")
	cert = flag.String("cert", "certs/localhost.pem", "TLS certificate file")
	key  = flag.String("key", "certs/localhost-key.pem", "TLS key file")
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

func main() {
	if !strings.Contains(os.Getenv("GODEBUG"), "http2xconnect=1") {
		panic("required to use GODEBUG=http2xconnect=1")
	}

	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
		Metrics: centrifuge.MetricsConfig{
			ExposeTransportAcceptProtocol: true,
		},
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
			// Subscribe to personal several server-side channel.
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				"#" + cred.UserID: {EnableRecovery: true, EmitPresence: true, EmitJoinLeave: true, PushJoinLeave: true},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnAlive(func() {
			log.Printf("user %s connection is still active", client.UserID())
		})

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("user %s connection is going to expire, refreshing", client.UserID())
			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 60,
			}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
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
					Data:              []byte(`{"msg": "welcome"}`),
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s (%s)", client.UserID(), e.Channel, e.Reason)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
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
				cb(centrifuge.RPCReply{Data: []byte(`{"year": "2025"}`)}, nil)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			log.Printf("user %s calls presence on %s", client.UserID(), e.Channel)
			if !client.IsSubscribed(e.Channel) {
				cb(centrifuge.PresenceReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.PresenceReply{}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected (%s)", client.UserID(), e.Reason)
		})
	})

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
			time.Sleep(5000 * time.Millisecond)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/connection/http_stream", authMiddleware(centrifuge.NewHTTPStreamHandler(node, centrifuge.HTTPStreamConfig{})))
	http.Handle("/connection/sse", authMiddleware(centrifuge.NewSSEHandler(node, centrifuge.SSEConfig{})))
	http.Handle("/emulation", centrifuge.NewEmulationHandler(node, centrifuge.EmulationConfig{}))
	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	})

	// Memory stats endpoint.
	http.HandleFunc("/memstats", func(w http.ResponseWriter, r *http.Request) {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "Memory Statistics\n")
		_, _ = fmt.Fprintf(w, "=================\n")
		_, _ = fmt.Fprintf(w, "Alloc:      %d bytes (%.2f KB, %.2f MB)\n", m.Alloc, float64(m.Alloc)/1024, float64(m.Alloc)/1024/1024)
		_, _ = fmt.Fprintf(w, "TotalAlloc: %d bytes (%.2f KB, %.2f MB)\n", m.TotalAlloc, float64(m.TotalAlloc)/1024, float64(m.TotalAlloc)/1024/1024)
		_, _ = fmt.Fprintf(w, "Sys:        %d bytes (%.2f KB, %.2f MB)\n", m.Sys, float64(m.Sys)/1024, float64(m.Sys)/1024/1024)
		_, _ = fmt.Fprintf(w, "HeapAlloc:  %d bytes (%.2f KB, %.2f MB)\n", m.HeapAlloc, float64(m.HeapAlloc)/1024, float64(m.HeapAlloc)/1024/1024)
		_, _ = fmt.Fprintf(w, "HeapInuse:  %d bytes (%.2f KB, %.2f MB)\n", m.HeapInuse, float64(m.HeapInuse)/1024, float64(m.HeapInuse)/1024/1024)
		_, _ = fmt.Fprintf(w, "NumGC:      %d\n", m.NumGC)
		_, _ = fmt.Fprintf(w, "\nConnections: %d\n", len(node.Hub().Connections()))
		if len(node.Hub().Connections()) >= 1000 {
			_, _ = fmt.Fprintf(w, "\nPer connection (HeapAlloc / connections): %.2f bytes\n", float64(m.HeapAlloc)/float64(len(node.Hub().Connections())))
		}
	})

	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/", http.FileServer(http.Dir("./")))

	addr := "0.0.0.0:" + strconv.Itoa(*port)
	srv := &http.Server{
		Addr: addr,
	}
	log.Printf("running with TLS, open https://localhost:%d", *port)
	go func() {
		if err := srv.ListenAndServeTLS(*cert, *key); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
