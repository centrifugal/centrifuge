package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
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
			UserID: "42",
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

	cfg.ChannelOptionsFunc = func(channel string) (centrifuge.ChannelOptions, bool, error) {
		if channel == exampleChannel || strings.HasPrefix(channel, "#") {
			// For exampleChannel and personal channel turn on all features. You should only
			// enable channel options where really needed to consume less resources on server.
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

	node, _ := centrifuge.New(cfg)

	engine, _ := centrifuge.NewMemoryEngine(node, centrifuge.MemoryEngineConfig{
		HistoryMetaTTL: 120 * time.Second,
	})
	node.SetEngine(engine)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		return centrifuge.ConnectReply{
			Data: []byte(`{}`),
			// Subscribe to personal several server-side channel.
			Channels: []string{"#" + cred.UserID},
		}, nil
	})

	node.OnConnect(func(c *centrifuge.Client) {
		transport := c.Transport()
		log.Printf("user %s connected via %s with protocol: %s", c.UserID(), transport.Name(), transport.Protocol())
	})

	node.OnAlive(func(c *centrifuge.Client) {
		log.Printf("user %s connection is still active", c.UserID())
	})

	node.OnSubscribe(func(c *centrifuge.Client, e centrifuge.SubscribeEvent) (centrifuge.SubscribeReply, error) {
		reply := centrifuge.SubscribeReply{}
		log.Printf("user %s subscribes on %s", c.UserID(), e.Channel)
		time.Sleep(1000 * time.Millisecond)
		return reply, nil
	})

	node.OnUnsubscribe(func(c *centrifuge.Client, e centrifuge.UnsubscribeEvent) {
		log.Printf("user %s unsubscribed from %s", c.UserID(), e.Channel)
	})

	node.OnRPC(func(c *centrifuge.Client, e centrifuge.RPCEvent) (centrifuge.RPCReply, error) {
		log.Printf("RPC from user: %s, data: %s, method: %s", c.UserID(), string(e.Data), e.Method)
		time.Sleep(100 * time.Millisecond)
		return centrifuge.RPCReply{
			Data: []byte(`{"year": "2020"}`),
		}, nil
	})

	node.OnDisconnect(func(c *centrifuge.Client, e centrifuge.DisconnectEvent) {
		log.Printf("user %s disconnected, disconnect: %s", c.UserID(), e.Disconnect)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

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
