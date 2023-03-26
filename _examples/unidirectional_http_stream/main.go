package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

var (
	port     = flag.Int("port", 8000, "Port to bind app to")
	redis    = flag.Bool("redis", false, "Use Redis")
	tls      = flag.Bool("tls", false, "Use TLS")
	keyFile  = flag.String("key_file", "server.key", "path to TLS key file")
	certFile = flag.String("cert_file", "server.crt", "path to TLS crt file")
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

var exampleChannel = "unidirectional"

func main() {
	flag.Parse()

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	if *redis {
		redisShardConfigs := []centrifuge.RedisShardConfig{
			{Address: "localhost:6379"},
		}
		var redisShards []*centrifuge.RedisShard
		for _, redisConf := range redisShardConfigs {
			redisShard, err := centrifuge.NewRedisShard(node, redisConf)
			if err != nil {
				log.Fatal(err)
			}
			redisShards = append(redisShards, redisShard)
		}
		// Using Redis Broker here to scale nodes.
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetBroker(broker)

		presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetPresenceManager(presenceManager)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				exampleChannel: {
					EnableRecovery:    true,
					EnablePositioning: true,
					Data:              []byte(`{"message": "welcome to a channel"}`),
				},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
		transport := client.Transport()
		log.Printf("user %s connected via %s", client.UserID(), transport.Name())
	})

	// Publish to a channel periodically.
	go func() {
		for {
			currentTime := strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.Publish(exampleChannel, []byte(`{"server_time": "`+currentTime+`"}`), centrifuge.WithHistory(10, time.Minute))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/stream", authMiddleware(handleStream(node)))
	http.Handle("/subscribe", handleSubscribe(node))
	http.Handle("/unsubscribe", handleUnsubscribe(node))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if *tls {
			//if *useHttp3 {
			//	if err := http3.ListenAndServe("0.0.0.0:443", *certFile, *keyFile, nil); err != nil {
			//		log.Fatal(err)
			//	}
			//} else {
			if err := http.ListenAndServeTLS(":"+strconv.Itoa(*port), *certFile, *keyFile, nil); err != nil {
				log.Fatal(err)
			}
			//}
		} else {
			if err := http.ListenAndServe(":"+strconv.Itoa(*port), nil); err != nil {
				log.Fatal(err)
			}
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func handleStream(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		transport := newStreamTransport(req)

		c, closeFn, err := centrifuge.NewClient(req.Context(), node, transport)
		if err != nil {
			log.Printf("error creating client: %v", err)
			return
		}
		defer func() { _ = closeFn() }()
		defer close(transport.closedCh) // need to execute this after client closeFn.

		c.Connect(centrifuge.ConnectRequest{})

		flusher, ok := w.(http.Flusher)
		if !ok {
			log.Printf("ResponseWriter does not support Flusher")
			return
		}

		pingInterval := 25 * time.Second
		tick := time.NewTicker(pingInterval)
		defer tick.Stop()

		for {
			select {
			case <-req.Context().Done():
				return
			case <-transport.disconnectCh:
				return
			case <-tick.C:
				_, err = w.Write([]byte("null\n"))
				if err != nil {
					log.Printf("error write: %v", err)
					return
				}
				flusher.Flush()
			case data, ok := <-transport.messages:
				if !ok {
					return
				}
				tick.Reset(pingInterval)
				_, err = w.Write(data)
				if err != nil {
					log.Printf("error write: %v", err)
					return
				}
				_, err = w.Write([]byte("\n"))
				if err != nil {
					log.Printf("error write: %v", err)
					return
				}
				flusher.Flush()
			}
		}
	}
}

func handleSubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err := node.Subscribe(
			"42", exampleChannel,
			centrifuge.WithSubscribeClient(clientID),
			centrifuge.WithSubscribeData([]byte(`{"message": "welcome to a channel"}`)),
		)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func handleUnsubscribe(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		clientID := req.URL.Query().Get("client")
		if clientID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err := node.Unsubscribe("42", exampleChannel, centrifuge.WithUnsubscribeClient(clientID))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

type streamTransport struct {
	mu           sync.Mutex
	req          *http.Request
	messages     chan []byte
	disconnectCh chan *centrifuge.Disconnect
	closedCh     chan struct{}
	closed       bool
}

func newStreamTransport(req *http.Request) *streamTransport {
	return &streamTransport{
		messages:     make(chan []byte),
		disconnectCh: make(chan *centrifuge.Disconnect),
		closedCh:     make(chan struct{}),
		req:          req,
	}
}

func (t *streamTransport) Name() string {
	return "http_stream"
}

func (t *streamTransport) Protocol() centrifuge.ProtocolType {
	return centrifuge.ProtocolTypeJSON
}

// ProtocolVersion ...
func (t *streamTransport) ProtocolVersion() centrifuge.ProtocolVersion {
	return centrifuge.ProtocolVersion2
}

// Unidirectional returns whether transport is unidirectional.
func (t *streamTransport) Unidirectional() bool {
	return true
}

// Emulation ...
func (t *streamTransport) Emulation() bool {
	return false
}

// DisabledPushFlags ...
func (t *streamTransport) DisabledPushFlags() uint64 {
	return 0
}

// PingPongConfig ...
func (t *streamTransport) PingPongConfig() centrifuge.PingPongConfig {
	return centrifuge.PingPongConfig{
		PingInterval: 25 * time.Second,
		PongTimeout:  10 * time.Second,
	}
}

func (t *streamTransport) Write(message []byte) error {
	return t.WriteMany(message)
}

func (t *streamTransport) WriteMany(messages ...[]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	for i := 0; i < len(messages); i++ {
		select {
		case t.messages <- messages[i]:
		case <-t.closedCh:
			return nil
		}
	}
	return nil
}

func (t *streamTransport) Close(_ centrifuge.Disconnect) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.disconnectCh)
	<-t.closedCh
	return nil
}
