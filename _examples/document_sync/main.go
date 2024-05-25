package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
)

// Counter is a document we sync here. For counter Version always matches Value – but
// that's not the rule for other more complex documents of course.
type Counter struct {
	Version int `json:"version"`
	Value   int `json:"value"`
}

var (
	counter     Counter
	counterLock sync.RWMutex
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

const exampleChannel = "counter"

// Check whether channel is allowed for subscribing. In real case permission
// check will probably be more complex than in this example.
func channelSubscribeAllowed(channel string) bool {
	return channel == exampleChannel
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

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)

			if !channelSubscribeAllowed(e.Channel) {
				log.Printf("user %s disallowed to subscribe on %s", client.UserID(), e.Channel)
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery: true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.HandleFunc("/api/counter", getCounterHandler)
	http.Handle("/", http.FileServer(http.Dir("./")))

	counter = Counter{Version: 0, Value: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the counter increment simulation.
	go simulateCounterIncrease(ctx, node)

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func getCounterHandler(w http.ResponseWriter, _ *http.Request) {
	counterLock.RLock()
	defer counterLock.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(counter)
}

func simulateCounterIncrease(ctx context.Context, node *centrifuge.Node) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
			counterLock.Lock()
			counter.Version++
			counter.Value++
			// Publishing under the lock here which is generally not good, but we want
			// to emulate transactional outbox or CDC guarantees.
			err := publishToChannel(node)
			if err != nil {
				// Emulate transaction rollback.
				log.Println("publish to channel error", err)
				counter.Version--
				counter.Value--
			}
			counterLock.Unlock()
		}
	}
}

func publishToChannel(node *centrifuge.Node) error {
	data, _ := json.Marshal(counter)
	_, err := node.Publish(exampleChannel, data,
		centrifuge.WithHistory(100, 10*time.Second))
	return err
}
