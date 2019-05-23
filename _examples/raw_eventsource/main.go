package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
			UserID: "42",
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

const exampleChannel = "chat_eventsource"

func main() {
	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog
	cfg.Namespaces = []centrifuge.ChannelNamespace{
		centrifuge.ChannelNamespace{
			Name: "chat",
			ChannelOptions: centrifuge.ChannelOptions{
				JoinLeave:       true,
				HistoryLifetime: 300,
				HistorySize:     100,
				HistoryRecover:  true,
			},
		},
	}

	node, _ := centrifuge.New(cfg)

	node.On().ClientConnected(func(ctx context.Context, client *centrifuge.Client) {

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			return centrifuge.SubscribeReply{}
		})

		client.On().Unsubscribe(func(e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
			return centrifuge.UnsubscribeReply{}
		})

		client.On().Disconnect(func(e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
			log.Printf("user %s disconnected, disconnect: %#v", client.UserID(), e.Disconnect)
			return centrifuge.DisconnectReply{}
		})

		transport := client.Transport()
		log.Printf("user %s connected via %s with encoding: %s", client.UserID(), transport.Name(), transport.Encoding())

		// Connect handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				err := client.Send(centrifuge.Raw(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
				if err != nil {
					if err != io.EOF {
						log.Println(err.Error())
					}
				}
				time.Sleep(5 * time.Second)
			}
		}()
	})

	// Also publish to channel periodically.
	go func() {
		for {
			err := node.Publish(exampleChannel, centrifuge.Raw(`{"channel time": "`+strconv.FormatInt(time.Now().Unix(), 10)+`"}`))
			if err != nil {
				if err != io.EOF {
					log.Println(err.Error())
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	http.Handle("/connection/eventsource", authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		transport := newEventsourceTransport(req)

		client, err := centrifuge.NewClient(req.Context(), node, transport, centrifuge.Unidirectional())
		if err != nil {
			return
		}
		defer client.Close(nil)

		client.Subscribe(exampleChannel)

		flusher := w.(http.Flusher)
		notifier := w.(http.CloseNotifier)
		flusher.Flush()
		for {
			select {
			case <-transport.closeCh:
				return
			case <-notifier.CloseNotify():
				return
			case data, ok := <-transport.messages:
				if !ok {
					return
				}
				parts := strings.Split(string(data), "\n")
				for _, part := range parts {
					if strings.TrimSpace(part) == "" {
						continue
					}
					w.Write([]byte("data: " + part + "\n\n"))
				}
				flusher.Flush()
			}
		}
	})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

type eventsourceTransport struct {
	mu       sync.Mutex
	req      *http.Request
	messages chan []byte
	closeCh  chan struct{}
	closed   bool
}

func newEventsourceTransport(req *http.Request) *eventsourceTransport {
	return &eventsourceTransport{
		messages: make(chan []byte, 128),
		closeCh:  make(chan struct{}),
		req:      req,
	}
}

func (t *eventsourceTransport) Name() string {
	return "raw-eventsource"
}

func (t *eventsourceTransport) Encoding() centrifuge.Encoding {
	return "json"
}

func (t *eventsourceTransport) Info() centrifuge.TransportInfo {
	return centrifuge.TransportInfo{
		Request: t.req,
	}
}

func (t *eventsourceTransport) Write(data []byte) error {
	select {
	case <-t.closeCh:
		return nil
	default:
		select {
		case t.messages <- data:
		default:
			t.Close(centrifuge.DisconnectSlow)
		}
		return nil
	}
}

func (t *eventsourceTransport) Close(disconnect *centrifuge.Disconnect) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	close(t.closeCh)
	t.mu.Unlock()
	return nil
}
