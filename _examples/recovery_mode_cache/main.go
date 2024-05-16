package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

var port = flag.Int("port", 8000, "Port to bind app to")

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "",
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

const exampleChannel = "speed"

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

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("[user %s] connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("[user %s] subscribes on %s", client.UserID(), e.Channel)

			if !channelSubscribeAllowed(e.Channel) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery: true,
					RecoveryMode:   centrifuge.RecoveryModeCache,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("[user %s] unsubscribed from %s: %s", client.UserID(), e.Channel, e.Reason)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("[user %s] disconnected: %s", client.UserID(), e.Reason)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		const (
			accelerationRate = 2.0  // Speed increment per 100 ms
			brakingRate      = 10.0 // Speed decrement per 100 ms
			maxSpeed         = 190.0
			minSpeed         = 50.0
		)

		speed := 0.0
		increasing := true

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if increasing {
					speed += accelerationRate
					if speed >= maxSpeed {
						increasing = false
					}
				} else {
					speed -= brakingRate
					if speed <= minSpeed {
						increasing = true
					}
				}
				_, err := node.Publish(
					exampleChannel,
					[]byte(`{"speed": `+fmt.Sprint(speed)+`}`),
					centrifuge.WithHistory(1, time.Minute),
				)
				if err != nil {
					log.Printf("error publishing to personal channel: %s", err)
				}
			}
		}
	}()

	mux := http.DefaultServeMux

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	mux.Handle("/connection/websocket", authMiddleware(websocketHandler))
	mux.Handle("/", http.FileServer(http.Dir("./")))

	server := &http.Server{
		Handler:      mux,
		Addr:         "127.0.0.1:" + strconv.Itoa(*port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Print("Starting server, visit http://localhost:8000")
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node, server)
	log.Println("bye!")
}
