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
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
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
		// Better to keep default in production. Here we are just speeding up things a bit.
		ClientExpiredCloseDelay: 5 * time.Second,
	})

	broker, _ := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	node.SetBroker(broker)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// We need to apply token parsing logic here and return connection credentials.
		if !strings.HasPrefix(e.Token, "I am ") {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInvalidToken
		}
		userID := strings.TrimPrefix(e.Token, "I am ")
		credentials := &centrifuge.Credentials{
			UserID:   userID,
			ExpireAt: time.Now().Unix() + 5, // Expire in 5 seconds.
		}

		return centrifuge.ConnectReply{
			ClientSideRefresh: true, // This is required to use client-side refresh.
			Credentials:       credentials,
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		log.Printf("user %s connected", client.UserID())

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("user %s sent refresh command with token: %s", client.UserID(), e.Token)
			if !strings.HasPrefix(e.Token, "I am ") {
				cb(centrifuge.RefreshReply{}, centrifuge.DisconnectInvalidToken)
				return
			}
			userID := strings.TrimPrefix(e.Token, "I am ")
			if userID != client.UserID() {
				cb(centrifuge.RefreshReply{}, centrifuge.DisconnectInvalidToken)
				return
			}
			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 5,
			}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", websocketHandler)
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
