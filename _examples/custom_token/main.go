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
		n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	cfg := centrifuge.DefaultConfig

	cfg.LogLevel = centrifuge.LogLevelInfo
	cfg.LogHandler = handleLog

	// Better to keep default in production. Here we just speeding up things a bit.
	cfg.ClientExpiredCloseDelay = 5 * time.Second

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	node, _ := centrifuge.New(cfg)

	engine, _ := centrifuge.NewMemoryEngine(node, centrifuge.MemoryEngineConfig{
		HistoryMetaTTL: 120 * time.Second,
	})
	node.SetEngine(engine)

	node.On().Connecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		// We need to apply token parsing logic here and return connection credentials.
		if !strings.HasPrefix(e.Token, "I am ") {
			return centrifuge.ConnectReply{
				Disconnect: centrifuge.DisconnectInvalidToken,
			}
		}
		userID := strings.TrimPrefix(e.Token, "I am ")
		credentials := &centrifuge.Credentials{
			UserID:   userID,
			ExpireAt: time.Now().Unix() + 5, // Expire in 5 seconds.
		}

		return centrifuge.ConnectReply{
			ClientSideRefresh: true, // This is required to use client-side refresh.
			Credentials:       credentials,
		}
	})

	node.On().Connected(func(ctx context.Context, client *centrifuge.Client) centrifuge.Event {
		log.Printf("user %s connected", client.UserID())
		return centrifuge.EventAll
	})

	node.On().Refresh(func(ctx context.Context, client *centrifuge.Client, e centrifuge.RefreshEvent) centrifuge.RefreshReply {
		log.Printf("user %s sent refresh command with token: %s", client.UserID(), e.Token)
		if !strings.HasPrefix(e.Token, "I am ") {
			return centrifuge.RefreshReply{
				Disconnect: centrifuge.DisconnectInvalidToken,
			}
		}
		userID := strings.TrimPrefix(e.Token, "I am ")
		if userID != client.UserID() {
			return centrifuge.RefreshReply{
				Disconnect: centrifuge.DisconnectInvalidToken,
			}
		}
		return centrifuge.RefreshReply{
			ExpireAt: time.Now().Unix() + 5,
		}
	})

	node.On().Disconnect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
		log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		return centrifuge.DisconnectReply{}
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
