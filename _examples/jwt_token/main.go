package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge/_examples/jwt_token/jwt"

	"github.com/centrifugal/centrifuge"
)

type clientMessage struct {
	Timestamp int64  `json:"timestamp"`
	Input     string `json:"input"`
}

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

// Check whether channel is allowed for subscribing. In real case permission
// most probably will be more complex than in this example.
func channelSubscribeAllowed(channel string) bool {
	return channel == "chat"
}

func main() {
	cfg := centrifuge.DefaultConfig

	cfg.LogLevel = centrifuge.LogLevelInfo
	cfg.LogHandler = handleLog

	if err := cfg.Validate(); err != nil {
		log.Fatal(err)
	}

	node, _ := centrifuge.New(cfg)

	engine, _ := centrifuge.NewMemoryEngine(node, centrifuge.MemoryEngineConfig{
		HistoryMetaTTL: 120 * time.Second,
	})
	node.SetEngine(engine)

	tokenVerifier := jwt.NewTokenVerifier(jwt.TokenVerifierConfig{
		HMACSecretKey: "secret",
	})

	node.On().Connecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		log.Printf("client connecting with token: %s", e.Token)
		token, err := tokenVerifier.VerifyConnectToken(e.Token)
		if err != nil {
			if err == jwt.ErrTokenExpired {
				return centrifuge.ConnectReply{Error: centrifuge.ErrorTokenExpired}
			}
			return centrifuge.ConnectReply{Disconnect: centrifuge.DisconnectInvalidToken}
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID:   token.UserID,
				ExpireAt: token.ExpireAt,
			},
			Channels: token.Channels,
		}
	})

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())
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

	node.On().Disconnect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.DisconnectEvent) {
		log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	websocketHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	http.Handle("/connection/websocket", websocketHandler)

	http.Handle("/token", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		token, err := jwt.BuildUserToken("secret", "42", time.Now().Unix()+10)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"token": "` + token + `"}`))
	}))

	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}
