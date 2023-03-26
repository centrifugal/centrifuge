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
		_ = n.Shutdown(context.Background())
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
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	broker, _ := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	node.SetBroker(broker)

	tokenVerifier := jwt.NewTokenVerifier(jwt.TokenVerifierConfig{
		HMACSecretKey: "secret",
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		log.Printf("client connecting with token: %s", e.Token)
		token, err := tokenVerifier.VerifyConnectToken(e.Token)
		if err != nil {
			if err == jwt.ErrTokenExpired {
				return centrifuge.ConnectReply{}, centrifuge.ErrorTokenExpired
			}
			return centrifuge.ConnectReply{}, centrifuge.DisconnectInvalidToken
		}
		subs := make(map[string]centrifuge.SubscribeOptions, len(token.Channels))
		for _, ch := range token.Channels {
			subs[ch] = centrifuge.SubscribeOptions{}
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID:   token.UserID,
				ExpireAt: token.ExpireAt,
			},
			Subscriptions: subs,
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s with protocol: %s", client.UserID(), transport.Name(), transport.Protocol())

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("user %s connection is going to expire, refreshing", client.UserID())
			cb(centrifuge.RefreshReply{ExpireAt: time.Now().Unix() + 60}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			if !channelSubscribeAllowed(e.Channel) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{}, nil)
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
			cb(centrifuge.PublishReply{}, nil)
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
