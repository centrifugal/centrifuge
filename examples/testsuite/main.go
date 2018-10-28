package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %+v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "testsuite",
		})
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

// ResolveFunc ...
type ResolveFunc func(error)

// TestCaseFunc ...
type TestCaseFunc func(fn ResolveFunc) *centrifuge.Node

// RegisterTestCase ...
func RegisterTestCase(ctx context.Context, port int, tc TestCaseFunc, name string, auth bool) {
	n := tc(handleTestResult(name))
	n.SetLogHandler(centrifuge.LogLevelError, handleLog)

	if err := n.Run(); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	handler := centrifuge.NewWebsocketHandler(n, centrifuge.WebsocketConfig{})
	if auth {
		mux.Handle("/connection/websocket", authMiddleware(handler))
	} else {
		mux.Handle("/connection/websocket", handler)
	}

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(port), mux); err != nil {
			panic(err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("exiting from test test %s on port %d", name, port)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		n.Shutdown(ctx)
	}
}

func testCustomHeader(fn ResolveFunc) *centrifuge.Node {
	cfg := centrifuge.DefaultConfig
	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		authHeader := client.Transport().Info().Request.Header.Get("Authorization")
		if authHeader != "testsuite" {
			fn(fmt.Errorf("No valid Authorization header found"))
		} else {
			fn(nil)
		}
		return centrifuge.ConnectReply{}
	})
	return node
}

func testJWTAuth(fn ResolveFunc) *centrifuge.Node {
	// Use this token in client:
	// eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0c3VpdGVfand0In0.hPmHsVqvtY88PvK4EmJlcdwNuKFuy3BGaF7dMaKdPlw
	cfg := centrifuge.DefaultConfig
	cfg.Secret = "secret"
	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		time.AfterFunc(5*time.Second, func() { fn(fmt.Errorf("timeout")) })
		if client.UserID() != "testsuite_jwt" {
			fn(fmt.Errorf("Wrong user id: %s", client.UserID()))
		} else {
			fn(nil)
		}
		return centrifuge.ConnectReply{}
	})
	return node
}

func testSimpleSubscribe(fn ResolveFunc) *centrifuge.Node {
	cfg := centrifuge.DefaultConfig
	node, _ := centrifuge.New(cfg)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		time.AfterFunc(5*time.Second, func() { fn(fmt.Errorf("timeout")) })

		client.On().Subscribe(func(e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
			if e.Channel == "testsuite" {
				fn(nil)
			} else {
				fn(fmt.Errorf("wrong channel name"))
			}
			return centrifuge.SubscribeReply{}
		})

		return centrifuge.ConnectReply{}
	})
	return node
}

type testsuiteMessage struct {
	Data string `json:"data"`
}

func testReceiveRPCReceiveMessageJSON(fn ResolveFunc) *centrifuge.Node {
	cfg := centrifuge.DefaultConfig
	node, _ := centrifuge.New(cfg)

	message := testsuiteMessage{
		Data: "testsuite",
	}
	jsonData, _ := json.Marshal(message)

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		time.AfterFunc(5*time.Second, func() { fn(fmt.Errorf("timeout")) })

		client.On().RPC(func(e centrifuge.RPCEvent) centrifuge.RPCReply {
			return centrifuge.RPCReply{
				Data: jsonData,
			}
		})

		client.On().Message(func(e centrifuge.MessageEvent) centrifuge.MessageReply {
			var msg testsuiteMessage
			err := json.Unmarshal(e.Data, &msg)
			if err != nil {
				fn(fmt.Errorf("Unmarshal error: %v", err))
			}
			if msg.Data != message.Data {
				fn(fmt.Errorf("Async message contains wrong data"))
			} else {
				fn(nil)
			}
			return centrifuge.MessageReply{}
		})

		return centrifuge.ConnectReply{}
	})
	return node
}

func testReceiveRPCReceiveMessageProtobuf(fn ResolveFunc) *centrifuge.Node {
	cfg := centrifuge.DefaultConfig
	node, _ := centrifuge.New(cfg)

	message := []byte("boom 👻 boom")

	node.On().Connect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		time.AfterFunc(5*time.Second, func() { fn(fmt.Errorf("timeout")) })

		client.On().RPC(func(e centrifuge.RPCEvent) centrifuge.RPCReply {
			return centrifuge.RPCReply{
				Data: message,
			}
		})

		client.On().Message(func(e centrifuge.MessageEvent) centrifuge.MessageReply {
			if !bytes.Equal(message, e.Data) {
				fn(fmt.Errorf("Async message contains wrong data"))
			} else {
				fn(nil)
			}
			return centrifuge.MessageReply{}
		})

		return centrifuge.ConnectReply{}
	})
	return node
}

func handleTestResult(name string) ResolveFunc {
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			if err != nil {
				log.Printf("%s FAIL: %v", name, err)
			} else {
				log.Printf("%s OK", name)
			}
		})
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go RegisterTestCase(ctx, 10000, testCustomHeader, "testCustomHeader", true)
	go RegisterTestCase(ctx, 10001, testJWTAuth, "testJWTAuth", false)
	go RegisterTestCase(ctx, 10002, testSimpleSubscribe, "testSimpleSubscribe", true)
	go RegisterTestCase(ctx, 10003, testReceiveRPCReceiveMessageJSON, "testReceiveRPCReceiveMessageJSON", true)
	go RegisterTestCase(ctx, 10004, testReceiveRPCReceiveMessageProtobuf, "testReceiveRPCReceiveMessageProtobuf", true)

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Interrupted")
		cancel()
		done <- true
	}()
	<-done
	time.Sleep(time.Second)
}
