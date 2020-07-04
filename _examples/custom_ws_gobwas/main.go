package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/gobwas/ws"
	"github.com/mailru/easygo/netpoll"
)

var (
	workers   = flag.Int("workers", 128, "max workers count")
	queue     = flag.Int("queue", 1, "workers task queue size")
	ioTimeout = flag.Duration("io_timeout", time.Millisecond*500, "i/o operations timeout")
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
	flag.Parse()

	cfg := centrifuge.DefaultConfig

	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	node.On().Connecting(func(ctx context.Context, t centrifuge.TransportInfo, e centrifuge.ConnectEvent) centrifuge.ConnectReply {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: "",
			},
		}
	})

	node.On().Connected(func(ctx context.Context, client *centrifuge.Client) centrifuge.Event {
		transport := client.Transport()
		log.Printf("user %s connected via %s with format: %s", client.UserID(), transport.Name(), transport.Protocol())

		// Connect handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				err := client.Send(centrifuge.Raw(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
				if err != nil {
					if err != io.EOF {
						log.Println(err.Error())
					} else {
						return
					}
				}
				time.Sleep(5 * time.Second)
			}
		}()
		return centrifuge.EventAll
	})

	node.On().Subscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.SubscribeEvent) centrifuge.SubscribeReply {
		log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
		return centrifuge.SubscribeReply{}
	})

	node.On().Unsubscribe(func(ctx context.Context, client *centrifuge.Client, e centrifuge.UnsubscribeEvent) centrifuge.UnsubscribeReply {
		log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		return centrifuge.UnsubscribeReply{}
	})

	node.On().Publish(func(ctx context.Context, client *centrifuge.Client, e centrifuge.PublishEvent) centrifuge.PublishReply {
		log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
		return centrifuge.PublishReply{}
	})

	node.On().Message(func(ctx context.Context, client *centrifuge.Client, e centrifuge.MessageEvent) centrifuge.MessageReply {
		log.Printf("Message from user: %s, data: %s", client.UserID(), string(e.Data))
		return centrifuge.MessageReply{}
	})

	node.On().Disconnect(func(ctx context.Context, client *centrifuge.Client, e centrifuge.DisconnectEvent) centrifuge.DisconnectReply {
		log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		return centrifuge.DisconnectReply{}
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Initialize netpoll instance. We will use it to be noticed about incoming
	// events from listener of user connections.
	poller, err := netpoll.New(nil)
	if err != nil {
		log.Fatal(err)
	}

	var (
		// Make pool of X size, Y sized work queue and one pre-spawned
		// goroutine.
		pool = NewPool(*workers, *queue, 1)
	)

	// handle is a new incoming connection handler.
	// It upgrades TCP connection to WebSocket, registers netpoll listener on
	// it and stores it as a chat user in Chat instance.
	//
	// We will call it below within accept() loop.
	handle := func(conn net.Conn) {
		// NOTE: we wrap conn here to show that ws could work with any kind of
		// io.ReadWriter.
		safeConn := deadliner{conn, *ioTimeout}

		protoType := centrifuge.ProtocolTypeJSON

		up := ws.Upgrader{
			OnRequest: func(uri []byte) error {
				if strings.Contains(string(uri), "format=protobuf") {
					protoType = centrifuge.ProtocolTypeProtobuf
				}
				return nil
			},
		}

		// Zero-copy upgrade to WebSocket connection.
		hs, err := up.Upgrade(safeConn)
		if err != nil {
			log.Printf("%s: upgrade error: %v", nameConn(conn), err)
			_ = conn.Close()
			return
		}

		log.Printf("%s: established websocket connection: %+v", nameConn(conn), hs)

		transport := newWebsocketTransport(safeConn, protoType)
		client, closeFn, err := centrifuge.NewClient(context.Background(), node, transport)
		if err != nil {
			log.Printf("%s: client create error: %v", nameConn(conn), err)
			_ = conn.Close()
			return
		}

		// Create netpoll event descriptor for conn.
		// We want to handle only read events of it.
		desc := netpoll.Must(netpoll.HandleReadOnce(conn))

		// Subscribe to events about conn.
		_ = poller.Start(desc, func(ev netpoll.Event) {
			if ev&(netpoll.EventReadHup|netpoll.EventHup) != 0 {
				// When ReadHup or Hup received, this mean that client has
				// closed at least write end of the connection or connections
				// itself. So we want to stop receive events about such conn
				// and remove it from the chat registry.
				_ = poller.Stop(desc)
				_ = closeFn()
				return
			}
			// Here we can read some new message from connection.
			// We can not read it right here in callback, because then we will
			// block the poller's inner loop.
			// We do not want to spawn a new goroutine to read single message.
			// But we want to reuse previously spawned goroutine.
			pool.Schedule(func() {
				if data, isControl, err := transport.read(); err != nil {
					// When receive failed, we can only disconnect broken
					// connection and stop to receive events about it.
					_ = poller.Stop(desc)
					_ = closeFn()
				} else {
					if !isControl {
						ok := client.Handle(data)
						if !ok {
							_ = poller.Stop(desc)
							return
						}
					}
					_ = poller.Resume(desc)
				}
			})
		})
	}

	// Create incoming connections listener.
	ln, err := net.Listen("tcp", ":3333")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("websocket is listening on %s", ln.Addr().String())

	// Create netpoll descriptor for the listener.
	// We use OneShot here to manually resume events stream when we want to.
	acceptDesc := netpoll.Must(netpoll.HandleListener(
		ln, netpoll.EventRead|netpoll.EventOneShot,
	))

	// accept is a channel to signal about next incoming connection Accept()
	// results.
	accept := make(chan error, 1)

	// Subscribe to events about listener.
	_ = poller.Start(acceptDesc, func(e netpoll.Event) {
		// We do not want to accept incoming connection when goroutine pool is
		// busy. So if there are no free goroutines during 1ms we want to
		// cooldown the server and do not receive connection for some short
		// time.
		err := pool.ScheduleTimeout(time.Millisecond, func() {
			conn, err := ln.Accept()
			if err != nil {
				accept <- err
				return
			}

			accept <- nil
			handle(conn)
		})
		if err == nil {
			err = <-accept
		}
		if err != nil {
			if err != ErrScheduleTimeout {
				goto cooldown
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				goto cooldown
			}

			log.Fatalf("accept error: %v", err)

		cooldown:
			delay := 5 * time.Millisecond
			log.Printf("accept error: %v; retrying in %s", err, delay)
			time.Sleep(delay)
		}

		_ = poller.Resume(acceptDesc)
	})

	go func() {
		http.Handle("/", http.FileServer(http.Dir("./")))
		log.Printf("run http server on :8000")
		if err := http.ListenAndServe(":8000", nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

func nameConn(conn net.Conn) string {
	return conn.LocalAddr().String() + " > " + conn.RemoteAddr().String()
}

// deadliner is a wrapper around net.Conn that sets read/write deadlines before
// every Read() or Write() call.
type deadliner struct {
	net.Conn
	t time.Duration
}

func (d deadliner) Write(p []byte) (int, error) {
	if err := d.Conn.SetWriteDeadline(time.Now().Add(d.t)); err != nil {
		return 0, err
	}
	return d.Conn.Write(p)
}

func (d deadliner) Read(p []byte) (int, error) {
	if err := d.Conn.SetReadDeadline(time.Now().Add(d.t)); err != nil {
		return 0, err
	}
	return d.Conn.Read(p)
}
