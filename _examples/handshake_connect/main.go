// Example demonstrates handshake connect: the client sends its connect and
// subscribe commands inside the WebSocket handshake request and is ready one
// round trip earlier. See README.md.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/centrifugal/centrifuge"
)

var (
	port         = flag.Int("port", 8000, "Port to open the page on, served through a proxy adding latency")
	rtt          = flag.Duration("rtt", 100*time.Millisecond, "Round trip time the proxy adds")
	proxyLimit   = flag.Int("proxy_header_limit", 128, "Sec-WebSocket-Protocol size the strict proxy endpoint accepts")
	centrifugeJS = flag.String("centrifuge_js", "http://localhost:2000/centrifuge.js", "URL of centrifuge-js browser build, the default is served by npm run dev in centrifuge-js")
)

const clockChannel = "clock"

func main() {
	flag.Parse()

	node, err := centrifuge.New(centrifuge.Config{LogLevel: centrifuge.LogLevelInfo})
	if err != nil {
		log.Fatal(err)
	}
	node.OnConnecting(func(_ context.Context, _ centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// Anonymous users, this example has no authentication.
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{}}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if e.Channel != clockChannel {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	go func() {
		for t := range time.Tick(time.Second) {
			_, _ = node.Publish(clockChannel, []byte(`{"time":"`+t.Format(time.TimeOnly)+`"}`))
		}
	}()

	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		HandshakeConnect: true,
	})
	http.Handle("/connection/websocket", wsHandler)
	// Simulates a proxy which rejects handshakes with large headers, like nginx
	// does above large_client_header_buffers, to show the client falling back.
	http.Handle("/connection/websocket/strict", strictProxy(wsHandler, *proxyLimit))
	http.HandleFunc("/centrifuge.js", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *centrifugeJS, http.StatusFound)
	})
	http.HandleFunc("/rtt", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(rtt.String()))
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "index.html")
	})

	// The app listens on an internal port, reached through the proxy.
	appListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := http.Serve(appListener, nil); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("open http://localhost:%d (a proxy adds %s round trip time)", *port, *rtt)
	if err := listenWithLatency(":"+strconv.Itoa(*port), appListener.Addr().String(), *rtt/2); err != nil {
		log.Fatal(err)
	}
}

func strictProxy(h http.Handler, limit int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header.Get("Sec-WebSocket-Protocol")) > limit {
			http.Error(w, "request header too large (simulated proxy limit)", http.StatusBadRequest)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// listenWithLatency proxies TCP connections to target, delaying data by delay
// in each direction.
func listenWithLatency(addr, target string, delay time.Duration) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				_ = conn.Close()
				return
			}
			go pipeWithDelay(upstream, conn, delay)
			pipeWithDelay(conn, upstream, delay)
		}()
	}
}

// pipeWithDelay copies src to dst, writing each chunk delay after it was read.
func pipeWithDelay(dst, src net.Conn, delay time.Duration) {
	type chunk struct {
		data []byte
		at   time.Time
	}
	chunks := make(chan chunk, 1024)
	go func() {
		defer close(chunks)
		buf := make([]byte, 32*1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				chunks <- chunk{append([]byte(nil), buf[:n]...), time.Now().Add(delay)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range chunks {
		time.Sleep(time.Until(c.at))
		if _, err := dst.Write(c.data); err != nil {
			break
		}
	}
	// Closing ends both directions.
	_ = dst.Close()
	_ = src.Close()
	for range chunks {
		// Let the reader exit.
	}
}
