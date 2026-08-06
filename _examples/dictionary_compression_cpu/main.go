// Command dictionary_compression_cpu measures the actual server CPU cost of
// connection-level dictionary compression, rather than extrapolating it from a
// microbenchmark.
//
// The whole question is how often the shared frame cache hits. Frames are only
// shareable between connections while the writer is keeping up and emitting one
// message per frame; once it falls behind it batches, frames diverge, and every
// connection pays for its own compression. That depends on real load, so it can
// not be answered from a benchmark - it has to be measured under one.
//
// Client load runs in a SEPARATE process on purpose. Running thousands of
// clients in the same process as the server makes the clients steal CPU from
// it, which both distorts the CPU reading and artificially induces the batching
// being measured.
//
//	# terminal 1
//	go run ./dictionary_compression_cpu -mode=server -compress
//	# terminal 2
//	go run ./dictionary_compression_cpu -mode=client -conns=2000
//
// The server prints its own CPU seconds, the achieved delivery rate, and the
// frame cache hit rate once the run finishes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/gorilla/websocket"
)

var (
	mode      = flag.String("mode", "server", "server or client")
	compress  = flag.Bool("compress", false, "enable dictionary compression (server)")
	addr      = flag.String("addr", "127.0.0.1:8300", "listen / connect address")
	cwarm     = flag.Duration("cwarm", 13*time.Second, "client: wait before snapshotting bytes (aligns with server warm-up)")
	cmeasure  = flag.Duration("cmeasure", 20*time.Second, "client: measured window")
	conns     = flag.Int("conns", 2000, "number of client connections (client)")
	waitConns = flag.Int("wait", 2000, "connections to wait for before measuring (server)")
	duration  = flag.Duration("duration", 20*time.Second, "measured publish duration (server)")
	rate      = flag.Int("rate", 2, "publications per second per shared channel (server)")
	shared    = flag.Int("shared", 5, "channels each client subscribes to")
	nocache   = flag.Bool("nocache", false, "disable the shared frame cache (server)")
	random    = flag.Bool("random", false, "publish incompressible random payloads (server)")
	wdelay    = flag.Duration("writedelay", 0, "ConnectReply.WriteDelay - batches messages per connection (server)")
	deflate   = flag.Bool("deflate", false, "use permessage-deflate instead of dictionary (server)")
	pool      = flag.Int("pool", 5, "total channel pool; >shared makes each client subscribe to a random subset, so batched frames differ between connections")
)

func cpuSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	u := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	s := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return u + s
}

func sharedChannel(i int) string { return fmt.Sprintf("odds:market:%02d", i) }

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

func runServer() {
	var engine *centrifuge.DictionaryCompressionEngine
	cfg := centrifuge.Config{LogLevel: centrifuge.LogLevelError, LogHandler: func(centrifuge.LogEntry) {}}
	if *compress {
		dcCfg := centrifuge.DictionaryCompressionConfig{
			UseChannelDictionary:   func(string) bool { return true },
			MaxChannelDictionaries: 64,
		}
		if *nocache {
			dcCfg.FrameCacheSize = -1
		}
		engine = centrifuge.NewDictionaryCompressionEngine(dcCfg)
		cfg.DictionaryCompression = engine
	}
	node, err := centrifuge.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	var connected int64
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "load"},
			WriteDelay:  *wdelay,
		}, nil
	})
	node.OnConnect(func(c *centrifuge.Client) {
		atomic.AddInt64(&connected, 1)
		c.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
		c.OnDisconnect(func(centrifuge.DisconnectEvent) { atomic.AddInt64(&connected, -1) })
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		CheckOrigin: func(r *http.Request) bool { return true },
		Compression: *deflate,
		// CompressionLevel defaults to 0, which is flate.NoCompression: the
		// connection negotiates permessage-deflate and then stores rather than
		// compresses. Level 6 matches what dictionary compression uses, so the two
		// are compared at the same setting.
		CompressionLevel: 6,
		// Pings are pushed out beyond the run so the load generator does not have
		// to implement pong handling, and so ping/pong work does not land in the
		// CPU measurement. Both runs use the same setting, so the comparison is
		// unaffected.
		PingPongConfig: centrifuge.PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	fmt.Printf("server listening on %s (compression=%v), waiting for %d connections...\n", *addr, *compress, *waitConns)

	for atomic.LoadInt64(&connected) < int64(*waitConns) {
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("%d connections up\n", atomic.LoadInt64(&connected))

	seed := uint32(99)
	randByte := func() byte { seed = seed*1664525 + 1013904223; return byte(seed >> 24) }
	payload := func(i int) []byte {
		if *random {
			// Incompressible: models already compressed or encrypted payloads.
			raw := make([]byte, 120)
			for j := range raw {
				raw[j] = randByte()
			}
			b, _ := json.Marshal(map[string]any{"blob": raw})
			return b
		}
		b, _ := json.Marshal(map[string]any{
			"eventId": fmt.Sprintf("evt-%06d", 100000+i%400),
			"market":  "1x2",
			"odds":    map[string]any{"h": 1.5 + float64(i%300)/100, "d": 3.2, "a": 2.1 + float64(i%200)/100},
			"ts":      1780000000000 + int64(i)*250,
		})
		return b
	}

	// Warm up so the dictionary is built, shipped and active before measuring.
	fmt.Println("warming up...")
	warmTicker := time.NewTicker(time.Second / time.Duration(*rate))
	warmUntil := time.Now().Add(12 * time.Second)
	i := 0
	for time.Now().Before(warmUntil) {
		<-warmTicker.C
		for c := 0; c < *pool; c++ {
			_, _ = node.Publish(sharedChannel(c), payload(i))
			i++
		}
	}
	warmTicker.Stop()

	before := cpuSeconds()
	beforeStats := engineStats(engine)
	startWall := time.Now()
	published := 0

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	until := time.Now().Add(*duration)
	for time.Now().Before(until) {
		<-ticker.C
		for c := 0; c < *pool; c++ {
			if _, err := node.Publish(sharedChannel(c), payload(i)); err != nil {
				log.Fatal(err)
			}
			i++
			published++
		}
	}
	ticker.Stop()
	time.Sleep(1500 * time.Millisecond) // let writes drain

	cpu := cpuSeconds() - before
	wall := time.Since(startWall).Seconds()
	st := engineStats(engine)
	comp := st.FrameCompressions - beforeStats.FrameCompressions
	hits := st.FrameCacheHits - beforeStats.FrameCacheHits
	nconn := atomic.LoadInt64(&connected)
	deliveries := float64(published) * float64(nconn) * float64(*shared) / float64(*pool)

	fmt.Printf("\n=========== compression=%v ===========\n", *compress)
	fmt.Printf("connections .............. %d\n", nconn)
	fmt.Printf("publications ............. %d over %.1fs\n", published, wall)
	fmt.Printf("message deliveries ....... %.0f (%.0f/s)\n", deliveries, deliveries/wall)
	fmt.Printf("server CPU ............... %.2f s  (%.2f cores)\n", cpu, cpu/wall)
	fmt.Printf("CPU per 1M deliveries .... %.2f core-seconds\n", cpu/deliveries*1e6)
	if *compress {
		total := comp + hits
		hitRate := 0.0
		if total > 0 {
			hitRate = 100 * float64(hits) / float64(total)
		}
		fmt.Printf("frames compressed ........ %d\n", comp)
		fmt.Printf("frames from cache ........ %d\n", hits)
		fmt.Printf("cache hit rate ........... %.1f%%\n", hitRate)
		if comp > 0 {
			fmt.Printf("avg frames per compression %.1f\n", float64(total)/float64(comp))
		}
	}
	_ = srv.Close()
	_ = node.Shutdown(context.Background())
}

// ---------------------------------------------------------------------------
// Client: deliberately minimal - connect, subscribe, discard.
// ---------------------------------------------------------------------------

var readBytes int64

// countingConn counts bytes as they arrive off the socket, before any
// transport-level decompression.
type countingConn struct {
	net.Conn
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	atomic.AddInt64(&readBytes, int64(n))
	return n, err
}

func runClient() {
	url := "ws://" + *addr + "/connection/websocket"
	var wg sync.WaitGroup
	var up int64
	for n := 0; n < *conns; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Offer permessage-deflate. It is only used if the server also
			// enables it, so this is safe for every mode.
			//
			// Bytes are counted on the raw socket, NOT from ReadMessage: gorilla
			// transparently inflates permessage-deflate frames, so counting
			// message payloads would compare deflate post-decompression against
			// dictionary pre-decompression and show deflate saving nothing.
			d := websocket.Dialer{
				HandshakeTimeout:  20 * time.Second,
				EnableCompression: true,
				NetDial: func(network, address string) (net.Conn, error) {
					nc, err := net.Dial(network, address)
					if err != nil {
						return nil, err
					}
					return &countingConn{Conn: nc}, nil
				},
			}
			c, _, err := d.Dial(url, nil)
			if err != nil {
				log.Printf("dial %d: %v", n, err)
				return
			}
			defer func() { _ = c.Close() }()

			// Advertise dictionary compression support (flag bit 1<<0).
			if err := c.WriteMessage(websocket.TextMessage, []byte(`{"id":1,"connect":{"flag":1}}`)); err != nil {
				return
			}
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
			perm := rand.New(rand.NewSource(int64(n))).Perm(*pool)[:*shared]
			var sb strings.Builder
			for i, ch := range perm {
				fmt.Fprintf(&sb, `{"id":%d,"subscribe":{"channel":"%s"}}`, i+2, sharedChannel(ch))
				if i < len(perm)-1 {
					sb.WriteByte('\n')
				}
			}
			if err := c.WriteMessage(websocket.TextMessage, []byte(sb.String())); err != nil {
				return
			}
			atomic.AddInt64(&up, 1)

			// Read and discard. No protocol decoding, so the client stays cheap
			// and the server is the only thing doing real work.
			for {
				_, msg, err := c.ReadMessage()
				if err != nil {
					return
				}
				_ = msg
			}
		}(n)
		if n%200 == 0 {
			time.Sleep(80 * time.Millisecond)
		}
	}
	go func() {
		for atomic.LoadInt64(&up) < int64(*conns) {
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Printf("client: %d connections up, waiting %s then measuring %s\n", *conns, *cwarm, *cmeasure)
		time.Sleep(*cwarm)
		start := atomic.LoadInt64(&readBytes)
		time.Sleep(*cmeasure)
		got := atomic.LoadInt64(&readBytes) - start
		fmt.Printf("CLIENT WIRE BYTES over measured window: %d\n", got)
	}()
	wg.Wait()
}

// engineStats returns stats for the built-in engine, or a zero value when
// compression is not enabled for this run.
func engineStats(e *centrifuge.DictionaryCompressionEngine) centrifuge.DictionaryCompressionStats {
	if e == nil {
		return centrifuge.DictionaryCompressionStats{}
	}
	return e.Stats()
}

func main() {
	flag.Parse()
	switch *mode {
	case "server":
		runServer()
	case "client":
		runClient()
	default:
		fmt.Println("use -mode=server or -mode=client")
		os.Exit(1)
	}
}
