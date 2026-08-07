// Command dictionary_compression_browser is a browser demo of connection-level
// dictionary compression.
//
// It runs three Centrifuge nodes side by side - plain, permessage-deflate, and
// dictionary compression - fed the identical publication stream, so the page can
// compare all three at once rather than switching between them.
//
// Wire bytes are counted on the SERVER, at the socket. That is not a detail: a
// browser cannot measure permessage-deflate at all, because the WebSocket API
// inflates those frames before JavaScript sees them, so any in-page byte count
// would report deflate as saving exactly nothing. Dictionary compression is
// decoded by the SDK itself and so can be measured in-page, but mixing the two
// would compare decompressed bytes against compressed ones.
//
// The feed is a live odds board: many small JSON messages at a steady rate,
// which is the shape where the feature helps most and where permessage-deflate
// does not.
//
//	go run ./dictionary_compression_browser     # or `go run .` from inside it
//	# then, from the centrifuge-js checkout, serve the SDK bundles:
//	yarn dev            # both bundles on http://localhost:2000
//
// Open http://localhost:8400.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifuge/_examples/dictionaryengine"
	"github.com/centrifugal/protocol"
)

// The page is embedded rather than read from disk so the demo runs the same
// whether you launch it from _examples or from inside this directory.
//
//go:embed static
var staticFS embed.FS

var addr = flag.String("addr", ":8400", "address to listen on")

const feedChannel = "odds:board"

// feedProfile is what the page declares at connect. It names the kind of client
// this is, and the server serves the dictionary trained for it.
const feedProfile = "odds-board"

// demoDictionarySize caps the trained dictionary. A few kilobytes is the useful
// range: bigger keeps helping, with falling returns and a rising cost to deliver.
const demoDictionarySize = 2048

// oddsDictionary is the artifact a trainer would produce for this profile: a
// sample of the traffic it carries, with the protocol structure dictionary
// underneath so it covers the envelope as well as the payload.
//
// Here it is generated from the same feed the demo publishes, using a separate
// seed. A real trainer builds it from captured traffic that has been anonymised
// and reviewed before anything is written into a dictionary handed to clients.
func oddsDictionary() []byte {
	rng := rand.New(rand.NewSource(11))
	samples := make([][]byte, 0, 256)
	for i := 0; i < 256; i++ {
		samples = append(samples, oddsUpdate(i, rng))
	}
	dict := append([]byte{}, protocol.StructureDictionary...)
	return append(dict, dictionaryengine.Train(samples, demoDictionarySize)...)
}

// mode is one server configuration under comparison.
type mode struct {
	key  string
	path string
	node *centrifuge.Node
	// bytes counts everything written to sockets serving this mode, after any
	// compression - i.e. what actually leaves the machine.
	bytes atomic.Int64
	// msgs counts protocol messages handed to those sockets. bytes/msgs is the
	// comparison metric, and it stays correct with any number of browser tabs.
	msgs atomic.Int64
	// deflate enables permessage-deflate for this mode.
	deflate bool
	// engine is set for both compressed modes.
	engine *dictionaryengine.Engine
}

func (m *mode) newNode() {
	cfg := centrifuge.Config{
		LogLevel:   centrifuge.LogLevelError,
		LogHandler: func(centrifuge.LogEntry) {},
	}
	switch m.key {
	case "structure":
		// Nothing trained for any profile, so every connection falls back to the
		// protocol structure dictionary. That is the tier a connection gets when
		// its profile has no dictionary - it holds no application data, so it
		// needs no disclosure decision from anyone.
		m.engine = dictionaryengine.New(dictionaryengine.Options{FrameCacheSize: 4096})
		cfg.DictionaryCompression = m.engine
	case "dict":
		dict := oddsDictionary()
		// The same dictionary is served on both protocols here because the demo
		// publishes JSON payloads either way, so only the envelope differs. A
		// real trainer builds one per protocol from that protocol's own frames.
		m.engine = dictionaryengine.New(dictionaryengine.Options{
			Dictionaries: map[dictionaryengine.Key][]byte{
				{Profile: feedProfile, Protocol: centrifuge.ProtocolTypeJSON}:     dict,
				{Profile: feedProfile, Protocol: centrifuge.ProtocolTypeProtobuf}: dict,
			},
			FrameCacheSize: 4096,
		})
		cfg.DictionaryCompression = m.engine
	}
	node, err := centrifuge.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "demo"}}, nil
	})
	node.OnTransportWrite(func(c *centrifuge.Client, e centrifuge.TransportWriteEvent) bool {
		m.msgs.Add(1)
		return true
	})
	node.OnConnect(func(c *centrifuge.Client) {
		c.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	m.node = node
}

// ---------------------------------------------------------------------------
// Socket-level byte accounting
// ---------------------------------------------------------------------------

type countingConn struct {
	net.Conn
	// written points at the counter of whichever mode this connection ended up
	// serving. It is redirected once, by the WebSocket handler, before the
	// upgrade - so handshake bytes land on a throwaway counter and payload bytes
	// on the right one.
	written atomic.Pointer[atomic.Int64]
}

func (c *countingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if ctr := c.written.Load(); ctr != nil {
		ctr.Add(int64(n))
	}
	return n, err
}

type countingListener struct{ net.Listener }

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	cc := &countingConn{Conn: c}
	cc.written.Store(&atomic.Int64{}) // discard until a mode claims it
	return cc, nil
}

type connKey struct{}

// attribute points a connection's byte counter at the mode it is about to serve.
func attribute(m *mode, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cc, ok := r.Context().Value(connKey{}).(*countingConn); ok {
			cc.written.Store(&m.bytes)
		}
		h.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------------------
// Feed
// ---------------------------------------------------------------------------

var teams = []string{"Arsenal", "Chelsea", "Liverpool", "Everton", "Fulham", "Brentford", "Wolves", "Burnley"}
var markets = []string{"1x2", "over_under", "both_score", "correct_score", "next_goal"}

func oddsUpdate(i int, rng *rand.Rand) []byte {
	b, _ := json.Marshal(map[string]any{
		"eventId": fmt.Sprintf("evt-%06d", 100000+rng.Intn(400)),
		"market":  markets[rng.Intn(len(markets))],
		"home":    teams[rng.Intn(len(teams))],
		"away":    teams[rng.Intn(len(teams))],
		"odds": map[string]any{
			"h": float64(100+rng.Intn(900)) / 100,
			"d": float64(200+rng.Intn(600)) / 100,
			"a": float64(100+rng.Intn(900)) / 100,
		},
		"suspended": rng.Intn(20) == 0,
		"ts":        time.Now().UnixMilli(),
		"seq":       i,
	})
	return b
}

// feed publishes the same stream into every node, so whichever endpoint a tab is
// connected to sees identical traffic and the comparison is fair.
func feed(modes []*mode, ratePerSec int) {
	rng := rand.New(rand.NewSource(7))
	ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
	defer ticker.Stop()
	i := 0
	for range ticker.C {
		data := oddsUpdate(i, rng)
		for _, m := range modes {
			_, _ = m.node.Publish(feedChannel, data)
		}
		i++
	}
}

func main() {
	flag.Parse()

	modes := []*mode{
		{key: "none", path: "/connection/websocket"},
		{key: "deflate", path: "/connection/websocket/deflate", deflate: true},
		{key: "structure", path: "/connection/websocket/structure"},
		{key: "dict", path: "/connection/websocket/compressed"},
	}
	for _, m := range modes {
		m.newNode()
	}
	go feed(modes, 30)

	mux := http.NewServeMux()
	for _, m := range modes {
		mux.Handle(m.path, attribute(m, centrifuge.NewWebsocketHandler(m.node, centrifuge.WebsocketConfig{
			CheckOrigin: func(r *http.Request) bool { return true },
			Compression: m.deflate,
			// CompressionLevel defaults to 0, which is flate.NoCompression: the
			// connection negotiates permessage-deflate and then stores rather than
			// compresses. Level 6 matches what dictionary compression uses.
			CompressionLevel: 6,
		})))
	}

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Per mode rather than global: two modes now run an engine, and they hold
		// different dictionaries.
		type modeStat struct {
			Bytes int64 `json:"bytes"`
			Msgs  int64 `json:"msgs"`
			// Present only for modes running an engine.
			FrameCompressions int64  `json:"frameCompressions,omitempty"`
			FrameCacheHits    int64  `json:"frameCacheHits,omitempty"`
			DictionaryID      string `json:"dictionaryId,omitempty"`
			DictionarySize    int    `json:"dictionarySize,omitempty"`
			// What delivering the dictionary costs once, which is what a
			// connection has to earn back.
			DictionaryWireBytes int `json:"dictionaryWireBytes,omitempty"`
		}
		ms := map[string]modeStat{}
		out := map[string]any{"modes": ms}
		for _, m := range modes {
			st := modeStat{Bytes: m.bytes.Load(), Msgs: m.msgs.Load()}
			if m.engine != nil {
				s := m.engine.Stats()
				st.FrameCompressions = s.FrameCompressions
				st.FrameCacheHits = s.FrameCacheHits
				// The structure mode has no profile dictionary, so it reports the
				// fallback - the one under the empty profile.
				want := feedProfile
				if m.key == "structure" {
					want = ""
				}
				for _, d := range s.Dictionaries {
					if d.Key.Profile == want {
						st.DictionaryID = d.ID
						st.DictionarySize = d.Size
						st.DictionaryWireBytes = d.WireSize
						break
					}
				}
			}
			ms[m.key] = st
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	static, err := fs.Sub(staticFS, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux.Handle("/", http.FileServer(http.FS(static)))

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		if errors.Is(err, syscall.EADDRINUSE) {
			log.Fatalf("%s is already in use - another copy of this demo is probably still running.\n"+
				"Find it with `lsof -nP -iTCP%s -sTCP:LISTEN`, or pass -addr=:8401.", *addr, *addr)
		}
		log.Fatal(err)
	}

	srv := &http.Server{
		Handler: mux,
		// Hand each request the socket it arrived on, so the WebSocket handler
		// can point that socket's byte counter at the right mode.
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if cc, ok := c.(*countingConn); ok {
				return context.WithValue(ctx, connKey{}, cc)
			}
			return ctx
		},
	}
	fmt.Printf("open http://localhost%s\n", *addr)
	fmt.Println("SDK bundles: run `yarn dev` in the centrifuge-js checkout (serves both on port 2000)")
	log.Fatal(srv.Serve(countingListener{ln}))
}
