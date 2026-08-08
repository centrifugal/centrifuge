// Command dictionary_compression measures what connection-level dictionary
// compression actually saves on the wire.
//
// It runs a matrix of {payload shape} x {JSON, Protobuf} x {feature off, on},
// counting real bytes delivered from server to client over a real WebSocket
// connection, and prints the comparison.
//
// The payload shapes are deliberately unrelated to each other. Each scenario
// trains its own dictionary offline from traffic of the same kind, using a
// separate seed, which is what a trainer would do from captured traffic.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge"
	centrifugego "github.com/centrifugal/centrifuge-go"
	"github.com/centrifugal/centrifuge/_examples/dictionaryengine"
)

// countingConn counts bytes arriving from the server. Measuring at the socket
// means the numbers include WebSocket framing and anything permessage-deflate
// does, not just protocol payloads.
type countingConn struct {
	net.Conn
	read *int64
}

func (c *countingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	atomic.AddInt64(c.read, int64(n))
	return n, err
}

// ---------------------------------------------------------------------------
// Payload shapes. Each is a different application with a different schema.
// ---------------------------------------------------------------------------

type shape struct {
	name string
	gen  func(i int, rng *rand.Rand) []byte
}

var shapes = []shape{
	{"chat messages", func(i int, rng *rand.Rand) []byte {
		return mustJSON(map[string]any{
			"room": fmt.Sprintf("room-%03d", rng.Intn(200)),
			"user": fmt.Sprintf("user_%04d", rng.Intn(9999)),
			"text": phrases[rng.Intn(len(phrases))],
			"ts":   1780000000 + i,
		})
	}},
	{"IoT telemetry", func(i int, rng *rand.Rand) []byte {
		return mustJSON(map[string]any{
			"device":   fmt.Sprintf("sensor-%05d", rng.Intn(20000)),
			"temp_c":   float64(rng.Intn(4000)) / 100,
			"humidity": rng.Intn(100),
			"battery":  rng.Intn(100),
			"rssi":     -rng.Intn(90),
			"firmware": "2.4.1",
		})
	}},
	{"order events", func(i int, rng *rand.Rand) []byte {
		return mustJSON(map[string]any{
			"orderId":  fmt.Sprintf("ord-%08d", rng.Intn(99999999)),
			"status":   []string{"created", "paid", "packed", "shipped", "delivered"}[rng.Intn(5)],
			"customer": fmt.Sprintf("cus-%06d", rng.Intn(999999)),
			"total":    float64(rng.Intn(50000)) / 100,
			"currency": "EUR",
			"items":    rng.Intn(8) + 1,
		})
	}},
	{"location updates", func(i int, rng *rand.Rand) []byte {
		return mustJSON(map[string]any{
			"vehicle": fmt.Sprintf("veh-%04d", rng.Intn(2000)),
			"lat":     52.0 + float64(rng.Intn(100000))/1e6,
			"lon":     13.0 + float64(rng.Intn(100000))/1e6,
			"heading": rng.Intn(360),
			"speed":   rng.Intn(130),
		})
	}},
	{"opaque blobs (random)", func(i int, rng *rand.Rand) []byte {
		// Deliberately incompressible: random bytes carry no redundancy for a
		// dictionary to exploit. This is the honest floor of the mechanism and
		// exercises the raw-frame fallback, which must not make things worse.
		raw := make([]byte, 96)
		for j := range raw {
			raw[j] = byte(rng.Intn(256))
		}
		return mustJSON(map[string]any{"blob": base64.StdEncoding.EncodeToString(raw)})
	}},
	{"document state (1.5KB)", func(i int, rng *rand.Rand) []byte {
		blocks := make([]any, 0, 20)
		for j := 0; j < 20; j++ {
			text := fmt.Sprintf("stable paragraph %02d", j)
			if j == i%20 {
				text = phrases[rng.Intn(len(phrases))]
			}
			blocks = append(blocks, map[string]any{
				"id": fmt.Sprintf("b%02d", j), "type": "paragraph", "text": text,
			})
		}
		return mustJSON(map[string]any{"docId": "doc-771", "rev": i, "blocks": blocks})
	}},
}

var phrases = []string{
	"are you around later today",
	"pushed the fix, please take a look",
	"the deploy finished without errors",
	"can we move the sync to tomorrow",
	"numbers look better after the change",
	"still investigating the timeout",
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const testChannel = "demo:channel"

// mode selects which server-side bandwidth strategy a scenario uses.
type mode int

const (
	modeOff        mode = iota // no compression at all
	modeDeflate                // permessage-deflate, the existing option
	modeDictionary             // connection-level dictionary compression
	// modeBuiltin has the feature enabled but serves only the protocol structure
	// dictionary, which carries no application data. This is the floor: what a
	// connection gets when nothing was trained for its profile.
	modeBuiltin
)

// dictionarySize is what a trained dictionary is capped at. Bigger dictionaries
// keep helping, but with falling returns and rising cost to deliver.
const dictionarySize = 4096

// trainDictionary builds the dictionary a scenario's connections are served.
//
// It is the offline half of the design in one function: take frames of the kind
// this profile carries and keep the most recent up to a size cap. A real trainer
// does this from captured traffic that has been anonymised and reviewed - the
// part an example cannot stand in for - but the shape of the artifact is the
// same.
//
// One per protocol. A dictionary is matched against frames, and a frame is
// envelope plus payload; the envelopes are nothing alike, so a JSON-trained
// dictionary covers no part of a Protobuf connection's envelope.
//
// The corpus uses a different seed from the measured run, so the dictionary is
// built from traffic of the same kind rather than from the very frames it is
// later scored against.
func trainDictionary(sh shape, proto centrifuge.ProtocolType) []byte {
	rng := rand.New(rand.NewSource(7))
	samples := make([][]byte, 0, 256)
	for i := 0; i < 256; i++ {
		samples = append(samples, dictionaryengine.Frame(proto, testChannel, sh.gen(i, rng)))
	}
	return dictionaryengine.Train(samples, dictionarySize)
}

func protoType(useProtobuf bool) centrifuge.ProtocolType {
	if useProtobuf {
		return centrifuge.ProtocolTypeProtobuf
	}
	return centrifuge.ProtocolTypeJSON
}

func newNode(m mode, sh shape, useProtobuf bool) *centrifuge.Node {
	cfg := centrifuge.Config{
		LogLevel:   centrifuge.LogLevelError,
		LogHandler: func(e centrifuge.LogEntry) {},
	}
	proto := protoType(useProtobuf)
	switch m {
	case modeBuiltin:
		// No dictionary for this profile, so every connection falls back to the
		// envelope-only dictionary for its protocol.
		cfg.DictionaryCompression = dictionaryengine.New(dictionaryengine.Options{
			Fallback:       map[centrifuge.ProtocolType][]byte{proto: dictionaryengine.StructureDictionary(proto)},
			FrameCacheSize: 4096,
		})
	case modeDictionary:
		cfg.DictionaryCompression = dictionaryengine.New(dictionaryengine.Options{
			Dictionaries:   map[dictionaryengine.Key][]byte{{Protocol: proto}: trainDictionary(sh, proto)},
			FrameCacheSize: 4096,
		})
	}
	node, err := centrifuge.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "demo"}}, nil
	})
	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	return node
}

type result struct {
	warmupBytes   int64
	measuredBytes int64
	measuredMsgs  int
	payloadBytes  int64
	compressed    bool
}

// run performs one full scenario end to end and returns the bytes actually
// delivered from server to client during the measured phase.
func run(sh shape, useProtobuf bool, m mode, warmup, measured int) result {
	node := newNode(m, sh, useProtobuf)
	defer func() { _ = node.Shutdown(context.Background()) }()

	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		Compression: m == modeDeflate,
		// CompressionLevel defaults to 0, which is flate.NoCompression: the
		// connection negotiates permessage-deflate and then stores rather than
		// compresses. Level 6 matches what dictionary compression uses, so the two
		// are compared at the same setting.
		CompressionLevel: 6,
	})
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", wsHandler)
	server := httptest.NewServer(mux)
	defer server.Close()

	var readBytes int64
	dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := net.Dial(network, addr)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: c, read: &readBytes}, nil
	}

	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/connection/websocket"
	cfg := centrifugego.Config{NetDialContext: dial, EnableCompression: m == modeDeflate}
	var client *centrifugego.Client
	if useProtobuf {
		client = centrifugego.NewProtobufClient(url, cfg)
	} else {
		client = centrifugego.NewJsonClient(url, cfg)
	}
	defer client.Close()

	connected := make(chan struct{})
	var once sync.Once
	client.OnConnected(func(e centrifugego.ConnectedEvent) { once.Do(func() { close(connected) }) })
	if err := client.Connect(); err != nil {
		log.Fatal(err)
	}
	<-connected

	sub, err := client.NewSubscription(testChannel)
	if err != nil {
		log.Fatal(err)
	}

	var mu sync.Mutex
	var received int
	var gotPayload int64
	var expect [][]byte
	var mismatch int
	wake := make(chan struct{}, 1)
	sub.OnPublication(func(e centrifugego.PublicationEvent) {
		mu.Lock()
		if received < len(expect) && string(expect[received]) != string(e.Data) {
			mismatch++
		}
		received++
		gotPayload += int64(len(e.Data))
		mu.Unlock()
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	subscribed := make(chan struct{})
	var subOnce sync.Once
	sub.OnSubscribed(func(e centrifugego.SubscribedEvent) { subOnce.Do(func() { close(subscribed) }) })
	if err := sub.Subscribe(); err != nil {
		log.Fatal(err)
	}
	<-subscribed

	rng := rand.New(rand.NewSource(42))
	publish := func(n int) {
		for i := 0; i < n; i++ {
			data := sh.gen(i, rng)
			mu.Lock()
			expect = append(expect, data)
			mu.Unlock()
			if _, err := node.Publish(testChannel, data); err != nil {
				log.Fatal(err)
			}
		}
	}
	waitFor := func(target int) {
		deadline := time.After(30 * time.Second)
		for {
			mu.Lock()
			got := received
			mu.Unlock()
			if got >= target {
				return
			}
			select {
			case <-wake:
			case <-time.After(20 * time.Millisecond):
			case <-deadline:
				log.Fatalf("timeout waiting for %d messages, got %d", target, got)
			}
		}
	}

	publish(warmup)
	waitFor(warmup)
	// Let any in-flight frames land before splitting the measurement.
	time.Sleep(150 * time.Millisecond)
	warmupBytes := atomic.LoadInt64(&readBytes)
	mu.Lock()
	payloadBefore := gotPayload
	mu.Unlock()

	publish(measured)
	waitFor(warmup + measured)
	time.Sleep(150 * time.Millisecond)
	totalBytes := atomic.LoadInt64(&readBytes)

	mu.Lock()
	payloadAfter := gotPayload
	bad := mismatch
	mu.Unlock()
	if bad != 0 {
		log.Fatalf("CORRUPTION: %d payload mismatches for %s", bad, sh.name)
	}

	return result{
		warmupBytes:   warmupBytes,
		measuredBytes: totalBytes - warmupBytes,
		measuredMsgs:  measured,
		payloadBytes:  payloadAfter - payloadBefore,
		compressed:    m == modeDictionary,
	}
}

func main() {
	const warmup = 600
	const measured = 1500

	for _, proto := range []struct {
		name string
		pb   bool
	}{{"JSON", false}, {"Protobuf", true}} {
		fmt.Printf("\n=== %s protocol ===\n", proto.name)
		fmt.Printf("%-24s %10s %11s %11s %11s %11s %11s\n",
			"payload shape", "payload", "no compr", "deflate", "built-in", "dictionary", "vs deflate")
		var totOff, totDef, totBuiltin, totDict, totPayload int64
		var e2eDef, e2eDict int64
		for _, sh := range shapes {
			off := run(sh, proto.pb, modeOff, warmup, measured)
			def := run(sh, proto.pb, modeDeflate, warmup, measured)
			builtin := run(sh, proto.pb, modeBuiltin, warmup, measured)
			dict := run(sh, proto.pb, modeDictionary, warmup, measured)
			totOff += off.measuredBytes
			totDef += def.measuredBytes
			totBuiltin += builtin.measuredBytes
			totDict += dict.measuredBytes
			totPayload += dict.payloadBytes
			e2eDef += def.warmupBytes + def.measuredBytes
			e2eDict += dict.warmupBytes + dict.measuredBytes
			fmt.Printf("%-24s %9dB %10dB %10dB %10dB %10dB %10.2fx\n",
				sh.name, dict.payloadBytes, off.measuredBytes, def.measuredBytes,
				builtin.measuredBytes, dict.measuredBytes,
				float64(def.measuredBytes)/float64(dict.measuredBytes))
		}
		fmt.Printf("%-24s %9dB %10dB %10dB %10dB %10dB %10.2fx\n",
			"TOTAL (steady state)", totPayload, totOff, totDef, totBuiltin, totDict,
			float64(totDef)/float64(totDict))
		fmt.Printf("%-24s %9s %10s %10s %10.2fx %9s\n",
			"  built-in alone", "", "", "", float64(totOff)/float64(totBuiltin),
			"vs no compression")
		fmt.Printf("%-24s %9s %10s %10dB %10s %10dB %10.2fx  <- includes warm-up + dictionary transfer\n",
			"TOTAL (whole session)", "", "", e2eDef, "", e2eDict,
			float64(e2eDef)/float64(e2eDict))
	}
	fmt.Printf("\nMeasured over %d publications per shape after a %d message warm-up,\n", measured, warmup)
	fmt.Println("so dictionary numbers are steady state and exclude the ramp-up.")
	fmt.Println("Bytes are counted at the client socket and include WebSocket framing.")
	fmt.Println("Every received payload is compared against what was published; any mismatch aborts.")
	fmt.Println("\n\"built-in\" is the engine enabled with nothing trained for this profile, so")
	fmt.Println("connections fall back to the envelope-only dictionary for their protocol. It")
	fmt.Println("holds no application data and is the floor a connection gets when nothing was")
	fmt.Println("trained for it. On Protobuf that floor is nearly the baseline: the tier works")
	fmt.Println("by remembering repeated key strings, and a Protobuf envelope has none.")
}
