// Command dictionary_compression_casino models a real-time casino / sportsbook
// workload and measures what connection-level dictionary compression saves for
// it, in bytes and in projected egress cost.
//
// The workload is deliberately shaped like a real deployment rather than a
// benchmark: several channel types with very different fan-out, message rates
// and payload sizes, all mixed on the same connection.
//
//	odds:*        large fan-out, high rate, small payloads   (every bettor watching a market)
//	jackpot       largest fan-out, steady ticker, tiny payloads
//	table:*       medium fan-out, medium payloads            (live dealer table state)
//	user:#<id>    fan-out of one, low rate                   (balance, bet settlement)
//
// The per-user channel is the interesting one: it is the topology where a
// dictionary helps most and also the one where a naive implementation would
// hurt, since a connection receiving a handful of frames can not amortise a
// dictionary that has to be shipped to it.
package main

import (
	"context"
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
// Workload
// ---------------------------------------------------------------------------

// stream is one channel a representative player is subscribed to, with the
// number of messages that channel delivers during the measured session.
type stream struct {
	channel string
	count   int
	gen     func(i int, rng *rand.Rand) []byte
}

var teams = []string{"Arsenal", "Chelsea", "Liverpool", "Everton", "Fulham", "Brentford", "Wolves", "Burnley"}
var markets = []string{"1x2", "over_under", "both_score", "correct_score", "next_goal"}

func oddsUpdate(i int, rng *rand.Rand) []byte {
	return mustJSON(map[string]any{
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
		"ts":        1780000000000 + int64(i)*250,
	})
}

func jackpotTick(i int, rng *rand.Rand) []byte {
	return mustJSON(map[string]any{
		"pool":     fmt.Sprintf("mega-%d", 1+rng.Intn(3)),
		"amount":   1000000 + i*37 + rng.Intn(50),
		"currency": "EUR",
		"ts":       1780000000000 + int64(i)*500,
	})
}

func tableState(i int, rng *rand.Rand) []byte {
	seats := make([]any, 0, 7)
	for s := 0; s < 7; s++ {
		seats = append(seats, map[string]any{
			"seat":  s,
			"user":  fmt.Sprintf("player_%05d", rng.Intn(60000)),
			"bet":   rng.Intn(500),
			"cards": []string{"AH", "10S", "7D"}[:1+rng.Intn(3)],
		})
	}
	return mustJSON(map[string]any{
		"tableId": "bj-07",
		"round":   4000 + i,
		"phase":   []string{"betting", "dealing", "player_turn", "settle"}[rng.Intn(4)],
		"dealer":  map[string]any{"cards": []string{"KH"}, "total": 10 + rng.Intn(11)},
		"seats":   seats,
	})
}

func personalEvent(i int, rng *rand.Rand) []byte {
	switch rng.Intn(3) {
	case 0:
		return mustJSON(map[string]any{
			"type": "balance", "balance": float64(rng.Intn(500000)) / 100,
			"currency": "EUR", "ts": 1780000000000 + int64(i)*1000,
		})
	case 1:
		return mustJSON(map[string]any{
			"type": "bet_settled", "betId": fmt.Sprintf("bet-%09d", rng.Intn(999999999)),
			"outcome": []string{"won", "lost", "void"}[rng.Intn(3)],
			"payout":  float64(rng.Intn(100000)) / 100, "currency": "EUR",
		})
	default:
		return mustJSON(map[string]any{
			"type": "bonus", "code": fmt.Sprintf("FS%03d", rng.Intn(999)),
			"freeSpins": 10 + rng.Intn(90), "expiresIn": 86400,
		})
	}
}

// A 10 minute session for one representative player, at rates typical for a
// live sportsbook plus one live table plus their own account channel.
func sessionStreams() []stream {
	const seconds = 600
	return []stream{
		{"odds:football:major", int(4.0 * seconds), oddsUpdate}, // 4/s
		{"odds:tennis:atp", int(1.5 * seconds), oddsUpdate},     // 1.5/s
		{"jackpot:global", int(2.0 * seconds), jackpotTick},     // 2/s
		{"table:blackjack:07", int(1.0 * seconds), tableState},  // 1/s
		{"user:#42", int(0.05 * seconds), personalEvent},        // one every 20s
	}
}

// casinoDictionarySize covers five different message shapes, so it is larger
// than a single-shape profile would need.
const casinoDictionarySize = 8192

// casinoDictionary is the artifact a trainer would produce for this profile on
// this protocol.
//
// One dictionary covers everything a player's connection carries - odds, the
// jackpot ticker, the live table, their own account events - because a profile
// is a kind of client, not a channel. One per protocol, because a dictionary is
// matched against frames and the two protocols share no envelope.
//
// Generated here from the same feeds with a separate seed; a real trainer builds
// it from captured traffic that has been anonymised and reviewed first.
func casinoDictionary(proto centrifuge.ProtocolType) []byte {
	rng := rand.New(rand.NewSource(4242))
	var samples [][]byte
	for _, st := range sessionStreams() {
		// Sample each stream in proportion to how much of the traffic it is.
		n := st.count / 20
		if n < 8 {
			n = 8
		}
		for i := 0; i < n; i++ {
			samples = append(samples, dictionaryengine.Frame(proto, st.channel, st.gen(i, rng)))
		}
	}
	return dictionaryengine.Train(samples, casinoDictionarySize)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
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

type mode int

const (
	modeOff mode = iota
	modeDeflate
	modeDictionary
)

func (m mode) String() string {
	switch m {
	case modeOff:
		return "no compression"
	case modeDeflate:
		return "permessage-deflate"
	default:
		return "dictionary"
	}
}

type outcome struct {
	wireBytes    int64
	payloadBytes int64
	messages     int
	stats        dictionaryengine.Stats
	clientStats  centrifugego.CompressionStats
}

func run(m mode, useProtobuf bool) outcome {
	var engine *dictionaryengine.Engine
	cfg := centrifuge.Config{LogLevel: centrifuge.LogLevelError, LogHandler: func(centrifuge.LogEntry) {}}
	if m == modeDictionary {
		proto := centrifuge.ProtocolTypeJSON
		if useProtobuf {
			proto = centrifuge.ProtocolTypeProtobuf
		}
		engine = dictionaryengine.New(dictionaryengine.Options{
			Dictionaries:   map[dictionaryengine.Key][]byte{{Protocol: proto}: casinoDictionary(proto)},
			FrameCacheSize: 4096,
		})
		cfg.DictionaryCompression = engine
	}
	node, err := centrifuge.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "player42"}}, nil
	})
	node.OnConnect(func(c *centrifuge.Client) {
		c.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		Compression: m == modeDeflate,
		// CompressionLevel defaults to 0, which is flate.NoCompression: the
		// connection negotiates permessage-deflate and then stores rather than
		// compresses. Level 6 matches what dictionary compression uses, so the two
		// are compared at the same setting.
		CompressionLevel: 6,
	}))
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
	ccfg := centrifugego.Config{NetDialContext: dial, EnableCompression: m == modeDeflate}
	var client *centrifugego.Client
	if useProtobuf {
		client = centrifugego.NewProtobufClient(url, ccfg)
	} else {
		client = centrifugego.NewJsonClient(url, ccfg)
	}
	defer client.Close()

	connected := make(chan struct{})
	var once sync.Once
	client.OnConnected(func(centrifugego.ConnectedEvent) { once.Do(func() { close(connected) }) })
	if err := client.Connect(); err != nil {
		log.Fatal(err)
	}
	<-connected

	streams := sessionStreams()
	var mu sync.Mutex
	var received int
	var payload int64
	var mismatch int
	expected := map[string][][]byte{}
	seen := map[string]int{}
	wake := make(chan struct{}, 1)

	total := 0
	for _, s := range streams {
		total += s.count
		sub, err := client.NewSubscription(s.channel)
		if err != nil {
			log.Fatal(err)
		}
		ch := s.channel
		sub.OnPublication(func(e centrifugego.PublicationEvent) {
			mu.Lock()
			idx := seen[ch]
			if idx < len(expected[ch]) && string(expected[ch][idx]) != string(e.Data) {
				mismatch++
			}
			seen[ch] = idx + 1
			received++
			payload += int64(len(e.Data))
			mu.Unlock()
			select {
			case wake <- struct{}{}:
			default:
			}
		})
		done := make(chan struct{})
		var so sync.Once
		sub.OnSubscribed(func(centrifugego.SubscribedEvent) { so.Do(func() { close(done) }) })
		if err := sub.Subscribe(); err != nil {
			log.Fatal(err)
		}
		<-done
	}

	// Interleave the streams the way they would actually arrive.
	rng := rand.New(rand.NewSource(7))
	type pending struct {
		s    stream
		left int
	}
	queue := make([]pending, 0, len(streams))
	for _, s := range streams {
		queue = append(queue, pending{s: s, left: s.count})
	}
	idx := map[string]int{}
	for {
		anyLeft := false
		for qi := range queue {
			if queue[qi].left == 0 {
				continue
			}
			anyLeft = true
			s := queue[qi].s
			data := s.gen(idx[s.channel], rng)
			idx[s.channel]++
			mu.Lock()
			expected[s.channel] = append(expected[s.channel], data)
			mu.Unlock()
			if _, err := node.Publish(s.channel, data); err != nil {
				log.Fatal(err)
			}
			queue[qi].left--
		}
		if !anyLeft {
			break
		}
	}

	deadline := time.After(120 * time.Second)
	for {
		mu.Lock()
		got := received
		mu.Unlock()
		if got >= total {
			break
		}
		select {
		case <-wake:
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			log.Fatalf("timeout: got %d of %d", got, total)
		}
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	bad := mismatch
	mu.Unlock()
	if bad != 0 {
		log.Fatalf("CORRUPTION: %d payload mismatches", bad)
	}

	return outcome{
		wireBytes:    atomic.LoadInt64(&readBytes),
		payloadBytes: payload,
		messages:     received,
		stats:        engineStats(engine),
		clientStats:  client.CompressionStats(),
	}
}

// runFanout models what actually happens in production: many players watching
// the same markets. It reports aggregate egress and how often the shared frame
// cache spared the server a compression.
func runFanout(m mode, players, warmupRounds, measuredRounds int) (int64, dictionaryengine.Stats) {
	var engine *dictionaryengine.Engine
	cfg := centrifuge.Config{LogLevel: centrifuge.LogLevelError, LogHandler: func(centrifuge.LogEntry) {}}
	if m == modeDictionary {
		engine = dictionaryengine.New(dictionaryengine.Options{
			Dictionaries: map[dictionaryengine.Key][]byte{
				{Protocol: centrifuge.ProtocolTypeJSON}: casinoDictionary(centrifuge.ProtocolTypeJSON),
			},
			FrameCacheSize: 4096,
		})
		cfg.DictionaryCompression = engine
	}
	node, err := centrifuge.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{Credentials: &centrifuge.Credentials{UserID: "p"}}, nil
	})
	node.OnConnect(func(c *centrifuge.Client) {
		c.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		Compression: m == modeDeflate,
		// CompressionLevel defaults to 0, which is flate.NoCompression: the
		// connection negotiates permessage-deflate and then stores rather than
		// compresses. Level 6 matches what dictionary compression uses, so the two
		// are compared at the same setting.
		CompressionLevel: 6,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/connection/websocket"

	// Shared high fan-out channels, exactly the case the frame cache exists for.
	shared := []stream{
		{"odds:football:major", 0, oddsUpdate},
		{"jackpot:global", 0, jackpotTick},
	}

	var readBytes int64
	var wg sync.WaitGroup
	var got int64
	clients := make([]*centrifugego.Client, 0, players)
	for p := 0; p < players; p++ {
		dial := func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := net.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			return &countingConn{Conn: c, read: &readBytes}, nil
		}
		cl := centrifugego.NewJsonClient(url, centrifugego.Config{
			NetDialContext: dial, EnableCompression: m == modeDeflate,
		})
		clients = append(clients, cl)
		connected := make(chan struct{})
		var once sync.Once
		cl.OnConnected(func(centrifugego.ConnectedEvent) { once.Do(func() { close(connected) }) })
		if err := cl.Connect(); err != nil {
			log.Fatal(err)
		}
		<-connected
		for _, s := range shared {
			sub, err := cl.NewSubscription(s.channel)
			if err != nil {
				log.Fatal(err)
			}
			sub.OnPublication(func(centrifugego.PublicationEvent) { atomic.AddInt64(&got, 1) })
			done := make(chan struct{})
			var so sync.Once
			sub.OnSubscribed(func(centrifugego.SubscribedEvent) { so.Do(func() { close(done) }) })
			if err := sub.Subscribe(); err != nil {
				log.Fatal(err)
			}
			<-done
		}
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	wg.Wait()

	rng := rand.New(rand.NewSource(11))
	idx := map[string]int{}
	// Publishing is paced. Firing everything as fast as possible would make every
	// connection fall behind and batch heavily, which measures a burst rather
	// than the steady state a live market actually produces.
	publish := func(rounds int) {
		for i := 0; i < rounds; i++ {
			for _, s := range shared {
				if _, err := node.Publish(s.channel, s.gen(idx[s.channel], rng)); err != nil {
					log.Fatal(err)
				}
				idx[s.channel]++
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	waitFor := func(want int64) {
		deadline := time.After(180 * time.Second)
		for atomic.LoadInt64(&got) < want {
			select {
			case <-time.After(25 * time.Millisecond):
			case <-deadline:
				log.Fatalf("fanout timeout: %d of %d", atomic.LoadInt64(&got), want)
			}
		}
	}

	perRound := int64(len(shared) * players)
	// Warm up so the dictionary is built and shipped, then measure steady state.
	publish(warmupRounds)
	waitFor(perRound * int64(warmupRounds))
	time.Sleep(300 * time.Millisecond)
	warmBytes := atomic.LoadInt64(&readBytes)
	hits0, comp0 := engineStats(engine).FrameCacheHits, engineStats(engine).FrameCompressions

	publish(measuredRounds)
	waitFor(perRound * int64(warmupRounds+measuredRounds))
	time.Sleep(300 * time.Millisecond)

	st := engineStats(engine)
	st.FrameCacheHits -= hits0
	st.FrameCompressions -= comp0
	return atomic.LoadInt64(&readBytes) - warmBytes, st
}

// engineStats returns stats for the engine, or a zero value when
// compression is not enabled for this run.
func engineStats(e *dictionaryengine.Engine) dictionaryengine.Stats {
	if e == nil {
		return dictionaryengine.Stats{}
	}
	return e.Stats()
}

func main() {
	const concurrentPlayers = 50000
	const sessionMinutes = 10.0
	const hoursPerMonth = 730.0
	const egressPerGB = 0.05 // USD, stated assumption

	for _, proto := range []struct {
		name string
		pb   bool
	}{{"JSON", false}, {"Protobuf", true}} {
		fmt.Printf("\n================ %s protocol ================\n", proto.name)
		off := run(modeOff, proto.pb)
		def := run(modeDeflate, proto.pb)
		dic := run(modeDictionary, proto.pb)

		fmt.Printf("one player, %.0f minute session: %d messages, %d B of payload\n\n",
			sessionMinutes, dic.messages, dic.payloadBytes)
		fmt.Printf("  %-22s %12s %12s %10s\n", "strategy", "wire bytes", "per message", "vs none")
		for _, r := range []struct {
			m mode
			o outcome
		}{{modeOff, off}, {modeDeflate, def}, {modeDictionary, dic}} {
			fmt.Printf("  %-22s %11dB %11.1fB %9.2fx\n", r.m.String(), r.o.wireBytes,
				float64(r.o.wireBytes)/float64(r.o.messages),
				float64(off.wireBytes)/float64(r.o.wireBytes))
		}

		// Fleet projection.
		perPlayerHour := float64(dic.wireBytes) * (60.0 / sessionMinutes)
		baseHour := float64(def.wireBytes) * (60.0 / sessionMinutes)
		gbMonth := func(bytesPerPlayerHour float64) float64 {
			return bytesPerPlayerHour * concurrentPlayers * hoursPerMonth / (1 << 30)
		}
		baseGB, dictGB := gbMonth(baseHour), gbMonth(perPlayerHour)
		fmt.Printf("\n  fleet projection at %d concurrent players, %.0f h/month:\n", concurrentPlayers, hoursPerMonth)
		fmt.Printf("    permessage-deflate ... %9.0f GB/month   ~$%.0f at $%.2f/GB\n", baseGB, baseGB*egressPerGB, egressPerGB)
		fmt.Printf("    dictionary ........... %9.0f GB/month   ~$%.0f\n", dictGB, dictGB*egressPerGB)
		fmt.Printf("    saved ................ %9.0f GB/month   ~$%.0f  (%.0f%%)\n",
			baseGB-dictGB, (baseGB-dictGB)*egressPerGB, 100*(1-dictGB/baseGB))

		s := dic.stats
		dictSize, dictWire, dictID := 0, 0, "-"
		if len(s.Dictionaries) > 0 {
			d0 := s.Dictionaries[0]
			dictSize, dictWire, dictID = d0.Size, d0.WireSize, d0.ID
		}
		fmt.Printf("\n  server cost: %d dictionary(s), %d B (id %s, %d B to deliver), %d frames compressed, %d from cache\n",
			len(s.Dictionaries), dictSize, dictID, dictWire, s.FrameCompressions, s.FrameCacheHits)

		// Accuracy check: what the client believes it saved, against what the
		// socket actually carried.
		cs := dic.clientStats
		socketSaved := off.wireBytes - dic.wireBytes
		fmt.Printf("\n  client-reported: active=%v %d frames, %d B in -> %d B out, dict %d B\n",
			cs.Active, cs.Frames, cs.BytesReceived, cs.BytesDecompressed, cs.DictionaryBytes)
		fmt.Printf("    client thinks it saved %d B (ratio %.2fx)\n", cs.BytesSaved(), cs.Ratio())
		fmt.Printf("    socket actually saved  %d B vs no compression\n", socketSaved)
		if socketSaved != 0 {
			fmt.Printf("    client estimate is %.1f%% of the real socket saving\n",
				100*float64(cs.BytesSaved())/float64(socketSaved))
		}
	}
	// Fan-out: many players on the same markets.
	const players = 120
	const warmupRounds = 220
	const measuredRounds = 400
	fmt.Printf("\n================ fan-out: %d players on shared markets ================\n", players)
	defBytes, _ := runFanout(modeDeflate, players, warmupRounds, measuredRounds)
	dicBytes, dicStats := runFanout(modeDictionary, players, warmupRounds, measuredRounds)
	fmt.Printf("  permessage-deflate ... %10dB egress\n", defBytes)
	fmt.Printf("  dictionary ........... %10dB egress   %.2fx less\n", dicBytes, float64(defBytes)/float64(dicBytes))
	fmt.Printf("  server work: %d frames compressed, %d served from the shared cache (%.0fx less compression work)\n",
		dicStats.FrameCompressions, dicStats.FrameCacheHits,
		float64(dicStats.FrameCompressions+dicStats.FrameCacheHits)/float64(max64(dicStats.FrameCompressions, 1)))

	fmt.Println("\nBytes counted at the client socket, including WebSocket framing.")
	fmt.Println("Every payload is verified against what was published; any mismatch aborts.")
	fmt.Printf("Egress price of $%.2f/GB is an assumption - substitute your own.\n", egressPerGB)
}
