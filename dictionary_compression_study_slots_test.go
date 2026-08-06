package centrifuge

// What would per-connection dictionary slots buy, and what would they cost?
//
// This is a study, not a test of shipped behaviour. It uses the real engine to
// build the dictionaries production would build, then evaluates two policies
// over the same traffic:
//
//	pinned  - what ships today: one dictionary per connection, from the busiest
//	          channel it subscribes to, used for every frame it receives.
//	slots   - the proposal: a connection holds up to N dictionaries and each
//	          frame is compressed against its own channel's, falling back to the
//	          best slot it does hold.
//
// Bandwidth is measured by actually compressing. Compression count is derived
// exactly, by counting distinct dictionaries among a channel's subscribers -
// which is what the shared frame cache collapses to.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/centrifugal/protocol"
)

// ---------------------------------------------------------------------------
// Traffic shapes
// ---------------------------------------------------------------------------

type shapeGen struct {
	name string
	size int // approximate payload bytes
	gen  func(ch string, i int, rng *rand.Rand) []byte
}

var studyShapes = []shapeGen{
	{"odds", 150, func(ch string, i int, rng *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"eventId":"evt-%06d","market":"%s","home":"%s","away":"%s","odds":{"h":%.2f,"d":%.2f,"a":%.2f},"suspended":%v,"ts":%d}`,
			100000+rng.Intn(400), []string{"1x2", "over_under", "both_score"}[rng.Intn(3)],
			studyTeams[rng.Intn(len(studyTeams))], studyTeams[rng.Intn(len(studyTeams))],
			float64(100+rng.Intn(900))/100, float64(200+rng.Intn(600))/100, float64(100+rng.Intn(900))/100,
			rng.Intn(20) == 0, 1785920062114+int64(i)))
	}},
	{"chat", 130, func(ch string, i int, rng *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"id":"m-%d","user":"%s","text":"%s","createdAt":"2026-08-06T09:%02d:%02dZ","edited":%v}`,
			900000+i, studyUsers[rng.Intn(len(studyUsers))], studyPhrases[rng.Intn(len(studyPhrases))],
			rng.Intn(60), rng.Intn(60), rng.Intn(10) == 0))
	}},
	{"telemetry", 120, func(ch string, i int, rng *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"deviceId":"dev-%04d","status":"%s","temp":%.1f,"battery":%d,"rssi":%d,"timestamp":%d}`,
			rng.Intn(9000), []string{"online", "degraded", "offline"}[rng.Intn(3)],
			float64(150+rng.Intn(200))/10, rng.Intn(100), -30-rng.Intn(70), 1785920062114+int64(i)))
	}},
	{"orders", 200, func(ch string, i int, rng *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"orderId":"ord-%08d","symbol":"%s","side":"%s","qty":%d,"price":%.2f,"status":"%s","filled":%d,"updatedAt":%d}`,
			10000000+i, studySymbols[rng.Intn(len(studySymbols))],
			[]string{"buy", "sell"}[rng.Intn(2)], 1+rng.Intn(500), float64(1000+rng.Intn(90000))/100,
			[]string{"new", "partial", "filled", "cancelled"}[rng.Intn(4)], rng.Intn(500), 1785920062114+int64(i)))
	}},
	{"presence", 110, func(ch string, i int, rng *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"user":"%s","action":"%s","room":"%s","clients":%d,"at":%d}`,
			studyUsers[rng.Intn(len(studyUsers))], []string{"join", "leave", "idle"}[rng.Intn(3)],
			ch, rng.Intn(400), 1785920062114+int64(i)))
	}},
}

var studyTeams = []string{"Arsenal", "Chelsea", "Liverpool", "Everton", "Fulham", "Brentford", "Wolves", "Burnley"}
var studyUsers = []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
var studyPhrases = []string{"see you at six", "on my way", "sounds good", "let me check that", "ok",
	"can you send the link", "will do", "not sure yet", "thanks a lot", "running late"}
var studySymbols = []string{"AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "GOOG", "NFLX"}

// encodePush builds the frame the server would actually write for one
// publication, so dictionaries are built from - and measured against - real
// protocol bytes rather than bare payloads.
func encodePush(channel string, data []byte, offset uint64) []byte {
	reply := &protocol.Reply{Push: &protocol.Push{
		Channel: channel,
		Pub:     &protocol.Publication{Data: data, Offset: offset},
	}}
	out, err := protocol.DefaultJsonReplyEncoder.Encode(reply)
	if err != nil {
		panic(err)
	}
	enc := protocol.GetDataEncoder(protocol.TypeJSON)
	defer protocol.PutDataEncoder(protocol.TypeJSON, enc)
	_ = enc.Encode(out)
	return append([]byte(nil), enc.Finish()...)
}

// ---------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------

type studyChannel struct {
	name    string
	shape   shapeGen
	rate    int // publications per interval, relative weight
	subs    []int
	dict    *protocol.DeflateFrameCodec
	egress  int64
	seqBase int
}

type studyScenario struct {
	name string
	// shapesPerChannel controls how alike channels are. 1 means every channel
	// carries the same kind of message; len(studyShapes) means each channel is a
	// different kind. This is the axis the whole proposal lives or dies on.
	distinctShapes int
	channels       int
	conns          int
	subsPerConn    int
}

func TestStudySlotDesign(t *testing.T) {
	skipUnlessStudy(t)
	scenarios := []studyScenario{
		{"homogeneous (all channels same kind)", 1, 40, 2000, 5},
		{"mixed (3 kinds)", 3, 40, 2000, 5},
		{"heterogeneous (5 kinds)", 5, 40, 2000, 5},
		{"heterogeneous, 12 subscriptions", 5, 40, 2000, 12},
		{"heterogeneous, 25 subscriptions", 5, 40, 2000, 25},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) { runStudy(t, sc) })
	}
}

func runStudy(t *testing.T, sc studyScenario) {
	rng := rand.New(rand.NewSource(11))

	// Build channels, each with its own shape and traffic rate. Rates are skewed
	// so there is a genuine "busiest channel" for the pinned policy to pick.
	chans := make([]*studyChannel, sc.channels)
	for i := range chans {
		chans[i] = &studyChannel{
			name:  fmt.Sprintf("ch:%02d", i),
			shape: studyShapes[i%sc.distinctShapes],
			rate:  1 + rng.Intn(10),
		}
	}

	// Subscriptions: each connection takes a random subset.
	for c := 0; c < sc.conns; c++ {
		perm := rng.Perm(len(chans))[:sc.subsPerConn]
		for _, ci := range perm {
			chans[ci].subs = append(chans[ci].subs, c)
		}
	}
	for _, ch := range chans {
		ch.egress = int64(ch.rate) * int64(ch.shape.size) * int64(len(ch.subs))
	}

	// Build a real dictionary per channel, from that channel's own traffic.
	engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		DictionarySize:       4096,
		MinSamples:           64,
		MaxChannelDictionaries:      len(chans),
		UseChannelDictionary: func(string) bool { return true },
	})
	for _, ch := range chans {
		for i := 0; i < 80; i++ {
			engine.observe(ch.name, protocol.TypeJSON,
				encodePush(ch.name, ch.shape.gen(ch.name, i, rng), uint64(i)), len(ch.subs))
		}
		codec, _ := engine.current(ch.name, protocol.TypeJSON)
		if codec == nil {
			t.Fatalf("no dictionary built for %s", ch.name)
		}
		ch.dict = codec
		ch.seqBase = 1000
	}

	byName := map[string]*studyChannel{}
	for _, ch := range chans {
		byName[ch.name] = ch
	}

	// Slot allocation per connection: top-N subscribed channels by egress. This
	// is what the engine would do, and it is also what the pinned policy does
	// with N=1, so the two are directly comparable.
	connSubs := make([][]*studyChannel, sc.conns)
	for _, ch := range chans {
		for _, c := range ch.subs {
			connSubs[c] = append(connSubs[c], ch)
		}
	}
	slotsFor := func(c int, n int) []*studyChannel {
		s := append([]*studyChannel(nil), connSubs[c]...)
		sort.Slice(s, func(i, j int) bool { return s[i].egress > s[j].egress })
		if len(s) > n {
			s = s[:n]
		}
		return s
	}

	structDict := protocol.StructureFrameCodec()

	// ---- bandwidth -------------------------------------------------------
	//
	// Compress a sample of each channel's traffic against (a) the dictionary a
	// connection would be pinned to and (b) the one it would select per frame,
	// weighted by how much traffic each connection actually receives.
	measure := func(slotCount int) (wire int64, raw int64) {
		mrng := rand.New(rand.NewSource(99))
		const framesPerChannel = 40
		for _, ch := range chans {
			frames := make([][]byte, framesPerChannel)
			for i := range frames {
				frames[i] = encodePush(ch.name, ch.shape.gen(ch.name, ch.seqBase+i, mrng), uint64(ch.seqBase+i))
			}
			for _, c := range ch.subs {
				slots := slotsFor(c, slotCount)
				// Which dictionary does this connection use for this channel?
				var use *protocol.DeflateFrameCodec
				for _, s := range slots {
					if s.name == ch.name {
						use = ch.dict
						break
					}
				}
				if use == nil && len(slots) > 0 {
					use = slots[0].dict // best slot it holds, cross-channel
				}
				if use == nil {
					use = structDict
				}
				for _, f := range frames {
					raw += int64(len(f))
					wire += int64(len(use.Compress(nil, f)))
				}
			}
		}
		return wire, raw
	}

	// ---- compression work -------------------------------------------------
	//
	// The shared frame cache collapses identical (dictionary, frame) pairs, so
	// the compressions a publication costs is the number of DISTINCT dictionaries
	// among that channel's subscribers.
	compressionsPer := func(slotCount int) float64 {
		var total, pubs float64
		for _, ch := range chans {
			seen := map[*protocol.DeflateFrameCodec]bool{}
			for _, c := range ch.subs {
				slots := slotsFor(c, slotCount)
				var use *protocol.DeflateFrameCodec
				for _, s := range slots {
					if s.name == ch.name {
						use = ch.dict
						break
					}
				}
				if use == nil && len(slots) > 0 {
					use = slots[0].dict
				}
				if use == nil {
					use = structDict
				}
				seen[use] = true
			}
			total += float64(len(seen))
			pubs++
		}
		return total / pubs
	}

	// ---- transfer cost ----------------------------------------------------
	dictTransfer := func(slotCount int) int64 {
		var total int64
		for c := 0; c < sc.conns; c++ {
			for _, s := range slotsFor(c, slotCount) {
				// JSON carries the dictionary base64-encoded.
				total += int64(len(s.dict.Dict()) * 4 / 3)
			}
		}
		return total
	}

	t.Logf("--- %s: %d channels, %d connections, %d subs each ---",
		sc.name, sc.channels, sc.conns, sc.subsPerConn)
	base, raw := measure(1)
	baseWork := compressionsPer(1)
	t.Logf("%-8s %12s %8s %14s %14s", "slots", "wire", "ratio", "compressions", "dict transfer")
	slotCounts := []int{1, 2, 3, 5}
	if sc.subsPerConn > 5 {
		slotCounts = append(slotCounts, 8, sc.subsPerConn)
	}
	for _, n := range slotCounts {
		wire, _ := measure(n)
		work := compressionsPer(n)
		t.Logf("%-8d %11dB %7.2fx %10.1f/pub %12dB   %+.1f%% bytes, %+.0f%% work",
			n, wire, float64(raw)/float64(wire), work, dictTransfer(n),
			100*(float64(wire)/float64(base)-1), 100*(work/baseWork-1))
	}
}
