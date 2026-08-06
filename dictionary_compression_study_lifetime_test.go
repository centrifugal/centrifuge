package centrifuge

// A connection's whole life, in bytes.
//
// Steady-state ratios flatter the feature: they ignore the traffic spent
// delivering dictionaries, and they ignore the period before one arrives. This
// simulates the full lifetime - every stage, every delivery cost - so a short
// or quiet connection can come out NEGATIVE, which is the case that decides
// whether the design is safe to enable everywhere.

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/centrifugal/protocol"
)

// stageSizes holds the average wire cost of one frame under each dictionary
// state, measured by actually compressing representative frames.
type stageSizes struct {
	batch      int
	raw        float64
	plain      float64 // permessage-deflate equivalent: deflate, no dictionary
	structOnly float64
	pinned     float64
	slots      float64

	// Delivery cost of each activation frame, as it would appear on the wire.
	costStruct float64 // no dictionary held yet, so it cannot be compressed
	costPinned float64 // compressed against the structure dictionary
	costSlot   float64 // compressed against whatever is already held
}

func measureStages(t *testing.T, batch int, subsPerConn int) stageSizes {
	rng := rand.New(rand.NewSource(77))
	const channels = 40
	engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		DictionarySize:       4096,
		MinSamples:           64,
		MaxChannelDictionaries:      channels,
		UseChannelDictionary: func(string) bool { return true },
	})
	type lchan struct {
		name   string
		shape  shapeGen
		rate   int
		dict   *protocol.DeflateFrameCodec
		egress int64
	}
	chans := make([]*lchan, channels)
	for i := range chans {
		c := &lchan{name: fmt.Sprintf("ch:%02d", i), shape: studyShapes[i%len(studyShapes)], rate: 1 + rng.Intn(10)}
		for j := 0; j < 80; j++ {
			engine.observe(c.name, protocol.TypeJSON, encodePush(c.name, c.shape.gen(c.name, j, rng), uint64(j)), 50)
		}
		codec, _ := engine.current(c.name, protocol.TypeJSON)
		if codec == nil {
			t.Fatal("no dictionary")
		}
		c.dict = codec
		c.egress = int64(c.rate) * int64(c.shape.size) * 50
		chans[i] = c
	}

	structDict := protocol.StructureFrameCodec()
	plain := protocol.NewDeflateFrameCodec("none", nil)

	var s stageSizes
	s.batch = batch
	const conns, framesPer = 200, 200
	var n float64
	for c := 0; c < conns; c++ {
		crng := rand.New(rand.NewSource(int64(500 + c)))
		perm := crng.Perm(len(chans))[:subsPerConn]
		subs := make([]*lchan, subsPerConn)
		for i, p := range perm {
			subs[i] = chans[p]
		}
		slots := append([]*lchan(nil), subs...)
		sort.Slice(slots, func(i, j int) bool { return slots[i].egress > slots[j].egress })
		held := map[string]*lchan{}
		for _, x := range slots {
			held[x.name] = x
		}

		var weights []int
		total := 0
		for _, x := range subs {
			total += x.rate
			weights = append(weights, total)
		}
		pick := func() *lchan {
			r := crng.Intn(total)
			for i, w := range weights {
				if r < w {
					return subs[i]
				}
			}
			return subs[len(subs)-1]
		}

		for f := 0; f < framesPer; f++ {
			var frame []byte
			counts := map[string]int{}
			for b := 0; b < batch; b++ {
				ch := pick()
				counts[ch.name]++
				frame = append(frame, encodePush(ch.name, ch.shape.gen(ch.name, crng.Intn(100000), crng), uint64(f))...)
			}
			best, bestN := "", 0
			for name, cnt := range counts {
				if cnt > bestN {
					best, bestN = name, cnt
				}
			}
			s.raw += float64(len(frame))
			s.plain += float64(len(plain.Compress(nil, frame)))
			s.structOnly += float64(len(structDict.Compress(nil, frame)))
			s.pinned += float64(len(slots[0].dict.Compress(nil, frame)))
			s.slots += float64(len(held[best].dict.Compress(nil, frame)))
			n++
		}
	}
	s.raw /= n
	s.plain /= n
	s.structOnly /= n
	s.pinned /= n
	s.slots /= n

	// Delivery costs, measured on the real activation frames.
	build := func(dict []byte) []byte {
		d := &protocol.Dictionary{Id: "x", DataB64: base64Std(dict)}
		data, err := protocol.DefaultJsonReplyEncoder.Encode(
			&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
		if err != nil {
			t.Fatal(err)
		}
		enc := protocol.GetDataEncoder(protocol.TypeJSON)
		defer protocol.PutDataEncoder(protocol.TypeJSON, enc)
		_ = enc.Encode(data)
		return append([]byte(nil), enc.Finish()...)
	}
	structFrame := build(protocol.StructureDictionary)
	chanFrame := build(chans[0].dict.Dict())
	s.costStruct = float64(len(plain.Compress(nil, structFrame))) // best available: plain deflate
	s.costPinned = float64(len(structDict.Compress(nil, chanFrame)))
	s.costSlot = s.costPinned
	return s
}

// lifetime returns total wire bytes for a connection under a policy.
type policyKind int

const (
	polNone policyKind = iota
	polDeflate
	polToday    // ships today: pinned dictionary only, activation frame uncompressed
	polProposed // structure slot + pinned + extra slots when batching is low
	polCached   // same, but every dictionary already held from a previous connection
)

var studyMargin = 1.5

func lifetime(s stageSizes, frames int, kind policyKind) float64 {
	switch kind {
	case polNone:
		return float64(frames) * s.raw
	case polDeflate:
		return float64(frames) * s.plain
	}

	// Break-even: ship a dictionary once the traffic already carried would have
	// saved more than the delivery costs, with margin. Measured against the real
	// delivery cost rather than the raw dictionary length.
	margin := studyMargin
	var wire, carried float64
	stage := 0 // 0 = nothing, 1 = structure, 2 = pinned, 3 = slots
	if kind == polCached {
		stage = 3
		if s.batch >= 4 {
			stage = 2
		}
	}
	if kind == polToday {
		stage = 0
	}

	for f := 0; f < frames; f++ {
		// Promote before writing this frame, exactly as activationFrame does.
		switch {
		case kind == polProposed && stage == 0:
			// The structure dictionary is cheap and universal, but it is not free
			// when it has to be sent, so it earns its way in like anything else.
			if carried*(1-s.structOnly/s.raw) >= s.costStruct*margin {
				wire += s.costStruct
				stage = 1
			}
		case kind == polProposed && stage == 1,
			kind == polToday && stage == 0:
			if carried*(1-s.pinned/s.raw) >= s.costPinned*margin {
				if kind == polToday {
					// Uncompressed activation frame, which is what ships today.
					wire += s.costPinned * 4.2
					stage = 2
				} else {
					wire += s.costPinned
					stage = 2
				}
			}
		case kind == polProposed && stage == 2:
			// Extra slots only where the measurement says they pay: batching above
			// ~4 messages per frame returns nothing, so they are never bought.
			if s.batch < 4 && carried*(1-s.slots/s.raw) >= s.costSlot*4*margin {
				wire += s.costSlot * 4
				stage = 3
			}
		}

		var sz float64
		switch stage {
		case 0:
			sz = s.raw
		case 1:
			sz = s.structOnly
		case 2:
			sz = s.pinned
		case 3:
			sz = s.slots
		}
		wire += sz
		carried += s.raw
	}
	return wire
}

// How large must the break-even margin be before a connection can never lose?
//
// Shipping a dictionary is a bet that the future resembles the past: nothing has
// actually been saved yet when it goes out, so a connection that ends right
// afterwards pays the delivery cost for nothing. The margin is how much evidence
// is demanded before taking the bet.
func TestStudyBreakEvenMargin(t *testing.T) {
	skipUnlessStudy(t)
	rates := []float64{0.2, 1, 5, 20}
	durations := []float64{30, 300, 3600}
	defer func() { studyMargin = 1.5 }()

	for _, batch := range []int{1, 4} {
		s := measureStages(t, batch, 5)
		t.Logf("=== batch %d ===", batch)
		t.Logf("  %-8s %10s %12s %12s", "margin", "worst cell", "1 msg/s, 5min", "5 msg/s, 1h")
		for _, m := range []float64{1.5, 3, 6, 12, 24} {
			studyMargin = m
			worst := math.Inf(1)
			for _, r := range rates {
				for _, d := range durations {
					frames := int(math.Ceil(r * d / float64(batch)))
					if frames < 1 {
						continue
					}
					ratio := lifetime(s, frames, polNone) / lifetime(s, frames, polProposed)
					if ratio < worst {
						worst = ratio
					}
				}
			}
			f1 := int(math.Ceil(1 * 300 / float64(batch)))
			f2 := int(math.Ceil(5 * 3600 / float64(batch)))
			t.Logf("  %-8.1f %9.2fx %11.2fx %11.2fx", m,
				worst,
				lifetime(s, f1, polNone)/lifetime(s, f1, polProposed),
				lifetime(s, f2, polNone)/lifetime(s, f2, polProposed))
		}
	}
}

// Where exactly does each stage begin?
func TestStudyStageOnsets(t *testing.T) {
	skipUnlessStudy(t)
	defer func() { studyMargin = 1.5 }()
	s := measureStages(t, 1, 5)
	t.Logf("batch 1: raw %.0f B/frame | struct %.0f (%.2fx) | pinned %.0f (%.2fx) | slots %.0f (%.2fx)",
		s.raw, s.structOnly, s.raw/s.structOnly, s.pinned, s.raw/s.pinned, s.slots, s.raw/s.slots)
	t.Logf("delivery: structure %.0f B | channel dict %.0f B | 4 extra slots %.0f B",
		s.costStruct, s.costPinned, s.costSlot*4)
	for _, m := range []float64{1.5, 6} {
		studyMargin = m
		var carried float64
		onset := map[int]int{}
		stage := 0
		for f := 0; f < 20000; f++ {
			if stage == 0 && carried*(1-s.structOnly/s.raw) >= s.costStruct*m {
				stage = 1
				onset[1] = f
			} else if stage == 1 && carried*(1-s.pinned/s.raw) >= s.costPinned*m {
				stage = 2
				onset[2] = f
			} else if stage == 2 && carried*(1-s.slots/s.raw) >= s.costSlot*4*m {
				stage = 3
				onset[3] = f
			}
			carried += s.raw
		}
		t.Logf("margin %.1f: structure at frame %d, channel dict at frame %d, extra slots at frame %d",
			m, onset[1], onset[2], onset[3])
		for _, r := range []float64{0.2, 1, 5, 20} {
			t.Logf("   at %.1f msg/s: structure %.0fs, channel dict %.0fs, slots %.0fs",
				r, float64(onset[1])/r, float64(onset[2])/r, float64(onset[3])/r)
		}
	}
}

func TestStudyConnectionLifetime(t *testing.T) {
	skipUnlessStudy(t)
	studyMargin = 6
	defer func() { studyMargin = 1.5 }()
	rates := []float64{0.2, 1, 5, 20}
	durations := []struct {
		name string
		secs float64
	}{{"30s", 30}, {"5min", 300}, {"1h", 3600}}

	for _, batch := range []int{1, 4, 16} {
		s := measureStages(t, batch, 5)
		t.Logf("=== batch %d: raw %.0f B/frame | deflate %.0f | struct %.0f | pinned %.0f | slots %.0f | delivery: struct %.0f B, dict %.0f B ===",
			batch, s.raw, s.plain, s.structOnly, s.pinned, s.slots, s.costStruct, s.costPinned)
		t.Logf("  %-6s %-6s %8s %10s %10s %10s %10s", "rate", "dur", "frames", "vs none", "deflate", "today", "proposed")
		for _, r := range rates {
			for _, d := range durations {
				msgs := r * d.secs
				frames := int(math.Ceil(msgs / float64(batch)))
				if frames < 1 {
					continue
				}
				none := lifetime(s, frames, polNone)
				def := lifetime(s, frames, polDeflate)
				today := lifetime(s, frames, polToday)
				prop := lifetime(s, frames, polProposed)
				cached := lifetime(s, frames, polCached)
				t.Logf("  %-6.1f %-6s %8d %10s %9.2fx %9.2fx %9.2fx  (cached %.2fx)",
					r, d.name, frames, "1.00x", none/def, none/today, none/prop, none/cached)
			}
		}
	}
}
