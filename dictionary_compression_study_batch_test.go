package centrifuge

// How do dictionary slots behave once WriteDelay batches several publications
// into one frame?
//
// This is the gap in the earlier study, and it matters in both directions:
//
//   - Bandwidth: a batched frame mixes channels, so only one dictionary can
//     apply. DEFLATE also finds cross-message redundancy inside the frame on its
//     own - exactly the redundancy a dictionary supplies - so the dictionary's
//     marginal value should fall as batches grow.
//   - CPU: batched frames are per-connection unique, so the shared frame cache
//     stops hitting regardless of dictionary policy. Slots should therefore
//     neither help nor hurt CPU here.
//
// Both are measured rather than assumed.

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/centrifugal/protocol"
)

type batchChan struct {
	name   string
	shape  shapeGen
	rate   int
	dict   *protocol.DeflateFrameCodec
	egress int64
	subs   int
}

// dominantDict returns the dictionary of whichever channel contributed the most
// messages to this frame, provided that channel holds one of the connection's
// first n slots. Otherwise it falls back to the best slot held - which is what
// today's pinned policy always does - and finally to the structure dictionary.
func dominantDict(batch []*batchChan, slots []*batchChan, n int, fallback *protocol.DeflateFrameCodec) *protocol.DeflateFrameCodec {
	if n > len(slots) {
		n = len(slots)
	}
	held := make(map[string]*batchChan, n)
	for _, s := range slots[:n] {
		held[s.name] = s
	}
	counts := map[string]int{}
	for _, c := range batch {
		counts[c.name]++
	}
	best, bestN := "", 0
	for name, c := range counts {
		if c > bestN {
			best, bestN = name, c
		}
	}
	if ch, ok := held[best]; ok {
		return ch.dict
	}
	if n > 0 {
		return slots[0].dict
	}
	return fallback
}

func TestStudyBatchedSlots(t *testing.T) {
	skipUnlessStudy(t)
	const (
		channels    = 40
		conns       = 300
		subsPerConn = 5
		framesPer   = 400
	)
	rng := rand.New(rand.NewSource(23))

	chans := make([]*batchChan, channels)
	engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		DictionarySize:       4096,
		MinSamples:           64,
		MaxChannelDictionaries:      channels,
		UseChannelDictionary: func(string) bool { return true },
	})
	for i := range chans {
		c := &batchChan{
			name:  fmt.Sprintf("ch:%02d", i),
			shape: studyShapes[i%len(studyShapes)],
			rate:  1 + rng.Intn(10),
			subs:  conns * subsPerConn / channels,
		}
		for j := 0; j < 80; j++ {
			engine.observe(c.name, protocol.TypeJSON,
				encodePush(c.name, c.shape.gen(c.name, j, rng), uint64(j)), c.subs)
		}
		codec, _ := engine.current(c.name, protocol.TypeJSON)
		if codec == nil {
			t.Fatalf("no dictionary for %s", c.name)
		}
		c.dict = codec
		c.egress = int64(c.rate) * int64(c.shape.size) * int64(c.subs)
		chans[i] = c
	}

	structDict := protocol.StructureFrameCodec()
	plain := protocol.NewDeflateFrameCodec("none", nil)

	type policy struct {
		name string
		pick func(batch []*batchChan, slots []*batchChan) *protocol.DeflateFrameCodec
	}
	policies := []policy{
		{"no compression", nil},
		{"plain deflate", func([]*batchChan, []*batchChan) *protocol.DeflateFrameCodec { return plain }},
		{"structure only", func([]*batchChan, []*batchChan) *protocol.DeflateFrameCodec { return structDict }},
		{"pinned (today)", func(_ []*batchChan, slots []*batchChan) *protocol.DeflateFrameCodec { return slots[0].dict }},
		{"slots=3, dominant", func(b []*batchChan, s []*batchChan) *protocol.DeflateFrameCodec {
			return dominantDict(b, s, 3, structDict)
		}},
		{"slots=5, dominant", func(b []*batchChan, s []*batchChan) *protocol.DeflateFrameCodec {
			return dominantDict(b, s, 5, structDict)
		}},
	}
	const pinnedIdx = 3

	for _, batchSize := range []int{1, 2, 4, 8, 16} {
		totals := make([]int64, len(policies))
		var raw int64
		var totalFrames int
		seen := map[string]bool{}

		for c := 0; c < conns; c++ {
			crng := rand.New(rand.NewSource(int64(1000 + c)))
			perm := crng.Perm(len(chans))[:subsPerConn]
			subs := make([]*batchChan, subsPerConn)
			for i, p := range perm {
				subs[i] = chans[p]
			}
			slots := append([]*batchChan(nil), subs...)
			sort.Slice(slots, func(i, j int) bool { return slots[i].egress > slots[j].egress })

			var weights []int
			total := 0
			for _, s := range subs {
				total += s.rate
				weights = append(weights, total)
			}
			pickChan := func() *batchChan {
				r := crng.Intn(total)
				for i, w := range weights {
					if r < w {
						return subs[i]
					}
				}
				return subs[len(subs)-1]
			}

			for f := 0; f < framesPer/batchSize; f++ {
				var frame []byte
				var batch []*batchChan
				for b := 0; b < batchSize; b++ {
					ch := pickChan()
					batch = append(batch, ch)
					frame = append(frame, encodePush(ch.name,
						ch.shape.gen(ch.name, crng.Intn(100000), crng), uint64(f))...)
				}
				raw += int64(len(frame))
				totalFrames++
				seen[string(frame)] = true
				for pi, p := range policies {
					if p.pick == nil {
						totals[pi] += int64(len(frame))
						continue
					}
					totals[pi] += int64(len(p.pick(batch, slots).Compress(nil, frame)))
				}
			}
		}

		t.Logf("--- batch %2d: %d frames, %.1f%% unique across connections (cache hit ceiling %.1f%%) ---",
			batchSize, totalFrames, 100*float64(len(seen))/float64(totalFrames),
			100*(1-float64(len(seen))/float64(totalFrames)))
		base := totals[0]
		for pi, p := range policies {
			t.Logf("  %-20s %11dB  %5.2fx  %+6.1f%% vs pinned",
				p.name, totals[pi], float64(base)/float64(totals[pi]),
				100*(float64(totals[pi])/float64(totals[pinnedIdx])-1))
		}
	}
}

// Studies are decision-support, not regression tests: they take minutes and
// their output is tables to read rather than assertions. Run them with
// CENTRIFUGE_STUDY=1.
func skipUnlessStudy(t *testing.T) {
	t.Helper()
	if os.Getenv("CENTRIFUGE_STUDY") == "" {
		t.Skip("set CENTRIFUGE_STUDY=1 to run compression studies")
	}
}
