package centrifuge

// The agreed design, measured end to end over a connection's whole life.
//
//	- structure dictionary is cached client-side, so a returning client holds it
//	  already and it activates immediately for the cost of an id
//	- channel dictionaries are never cached: they are per node, so a cached copy
//	  would rarely match, and they carry other users' message fragments
//	- activation frames are compressed against whatever dictionary is already held
//	- break-even margin 6, so a connection that ends early is never made worse
//	- extra slots only when the writer is batching fewer than 4 messages per frame
//
// A cold client is one connecting for the very first time; a warm client has
// connected before and holds the structure dictionary. In any real fleet the
// overwhelming majority of connections are warm.

import (
	"fmt"
	"math"
	"testing"

	"github.com/centrifugal/protocol"
)

type finalPolicy struct {
	name string
	// structHeld means the client already has the structure dictionary, so it
	// applies from the first frame for the price of announcing an id.
	structHeld bool
	// channelDict is false for connections whose channels are not opted in - they
	// never get anything beyond the structure dictionary.
	channelDict bool
	// legacy reproduces what ships today: no structure stage, uncompressed
	// activation frame, margin 1.5.
	legacy bool
}

// idOnlyFrameCost is what it costs to tell a client "use the dictionary you
// already have", measured on the real frame.
func idOnlyFrameCost(t *testing.T) float64 {
	d := &protocol.Dictionary{Id: protocol.StructureDictionaryID}
	data, err := protocol.DefaultJsonReplyEncoder.Encode(
		&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
	if err != nil {
		t.Fatal(err)
	}
	enc := protocol.GetDataEncoder(protocol.TypeJSON)
	defer protocol.PutDataEncoder(protocol.TypeJSON, enc)
	_ = enc.Encode(data)
	return float64(len(enc.Finish()))
}

func finalLifetime(s stageSizes, frames int, p finalPolicy, idCost float64) float64 {
	margin := 6.0
	if p.legacy {
		margin = 1.5
	}
	var wire, carried float64

	// stage: 0 nothing, 1 structure, 2 channel dictionary, 3 extra slots
	stage := 0
	if p.structHeld && !p.legacy {
		// Announced by id on the first write, then active for the whole life.
		wire += idCost
		stage = 1
	}

	for f := 0; f < frames; f++ {
		switch {
		case stage == 0 && !p.legacy:
			// A cold client only pays for the structure dictionary when no channel
			// dictionary is coming - otherwise it would be superseded a frame later.
			if !p.channelDict {
				if carried*(s.raw-s.structOnly)/s.raw >= s.costStruct*margin {
					wire += s.costStruct
					stage = 1
				}
			} else if carried*(s.raw-s.pinned)/s.raw >= s.costPinned*margin {
				wire += s.costPinned
				stage = 2
			}
		case stage == 0 && p.legacy:
			if carried*(s.raw-s.pinned)/s.raw >= s.costPinned*4.2*margin {
				wire += s.costPinned * 4.2 // uncompressed activation frame
				stage = 2
			}
		case stage == 1 && p.channelDict:
			// The gain is measured against what this connection is ALREADY paying,
			// not against uncompressed. A connection running on the structure
			// dictionary only gains structOnly-pinned per frame by upgrading, and
			// testing against raw would ship a dictionary that never pays back.
			if carried*(s.structOnly-s.pinned)/s.raw >= s.costPinned*margin {
				wire += s.costPinned
				stage = 2
			}
		case stage == 2 && p.channelDict && !p.legacy:
			if s.batch < 4 && carried*(s.pinned-s.slots)/s.raw >= s.costSlot*4*margin {
				wire += s.costSlot * 4
				stage = 3
			}
		}

		switch stage {
		case 0:
			wire += s.raw
		case 1:
			wire += s.structOnly
		case 2:
			wire += s.pinned
		case 3:
			wire += s.slots
		}
		carried += s.raw
	}
	return wire
}

func TestStudyFinalDesign(t *testing.T) {
	skipUnlessStudy(t)
	idCost := idOnlyFrameCost(t)
	t.Logf("announcing a cached dictionary by id costs %.0f B", idCost)

	policies := []finalPolicy{
		{name: "today (shipped)", channelDict: true, legacy: true},
		{name: "cold client", channelDict: true},
		{name: "warm client", structHeld: true, channelDict: true},
		{name: "warm, no channel dict", structHeld: true},
	}

	rates := []float64{0.2, 1, 5, 20}
	durations := []struct {
		name string
		secs float64
	}{{"30s", 30}, {"5min", 300}, {"1h", 3600}}

	for _, batch := range []int{1, 4, 16} {
		s := measureStages(t, batch, 5)
		t.Logf("")
		t.Logf("=== batch %d | raw %.0f B/frame | permessage-deflate %.2fx ===",
			batch, s.raw, s.raw/s.plain)
		t.Logf("  %-5s %-6s %14s %13s %13s %15s", "rate", "dur", "today", "cold", "warm", "warm/no chan")
		for _, r := range rates {
			for _, d := range durations {
				frames := int(math.Ceil(r * d.secs / float64(batch)))
				if frames < 1 {
					continue
				}
				none := float64(frames) * s.raw
				out := make([]string, len(policies))
				for i, p := range policies {
					out[i] = fmt.Sprintf("%.2fx", none/finalLifetime(s, frames, p, idCost))
				}
				t.Logf("  %-5.1f %-6s %14s %13s %13s %15s", r, d.name, out[0], out[1], out[2], out[3])
			}
		}
	}
}
