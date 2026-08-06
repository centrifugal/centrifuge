package centrifuge

import (
	"testing"

	"github.com/centrifugal/protocol"
)

// A cold client must carry enough traffic to justify the structure dictionary
// before it gets one, so the dictionary's own size sets how long it waits with
// no compression at all. Smaller is sooner but weaker - this is the trade.
func TestStudyStructureSizeTradeoff(t *testing.T) {
	skipUnlessStudy(t)
	full := protocol.StructureDictionary
	shapes := map[string][]byte{
		"chat": []byte(`{"push":{"channel":"room:42","pub":{"data":{"id":"m-90183","user":"alice","text":"see you at six","createdAt":"2026-08-06T09:14:22Z"},"offset":9013}}}`),
		"odds": []byte(`{"push":{"channel":"odds:board","pub":{"data":{"eventId":"evt-100317","market":"1x2","home":"Arsenal","away":"Chelsea","odds":{"h":2.15,"d":3.4,"a":3.05},"ts":1785920062114}}}}`),
	}
	for _, size := range []int{256, 512, 768, 1186} {
		dict := full
		if size < len(full) {
			// Keep the tail: the envelope sits at the front, but DEFLATE matches
			// nearest the end most cheaply, so trimming from the front is wrong.
			// The default is written front-loaded with envelope, so trim the tail.
			dict = full[:size]
		}
		e := NewDictionaryCompressionEngine(DictionaryCompressionConfig{StructureDictionary: dict})
		codec := e.structureCodec()
		cost := e.structureFrameCost(protocol.TypeJSON)
		var raw, out int
		for _, f := range shapes {
			raw += len(f)
			out += len(codec.Compress(nil, f))
		}
		ratio := float64(raw) / float64(out)
		saving := 1 - float64(out)/float64(raw)
		// Bytes a cold connection must carry before this is worth sending.
		threshold := float64(cost) * shipMargin / structureRatio
		t.Logf("%4d B dictionary | delivery %4d B | ratio %.2fx (saves %.0f%%) | cold client waits %.1f KB",
			len(dict), cost, ratio, saving*100, threshold/1024)
	}
}
