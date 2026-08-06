package centrifuge

// What does it actually cost to deliver a dictionary, and how much of that is
// avoidable?
//
// Today a 4 KB dictionary travels as raw bytes, base64 encoded on JSON - 5.5 KB
// on the wire. But a dictionary is a concatenation of real message samples: it
// is ordinary text, and text compresses. Nothing currently exploits that.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/centrifugal/protocol"
)

func TestStudyDictionaryDeliveryCost(t *testing.T) {
	skipUnlessStudy(t)
	rng := rand.New(rand.NewSource(5))
	empty := protocol.NewDeflateFrameCodec("none", nil)
	structDict := protocol.StructureFrameCodec()

	for _, size := range []int{1024, 2048, 4096, 8192} {
		for _, shape := range studyShapes[:3] {
			engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
				DictionarySize:         size,
				MinSamples:             64,
				UseChannelDictionary:   func(string) bool { return true },
				MaxChannelDictionaries: 64,
			})
			ch := "study:" + shape.name
			for i := 0; i < 90; i++ {
				engine.observe(ch, protocol.TypeJSON,
					encodePush(ch, shape.gen(ch, i, rng), uint64(i)), 1)
			}
			codec, _ := engine.current(ch, protocol.TypeJSON)
			if codec == nil {
				t.Fatalf("no dictionary for %s", ch)
			}
			d := codec.Dict()

			// What ships today: raw bytes, base64 on JSON.
			today := len(d) * 4 / 3

			// Deflate the dictionary before encoding it.
			selfDeflated := len(empty.Compress(nil, d))

			// Deflate it against the structure dictionary the connection already
			// holds - it costs nothing extra, it is already installed.
			viaStruct := len(structDict.Compress(nil, d))

			t.Logf("%-10s %5d B dict | today(b64) %5d B | deflated %4d B -> b64 %4d B | vs struct dict %4d B -> b64 %4d B | saving %.1fx",
				shape.name, len(d), today,
				selfDeflated, selfDeflated*4/3,
				viaStruct, viaStruct*4/3,
				float64(today)/float64(viaStruct*4/3))
		}
	}
}

// Protobuf connections carry the dictionary raw, with no base64 expansion, so
// the saving there is purely from compression.
func TestStudyDictionaryDeliveryProtobuf(t *testing.T) {
	skipUnlessStudy(t)
	rng := rand.New(rand.NewSource(5))
	structDict := protocol.StructureFrameCodec()
	engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
		DictionarySize:         4096,
		MinSamples:             64,
		UseChannelDictionary:   func(string) bool { return true },
		MaxChannelDictionaries: 64,
	})
	ch := "study:pb"
	for i := 0; i < 90; i++ {
		engine.observe(ch, protocol.TypeProtobuf,
			encodePush(ch, studyShapes[0].gen(ch, i, rng), uint64(i)), 1)
	}
	codec, _ := engine.current(ch, protocol.TypeProtobuf)
	d := codec.Dict()
	out := structDict.Compress(nil, d)
	fmt.Printf("protobuf: %d B raw -> %d B compressed (%.2fx)\n",
		len(d), len(out), float64(len(d))/float64(len(out)))
}
