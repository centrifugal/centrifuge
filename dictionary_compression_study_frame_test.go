package centrifuge

// The activation frame is currently sent through Passthrough - uncompressed -
// on the reasoning that the client cannot decode it yet. That reasoning only
// holds for the very first dictionary. Once a connection already holds one, the
// frame announcing the NEXT dictionary can be compressed against the current
// one, because the client decodes the frame before installing what it carries.
//
// This measures the real end-to-end frame, base64 and JSON envelope included,
// rather than the dictionary bytes in isolation.

import (
	"math/rand"
	"testing"

	"github.com/centrifugal/protocol"
)

func TestStudyActivationFrameOnTheWire(t *testing.T) {
	rng := rand.New(rand.NewSource(5))

	for _, pt := range []protocol.Type{protocol.TypeJSON, protocol.TypeProtobuf} {
		for _, size := range []int{2048, 4096, 8192} {
			engine := NewDictionaryCompressionEngine(DictionaryCompressionConfig{
				DictionarySize:         size,
				MinSamples:             64,
				UseChannelDictionary:   func(string) bool { return true },
				MaxChannelDictionaries: 64,
			})
			ch := "study:frame"
			for i := 0; i < 90; i++ {
				engine.observe(ch, pt, encodePush(ch, studyShapes[0].gen(ch, i, rng), uint64(i)), 1)
			}
			codec, encoded := engine.current(ch, pt)
			if codec == nil {
				t.Fatalf("no dictionary")
			}

			// Build the activation frame exactly as the server does.
			d := &protocol.Dictionary{Id: codec.ID()}
			var data []byte
			var err error
			if pt == protocol.TypeJSON {
				d.DataB64 = encoded
				data, err = protocol.DefaultJsonReplyEncoder.Encode(
					&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
			} else {
				d.Data = codec.Dict()
				data, err = protocol.DefaultProtobufReplyEncoder.Encode(
					&protocol.Reply{Push: &protocol.Push{State: &protocol.ConnectionState{Dictionary: d}}})
			}
			if err != nil {
				t.Fatal(err)
			}
			enc := protocol.GetDataEncoder(pt)
			frame := append([]byte(nil), func() []byte {
				_ = enc.Encode(data)
				return enc.Finish()
			}()...)
			protocol.PutDataEncoder(pt, enc)

			structDict := protocol.StructureFrameCodec()
			today := len(structDict.Passthrough(nil, frame))
			compressed := len(structDict.Compress(nil, frame))

			name := "JSON"
			if pt == protocol.TypeProtobuf {
				name = "Protobuf"
			}
			t.Logf("%-8s dict %5d B | frame today %6d B | frame compressed %5d B | %.1fx smaller",
				name, size, today, compressed, float64(today)/float64(compressed))
		}
	}
}
