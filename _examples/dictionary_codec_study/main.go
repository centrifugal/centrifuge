// Command dictionary_codec_study compares compression algorithms for
// connection-level frame compression with a trained dictionary.
//
// DEFLATE's "preset dictionary" is only an LZ77 window prefill - the decoder is
// handed 32 KB of prior bytes and nothing else. A zstd dictionary is a trained
// artifact: it carries entropy tables alongside the content, which should matter
// far more on the small frames this feature targets, where Huffman table
// overhead dominates.
//
// That difference was academic while dictionaries were sampled at runtime from
// whatever traffic happened by. With dictionaries trained offline it is worth
// measuring, because the codec is negotiated per connection at connect - so a
// Go or mobile SDK could use a better one while browsers stay on DEFLATE.
//
//	go run ./dictionary_codec_study
package main

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/klauspost/compress/zstd"
)

// ---------------------------------------------------------------------------
// Traffic
// ---------------------------------------------------------------------------

var (
	symbols  = []string{"AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "GOOG", "NFLX"}
	statuses = []string{"new", "partial", "filled", "cancelled", "rejected"}
	phrases  = []string{"see you at six", "on my way", "sounds good", "let me check that",
		"ok", "can you send the link", "will do", "not sure yet", "thanks", "running late"}
	people = []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
)

type shape struct {
	name string
	gen  func(i int, r *rand.Rand) []byte
}

var shapes = []shape{
	{"delta ticker", func(i int, r *rand.Rand) []byte {
		return []byte(fmt.Sprintf(`{"s":"%s","p":%.2f,"t":%d}`,
			symbols[r.Intn(len(symbols))], float64(1000+r.Intn(90000))/100,
			1785920062114+int64(i)))
	}},
	{"chat message", func(i int, r *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"id":"m-%d","user":"%s","text":"%s","createdAt":"2026-08-06T09:%02d:%02dZ","edited":%v}`,
			900000+i, people[r.Intn(len(people))], phrases[r.Intn(len(phrases))],
			r.Intn(60), r.Intn(60), r.Intn(10) == 0))
	}},
	{"order event", func(i int, r *rand.Rand) []byte {
		return []byte(fmt.Sprintf(
			`{"orderId":"ord-%08d","symbol":"%s","side":"%s","qty":%d,"price":%.2f,"status":"%s","filled":%d,"updatedAt":%d}`,
			10000000+i, symbols[r.Intn(len(symbols))],
			[]string{"buy", "sell"}[r.Intn(2)], 1+r.Intn(500),
			float64(1000+r.Intn(90000))/100, statuses[r.Intn(len(statuses))],
			r.Intn(500), 1785920062114+int64(i)))
	}},
	{"large document", func(i int, r *rand.Rand) []byte {
		var b bytes.Buffer
		b.WriteString(`{"docId":"doc-4471","rev":` + fmt.Sprint(i) + `,"blocks":[`)
		for j := 0; j < 20; j++ {
			if j > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"id":%d,"type":"paragraph","text":"%s","author":"%s"}`,
				j, phrases[r.Intn(len(phrases))], people[r.Intn(len(people))])
		}
		b.WriteString(`]}`)
		return b.Bytes()
	}},
}

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

func frames(sh shape, n int, seed int64) [][]byte {
	r := rand.New(rand.NewSource(seed))
	out := make([][]byte, n)
	for i := range out {
		out[i] = encodePush(fmt.Sprintf("ch:%02d", r.Intn(6)), sh.gen(i, r), uint64(i))
	}
	return out
}

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

// deflateDict is how the current implementation builds one: concatenate samples
// and keep the tail. DEFLATE has no notion of training.
func deflateDict(train [][]byte, size int) []byte {
	var buf []byte
	for i := len(train) - 1; i >= 0 && len(buf) < size; i-- {
		buf = append(buf, train[i]...)
	}
	if len(buf) > size {
		buf = buf[:size]
	}
	return buf
}

func deflateCompress(dict, src []byte, level int) []byte {
	var buf bytes.Buffer
	w, err := flate.NewWriterDict(&buf, level, dict)
	if err != nil {
		panic(err)
	}
	_, _ = w.Write(src)
	_ = w.Close()
	return buf.Bytes()
}

func deflateDecompress(dict, src []byte) []byte {
	r := flate.NewReaderDict(bytes.NewReader(src), dict)
	out, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return out
}

func main() {
	const (
		trainN   = 800
		measureN = 400
		dictSize = 4096
	)

	fmt.Printf("%-16s %-22s %9s %8s %8s %10s %10s\n",
		"shape", "codec", "bytes", "per frame", "ratio", "vs deflate", "us/frame")
	fmt.Println("  (dictionary 4096 B where used; measured on held-out frames)")

	for _, sh := range shapes {
		train := frames(sh, trainN, 11)
		measure := frames(sh, measureN, 97)

		var raw int
		for _, f := range measure {
			raw += len(f)
		}

		ddict := deflateDict(train, dictSize)

		// zstd gets exactly the 4096 B of content DEFLATE gets, as History.
		// Contents is the corpus it trains entropy tables from - that difference
		// is the whole point of the comparison.
		zd, err := zstd.BuildDict(zstd.BuildDictOptions{
			ID: 1, History: ddict, Contents: train, Level: zstd.SpeedBestCompression,
		})
		if err != nil {
			fmt.Println("zstd BuildDict failed:", err)
			return
		}

		// Each frame is compressed independently, so zstd pays its frame header
		// every time: magic, descriptor, dictionary id, and a checksum unless
		// disabled. On payloads this small that overhead is the whole story, so
		// measure it explicitly rather than let it hide in the totals.
		zEncDict, _ := zstd.NewWriter(nil,
			zstd.WithEncoderDict(zd), zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderCRC(false), zstd.WithWindowSize(1<<17))
		zEncBest, _ := zstd.NewWriter(nil,
			zstd.WithEncoderDict(zd), zstd.WithEncoderLevel(zstd.SpeedBestCompression),
			zstd.WithEncoderCRC(false), zstd.WithWindowSize(1<<17))
		zEncNone, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		zDec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(zd))

		type codec struct {
			name     string
			compress func([]byte) []byte
			verify   func(out, want []byte)
		}
		codecs := []codec{
			{"deflate, no dict", func(b []byte) []byte { return deflateCompress(nil, b, 6) },
				func(out, want []byte) { checkEqual(deflateDecompress(nil, out), want) }},
			{"deflate + dict", func(b []byte) []byte { return deflateCompress(ddict, b, 6) },
				func(out, want []byte) { checkEqual(deflateDecompress(ddict, out), want) }},
			{"zstd, no dict", func(b []byte) []byte { return zEncNone.EncodeAll(b, nil) }, nil},
			{"zstd + dict", func(b []byte) []byte { return zEncDict.EncodeAll(b, nil) },
				func(out, want []byte) {
					got, err := zDec.DecodeAll(out, nil)
					if err != nil {
						panic(err)
					}
					checkEqual(got, want)
				}},
			{"zstd + dict, best", func(b []byte) []byte { return zEncBest.EncodeAll(b, nil) }, nil},
		}

		var deflateWithDict int
		for _, c := range codecs {
			total := 0
			start := time.Now()
			for _, f := range measure {
				out := c.compress(f)
				total += len(out)
			}
			elapsed := time.Since(start)
			if c.verify != nil {
				c.verify(c.compress(measure[0]), measure[0])
			}
			if c.name == "deflate + dict" {
				deflateWithDict = total
			}
			vs := ""
			if deflateWithDict > 0 {
				vs = fmt.Sprintf("%+.1f%%", 100*(float64(total)/float64(deflateWithDict)-1))
			}
			fmt.Printf("%-16s %-22s %8dB %7.1fB %7.2fx %10s %9.1f\n",
				sh.name, c.name, total, float64(total)/float64(len(measure)),
				float64(raw)/float64(total), vs,
				float64(elapsed.Microseconds())/float64(len(measure)))
		}
		tiny := []byte("{}")
		fmt.Printf("%-16s %-22s zstd dict %d B, deflate dict %d B | floor per frame: deflate %d B, zstd %d B\n",
			"", "", len(zd), len(ddict),
			len(deflateCompress(ddict, tiny, 6)), len(zEncDict.EncodeAll(tiny, nil)))
	}
}

func checkEqual(got, want []byte) {
	if !bytes.Equal(got, want) {
		panic("round trip mismatch")
	}
}
