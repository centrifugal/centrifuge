// Command dictionary_window_study asks whether streaming compression can be
// made cheap enough to use on a server with many connections.
//
// Background. Everything measured for the dictionary feature compared against
// permessage-deflate as Centrifugo ships it, which always negotiates
// server_no_context_takeover. RFC 7692 allows context takeover, and measured on
// the same traffic it beats a dictionary by 27 to 114 percent. Discord ship
// streaming zstd for exactly this reason and abandoned their dictionary
// experiments after finding the gains were small and inconsistent.
//
// The reason we ruled it out was memory: a streaming compressor holds state per
// connection. Discord tuned zstd's window down to 256 KB to control that. This
// measures what the options actually cost - ratio, memory per connection, and
// time - so the decision rests on numbers rather than on one estimate.
//
//	go run ./dictionary_window_study
package main

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sync"
	"time"

	kflate "github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
)

// ---------------------------------------------------------------------------
// Traffic: one connection's stream, mixed feeds, the shape a dashboard sees.
// ---------------------------------------------------------------------------

var (
	symbols  = []string{"AAPL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "GOOG", "NFLX"}
	statuses = []string{"new", "partial", "filled", "cancelled", "rejected"}
	phrases  = []string{"see you at six", "on my way", "sounds good", "let me check that",
		"ok", "can you send the link", "will do", "not sure yet", "thanks", "running late"}
	people = []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
)

func stream(n int, seed int64) [][]byte {
	r := rand.New(rand.NewSource(seed))
	out := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		var payload string
		switch r.Intn(4) {
		case 0:
			payload = fmt.Sprintf(`{"s":"%s","p":%.2f,"t":%d}`,
				symbols[r.Intn(len(symbols))], float64(1000+r.Intn(90000))/100,
				1785920062114+int64(i))
		case 1:
			payload = fmt.Sprintf(
				`{"id":"m-%d","user":"%s","text":"%s","createdAt":"2026-08-07T09:%02d:%02dZ"}`,
				900000+i, people[r.Intn(len(people))], phrases[r.Intn(len(phrases))],
				r.Intn(60), r.Intn(60))
		case 2:
			payload = fmt.Sprintf(
				`{"orderId":"ord-%08d","symbol":"%s","side":"%s","qty":%d,"status":"%s","updatedAt":%d}`,
				10000000+i, symbols[r.Intn(len(symbols))],
				[]string{"buy", "sell"}[r.Intn(2)], 1+r.Intn(500),
				statuses[r.Intn(len(statuses))], 1785920062114+int64(i))
		default:
			payload = fmt.Sprintf(
				`{"deviceId":"dev-%04d","status":"%s","temp":%.1f,"battery":%d,"at":%d}`,
				r.Intn(9000), []string{"online", "degraded", "offline"}[r.Intn(3)],
				float64(150+r.Intn(200))/10, r.Intn(100), 1785920062114+int64(i))
		}
		out = append(out, []byte(fmt.Sprintf(
			`{"push":{"channel":"ch:%02d","pub":{"data":%s,"offset":%d}}}`,
			r.Intn(6), payload, i)))
	}
	return out
}

// dictionaryFrom builds the kind of dictionary the feature would ship: the tail
// of a training corpus, capped at size.
func dictionaryFrom(train [][]byte, size int) []byte {
	var buf []byte
	for i := len(train) - 1; i >= 0 && len(buf) < size; i-- {
		buf = append(buf, train[i]...)
	}
	if len(buf) > size {
		buf = buf[:size]
	}
	return buf
}

// ---------------------------------------------------------------------------
// One candidate: how to compress a connection's stream.
// ---------------------------------------------------------------------------

type candidate struct {
	name string
	// newWriter returns a per-connection compressor plus a flush-per-frame
	// function. Nil newWriter means the mode compresses each frame alone.
	streaming bool
	compress  func(frames [][]byte) int
	// alloc builds one live compressor, for the memory measurement.
	alloc func() any
}

func main() {
	train := stream(800, 11)
	// Long enough that a connection's history exceeds the largest window under
	// test, otherwise window size cannot matter and the comparison is empty.
	measure := stream(4000, 97)
	dict := dictionaryFrom(train, 8192)

	var raw int
	for _, f := range measure {
		raw += len(f)
	}

	// Independent modes pool their writers, which is what the server does. Not
	// pooling would charge dictionary priming to every frame and overstate the
	// cost several fold.
	independent := func(level int, dict []byte) func([][]byte) int {
		return func(frames [][]byte) int {
			pool := sync.Pool{New: func() any {
				var w *flate.Writer
				if dict != nil {
					w, _ = flate.NewWriterDict(io.Discard, level, dict)
				} else {
					w, _ = flate.NewWriter(io.Discard, level)
				}
				return w
			}}
			total := 0
			for _, f := range frames {
				var b bytes.Buffer
				w := pool.Get().(*flate.Writer)
				w.Reset(&b)
				_, _ = w.Write(f)
				_ = w.Close()
				pool.Put(w)
				total += b.Len()
			}
			return total
		}
	}

	cands := []candidate{
		{
			name:     "independent, no dictionary (today)",
			compress: independent(6, nil),
		},
		{
			name:     "independent + 8 KB dictionary (our design)",
			compress: independent(6, dict),
		},
		{
			name: "streaming stdlib flate, level 1", streaming: true,
			compress: streamFlate(1, nil), alloc: allocFlate(1, nil),
		},
		{
			name: "streaming stdlib flate, level 6", streaming: true,
			compress: streamFlate(6, nil), alloc: allocFlate(6, nil),
		},
		{
			name: "streaming stdlib flate, level 6 + dict", streaming: true,
			compress: streamFlate(6, dict), alloc: allocFlate(6, dict),
		},
		{
			name: "streaming klauspost flate, level 1", streaming: true,
			compress: streamKFlate(1), alloc: allocKFlate(1),
		},
		{
			name: "streaming klauspost flate, level 6", streaming: true,
			compress: streamKFlate(6), alloc: allocKFlate(6),
		},
	}
	for _, wl := range []int{15, 17, 18, 20} {
		wl := wl
		cands = append(cands, candidate{
			name:      fmt.Sprintf("streaming zstd, window %d KB", (1<<wl)/1024),
			streaming: true,
			compress:  streamZstd(wl),
			alloc:     allocZstd(wl),
		})
	}

	fmt.Printf("%d frames, %d B raw, %.0f B per frame\n\n",
		len(measure), raw, float64(raw)/float64(len(measure)))
	fmt.Printf("%-44s %9s %8s %11s %10s\n",
		"mode", "bytes", "ratio", "mem/conn", "us/frame")
	for _, c := range cands {
		start := time.Now()
		out := c.compress(measure)
		el := time.Since(start)
		mem := "         -"
		if c.alloc != nil {
			mem = fmt.Sprintf("%6d KB", perConnKB(c.alloc)/1024)
		}
		fmt.Printf("%-44s %8dB %7.2fx %10s %9.2f\n",
			c.name, out, float64(raw)/float64(out), mem,
			float64(el.Microseconds())/float64(len(measure)))
	}
	fmt.Println()
	fmt.Println("mem/conn is the live heap cost of one compressor, over 200 of them. It is")
	fmt.Println("shown only for streaming modes: independent modes pool their writers, so the")
	fmt.Println("cost is per concurrent compression, not per connection.")
	fmt.Println("Streaming modes flush after every frame so each frame stays separately")
	fmt.Println("deliverable, which is what a WebSocket connection needs.")
	fmt.Println("zstd is shown for reference only: browsers cannot decode it over WebSocket.")
}

func streamFlate(level int, dict []byte) func([][]byte) int {
	return func(frames [][]byte) int {
		var b bytes.Buffer
		var w *flate.Writer
		if dict != nil {
			w, _ = flate.NewWriterDict(&b, level, dict)
		} else {
			w, _ = flate.NewWriter(&b, level)
		}
		for _, f := range frames {
			_, _ = w.Write(f)
			_ = w.Flush()
		}
		_ = w.Close()
		return b.Len()
	}
}

func allocFlate(level int, dict []byte) func() any {
	return func() any {
		var w *flate.Writer
		if dict != nil {
			w, _ = flate.NewWriterDict(io.Discard, level, dict)
		} else {
			w, _ = flate.NewWriter(io.Discard, level)
		}
		_, _ = w.Write([]byte(`{"push":{}}`))
		_ = w.Flush()
		return w
	}
}

func streamKFlate(level int) func([][]byte) int {
	return func(frames [][]byte) int {
		var b bytes.Buffer
		w, _ := kflate.NewWriter(&b, level)
		for _, f := range frames {
			_, _ = w.Write(f)
			_ = w.Flush()
		}
		_ = w.Close()
		return b.Len()
	}
}

func allocKFlate(level int) func() any {
	return func() any {
		w, _ := kflate.NewWriter(io.Discard, level)
		_, _ = w.Write([]byte(`{"push":{}}`))
		_ = w.Flush()
		return w
	}
}

func streamZstd(windowLog int) func([][]byte) int {
	return func(frames [][]byte) int {
		var b bytes.Buffer
		w, err := zstd.NewWriter(&b,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithWindowSize(1<<windowLog),
			zstd.WithEncoderCRC(false))
		if err != nil {
			panic(err)
		}
		for _, f := range frames {
			_, _ = w.Write(f)
			_ = w.Flush()
		}
		_ = w.Close()
		return b.Len()
	}
}

func allocZstd(windowLog int) func() any {
	return func() any {
		w, _ := zstd.NewWriter(io.Discard,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithWindowSize(1<<windowLog),
			zstd.WithEncoderCRC(false))
		_, _ = w.Write([]byte(`{"push":{}}`))
		_ = w.Flush()
		return w
	}
}

// perConnKB measures the live heap cost of one compressor by building many and
// keeping them all alive, which is what a server with many connections does.
func perConnKB(alloc func() any) uint64 {
	const n = 200
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	held := make([]any, n)
	for i := range held {
		held[i] = alloc()
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	per := (after.HeapAlloc - before.HeapAlloc) / n
	runtime.KeepAlive(held)
	return per
}
