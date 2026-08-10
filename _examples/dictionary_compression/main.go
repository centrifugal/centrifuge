// Command dictionary_compression shows how to implement
// centrifuge.DictionaryCompression: the interface Centrifuge exposes for
// compressing every frame of a connection against a dictionary both sides
// already hold.
//
// Centrifuge ships no implementation. Where dictionaries come from, which
// connection gets which one, and what may go into one are product decisions -
// a dictionary is sent to clients, so its contents are disclosed to them - and
// they belong to whoever supplies the engine. This example takes the simplest
// possible position on all three: one dictionary, built at startup from sample
// traffic, served to every client that can decode it.
//
// Run it and connect a client that supports dictionary compression
// (centrifuge-go or centrifuge-js):
//
//	go run main.go
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// --- the engine -------------------------------------------------------------

// engine is a centrifuge.DictionaryCompression serving one dictionary to
// everyone. A real one would pick per profile, roll out gradually, and be able
// to withdraw a dictionary; none of that changes the shape below.
type engine struct {
	id    string
	raw   []byte // the dictionary itself, what frames are compressed against
	wire  string // base64 of the deflated dictionary, for a JSON connect reply
	codec *protocol.DeflateFrameCodec

	mu    sync.RWMutex
	cache map[string][]byte
}

func newEngine(dict []byte) *engine {
	id := protocol.DictionaryID(dict) // content hash: clients cache by it
	return &engine{
		id:    id,
		raw:   dict,
		wire:  base64.StdEncoding.EncodeToString(protocol.DeflateDictionary(dict)),
		codec: protocol.NewDeflateFrameCodec(id, dict),
		cache: map[string][]byte{},
	}
}

func (e *engine) NewConnection(p centrifuge.ConnectionParams) centrifuge.ConnectionCompression {
	// A client that cannot decode a compressed frame must not be sent one.
	if p.ClientFlags&centrifuge.ConnectionFlagDictionaryCompression == 0 {
		return nil
	}
	// This dictionary is built from JSON frames, so it is useless on Protobuf.
	if p.ProtocolType != centrifuge.ProtocolTypeJSON {
		return nil
	}
	return &conn{engine: e, held: p.HeldDictionaryID}
}

type conn struct {
	engine *engine
	held   string
}

// Dictionary goes into the connect reply. When the client already holds these
// bytes - it says so with an id, and ids are content hashes - only the id
// travels and the client pays nothing to reconnect.
func (c *conn) Dictionary() *protocol.Dictionary {
	d := &protocol.Dictionary{Id: c.engine.id}
	if c.held != c.engine.id {
		d.DataB64 = c.engine.wire
	}
	return d
}

func (c *conn) Encode(frame []byte) ([]byte, bool) {
	return c.engine.compress(frame), true
}

func (c *conn) Close() {}

// compress reuses an earlier result for the same frame, which is what makes
// fan-out affordable: one publication reaching a thousand subscribers is one
// compression rather than a thousand.
//
// NOTE for anyone building on this: the cache alone is not enough. Those
// thousand subscribers are written by that many goroutines at nearly the same
// instant, so they all miss - the first has not stored its result yet - and all
// compress the same bytes. Measured on a four-subscriber channel, every frame
// was compressed three or four times over while the cache reported a hit rate
// that looked fine. Have the first arrival compress and the rest wait for it;
// on that workload it removed half the compressions and a third of the CPU.
// It is left out here to keep the example about the interface.
func (e *engine) compress(frame []byte) []byte {
	e.mu.RLock()
	out, ok := e.cache[string(frame)]
	e.mu.RUnlock()
	if ok {
		return out
	}
	out = e.codec.Compress(nil, frame)

	e.mu.Lock()
	if len(e.cache) >= 1024 {
		clear(e.cache) // crude; an example needs no eviction policy
	}
	e.cache[string(frame)] = out
	e.mu.Unlock()
	return out
}

// --- building a dictionary --------------------------------------------------

// buildDictionary makes a dictionary out of sample messages, wrapped in the
// frames a client will actually receive so that the protocol envelope is in it
// too. Centrifugo PRO trains this from live traffic and has a human approve
// what goes in; here it is hardcoded, which is fine because the contents are
// ours rather than a user's.
func buildDictionary() []byte {
	samples := []map[string]any{
		{"event": "price.changed", "symbol": "AAPL", "price": 192.4, "currency": "USD", "venue": "NASDAQ"},
		{"event": "price.changed", "symbol": "MSFT", "price": 411.2, "currency": "USD", "venue": "NASDAQ"},
		{"event": "order.filled", "symbol": "AAPL", "quantity": 100, "side": "buy", "status": "filled"},
		{"event": "order.filled", "symbol": "MSFT", "quantity": 250, "side": "sell", "status": "filled"},
	}
	// The structure dictionary ships with the protocol package and holds the
	// Centrifugo envelope - every frame carries it, so it is worth having in
	// front of anything trained.
	out := append([]byte(nil), protocol.StructureDictionary...)
	for _, s := range samples {
		data, _ := json.Marshal(s)
		rep := &protocol.Reply{Push: &protocol.Push{
			Channel: "market", Pub: &protocol.Publication{Data: data},
		}}
		b, err := protocol.GetReplyEncoder(protocol.TypeJSON).Encode(rep)
		if err != nil {
			continue
		}
		enc := protocol.GetDataEncoder(protocol.TypeJSON)
		if enc.Encode(b) == nil {
			out = append(out, enc.FinishNoCopy()...)
		}
		protocol.PutDataEncoder(protocol.TypeJSON, enc)
	}
	return out
}

// --- wiring -----------------------------------------------------------------

func main() {
	dict := buildDictionary()
	eng := newEngine(dict)
	log.Printf("dictionary: %d bytes, id %s", len(dict), eng.id)

	node, err := centrifuge.New(centrifuge.Config{
		DictionaryCompression: eng,
	})
	if err != nil {
		log.Fatal(err)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{UserID: "42"},
			// Profile classifies a connection so an engine can serve different
			// dictionaries to different kinds of client. This one ignores it.
			Profile: e.Profile,
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Publish something shaped like the dictionary, so a connected client can
	// watch frames get small.
	go func() {
		for i := 0; ; i++ {
			data, _ := json.Marshal(map[string]any{
				"event": "price.changed", "symbol": "AAPL",
				"price": 192.4 + float64(i%10)/10, "currency": "USD", "venue": "NASDAQ",
			})
			if _, err := node.Publish("market", data); err != nil {
				log.Printf("publish: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()

	http.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))
	fmt.Println("listening on :8000, publishing to channel \"market\"")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
