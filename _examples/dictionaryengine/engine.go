// Package dictionaryengine is a small centrifuge.DictionaryCompression
// implementation for the examples.
//
// centrifuge ships none of its own. It defines the interface and the points
// where a connection hands work to it, and leaves every decision outside: where
// dictionaries come from, which connection gets which one, and what is allowed
// into one. Those are product and disclosure decisions, not protocol ones.
//
// This engine takes the dictionaries as an argument. It is enough to show the
// wiring and to measure what compression does on the wire, and it stands in for
// a real implementation, where the dictionaries would be trained offline per
// profile from anonymised traffic, reviewed, versioned and served.
package dictionaryengine

import (
	"encoding/base64"
	"sync"
	"sync/atomic"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/protocol"
)

// Key identifies a dictionary. The protocol is part of it because a dictionary
// is a sample of frames: one built from JSON is worth nothing on a Protobuf
// connection, and serving it there costs bandwidth for no gain.
type Key struct {
	Profile  string
	Protocol centrifuge.ProtocolType
}

// Options configures an Engine.
type Options struct {
	// Dictionaries holds the dictionary to serve for each profile and protocol.
	// A connection that matches nothing here falls back to the protocol
	// structure dictionary, which contains no application data and so needs no
	// decision about who may see it.
	Dictionaries map[Key][]byte

	// NoFallback drops the structure dictionary fallback, so unmatched
	// connections are left uncompressed. Useful for measuring one layer at a
	// time.
	NoFallback bool

	// FrameCacheSize bounds how many compressed frames are remembered per
	// dictionary. It is what makes fan-out cheap: one publication reaching a
	// thousand subscribers is compressed once. Zero disables the cache.
	FrameCacheSize int
}

// Engine implements centrifuge.DictionaryCompression.
type Engine struct {
	opts  Options
	dicts map[Key]*dictionary
	// fallback is shared by every connection that matched no profile, so its
	// frame cache is shared too.
	fallback *dictionary

	frameCompressions atomic.Int64
	frameCacheHits    atomic.Int64
}

// dictionary is one dictionary plus the cache of frames already compressed
// against it. The cache belongs to the dictionary rather than the engine: two
// connections may only share a compressed frame if they hold the same bytes.
type dictionary struct {
	codec *protocol.DeflateFrameCodec
	// encoded is the dictionary as it travels to a JSON client, computed once.
	encoded string
	// packed is the dictionary as it travels to a Protobuf client. Dictionaries
	// compress well against themselves, so this is a fraction of the raw size.
	packed []byte

	mu    sync.RWMutex
	cache map[string][]byte
	limit int
}

func newDictionary(dict []byte, cacheSize int) *dictionary {
	packed := protocol.DeflateDictionary(dict)
	d := &dictionary{
		codec:   protocol.NewDeflateFrameCodec(protocol.DictionaryID(dict), dict),
		packed:  packed,
		encoded: base64.StdEncoding.EncodeToString(packed),
		limit:   cacheSize,
	}
	if cacheSize > 0 {
		d.cache = make(map[string][]byte, cacheSize)
	}
	return d
}

// New builds an engine serving the given dictionaries.
func New(opts Options) *Engine {
	e := &Engine{opts: opts, dicts: make(map[Key]*dictionary, len(opts.Dictionaries))}
	for k, dict := range opts.Dictionaries {
		e.dicts[k] = newDictionary(dict, opts.FrameCacheSize)
	}
	if !opts.NoFallback {
		e.fallback = newDictionary(protocol.StructureDictionary, opts.FrameCacheSize)
	}
	return e
}

// NewConnection implements centrifuge.DictionaryCompression.
func (e *Engine) NewConnection(params centrifuge.ConnectionParams) centrifuge.ConnectionCompression {
	if params.ClientFlags&centrifuge.ConnectionFlagDictionaryCompression == 0 {
		// The client cannot decode, so there is nothing to offer it.
		return nil
	}
	d, ok := e.dicts[Key{Profile: params.Profile, Protocol: params.ProtocolType}]
	if !ok {
		d = e.fallback
	}
	if d == nil {
		return nil
	}
	return &conn{
		engine: e,
		dict:   d,
		json:   params.ProtocolType == centrifuge.ProtocolTypeJSON,
		held:   params.HeldDictionaryID,
	}
}

// Stats reports what the engine has done. Compressions against cache hits is
// the number to watch under fan-out: hits should dominate, and if they do not,
// frames are not shareable - usually because connections batch differently or
// because most channels have a single subscriber.
type Stats struct {
	// Dictionaries lists what the engine holds. Its length is what the memory
	// overhead scales with.
	Dictionaries      []DictionaryStats
	FrameCompressions int64
	FrameCacheHits    int64
}

// DictionaryStats describes one dictionary.
type DictionaryStats struct {
	Key Key
	ID  string
	// Size is the raw dictionary, which is what compression matches against and
	// what the connection holds in memory.
	Size int
	// WireSize is what delivering it costs, once, on a connection that does not
	// already hold it. Dictionaries compress well against themselves, so this is
	// a fraction of Size.
	WireSize int
}

// Stats returns a snapshot.
func (e *Engine) Stats() Stats {
	st := Stats{
		FrameCompressions: e.frameCompressions.Load(),
		FrameCacheHits:    e.frameCacheHits.Load(),
	}
	for k, d := range e.dicts {
		st.Dictionaries = append(st.Dictionaries, d.stats(k))
	}
	if e.fallback != nil {
		st.Dictionaries = append(st.Dictionaries, e.fallback.stats(Key{}))
	}
	return st
}

func (d *dictionary) stats(k Key) DictionaryStats {
	return DictionaryStats{
		Key:      k,
		ID:       d.codec.ID(),
		Size:     len(d.codec.Dict()),
		WireSize: len(d.packed),
	}
}

// conn is the per-connection encoder.
type conn struct {
	engine *Engine
	dict   *dictionary
	json   bool
	held   string
}

// Dictionary implements centrifuge.ConnectionCompression.
func (c *conn) Dictionary() *protocol.Dictionary {
	d := &protocol.Dictionary{Id: c.dict.codec.ID()}
	if c.held == d.Id {
		// The client kept these bytes from an earlier connection. An id
		// identifies content, so naming it is enough and the transfer is saved.
		return d
	}
	if c.json {
		d.DataB64 = c.dict.encoded
	} else {
		d.Data = c.dict.packed
	}
	return d
}

// Encode implements centrifuge.ConnectionCompression.
func (c *conn) Encode(frame []byte) ([]byte, bool) {
	return c.dict.compress(c.engine, frame), true
}

func (d *dictionary) compress(e *Engine, frame []byte) []byte {
	if d.cache == nil {
		e.frameCompressions.Add(1)
		return d.codec.Compress(nil, frame)
	}
	key := string(frame)
	d.mu.RLock()
	out, ok := d.cache[key]
	d.mu.RUnlock()
	if ok {
		e.frameCacheHits.Add(1)
		return out
	}
	out = d.codec.Compress(nil, frame)
	e.frameCompressions.Add(1)
	d.mu.Lock()
	if len(d.cache) >= d.limit {
		// Crude, but an example does not need an eviction policy: the cache
		// exists to catch the fan-out of one publication, which happens at once.
		clear(d.cache)
	}
	d.cache[key] = out
	d.mu.Unlock()
	return out
}

// Train builds a dictionary from sample frames, newest first, up to size bytes.
//
// This is the whole of the "training" here: DEFLATE looks back over the
// dictionary for matches, so a dictionary is useful exactly to the degree that
// it contains the strings later frames repeat. A real trainer removes anything
// that identifies a user before it gets this far, which is the part an example
// cannot demonstrate.
func Train(samples [][]byte, size int) []byte {
	buf := make([]byte, 0, size)
	for i := len(samples) - 1; i >= 0 && len(buf) < size; i-- {
		buf = append(buf, samples[i]...)
	}
	if len(buf) > size {
		buf = buf[:size]
	}
	return buf
}
