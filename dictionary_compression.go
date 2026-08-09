package centrifuge

import (
	"github.com/centrifugal/protocol"
)

// ConnectionFlagDictionaryCompression is advertised by a client in
// ConnectRequest.flag to say it can decode dictionary-compressed frames, and
// echoed in ConnectResult.flag when the server enabled it.
//
// A client must not assume a feature it advertised was accepted: the server may
// have it switched off, or may decline for this particular connection.
const ConnectionFlagDictionaryCompression int64 = 1 << 0

// DictionaryCompression compresses outgoing frames against a shared dictionary.
//
// This package provides no implementation. It defines the shape of one and the
// points where a connection hands work to it, so compression can be supplied
// from outside - which is where the decisions live: which dictionary a
// connection gets, where dictionaries come from, and what may go in one.
//
// A nil DictionaryCompression means the feature is absent. Nothing is allocated
// and no code path here is entered.
type DictionaryCompression interface {
	// NewConnection is called once per connection, after the client has been
	// authenticated and before the connect reply is written.
	//
	// Returning nil leaves the connection uncompressed, which is the right answer
	// for a client that cannot decode, a profile with nothing to offer, or any
	// case the implementation would rather sit out.
	NewConnection(params ConnectionParams) ConnectionCompression
}

// ConnectionParams describes the client a ConnectionCompression is being made
// for. It is a struct rather than an argument list so more can be negotiated
// later without breaking implementations outside this package.
type ConnectionParams struct {
	// ProtocolType is the connection's protocol. A dictionary built from JSON
	// frames is useless on Protobuf, so implementations must keep them apart.
	ProtocolType ProtocolType

	// ClientFlags is the capability bitmask the client advertised.
	ClientFlags int64

	// Profile is the application context this connection belongs to, resolved by
	// the server. Empty means unclassified.
	Profile string

	// UserID is the authenticated user, or empty for an anonymous connection.
	//
	// It is here because it is the only identity that survives a reconnect, and
	// a staged rollout needs one: a cohort re-drawn on every reconnect is not a
	// cohort, and a client that fell out of one would pay to be sent a
	// dictionary it already had. Implementations that split traffic should
	// derive the split from this rather than from anything per-connection.
	UserID string

	// HeldDictionaryID is the dictionary the client says it already has from an
	// earlier connection. An id identifies dictionary content, so a match means
	// both sides hold the same bytes and the dictionary can be named rather than
	// sent. An unrecognised id is simply ignored.
	HeldDictionaryID string
}

// ConnectionCompression encodes outgoing frames for one connection.
type ConnectionCompression interface {
	// Dictionary returns what to put in ConnectResult.dict, or nil to send
	// nothing.
	//
	// It is called once, before the connect reply is written.
	//
	// Compression begins as soon as both sides provably hold the dictionary.
	// Returning bytes here means the client does not have them yet, so this
	// reply is what delivers them and encoding starts on the frame after it.
	// Returning only an id means the client presented that id and already has
	// the bytes, so there is nothing to wait for and this reply is itself
	// compressed. Either way there is no window in which a compressed frame
	// can reach a client that does not hold the dictionary, and a client never
	// has to predict which it got: every frame carries a codec marker.
	//
	// Set only the id when the client advertised that same id - it already holds
	// the bytes.
	Dictionary() *protocol.Dictionary

	// Encode is called for every frame this connection compresses, on the
	// connection's write goroutine - including the connect reply when the
	// client already held the dictionary.
	//
	// It returns the bytes to write and whether they must go out as a binary
	// message, which compressed payloads need even on a JSON connection.
	Encode(frame []byte) (out []byte, binary bool)

	// Close is called once when the connection goes away, on the same goroutine
	// as the final Encode and never concurrently with one.
	//
	// Implementations that batch their accounting need this, and need it to be
	// exact rather than best-effort. Without it, everything a connection did
	// since its last flush is lost - and what is lost is not a random sample:
	// short connections are the ones that end before a flush, and they are also
	// the ones that most often pay to be sent a dictionary. Dropping them makes
	// compression look better than it is, in a number an operator uses to decide
	// whether the feature is worth keeping on.
	Close()
}

// compressionAware is implemented by transports that can compress frames. It is
// unexported: a transport either supports this or it does not, and nothing
// outside this package needs to ask.
type compressionAware interface {
	// setConnectionCompression installs a codec that must not encode the next
	// frame written, because that frame is the connect reply carrying the
	// dictionary this codec uses.
	setConnectionCompression(cc ConnectionCompression)
	// setConnectionCompressionNow installs a codec that encodes every frame
	// including the connect reply. Used when the client presented an id the
	// server recognised: it already holds the bytes, so there is nothing to
	// deliver and nothing to wait for.
	setConnectionCompressionNow(cc ConnectionCompression)
	// closeConnectionCompression is called once the writer has stopped, so an
	// implementation's Close cannot race the last Encode.
	closeConnectionCompression()
	// markNextFrame says the client already installed a codec of its own - it
	// advertised a dictionary it holds - so the next frame written must carry a
	// codec marker even though this server is not compressing it.
	//
	// A client that advertises has no choice but to install first: a server
	// that recognises the id compresses the connect reply itself, and a codec
	// learned from that reply could never read it. So it will strip a marker
	// from whatever comes back, and an unmarked reply is read as a frame in an
	// unknown codec.
	//
	// Only the next frame. After the connect reply the client knows from the
	// reply's flags whether compression was accepted, and drops its codec if it
	// was not - so marking beyond this point would both cost a byte and force
	// every frame binary, which turns permessage-deflate off for a connection
	// getting nothing in return.
	markNextFrame()
}
