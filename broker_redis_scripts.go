package centrifuge

import (
	_ "embed"
)

// The scripts publishing one publication are each a function and a caller for
// it. Keeping the two apart gives the logic a single definition and lets the
// caller stay as small as what it does: read the keys and arguments of one
// publication and hand them over. The Go side joins them into the script it
// sends, so Redis receives one self-contained program as it always has.
var (
	//go:embed internal/redis_lua/broker_history_add_stream_fn.lua
	addHistoryStreamFnSource string

	//go:embed internal/redis_lua/broker_history_add_stream_call.lua
	addHistoryStreamCallSource string

	//go:embed internal/redis_lua/broker_publish_idempotent_fn.lua
	publishIdempotentFnSource string

	//go:embed internal/redis_lua/broker_publish_idempotent_call.lua
	publishIdempotentCallSource string
)

// addHistoryStreamSource is the script publishing one publication with history.
var addHistoryStreamSource = addHistoryStreamFnSource + "\n" + addHistoryStreamCallSource

// publishIdempotentSource is the script publishing one publication whose result
// is remembered against a retry.
var publishIdempotentSource = publishIdempotentFnSource + "\n" + publishIdempotentCallSource
