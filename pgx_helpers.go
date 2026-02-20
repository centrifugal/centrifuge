package centrifuge

import (
	"encoding/binary"
	"encoding/json"

	"github.com/centrifugal/centrifuge/internal/convert"
)

// byteArena is a chunked arena allocator for byte data. It copies incoming
// slices into a contiguous buffer and hands back sub-slices (or strings via
// unsafe conversion). When the current chunk is full a new one is allocated;
// old chunks stay alive via the slices/strings already referencing them.
type byteArena struct {
	buf []byte
}

// copyBytes copies src into the arena and returns a sub-slice of the arena buffer.
// The returned slice has cap==len (three-index slice) so that append on it
// cannot silently overwrite adjacent arena data.
func (a *byteArena) copyBytes(src []byte) []byte {
	n := len(src)
	if n == 0 {
		return nil
	}
	if cap(a.buf)-len(a.buf) < n {
		newCap := 2 * cap(a.buf)
		if newCap < n {
			newCap = n
		}
		if newCap < 4096 {
			newCap = 4096
		}
		a.buf = make([]byte, 0, newCap)
	}
	start := len(a.buf)
	a.buf = append(a.buf, src...)
	return a.buf[start : start+n : start+n]
}

// copyString copies src into the arena and returns a string backed by arena memory.
func (a *byteArena) copyString(src []byte) string {
	if len(src) == 0 {
		return ""
	}
	return convert.BytesToString(a.copyBytes(src))
}

// pgRawInt64 parses a pgx int8 binary wire format value as int64.
// Returns 0 for nil input.
func pgRawInt64(b []byte) int64 {
	if b == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// pgRawUint64 parses a pgx int8 binary wire format value as uint64.
// Returns 0 for nil input.
func pgRawUint64(b []byte) uint64 {
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// pgRawInt32 parses a pgx int4 binary wire format value as int32.
// Returns 0 for nil input.
func pgRawInt32(b []byte) int32 {
	if b == nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

// pgRawBool parses a pgx bool binary wire format value.
// Returns false for nil input.
func pgRawBool(b []byte) bool {
	if b == nil {
		return false
	}
	return b[0] == 1
}

// pgRawString copies a pgx text binary wire format value into the arena
// and returns the resulting string. Returns "" for nil input.
func pgRawString(a *byteArena, b []byte) string {
	if b == nil {
		return ""
	}
	return a.copyString(b)
}

// pgRawBytes copies a pgx bytea binary wire format value into the arena.
// Returns nil for nil input.
func pgRawBytes(a *byteArena, b []byte) []byte {
	if b == nil {
		return nil
	}
	return a.copyBytes(b)
}

// pgRawJSONBMap parses a JSONB binary wire format value (1-byte version
// header + JSON body) into a map[string]string. Returns nil for nil or
// too-short input.
func pgRawJSONBMap(b []byte) map[string]string {
	if len(b) <= 1 {
		return nil
	}
	var m map[string]string
	_ = json.Unmarshal(b[1:], &m) // skip version byte
	return m
}
