// Package fossilutf8 aligns fossil deltas to UTF-8 character boundaries.
package fossilutf8

import (
	"math"
	"unicode/utf8"
)

// digits of the base-64 integers used by the fossil delta format.
const digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~"

var digitValues = func() (values [256]int8) {
	for i := range values {
		values[i] = -1
	}
	for i := range len(digits) {
		values[digits[i]] = int8(i)
	}
	return values
}()

// Align returns a fossil delta equivalent to patch whose inserts carry only
// whole UTF-8 characters of target, so the delta is valid UTF-8 and can be sent
// as a JSON string. patch must be a delta creating target.
//
// A fossil delta works on bytes: a copy may end or start in the middle of a
// multi-byte character, leaving a partial character in the adjacent insert.
// Align moves such boundaries to character boundaries, shortening the copy and
// extending the insert, which keeps the output of the delta and its checksum.
// A delta that is already valid UTF-8 is returned as is.
//
// It returns nil if patch can't be aligned: it's malformed or target isn't
// valid UTF-8.
func Align(patch, target []byte) []byte {
	if utf8.Valid(patch) {
		return patch
	}
	segments, checksum, ok := parse(patch, len(target))
	if !ok {
		return nil
	}
	segments, ok = alignSegments(segments, target)
	if !ok {
		return nil
	}
	aligned := encode(segments, target, checksum, len(patch))
	if !utf8.Valid(aligned) {
		return nil
	}
	return aligned
}

// segment is a copy or an insert command of a delta.
type segment struct {
	copy bool
	// start and end are the range of the command output in the target.
	start, end int
	// offset is the source offset of a copy.
	offset int
}

func parse(patch []byte, targetLen int) ([]segment, uint64, bool) {
	limit, pos := getInt(patch, 0)
	if limit != uint64(targetLen) || pos >= len(patch) || patch[pos] != '\n' {
		return nil, 0, false
	}
	pos++
	segments := make([]segment, 0, 8)
	t := 0
	for pos < len(patch) {
		var cnt uint64
		cnt, pos = getInt(patch, pos)
		if pos >= len(patch) {
			return nil, 0, false
		}
		op := patch[pos]
		pos++
		if op == ';' {
			if t != targetLen || cnt > math.MaxUint32 {
				return nil, 0, false
			}
			return segments, cnt, true
		}
		if cnt > uint64(targetLen-t) {
			return nil, 0, false
		}
		n := int(cnt)
		switch op {
		case '@':
			var offset uint64
			offset, pos = getInt(patch, pos)
			if offset > math.MaxInt32 || pos >= len(patch) || patch[pos] != ',' {
				return nil, 0, false
			}
			pos++
			segments = append(segments, segment{copy: true, start: t, end: t + n, offset: int(offset)})
		case ':':
			if n > len(patch)-pos {
				return nil, 0, false
			}
			pos += n
			segments = append(segments, segment{start: t, end: t + n})
		default:
			return nil, 0, false
		}
		t += n
	}
	return nil, 0, false
}

// alignSegments extends inserts to character boundaries, taking the bytes from
// the adjacent copies. It reuses the segments slice. It returns false if target
// isn't valid UTF-8 around an insert.
func alignSegments(segments []segment, target []byte) ([]segment, bool) {
	aligned := segments[:0]
	var ok bool
	for _, s := range segments {
		end := 0
		if n := len(aligned); n > 0 {
			end = aligned[n-1].end
		}
		if s.copy {
			// The previous insert may have been extended over the copy start.
			if s.start < end {
				s.offset += end - s.start
				s.start = end
			}
			if s.start < s.end {
				aligned = append(aligned, s)
			}
			continue
		}
		if s.start, ok = charBoundary(target, s.start, -1); !ok {
			return nil, false
		}
		if s.end, ok = charBoundary(target, s.end, 1); !ok {
			return nil, false
		}
		// Take back the bytes of the previous segments the insert now covers.
		for n := len(aligned); n > 0 && aligned[n-1].end > s.start; n = len(aligned) {
			last := &aligned[n-1]
			if last.copy && last.start < s.start {
				last.end = s.start
				break
			}
			s.start = min(s.start, last.start)
			s.end = max(s.end, last.end)
			aligned = aligned[:n-1]
		}
		if n := len(aligned); n > 0 && !aligned[n-1].copy {
			aligned[n-1].end = s.end
		} else {
			aligned = append(aligned, s)
		}
	}
	return aligned, true
}

// charBoundary moves pos by step to a character boundary of target. In valid
// UTF-8 a boundary is at most utf8.UTFMax-1 bytes away: stopping there keeps the
// work constant on a long run of continuation bytes, which isn't valid UTF-8.
func charBoundary(target []byte, pos, step int) (int, bool) {
	for range utf8.UTFMax {
		if pos <= 0 || pos >= len(target) || utf8.RuneStart(target[pos]) {
			return pos, true
		}
		pos += step
	}
	return pos, false
}

func encode(segments []segment, target []byte, checksum uint64, sizeHint int) []byte {
	b := make([]byte, 0, sizeHint+16)
	b = putInt(b, uint64(len(target)))
	b = append(b, '\n')
	for _, s := range segments {
		b = putInt(b, uint64(s.end-s.start))
		if s.copy {
			b = append(b, '@')
			b = putInt(b, uint64(s.offset))
			b = append(b, ',')
		} else {
			b = append(b, ':')
			b = append(b, target[s.start:s.end]...)
		}
	}
	b = putInt(b, checksum)
	return append(b, ';')
}

// getInt reads an integer at pos, returning it and the position after it. An
// integer too large for a delta reads as math.MaxUint64.
func getInt(b []byte, pos int) (uint64, int) {
	var v uint64
	for ; pos < len(b) && digitValues[b[pos]] >= 0; pos++ {
		if v > math.MaxUint32 {
			v = math.MaxUint64
			continue
		}
		v = v<<6 | uint64(digitValues[b[pos]])
	}
	return v, pos
}

func putInt(b []byte, v uint64) []byte {
	if v == 0 {
		return append(b, '0')
	}
	var buf [11]byte
	i := len(buf)
	for ; v > 0; v >>= 6 {
		i--
		buf[i] = digits[v&0x3f]
	}
	return append(b, buf[i:]...)
}
