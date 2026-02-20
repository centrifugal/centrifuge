package centrifuge

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestByteArena_CopyBytes(t *testing.T) {
	a := byteArena{}

	t.Run("nil_input", func(t *testing.T) {
		require.Nil(t, a.copyBytes(nil))
	})
	t.Run("empty_input", func(t *testing.T) {
		require.Nil(t, a.copyBytes([]byte{}))
	})
	t.Run("copy_is_independent", func(t *testing.T) {
		src := []byte("hello")
		got := a.copyBytes(src)
		require.Equal(t, []byte("hello"), got)
		// Mutating source must not affect copy.
		src[0] = 'H'
		require.Equal(t, byte('h'), got[0])
	})
	t.Run("cap_equals_len", func(t *testing.T) {
		got := a.copyBytes([]byte("abc"))
		require.Equal(t, len(got), cap(got))
	})
	t.Run("growth", func(t *testing.T) {
		a2 := byteArena{}
		// Fill beyond initial chunk.
		big := make([]byte, 5000)
		for i := range big {
			big[i] = byte(i % 256)
		}
		got := a2.copyBytes(big)
		require.Equal(t, big, got)
		require.Equal(t, len(got), cap(got))
	})
}

func TestByteArena_CopyString(t *testing.T) {
	a := byteArena{}

	t.Run("nil_input", func(t *testing.T) {
		require.Equal(t, "", a.copyString(nil))
	})
	t.Run("empty_input", func(t *testing.T) {
		require.Equal(t, "", a.copyString([]byte{}))
	})
	t.Run("copies_content", func(t *testing.T) {
		src := []byte("world")
		got := a.copyString(src)
		require.Equal(t, "world", got)
		src[0] = 'W'
		require.Equal(t, "world", got)
	})
}

func putUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func putUint32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func TestPgRawInt64(t *testing.T) {
	require.Equal(t, int64(0), pgRawInt64(nil))
	require.Equal(t, int64(0), pgRawInt64(putUint64(0)))
	require.Equal(t, int64(42), pgRawInt64(putUint64(42)))
	require.Equal(t, int64(-1), pgRawInt64(putUint64(math.MaxUint64)))
	require.Equal(t, int64(math.MinInt64), pgRawInt64(putUint64(1<<63)))
}

func TestPgRawUint64(t *testing.T) {
	require.Equal(t, uint64(0), pgRawUint64(nil))
	require.Equal(t, uint64(0), pgRawUint64(putUint64(0)))
	require.Equal(t, uint64(123456789), pgRawUint64(putUint64(123456789)))
	require.Equal(t, uint64(math.MaxUint64), pgRawUint64(putUint64(math.MaxUint64)))
}

func TestPgRawInt32(t *testing.T) {
	require.Equal(t, int32(0), pgRawInt32(nil))
	require.Equal(t, int32(0), pgRawInt32(putUint32(0)))
	require.Equal(t, int32(100), pgRawInt32(putUint32(100)))
	require.Equal(t, int32(-1), pgRawInt32(putUint32(math.MaxUint32)))
}

func TestPgRawBool(t *testing.T) {
	require.False(t, pgRawBool(nil))
	require.True(t, pgRawBool([]byte{1}))
	require.False(t, pgRawBool([]byte{0}))
}

func TestPgRawString(t *testing.T) {
	a := byteArena{}
	require.Equal(t, "", pgRawString(&a, nil))
	require.Equal(t, "hello", pgRawString(&a, []byte("hello")))
}

func TestPgRawBytes(t *testing.T) {
	a := byteArena{}
	require.Nil(t, pgRawBytes(&a, nil))
	require.Equal(t, []byte{0xde, 0xad}, pgRawBytes(&a, []byte{0xde, 0xad}))
}

func TestPgRawJSONBMap(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, pgRawJSONBMap(nil))
	})
	t.Run("too_short", func(t *testing.T) {
		require.Nil(t, pgRawJSONBMap([]byte{1}))
	})
	t.Run("empty_object", func(t *testing.T) {
		// Version byte (1) + empty JSON object.
		b := append([]byte{1}, []byte("{}")...)
		got := pgRawJSONBMap(b)
		require.NotNil(t, got)
		require.Empty(t, got)
	})
	t.Run("valid_map", func(t *testing.T) {
		b := append([]byte{1}, []byte(`{"a":"1","b":"2"}`)...)
		got := pgRawJSONBMap(b)
		require.Equal(t, map[string]string{"a": "1", "b": "2"}, got)
	})
}
