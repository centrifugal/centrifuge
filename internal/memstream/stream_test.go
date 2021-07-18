package memstream

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	s := New()

	const streamSize = 5

	// Fill stream with values.
	for i := 0; i < streamSize; i++ {
		seq, err := s.Add([]byte(strconv.Itoa(i+1)), streamSize)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), seq)
	}
	require.Equal(t, 5, s.list.Len())
	require.Equal(t, 5, len(s.index))

	items, streamTop, err := s.Get(5, true, 3, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Equal(t, []Item{{5, []byte("5")}}, items)

	items, streamTop, err = s.Get(5, true, 2, true)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Equal(t, []Item{{5, []byte("5")}, {4, []byte("4")}}, items)

	items, streamTop, err = s.Get(5, true, 0, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Nil(t, items)

	items, streamTop, err = s.Get(5, true, 0, true)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Nil(t, items)

	items, streamTop, err = s.Get(6, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Nil(t, items)

	items, streamTop, err = s.Get(0, false, 2, true)
	// Returns all items.
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Len(t, items, 2)

	items, streamTop, err = s.Get(1, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Equal(t, []Item{{1, []byte("1")}, {2, []byte("2")}}, items)

	_, err = s.Add([]byte("6"), streamSize)
	require.NoError(t, err)

	items, streamTop, err = s.Get(1, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{{2, []byte("2")}, {3, []byte("3")}}, items)

	items, streamTop, err = s.Get(2, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{{2, []byte("2")}, {3, []byte("3")}}, items)

	items, streamTop, err = s.Get(5, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{{5, []byte("5")}, {6, []byte("6")}}, items)

	items, streamTop, err = s.Get(5, true, 2, true)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{{5, []byte("5")}, {4, []byte("4")}}, items)

	_, err = s.Add([]byte("7"), streamSize)
	require.NoError(t, err)
	_, streamTop, err = s.Get(5, true, 2, false)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(7))
	require.Equal(t, 5, len(s.index))

	s.Reset()
	require.Equal(t, uint64(0), s.Top())
	require.Equal(t, 0, len(s.index))
	require.Equal(t, 0, s.list.Len())
	_, err = s.Add([]byte("8"), streamSize)
	require.NoError(t, err)
}

func TestStreamGetAll(t *testing.T) {
	s := New()
	items, streamTop, err := s.Get(0, false, 0, false)
	require.NoError(t, err)
	require.Equal(t, uint64(0), streamTop)
	require.Nil(t, items)
	_, err = s.Add([]byte("1"), 2)
	require.NoError(t, err)
	items, _, err = s.Get(0, false, 200, false)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestStreamClear(t *testing.T) {
	s := New()
	const streamSize = 5
	_, err := s.Add([]byte("1"), streamSize)
	require.NoError(t, err)
	s.Clear()
	require.NotZero(t, s.Top())
	require.Equal(t, 0, len(s.index))
	require.Equal(t, 0, s.list.Len())
}

func TestStreamReset(t *testing.T) {
	s := New()
	epoch := s.Epoch()
	const streamSize = 5
	_, err := s.Add([]byte("1"), streamSize)
	require.NoError(t, err)
	s.Reset()
	require.Zero(t, s.Top())
	require.Equal(t, 0, len(s.index))
	require.Equal(t, 0, s.list.Len())
	require.NotEqual(t, epoch, s.Epoch())
}

func TestStreamOffset(t *testing.T) {
	s := New()
	const streamSize = 5
	for i := 0; i < streamSize; i++ {
		_, err := s.Add([]byte("elem"), streamSize)
		require.NoError(t, err)
	}
	items, _, err := s.Get(0, true, 1, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), items[0].Offset)
	items, _, err = s.Get(1, true, 1, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), items[0].Offset)
	items, _, err = s.Get(2, true, 1, false)
	require.NoError(t, err)
	require.Equal(t, uint64(2), items[0].Offset)
}

func TestStreamOffsetReverseNonExistingOffset(t *testing.T) {
	s := New()
	const streamSize = 2
	for i := 0; i < 10; i++ {
		_, err := s.Add([]byte("elem"), streamSize)
		require.NoError(t, err)
	}
	items, _, err := s.Get(4, true, 1, true)
	require.NoError(t, err)
	require.Len(t, items, 0)
}

func TestStreamNoLimitWithMiddle(t *testing.T) {
	s := New()
	const streamSize = 10
	for i := 0; i < streamSize; i++ {
		_, err := s.Add([]byte("elem"), streamSize)
		require.NoError(t, err)
	}
	items, _, err := s.Get(5, true, -1, false)
	require.NoError(t, err)
	require.Len(t, items, 6)
}
