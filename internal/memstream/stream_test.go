package memstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	s := New()

	const streamSize = 5

	seq, err := s.Add([]byte("1"), streamSize)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
	seq, err = s.Add([]byte("2"), streamSize)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)

	_, err = s.Add([]byte("3"), streamSize)
	require.NoError(t, err)
	_, err = s.Add([]byte("4"), streamSize)
	require.NoError(t, err)
	_, err = s.Add([]byte("5"), streamSize)
	require.NoError(t, err)

	require.Equal(t, 5, s.list.Len())
	require.Equal(t, 5, len(s.index))

	items, streamTop, err := s.Get(5, 3)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Equal(t, []Item{Item{5, []byte("5")}}, items)

	items, streamTop, err = s.Get(6, 2)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Nil(t, items)

	_, _, err = s.Get(7, 2)
	require.Error(t, err)

	items, streamTop, err = s.Get(1, 2)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(5))
	require.Equal(t, []Item{Item{1, []byte("1")}, Item{2, []byte("2")}}, items)

	_, err = s.Add([]byte("6"), streamSize)
	require.NoError(t, err)

	items, streamTop, err = s.Get(1, 2)
	require.Nil(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{Item{2, []byte("2")}, Item{3, []byte("3")}}, items)

	items, streamTop, err = s.Get(2, 2)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{Item{2, []byte("2")}, Item{3, []byte("3")}}, items)

	items, streamTop, err = s.Get(5, 2)
	require.NoError(t, err)
	require.Equal(t, streamTop, uint64(6))
	require.Equal(t, []Item{Item{5, []byte("5")}, Item{6, []byte("6")}}, items)

	_, err = s.Add([]byte("7"), streamSize)
	require.NoError(t, err)
	_, streamTop, err = s.Get(5, 2)
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
