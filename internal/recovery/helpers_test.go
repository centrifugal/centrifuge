package recovery

import (
	"math"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUnique(t *testing.T) {
	pubs := []*protocol.Publication{
		{Offset: 101},
		{Offset: 102},
		{Offset: 100},
		{Offset: 101},
		{Offset: 99},
		{Offset: 98},
	}
	pubs = uniquePublications(pubs)
	require.Equal(t, 5, len(pubs))
}

func TestUint64Sequence(t *testing.T) {
	s := PackUint64(0, 0)
	require.Equal(t, uint64(0), s)

	s = PackUint64(1, 0)
	require.Equal(t, uint64(1), s)

	s = PackUint64(0, 1)
	require.Equal(t, uint64(1<<32-1), s)

	s = PackUint64(1, 1)
	require.Equal(t, uint64(1<<32), s)
}

func TestMergePublicationsNoBuffered(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	pubs, ok := MergePublications(recoveredPubs, nil, false)
	require.True(t, ok)
	require.Len(t, pubs, 2)
}

func TestMergePublicationsBuffered(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	bufferedPubs := []*protocol.Publication{
		{Offset: 3},
	}
	pubs, ok := MergePublications(recoveredPubs, bufferedPubs, false)
	require.True(t, ok)
	require.Len(t, pubs, 3)
}

func TestMergePublicationsOrder(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	bufferedPubs := []*protocol.Publication{
		{Offset: 3},
	}
	pubs, ok := MergePublications(recoveredPubs, bufferedPubs, true)
	require.True(t, ok)
	require.Len(t, pubs, 3)
	require.True(t, pubs[0].Offset > pubs[1].Offset)

	pubs, ok = MergePublications(recoveredPubs, bufferedPubs, false)
	require.True(t, ok)
	require.Len(t, pubs, 3)
	require.True(t, pubs[0].Offset < pubs[1].Offset)
}

func TestUnpackUint64(t *testing.T) {
	seq, gen := UnpackUint64(1)
	require.Equal(t, uint32(1), seq)
	require.Equal(t, uint32(0), gen)

	seq, gen = UnpackUint64(math.MaxUint64)
	require.Equal(t, uint32(math.MaxUint32), seq)
	require.Equal(t, uint32(math.MaxUint32), gen)

	seq, gen = UnpackUint64(math.MaxUint32 + 1)
	require.Equal(t, uint32(0), seq)
	require.Equal(t, uint32(1), gen)
}
