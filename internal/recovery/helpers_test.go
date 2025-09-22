package recovery

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUnique(t *testing.T) {
	pubs := []*protocol.Publication{
		{Offset: 101, Data: protocol.Raw(`{}`)},
		{Offset: 102},
		{Offset: 100},
		{Offset: 101},
		{Offset: 99},
		{Offset: 98},
		{Offset: 97, Time: -1}, // Filtered publication.
	}
	pubs, maxSeenOffset, _ := uniqueNonFilteredPublications(pubs)
	require.Equal(t, 5, len(pubs))
	require.Equal(t, uint64(102), maxSeenOffset)
}

func TestMergePublicationsNoBuffered(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	pubs, maxSeenOffset, ok := MergePublications(recoveredPubs, nil)
	require.True(t, ok)
	require.Len(t, pubs, 2)
	require.Equal(t, uint64(0), maxSeenOffset)
}

func TestMergePublicationsBuffered(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	bufferedPubs := []*protocol.Publication{
		{Offset: 3},
	}
	pubs, maxSeenOffset, ok := MergePublications(recoveredPubs, bufferedPubs)
	require.True(t, ok)
	require.Len(t, pubs, 3)
	require.Equal(t, uint64(3), maxSeenOffset)
}
