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

// TestMergePublications_AllBufferedFiltered_NoBrokerPubs asserts that
// MergePublications does not panic when there are 2+ buffered pubs all marked
// filtered (Time = -1) and broker history is empty.
//
// Bug: after `uniqueNonFilteredPublications` strips all Time=-1 entries, the
// internal slice is empty, but the next line dereferences `recoveredPubs[0]`
// without a length guard — index out of range panic. Reachable when a
// recovery+tagsFilter subscription's buffered window only saw filtered pubs.
func TestMergePublications_AllBufferedFiltered_NoBrokerPubs(t *testing.T) {
	bufferedPubs := []*protocol.Publication{
		{Offset: 5, Time: -1},
		{Offset: 6, Time: -1},
	}
	pubs, maxSeenOffset, ok := MergePublications(nil, bufferedPubs)
	require.True(t, ok,
		"merge must succeed when no real pubs survive filtering; got ok=false (a downstream check would re-subscribe the client even though continuity is intact)")
	require.Empty(t, pubs)
	require.Equal(t, uint64(6), maxSeenOffset,
		"maxSeenOffset should reflect the highest offset across all merged inputs (including filtered) so the caller can advance position past skipped offsets")
}

// TestMergePublications_AllBufferedFiltered_WithBrokerPubs asserts the same
// safety property when broker has a recovered pub plus several filtered
// buffered pubs. After dedup, the broker pub remains; the filtered ones go to
// skippedOffsets. The result should not panic, ok must be true, and
// maxSeenOffset must reflect the largest offset including filtered.
func TestMergePublications_AllBufferedFiltered_WithBrokerPubs(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 5},
	}
	bufferedPubs := []*protocol.Publication{
		{Offset: 6, Time: -1},
		{Offset: 7, Time: -1},
	}
	pubs, maxSeenOffset, ok := MergePublications(recoveredPubs, bufferedPubs)
	require.True(t, ok)
	require.Len(t, pubs, 1)
	require.Equal(t, uint64(5), pubs[0].Offset)
	require.Equal(t, uint64(7), maxSeenOffset)
}
