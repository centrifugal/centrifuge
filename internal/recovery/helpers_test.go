package recovery

import (
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

func TestMergePublicationsNoBuffered(t *testing.T) {
	recoveredPubs := []*protocol.Publication{
		{Offset: 1},
		{Offset: 2},
	}
	pubs, ok := MergePublications(recoveredPubs, nil)
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
	pubs, ok := MergePublications(recoveredPubs, bufferedPubs)
	require.True(t, ok)
	require.Len(t, pubs, 3)
}
