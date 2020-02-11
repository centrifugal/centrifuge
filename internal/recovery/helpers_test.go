package recovery

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/assert"
)

func TestUnique(t *testing.T) {
	pubs := []*protocol.Publication{
		{Seq: 101, Gen: 0},
		{Seq: 101, Gen: 1},
		{Seq: 101, Gen: 1},
		{Seq: 100, Gen: 2},
		{Seq: 99},
		{Seq: 98},
		{Seq: 4294967295, Gen: 0},
		{Seq: 4294967295, Gen: 1},
		{Seq: 4294967295, Gen: 4294967295},
		{Seq: 4294967295, Gen: 4294967295},
	}
	pubs = UniquePublications(pubs)
	assert.Equal(t, 8, len(pubs))
}
