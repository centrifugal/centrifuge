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
	pubs = uniquePublications(pubs)
	assert.Equal(t, 8, len(pubs))
}

func TestUint64Sequence(t *testing.T) {
	s := Uint64Sequence(0, 0)
	assert.Equal(t, uint64(0), s)

	s = Uint64Sequence(1, 0)
	assert.Equal(t, uint64(1), s)

	s = Uint64Sequence(0, 1)
	assert.Equal(t, uint64(1<<32-1), s)

	s = Uint64Sequence(1, 1)
	assert.Equal(t, uint64(1<<32), s)
}

func TestNextSeqGen(t *testing.T) {
	nextSeq, nextGen := NextSeqGen(0, 0)
	assert.Equal(t, uint32(1), nextSeq)
	assert.Equal(t, uint32(0), nextGen)

	nextSeq, nextGen = NextSeqGen(1, 0)
	assert.Equal(t, uint32(2), nextSeq)
	assert.Equal(t, uint32(0), nextGen)

	nextSeq, nextGen = NextSeqGen(1, 1)
	assert.Equal(t, uint32(2), nextSeq)
	assert.Equal(t, uint32(1), nextGen)

	nextSeq, nextGen = NextSeqGen(maxSeq, 0)
	assert.Equal(t, uint32(0), nextSeq)
	assert.Equal(t, uint32(1), nextGen)

	nextSeq, nextGen = NextSeqGen(maxSeq-1, 0)
	assert.Equal(t, maxSeq, nextSeq)
	assert.Equal(t, uint32(0), nextGen)
}
