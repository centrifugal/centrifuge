package centrifuge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextSeqGen(t *testing.T) {
	nextSeq, nextGen := nextSeqGen(0, 0)
	assert.Equal(t, uint32(1), nextSeq)
	assert.Equal(t, uint32(0), nextGen)

	nextSeq, nextGen = nextSeqGen(1, 0)
	assert.Equal(t, uint32(2), nextSeq)
	assert.Equal(t, uint32(0), nextGen)

	nextSeq, nextGen = nextSeqGen(1, 1)
	assert.Equal(t, uint32(2), nextSeq)
	assert.Equal(t, uint32(1), nextGen)

	nextSeq, nextGen = nextSeqGen(maxSeq, 0)
	assert.Equal(t, uint32(0), nextSeq)
	assert.Equal(t, uint32(1), nextGen)

	nextSeq, nextGen = nextSeqGen(maxSeq-1, 0)
	assert.Equal(t, maxSeq, nextSeq)
	assert.Equal(t, uint32(0), nextGen)
}

func TestUint64Sequence(t *testing.T) {
	s := uint64Sequence(0, 0)
	assert.Equal(t, uint64(0), s)

	s = uint64Sequence(1, 0)
	assert.Equal(t, uint64(1), s)

	s = uint64Sequence(0, 1)
	assert.Equal(t, uint64(1<<32-1), s)

	s = uint64Sequence(1, 1)
	assert.Equal(t, uint64(1<<32), s)
}

func TestStringInSlice(t *testing.T) {
	assert.True(t, stringInSlice("test", []string{"boom", "test"}))
	assert.False(t, stringInSlice("test", []string{"boom", "testing"}))
}
