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

func TestStringInSlice(t *testing.T) {
	assert.True(t, stringInSlice("test", []string{"boom", "test"}))
	assert.False(t, stringInSlice("test", []string{"boom", "testing"}))
}
