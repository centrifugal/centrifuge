package bpool

import (
	"math/bits"
	"sync"
)

const (
	// maxByteSlicesBufLength is the maximum length for pooled [][]byte buffers.
	maxByteSlicesBufLength = 4096 // 2^12
)

// ByteSlicesBuf wraps [][]byte to avoid allocations when using sync.Pool
type ByteSlicesBuf struct {
	B [][]byte
}

// byteSlicesBufPools contain pools for [][]byte slices of various capacities (power of 2).
// Supports up to 2^12 = 4096 items.
var byteSlicesBufPools [13]sync.Pool

// GetByteSlicesBuf returns a ByteSlicesBuf with capacity >= length
func GetByteSlicesBuf(length int) *ByteSlicesBuf {
	if length <= 0 {
		length = 16 // default
	}
	if length > maxByteSlicesBufLength {
		return &ByteSlicesBuf{
			B: make([][]byte, 0, length),
		}
	}
	idx := nextLogBase2ByteSlices(uint32(length))
	if v := byteSlicesBufPools[idx].Get(); v != nil {
		buf := v.(*ByteSlicesBuf)
		buf.B = buf.B[:0]
		return buf
	}
	capacity := 1 << idx
	return &ByteSlicesBuf{
		B: make([][]byte, 0, capacity),
	}
}

// PutByteSlicesBuf returns buf to the pool
func PutByteSlicesBuf(buf *ByteSlicesBuf) {
	capacity := cap(buf.B)
	if capacity == 0 || capacity > maxByteSlicesBufLength {
		return // drop oversized buffers
	}
	idx := prevLogBase2ByteSlices(uint32(capacity))
	// Clear the buffer
	for i := range buf.B {
		buf.B[i] = nil
	}
	buf.B = buf.B[:0]
	byteSlicesBufPools[idx].Put(buf)
}

// nextLogBase2ByteSlices returns log2(v) rounded up
func nextLogBase2ByteSlices(v uint32) uint32 {
	if v == 0 {
		return 0
	}
	return uint32(32 - bits.LeadingZeros32(v-1))
}

// prevLogBase2ByteSlices returns log2(v) rounded down
func prevLogBase2ByteSlices(v uint32) uint32 {
	if v == 0 {
		return 0
	}
	next := nextLogBase2ByteSlices(v)
	if v == (1 << next) {
		return next
	}
	return next - 1
}
