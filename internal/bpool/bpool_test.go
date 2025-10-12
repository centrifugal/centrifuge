package bpool

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetPutConcurrent(t *testing.T) {
	const concurrency = 10
	doneCh := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for capacity := 0; capacity < 100; capacity++ {
				bb := GetByteBuffer(capacity)
				if len(bb.B) > 0 {
					panic(fmt.Errorf("len(bb.B) must be zero; got %d", len(bb.B)))
				}
				if capacity < 0 {
					capacity = 0
				}
				bb.B = append(bb.B, make([]byte, capacity)...)
				PutByteBuffer(bb)
			}
			doneCh <- struct{}{}
		}()
	}
	tc := time.After(10 * time.Second)
	for i := 0; i < concurrency; i++ {
		select {
		case <-tc:
			t.Fatalf("timeout")
		case <-doneCh:
		}
	}
}

func TestGetCapacity(t *testing.T) {
	for i := 1; i < 130; i++ {
		idx := nextLogBase2(uint32(i))
		b := GetByteBuffer(i)
		require.Equal(t, 1<<idx, cap(b.B))
		PutByteBuffer(b)
	}
}

func TestByteBufferWrite(t *testing.T) {
	tests := []struct {
		name     string
		initial  []byte
		write    []byte
		expected []byte
	}{
		{
			name:     "write to empty buffer",
			initial:  nil,
			write:    []byte("hello"),
			expected: []byte("hello"),
		},
		{
			name:     "write to non-empty buffer",
			initial:  []byte("hello"),
			write:    []byte(" world"),
			expected: []byte("hello world"),
		},
		{
			name:     "write empty slice",
			initial:  []byte("test"),
			write:    []byte{},
			expected: []byte("test"),
		},
		{
			name:     "write to nil buffer",
			initial:  nil,
			write:    []byte("data"),
			expected: []byte("data"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := &ByteBuffer{B: tt.initial}
			n, err := bb.Write(tt.write)
			require.NoError(t, err)
			require.Equal(t, len(tt.write), n)
			require.Equal(t, tt.expected, bb.B)
		})
	}
}

func TestPrevLogBase2ReturnNextMinus1(t *testing.T) {
	// Test cases where prevLogBase2 returns next - 1
	// This happens when num is not a power of 2
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{3, 1},   // nextLogBase2(3) = 2, but 3 != 2^2, so return 2-1 = 1
		{5, 2},   // nextLogBase2(5) = 3, but 5 != 2^3, so return 3-1 = 2
		{6, 2},   // nextLogBase2(6) = 3, but 6 != 2^3, so return 3-1 = 2
		{7, 2},   // nextLogBase2(7) = 3, but 7 != 2^3, so return 3-1 = 2
		{9, 3},   // nextLogBase2(9) = 4, but 9 != 2^4, so return 4-1 = 3
		{15, 3},  // nextLogBase2(15) = 4, but 15 != 2^4, so return 4-1 = 3
		{17, 4},  // nextLogBase2(17) = 5, but 17 != 2^5, so return 5-1 = 4
		{31, 4},  // nextLogBase2(31) = 5, but 31 != 2^5, so return 5-1 = 4
		{100, 6}, // nextLogBase2(100) = 7, but 100 != 2^7, so return 7-1 = 6
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input_%d", tt.input), func(t *testing.T) {
			result := prevLogBase2(tt.input)
			require.Equal(t, tt.expected, result)

			// Verify this is indeed the "next - 1" case
			next := nextLogBase2(tt.input)
			powerOfTwo := uint32(1) << next
			require.NotEqual(t, tt.input, powerOfTwo, "input should not be a power of 2 for this test")
			require.Equal(t, next-1, result, "result should be next - 1")
		})
	}
}

func TestGetByteBufferMaxBufferLength(t *testing.T) {
	// Test the condition: if length > maxBufferLength
	tests := []struct {
		name     string
		length   int
		expected int
	}{
		{
			name:     "exactly max buffer length",
			length:   maxBufferLength,
			expected: maxBufferLength,
		},
		{
			name:     "one more than max buffer length",
			length:   maxBufferLength + 1,
			expected: maxBufferLength + 1,
		},
		{
			name:     "much larger than max buffer length",
			length:   maxBufferLength * 2,
			expected: maxBufferLength * 2,
		},
		{
			name:     "significantly larger than max buffer length",
			length:   1000000,
			expected: 1000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bb := GetByteBuffer(tt.length)
			require.NotNil(t, bb)
			require.NotNil(t, bb.B)
			require.Equal(t, 0, len(bb.B), "buffer should be empty")
			require.Equal(t, tt.expected, cap(bb.B), "capacity should match requested length")

			if tt.length > maxBufferLength {
				// For buffers larger than maxBufferLength, they should not be pooled
				// This means PutByteBuffer should drop them
				originalCap := cap(bb.B)
				PutByteBuffer(bb)

				// Get a new buffer of the same size - it should be a fresh allocation
				bb2 := GetByteBuffer(tt.length)
				require.Equal(t, originalCap, cap(bb2.B))
				require.Equal(t, 0, len(bb2.B))
			}
		})
	}
}
