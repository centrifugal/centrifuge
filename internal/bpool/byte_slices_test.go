package bpool

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetPutByteSlicesConcurrent(t *testing.T) {
	const concurrency = 10
	doneCh := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for capacity := 0; capacity < 100; capacity++ {
				buf := GetByteSlicesBuf(capacity)
				if len(buf.B) > 0 {
					panic(fmt.Errorf("len(buf.B) must be zero; got %d", len(buf.B)))
				}
				if capacity < 0 {
					capacity = 0
				}
				// Simulate usage by appending items
				for j := 0; j < capacity; j++ {
					buf.B = append(buf.B, []byte(fmt.Sprintf("item-%d", j)))
				}
				PutByteSlicesBuf(buf)
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

func TestGetByteSlicesBufCapacity(t *testing.T) {
	tests := []struct {
		length   int
		expected int
	}{
		{0, 16},    // default
		{1, 1},     // 2^0
		{2, 2},     // 2^1
		{3, 4},     // 2^2
		{4, 4},     // 2^2
		{5, 8},     // 2^3
		{8, 8},     // 2^3
		{9, 16},    // 2^4
		{16, 16},   // 2^4
		{17, 32},   // 2^5
		{32, 32},   // 2^5
		{33, 64},   // 2^6
		{64, 64},   // 2^6
		{65, 128},  // 2^7
		{128, 128}, // 2^7
		{129, 256}, // 2^8
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("length_%d", tt.length), func(t *testing.T) {
			buf := GetByteSlicesBuf(tt.length)
			require.Equal(t, 0, len(buf.B), "buffer should start empty")
			require.Equal(t, tt.expected, cap(buf.B), "capacity should be next power of 2")
			PutByteSlicesBuf(buf)
		})
	}
}

func TestGetByteSlicesBufMaxLength(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		expected int
	}{
		{
			name:     "exactly max buffer length",
			length:   maxByteSlicesBufLength,
			expected: maxByteSlicesBufLength,
		},
		{
			name:     "one more than max buffer length",
			length:   maxByteSlicesBufLength + 1,
			expected: maxByteSlicesBufLength + 1,
		},
		{
			name:     "much larger than max buffer length",
			length:   maxByteSlicesBufLength * 2,
			expected: maxByteSlicesBufLength * 2,
		},
		{
			name:     "significantly larger than max buffer length",
			length:   10000,
			expected: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := GetByteSlicesBuf(tt.length)
			require.NotNil(t, buf)
			require.NotNil(t, buf.B)
			require.Equal(t, 0, len(buf.B), "buffer should be empty")
			require.Equal(t, tt.expected, cap(buf.B), "capacity should match requested length")

			if tt.length > maxByteSlicesBufLength {
				// For buffers larger than maxByteSlicesBufLength, they should not be pooled
				// This means PutByteSlicesBuf should drop them
				originalCap := cap(buf.B)
				PutByteSlicesBuf(buf)

				// Get a new buffer of the same size - it should be a fresh allocation
				buf2 := GetByteSlicesBuf(tt.length)
				require.Equal(t, originalCap, cap(buf2.B))
				require.Equal(t, 0, len(buf2.B))
			}
		})
	}
}

func TestByteSlicesBufReuse(t *testing.T) {
	// Get a buffer, use it, put it back, get it again
	length := 10
	buf1 := GetByteSlicesBuf(length)
	require.Equal(t, 0, len(buf1.B))
	require.GreaterOrEqual(t, cap(buf1.B), length)

	// Use the buffer
	for i := 0; i < length; i++ {
		buf1.B = append(buf1.B, []byte(fmt.Sprintf("data-%d", i)))
	}
	require.Equal(t, length, len(buf1.B))

	// Return to pool
	PutByteSlicesBuf(buf1)

	// The buffer should have been cleared
	require.Equal(t, 0, len(buf1.B))
	for i := range buf1.B {
		require.Nil(t, buf1.B[i], "elements should be cleared")
	}

	// Get another buffer - might be the same one from pool
	buf2 := GetByteSlicesBuf(length)
	require.Equal(t, 0, len(buf2.B))
	require.GreaterOrEqual(t, cap(buf2.B), length)
}

func TestByteSlicesBufClearing(t *testing.T) {
	// Test that PutByteSlicesBuf properly clears references
	buf := GetByteSlicesBuf(5)

	// Add some data
	testData := [][]byte{
		[]byte("test1"),
		[]byte("test2"),
		[]byte("test3"),
	}
	for _, data := range testData {
		buf.B = append(buf.B, data)
	}

	require.Equal(t, 3, len(buf.B))

	// Put it back - should clear all references
	PutByteSlicesBuf(buf)

	// Buffer should be empty
	require.Equal(t, 0, len(buf.B))
}

func TestPrevLogBase2ByteSlicesReturnNextMinus1(t *testing.T) {
	// Test cases where prevLogBase2ByteSlices returns next - 1
	// This happens when num is not a power of 2
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{3, 1},   // nextLogBase2ByteSlices(3) = 2, but 3 != 2^2, so return 2-1 = 1
		{5, 2},   // nextLogBase2ByteSlices(5) = 3, but 5 != 2^3, so return 3-1 = 2
		{6, 2},   // nextLogBase2ByteSlices(6) = 3, but 6 != 2^3, so return 3-1 = 2
		{7, 2},   // nextLogBase2ByteSlices(7) = 3, but 7 != 2^3, so return 3-1 = 2
		{9, 3},   // nextLogBase2ByteSlices(9) = 4, but 9 != 2^4, so return 4-1 = 3
		{15, 3},  // nextLogBase2ByteSlices(15) = 4, but 15 != 2^4, so return 4-1 = 3
		{17, 4},  // nextLogBase2ByteSlices(17) = 5, but 17 != 2^5, so return 5-1 = 4
		{31, 4},  // nextLogBase2ByteSlices(31) = 5, but 31 != 2^5, so return 5-1 = 4
		{100, 6}, // nextLogBase2ByteSlices(100) = 7, but 100 != 2^7, so return 7-1 = 6
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input_%d", tt.input), func(t *testing.T) {
			result := prevLogBase2ByteSlices(tt.input)
			require.Equal(t, tt.expected, result)

			// Verify this is indeed the "next - 1" case
			next := nextLogBase2ByteSlices(tt.input)
			powerOfTwo := uint32(1) << next
			require.NotEqual(t, tt.input, powerOfTwo, "input should not be a power of 2 for this test")
			require.Equal(t, next-1, result, "result should be next - 1")
		})
	}
}

func TestNextLogBase2ByteSlicesPowerOfTwo(t *testing.T) {
	// Test that for powers of 2, nextLogBase2ByteSlices returns the exact log
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{1, 0},     // 2^0
		{2, 1},     // 2^1
		{4, 2},     // 2^2
		{8, 3},     // 2^3
		{16, 4},    // 2^4
		{32, 5},    // 2^5
		{64, 6},    // 2^6
		{128, 7},   // 2^7
		{256, 8},   // 2^8
		{512, 9},   // 2^9
		{1024, 10}, // 2^10
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input_%d", tt.input), func(t *testing.T) {
			result := nextLogBase2ByteSlices(tt.input)
			require.Equal(t, tt.expected, result)
			require.Equal(t, tt.input, uint32(1)<<result, "2^result should equal input")
		})
	}
}

func TestPrevLogBase2ByteSlicesPowerOfTwo(t *testing.T) {
	// Test that for powers of 2, prevLogBase2ByteSlices returns the exact log
	tests := []struct {
		input    uint32
		expected uint32
	}{
		{1, 0},     // 2^0
		{2, 1},     // 2^1
		{4, 2},     // 2^2
		{8, 3},     // 2^3
		{16, 4},    // 2^4
		{32, 5},    // 2^5
		{64, 6},    // 2^6
		{128, 7},   // 2^7
		{256, 8},   // 2^8
		{512, 9},   // 2^9
		{1024, 10}, // 2^10
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("input_%d", tt.input), func(t *testing.T) {
			result := prevLogBase2ByteSlices(tt.input)
			require.Equal(t, tt.expected, result)
			require.Equal(t, tt.input, uint32(1)<<result, "2^result should equal input")
		})
	}
}

func TestByteSlicesBufZeroLength(t *testing.T) {
	// Test that requesting 0 length returns default capacity
	buf := GetByteSlicesBuf(0)
	require.NotNil(t, buf)
	require.Equal(t, 0, len(buf.B))
	require.Equal(t, 16, cap(buf.B), "should use default capacity of 16")
	PutByteSlicesBuf(buf)
}

func TestByteSlicesBufNegativeLength(t *testing.T) {
	// Test that negative length is treated as 0 and returns default capacity
	buf := GetByteSlicesBuf(-5)
	require.NotNil(t, buf)
	require.Equal(t, 0, len(buf.B))
	require.Equal(t, 16, cap(buf.B), "should use default capacity of 16")
	PutByteSlicesBuf(buf)
}

func TestGetByteSlicesBufZeros(t *testing.T) {
	require.Zero(t, prevLogBase2ByteSlices(0))
	require.Zero(t, nextLogBase2ByteSlices(0))
}
