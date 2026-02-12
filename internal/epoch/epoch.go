// Package epoch provides epoch string generation for stream position tracking.
package epoch

import (
	"crypto/rand"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Generate creates a random 8-character epoch string using crypto/rand.
// With 52^8 ≈ 5.3×10^13 possible values, collision probability is negligible.
func Generate() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = letters[b[i]%byte(len(letters))]
	}
	return string(b)
}
