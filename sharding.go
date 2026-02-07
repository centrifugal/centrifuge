package centrifuge

import (
	"hash/fnv"
)

// ShardFunc determines which shard handles a channel.
// Returns shard index in range [0, numShards).
type ShardFunc func(channel string, numShards int) int

// DefaultShardFunc uses Jump Consistent Hash for minimal key redistribution
// when adding shards. This is the recommended default for most use cases.
var DefaultShardFunc ShardFunc = consistentIndex

// consistentIndex is an adapted function from https://github.com/dgryski/go-jump
// package by Damian Gryski. It implements the Jump Consistent Hash algorithm
// from the Google paper "A Fast, Minimal Memory, Consistent Hash Algorithm"
// (Lamping & Veach, 2014).
//
// It consistently chooses a hash bucket number in the range [0, numBuckets)
// for the given string. numBuckets must be >= 1.
//
// Key property: When adding a shard, only ~1/(n+1) keys are redistributed.
// This is critical for minimizing data movement when scaling.
func consistentIndex(s string, numBuckets int) int {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(s))
	key := hash.Sum64()

	var (
		b int64 = -1
		j int64
	)

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int(b)
}
