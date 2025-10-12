package filter

import (
	"fmt"
	"sync"
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/require"
)

func TestFilterAll(t *testing.T) {
	tests := []struct {
		filter   *protocol.FilterNode
		tags     map[string]string
		expected bool
		desc     string
	}{
		// --- Leaf EQ ---
		{&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}, map[string]string{"env": "prod"}, true, "EQ matches"},
		{&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}, map[string]string{"env": "staging"}, false, "EQ does not match"},
		{&protocol.FilterNode{Cmp: CompareEQ, Key: "env", Val: "prod"}, map[string]string{}, false, "EQ missing key"},

		// --- Leaf NOT_EQ ---
		{&protocol.FilterNode{Cmp: CompareNotEQ, Key: "tier", Val: "bronze"}, map[string]string{"tier": "silver"}, true, "NOT_EQ different value"},
		{&protocol.FilterNode{Cmp: CompareNotEQ, Key: "tier", Val: "bronze"}, map[string]string{"tier": "bronze"}, false, "NOT_EQ same value"},
		{&protocol.FilterNode{Cmp: CompareNotEQ, Key: "tier", Val: "bronze"}, map[string]string{}, true, "NOT_EQ missing key counts as not equal"},

		// --- Leaf IN ---
		{&protocol.FilterNode{Cmp: CompareIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{"region": "us"}, true, "IN value present"},
		{&protocol.FilterNode{Cmp: CompareIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{"region": "asia"}, false, "IN value absent"},
		{&protocol.FilterNode{Cmp: CompareIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{}, false, "IN missing key"},

		// --- Leaf NOT_IN ---
		{&protocol.FilterNode{Cmp: CompareNotIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{"region": "asia"}, true, "NOT_IN value absent"},
		{&protocol.FilterNode{Cmp: CompareNotIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{"region": "us"}, false, "NOT_IN value present"},
		{&protocol.FilterNode{Cmp: CompareNotIn, Key: "region", Vals: []string{"us", "eu"}}, map[string]string{}, true, "NOT_IN missing key counts as not in set"},

		// --- Leaf EXISTS ---
		{&protocol.FilterNode{Cmp: CompareExists, Key: "debug"}, map[string]string{"debug": "1"}, true, "EXISTS present"},
		{&protocol.FilterNode{Cmp: CompareExists, Key: "debug"}, map[string]string{}, false, "EXISTS missing"},

		// --- Leaf NOT_EXISTS ---
		{&protocol.FilterNode{Cmp: CompareNotExists, Key: "debug"}, map[string]string{}, true, "NOT_EXISTS missing"},
		{&protocol.FilterNode{Cmp: CompareNotExists, Key: "debug"}, map[string]string{"debug": "1"}, false, "NOT_EXISTS present"},

		// -- Numeric comparisons ---
		{&protocol.FilterNode{Cmp: CompareGT, Key: "amount", Val: "42"}, map[string]string{"amount": "100"}, true, "GT greater"},
		{&protocol.FilterNode{Cmp: CompareGT, Key: "amount", Val: "42"}, map[string]string{"amount": "10"}, false, "GT less"},
		{&protocol.FilterNode{Cmp: CompareGTE, Key: "amount", Val: "42"}, map[string]string{"amount": "100"}, true, "GTE greater"},
		{&protocol.FilterNode{Cmp: CompareGTE, Key: "amount", Val: "42"}, map[string]string{"amount": "42"}, true, "GTE equal"},
		{&protocol.FilterNode{Cmp: CompareGTE, Key: "amount", Val: "42"}, map[string]string{"amount": "42.00000001"}, true, "GTE equal close"},
		{&protocol.FilterNode{Cmp: CompareGTE, Key: "amount", Val: "42"}, map[string]string{"amount": "41.99999999"}, false, "GTE equal close"},
		{&protocol.FilterNode{Cmp: CompareGTE, Key: "amount", Val: "42"}, map[string]string{"amount": "10"}, false, "GTE less"},
		{&protocol.FilterNode{Cmp: CompareLT, Key: "amount", Val: "42"}, map[string]string{"amount": "10"}, true, "LT less"},
		{&protocol.FilterNode{Cmp: CompareLT, Key: "amount", Val: "42"}, map[string]string{"amount": "100"}, false, "LT greater"},
		{&protocol.FilterNode{Cmp: CompareLT, Key: "amount", Val: "42"}, map[string]string{"amount": "42"}, false, "LT equal"},
		{&protocol.FilterNode{Cmp: CompareLTE, Key: "amount", Val: "42"}, map[string]string{"amount": "10"}, true, "LTE less"},
		{&protocol.FilterNode{Cmp: CompareLTE, Key: "amount", Val: "42"}, map[string]string{"amount": "42"}, true, "LTE equal"},
		{&protocol.FilterNode{Cmp: CompareLTE, Key: "amount", Val: "42"}, map[string]string{"amount": "100"}, false, "LTE greater"},

		// --- AND ---
		{
			&protocol.FilterNode{
				Op: OpAnd,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareEQ, Key: "env", Val: "prod"},
					{Cmp: CompareExists, Key: "version"},
				},
			},
			map[string]string{"env": "prod", "version": "1.0"},
			true,
			"AND both children true",
		},
		{
			&protocol.FilterNode{
				Op: OpAnd,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareEQ, Key: "env", Val: "prod"},
					{Cmp: CompareExists, Key: "version"},
				},
			},
			map[string]string{"env": "prod"},
			false,
			"AND one child false",
		},

		// --- OR ---
		{
			&protocol.FilterNode{
				Op: OpOr,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareEQ, Key: "env", Val: "prod"},
					{Cmp: CompareEQ, Key: "env", Val: "staging"},
				},
			},
			map[string]string{"env": "staging"},
			true,
			"OR one child true",
		},
		{
			&protocol.FilterNode{
				Op: OpOr,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareEQ, Key: "env", Val: "prod"},
					{Cmp: CompareEQ, Key: "env", Val: "staging"},
				},
			},
			map[string]string{"env": "qa"},
			false,
			"OR both children false",
		},

		// --- NOT ---
		{
			&protocol.FilterNode{
				Op: OpNot,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareExists, Key: "debug"},
				},
			},
			map[string]string{},
			true,
			"NOT EXISTS key missing",
		},
		{
			&protocol.FilterNode{
				Op: OpNot,
				Nodes: []*protocol.FilterNode{
					{Cmp: CompareExists, Key: "debug"},
				},
			},
			map[string]string{"debug": "1"},
			false,
			"NOT EXISTS key present",
		},

		// --- Nested complex filter ---
		{
			&protocol.FilterNode{
				Op: OpOr,
				Nodes: []*protocol.FilterNode{
					{
						Op: OpAnd,
						Nodes: []*protocol.FilterNode{
							{Cmp: CompareEQ, Key: "env", Val: "prod"},
							{Cmp: CompareIn, Key: "region", Vals: []string{"us", "eu"}},
						},
					},
					{
						Op: OpAnd,
						Nodes: []*protocol.FilterNode{
							{Cmp: CompareNotEQ, Key: "tier", Val: "bronze"},
							{
								Op: OpNot,
								Nodes: []*protocol.FilterNode{
									{Cmp: CompareExists, Key: "debug"},
								},
							},
						},
					},
				},
			},
			map[string]string{"env": "staging", "region": "us"},
			true,
			"nested complex OR filter",
		},
	}

	for i, tt := range tests {
		d, err := json.Marshal(tt.filter)
		require.NoError(t, err)
		err = Validate(tt.filter)
		require.NoError(t, err)
		t.Logf("test %d: %s over %#v, expected: %v", i, d, tt.tags, tt.expected)
		got, err := Match(tt.filter, tt.tags)
		if err != nil {
			t.Errorf("case %d (%s): unexpected error: %v", i, tt.desc, err)
			continue
		}
		if got != tt.expected {
			t.Errorf("case %d (%s): expected %v, got %v", i, tt.desc, tt.expected, got)
		}
	}
}

func TestInvalidFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *protocol.FilterNode
	}{
		{
			name: "leaf missing cmp",
			filter: &protocol.FilterNode{
				Op:   OpLeaf,
				Key:  "foo",
				Val:  "bar",
				Cmp:  "",
				Vals: nil,
			},
		},
		{
			name: "leaf eq with vals set",
			filter: &protocol.FilterNode{
				Op:   OpLeaf,
				Key:  "foo",
				Cmp:  CompareEQ,
				Val:  "bar",
				Vals: []string{"baz"},
			},
		},
		{
			name: "in without vals",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "foo",
				Cmp: CompareIn,
			},
		},
		{
			name: "in with val instead of vals",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "foo",
				Cmp: CompareIn,
				Val: "bar",
			},
		},
		{
			name: "exists with val set",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "foo",
				Cmp: CompareExists,
				Val: "bar",
			},
		},
		{
			name: "nex with vals set",
			filter: &protocol.FilterNode{
				Op:   OpLeaf,
				Key:  "foo",
				Cmp:  CompareNotExists,
				Vals: []string{"a"},
			},
		},
		{
			name: "and without children",
			filter: &protocol.FilterNode{
				Op: OpAnd,
			},
		},
		{
			name: "or without children",
			filter: &protocol.FilterNode{
				Op: OpOr,
			},
		},
		{
			name: "not with no children",
			filter: &protocol.FilterNode{
				Op: OpNot,
			},
		},
		{
			name: "not with more than one child",
			filter: &protocol.FilterNode{
				Op: OpNot,
				Nodes: []*protocol.FilterNode{
					{Op: OpLeaf, Key: "foo", Cmp: CompareEQ, Val: "bar"},
					{Op: OpLeaf, Key: "baz", Cmp: CompareEQ, Val: "qux"},
				},
			},
		},
		{
			name: "leaf without key",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Cmp: CompareEQ,
				Val: "bar",
			},
		},
		{
			name: "invalid op",
			filter: &protocol.FilterNode{
				Op: "xxx",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.filter)
			require.Error(t, err, "expected error but got nil")
		})
	}

	// A valid case (sanity check)
	valid := &protocol.FilterNode{
		Op:  OpLeaf,
		Key: "foo",
		Cmp: CompareEQ,
		Val: "bar",
	}
	require.NoError(t, Validate(valid))
}

func buildComplexFilter() *protocol.FilterNode {
	return &protocol.FilterNode{
		Op: OpOr,
		Nodes: []*protocol.FilterNode{
			{
				Op: OpAnd,
				Nodes: []*protocol.FilterNode{
					{Key: "env", Cmp: CompareEQ, Val: "prod"},
					{Key: "region", Cmp: CompareIn, Vals: []string{"us", "eu"}},
				},
			},
			{
				Op: OpAnd,
				Nodes: []*protocol.FilterNode{
					{Key: "tier", Cmp: CompareNotEQ, Val: "bronze"},
					{
						Op: OpNot,
						Nodes: []*protocol.FilterNode{
							{Key: "debug", Cmp: CompareExists},
						},
					},
				},
			},
		},
	}
}

func TestFilterMatchesComplex(t *testing.T) {
	filter := buildComplexFilter()

	cases := []struct {
		tags     map[string]string
		expected bool
	}{
		{map[string]string{"env": "prod", "region": "us"}, true},
		{map[string]string{"tier": "silver"}, true},
		{map[string]string{"env": "staging", "region": "us"}, true},
	}

	for i, tt := range cases {
		got, err := Match(filter, tt.tags)
		if err != nil {
			t.Errorf("case %d: unexpected error: %v", i, err)
		}
		if got != tt.expected {
			t.Errorf("case %d: expected %v, got %v", i, tt.expected, got)
		}
	}
}

func BenchmarkFilter10k(b *testing.B) {
	ns := []int{10, 100, 1000, 10000}
	const subscribers = 10000

	pubTags := map[string]string{"env": "prod", "region": "us"}

	for _, n := range ns {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]*protocol.FilterNode, subscribers)
			for i := 0; i < subscribers; i++ {
				idx := i % n
				subs[i] = buildComplexFilterVariant(idx)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				for _, sub := range subs {
					_, _ = Match(sub, pubTags)
				}
			}
		})
	}
}

func buildComplexFilterVariant(i int) *protocol.FilterNode {
	// Base filter
	base := &protocol.FilterNode{
		Op: OpAnd,
		Nodes: []*protocol.FilterNode{
			{Key: "env", Cmp: CompareEQ, Val: "prod"},
			{Key: "region", Cmp: CompareIn, Vals: []string{"us", "eu"}},
		},
	}
	// Add a simple extra Or node to make each variant different
	var extra *protocol.FilterNode
	if i > 0 {
		extra = &protocol.FilterNode{
			Op: OpOr,
			Nodes: []*protocol.FilterNode{
				{Key: fmt.Sprintf("extra-%d", i), Cmp: CompareEQ, Val: "1"},
			},
		}
		base = &protocol.FilterNode{
			Op:    OpAnd,
			Nodes: []*protocol.FilterNode{base, extra},
		}
	}
	return base
}

func BenchmarkFilter10kCachedN(b *testing.B) {
	ns := []int{10, 100, 1000, 10000}
	const subscribers = 10000

	pubTags := map[string]string{"env": "prod", "region": "us"}

	for _, n := range ns {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]*protocol.FilterNode, subscribers)
			subHashes := make([][32]byte, subscribers)
			for i := 0; i < subscribers; i++ {
				idx := i % n
				subs[i] = buildComplexFilterVariant(idx)
				subHashes[i] = Hash(subs[i])
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cache := make(map[[32]byte]bool) // simple per-iteration cache
				for i, sub := range subs {
					key := subHashes[i]
					if res, ok := cache[key]; ok {
						_ = res
						continue
					}
					res, _ := Match(sub, pubTags)
					cache[key] = res
				}
			}
		})
	}
}

var evaluationCachePool = sync.Pool{
	New: func() interface{} {
		// Pre-size for largest expected case to avoid map growth.
		return make(map[[32]byte]bool, 10000)
	},
}

// Alternative sync.Pool version using clear() for Go 1.21+
func BenchmarkFilter10kCachedN_SyncPoolWithClear(b *testing.B) {
	ns := []int{10, 100, 1000, 10000}
	const subscribers = 10000

	pubTags := map[string]string{"env": "prod", "region": "us"}

	for _, n := range ns {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]*protocol.FilterNode, subscribers)
			subHashes := make([][32]byte, subscribers)
			for i := 0; i < subscribers; i++ {
				idx := i % n
				subs[i] = buildComplexFilterVariant(idx)
				subHashes[i] = Hash(subs[i])
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cache := evaluationCachePool.Get().(map[[32]byte]bool)

				for i, sub := range subs {
					key := subHashes[i]
					if res, ok := cache[key]; ok {
						_ = res
						continue
					}
					res, _ := Match(sub, pubTags)
					cache[key] = res
				}

				// Much faster clearing in Go 1.21+
				clear(cache)
				evaluationCachePool.Put(cache)
			}
		})
	}
}

// Different sized pools based on subscriber counts.
var (
	// Pool for up to 100 possible unique filters.
	smallCachePool = sync.Pool{
		New: func() interface{} {
			return make(map[[32]byte]bool, 100)
		},
	}

	// Pool for ~100-1000 possible unique filters.
	mediumCachePool = sync.Pool{
		New: func() interface{} {
			return make(map[[32]byte]bool, 1000)
		},
	}

	// Pool for ~1000-10000 possible unique filters.
	largeCachePool = sync.Pool{
		New: func() interface{} {
			return make(map[[32]byte]bool, 10000)
		},
	}

	// Pool for ~10000+ possible unique filters.
	xLargeCachePool = sync.Pool{
		New: func() interface{} {
			return make(map[[32]byte]bool, 100000)
		},
	}
)

// Function to select appropriate pool based on subscriber count
func getEvaluationsCachePool(numSubscribers int) *sync.Pool {
	switch {
	case numSubscribers <= 100:
		return &smallCachePool
	case numSubscribers <= 1000:
		return &mediumCachePool
	case numSubscribers <= 10000:
		return &largeCachePool
	default:
		return &xLargeCachePool
	}
}

// Benchmark with dynamic pool selection
func BenchmarkFilter10kCachedN_SyncPoolSized(b *testing.B) {
	ns := []int{10, 100, 1000, 10000}
	const subscribers = 10000

	pubTags := map[string]string{"env": "prod", "region": "us"}

	for _, n := range ns {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			subs := make([]*protocol.FilterNode, subscribers)
			subHashes := make([][32]byte, subscribers)

			for i := 0; i < subscribers; i++ {
				idx := i % n
				subs[i] = buildComplexFilterVariant(idx)
				subHashes[i] = Hash(subs[i])
			}

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				pool := getEvaluationsCachePool(subscribers)
				cache := pool.Get().(map[[32]byte]bool)

				for i, sub := range subs {
					key := subHashes[i]
					if res, ok := cache[key]; ok {
						_ = res
						continue
					}
					res, _ := Match(sub, pubTags)
					cache[key] = res
				}

				// Clear and return to appropriate pool.
				clear(cache)
				pool.Put(cache)
			}
		})
	}
}

func BenchmarkFilterNumeric10k(b *testing.B) {
	// Build a filter that requires both an integer and a float condition to pass
	buildNumericFilter := func() *protocol.FilterNode {
		return &protocol.FilterNode{
			Op: OpAnd,
			Nodes: []*protocol.FilterNode{
				{
					Op:  OpLeaf,
					Key: "count",
					Cmp: CompareGT, // integer comparison
					Val: "42",
				},
				{
					Op:  OpLeaf,
					Key: "price",
					Cmp: CompareGTE, // float comparison
					Val: "99.5",
				},
			},
		}
	}

	const subscribers = 10000

	// Example publication tags
	tags := map[string]string{
		"count": "100",
		"price": "120.0",
	}

	// Simulate 10k subscribers using the same filter
	subs := make([]*protocol.FilterNode, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = buildNumericFilter()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, sub := range subs {
			_, _ = Match(sub, tags)
		}
	}
}

func BenchmarkFilterNumeric10kCached(b *testing.B) {
	// Build a filter that requires both an integer and a float condition to pass
	buildNumericFilter := func() *protocol.FilterNode {
		return &protocol.FilterNode{
			Op: OpAnd,
			Nodes: []*protocol.FilterNode{
				{
					Op:  OpLeaf,
					Key: "count",
					Cmp: CompareGT, // integer comparison
					Val: "42",
				},
				{
					Op:  OpLeaf,
					Key: "price",
					Cmp: CompareGTE, // float comparison
					Val: "99.5",
				},
			},
		}
	}

	const subscribers = 10000

	// Example publication tags
	tags := map[string]string{
		"count": "100",
		"price": "120.0",
	}

	hashes := make([][32]byte, subscribers)
	subs := make([]*protocol.FilterNode, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = buildNumericFilter() // each sub has separate pointer
		hashes[i] = Hash(subs[i])
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		cache := make(map[[32]byte]bool) // simple per-iteration cache.
		for i, sub := range subs {
			key := hashes[i]
			if res, ok := cache[key]; ok {
				_ = res
				continue
			}
			res, _ := Match(sub, tags)
			cache[key] = res
		}
	}
}

func TestTagsFilter_FilterHashConsistency(t *testing.T) {
	// Test that identical filters produce the same hash
	filter1 := &protocol.FilterNode{
		Cmp: CompareEQ,
		Key: "env",
		Val: "prod",
	}

	filter2 := &protocol.FilterNode{
		Cmp: CompareEQ,
		Key: "env",
		Val: "prod",
	}

	hash1 := Hash(filter1)
	hash2 := Hash(filter2)

	require.Equal(t, hash1, hash2, "Identical filters should produce the same hash")

	// Test that different filters produce different hashes
	filter3 := &protocol.FilterNode{
		Cmp: CompareEQ,
		Key: "env",
		Val: "staging",
	}

	hash3 := Hash(filter3)
	require.NotEqual(t, hash1, hash3, "Different filters should produce different hashes")
}

func TestFilterMatchErrors(t *testing.T) {
	tags := map[string]string{"env": "prod", "amount": "100"}

	tests := []struct {
		name   string
		filter *protocol.FilterNode
		errMsg string
	}{
		{
			name: "invalid comparison operator",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "env",
				Cmp: "invalid",
				Val: "prod",
			},
			errMsg: "invalid Compare value: invalid",
		},
		{
			name: "invalid filter operation",
			filter: &protocol.FilterNode{
				Op: "invalid_op",
			},
			errMsg: "invalid filter op: invalid_op",
		},
		{
			name: "NOT with zero children",
			filter: &protocol.FilterNode{
				Op:    OpNot,
				Nodes: []*protocol.FilterNode{},
			},
			errMsg: "NOT must have exactly one child",
		},
		{
			name: "NOT with multiple children",
			filter: &protocol.FilterNode{
				Op: OpNot,
				Nodes: []*protocol.FilterNode{
					{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
					{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "staging"},
				},
			},
			errMsg: "NOT must have exactly one child",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Match(tt.filter, tags)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestFilterMatchErrorPropagation(t *testing.T) {
	tags := map[string]string{"env": "prod"}

	// Test error propagation in AND operation
	t.Run("error propagation in AND", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpAnd,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
				{Op: OpLeaf, Key: "test", Cmp: "invalid", Val: "value"},
			},
		}
		_, err := Match(filter, tags)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Compare value: invalid")
	})

	// Test error propagation in OR operation
	t.Run("error propagation in OR", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpOr,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "test", Cmp: "invalid", Val: "value"},
				{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "staging"},
			},
		}
		_, err := Match(filter, tags)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Compare value: invalid")
	})

	// Test error propagation in NOT operation
	t.Run("error propagation in NOT", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpNot,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "test", Cmp: "invalid", Val: "value"},
			},
		}
		_, err := Match(filter, tags)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Compare value: invalid")
	})

	// Test error propagation in nested operations
	t.Run("error propagation in nested operations", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpOr,
			Nodes: []*protocol.FilterNode{
				{
					Op: OpAnd,
					Nodes: []*protocol.FilterNode{
						{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
						{Op: OpLeaf, Key: "test", Cmp: "invalid", Val: "value"},
					},
				},
				{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "staging"},
			},
		}
		_, err := Match(filter, tags)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Compare value: invalid")
	})
}

func TestFilterNumericComparisons(t *testing.T) {
	tests := []struct {
		name     string
		filter   *protocol.FilterNode
		tags     map[string]string
		expected bool
		desc     string
	}{
		// Test numeric EQ branch that falls through to the numeric comparison section
		{
			name: "numeric EQ equal",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "amount",
				Cmp: CompareEQ,
				Val: "42.5",
			},
			tags:     map[string]string{"amount": "42.5"},
			expected: true,
			desc:     "numeric EQ with equal values",
		},
		{
			name: "numeric EQ not equal",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "amount",
				Cmp: CompareEQ,
				Val: "42.5",
			},
			tags:     map[string]string{"amount": "43.0"},
			expected: false,
			desc:     "numeric EQ with different values",
		},
		// Test missing key for numeric comparisons
		{
			name: "numeric GT missing key",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "amount",
				Cmp: CompareGT,
				Val: "42",
			},
			tags:     map[string]string{},
			expected: false,
			desc:     "numeric GT with missing key returns false",
		},
		// Test invalid numeric value in tags
		{
			name: "numeric GT invalid tag value",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "amount",
				Cmp: CompareGT,
				Val: "42",
			},
			tags:     map[string]string{"amount": "not-a-number"},
			expected: false,
			desc:     "numeric GT with invalid tag value returns false",
		},
		// Test invalid numeric value in filter
		{
			name: "numeric GT invalid filter value",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "amount",
				Cmp: CompareGT,
				Val: "not-a-number",
			},
			tags:     map[string]string{"amount": "42"},
			expected: false,
			desc:     "numeric GT with invalid filter value returns false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Match(tt.filter, tt.tags)
			require.NoError(t, err)
			require.Equal(t, tt.expected, got, tt.desc)
		})
	}
}

func TestValidateErrorMessages(t *testing.T) {
	tests := []struct {
		name   string
		filter *protocol.FilterNode
		errMsg string
	}{
		// Test comparison requires Val errors
		{
			name: "EQ comparison requires Val",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "test",
				Cmp: CompareEQ,
				Val: "",
			},
			errMsg: "eq comparison requires Val",
		},
		{
			name: "GT comparison requires Val",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "test",
				Cmp: CompareGT,
				Val: "",
			},
			errMsg: "gt comparison requires Val",
		},
		// Test comparison must not use Val errors
		{
			name: "IN comparison must not use Val",
			filter: &protocol.FilterNode{
				Op:   OpLeaf,
				Key:  "test",
				Cmp:  CompareIn,
				Val:  "should-not-be-set",
				Vals: []string{"valid"},
			},
			errMsg: "in comparison must not use Val",
		},
		{
			name: "EXISTS comparison must not use Val",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "test",
				Cmp: CompareExists,
				Val: "should-not-be-set",
			},
			errMsg: "ex comparison must not use Val or Vals",
		},
		// Test unknown comparison operator
		{
			name: "unknown comparison operator",
			filter: &protocol.FilterNode{
				Op:  OpLeaf,
				Key: "test",
				Cmp: "unknown_op",
				Val: "value",
			},
			errMsg: "unknown comparison operator: unknown_op",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.filter)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestValidateNestedErrors(t *testing.T) {
	// Test error propagation in nested validation
	t.Run("error propagation in AND validation", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpAnd,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
				{Op: OpLeaf, Key: "test", Cmp: "unknown_op", Val: "value"},
			},
		}
		err := Validate(filter)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown comparison operator: unknown_op")
	})

	t.Run("error propagation in OR validation", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpOr,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
				{Op: OpLeaf, Key: "test", Cmp: CompareEQ, Val: ""}, // missing Val
			},
		}
		err := Validate(filter)
		require.Error(t, err)
		require.Contains(t, err.Error(), "eq comparison requires Val")
	})

	t.Run("error propagation in NOT validation", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpNot,
			Nodes: []*protocol.FilterNode{
				{Op: OpLeaf, Key: "test", Cmp: "invalid_cmp", Val: "value"},
			},
		}
		err := Validate(filter)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown comparison operator: invalid_cmp")
	})

	t.Run("error propagation in deeply nested validation", func(t *testing.T) {
		filter := &protocol.FilterNode{
			Op: OpOr,
			Nodes: []*protocol.FilterNode{
				{
					Op: OpAnd,
					Nodes: []*protocol.FilterNode{
						{Op: OpLeaf, Key: "env", Cmp: CompareEQ, Val: "prod"},
						{
							Op: OpNot,
							Nodes: []*protocol.FilterNode{
								{Op: OpLeaf, Key: "test", Cmp: CompareIn, Val: "should-not-have-val", Vals: []string{"valid"}},
							},
						},
					},
				},
			},
		}
		err := Validate(filter)
		require.Error(t, err)
		require.Contains(t, err.Error(), "in comparison must not use Val")
	})
}
