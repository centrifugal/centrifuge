package redispartition

import (
	"fmt"
	"testing"
)

func TestFindTagsRejectsUnsupportedSizes(t *testing.T) {
	for _, n := range []int{0, -1, 1, 3, 17, 100, 16385} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			_, err := FindTags(n)
			if err == nil {
				t.Fatalf("expected error for unsupported size %d", n)
			}
		})
	}
}

func TestFindTagsAcceptsPrecomputedSizes(t *testing.T) {
	for _, n := range PrecomputedSizes() {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			tags, err := FindTags(n)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tags) != n {
				t.Fatalf("got %d tags, want %d", len(tags), n)
			}
		})
	}
}

// TestFindTagsReturnsSharedSlice documents that FindTags returns the
// underlying precomputed slice (not a copy). This is intentional — at
// runtime callers only read; the tradeoff is that callers MUST NOT mutate.
func TestFindTagsReturnsSharedSlice(t *testing.T) {
	tags1, err := FindTags(64)
	if err != nil {
		t.Fatal(err)
	}
	tags2, err := FindTags(64)
	if err != nil {
		t.Fatal(err)
	}
	if &tags1[0] != &tags2[0] {
		t.Fatal("FindTags returned distinct slice headers; expected shared backing array")
	}
}

func TestPrecomputedSizes(t *testing.T) {
	sizes := PrecomputedSizes()
	if len(sizes) != len(precomputed) {
		t.Fatalf("got %d sizes, want %d", len(sizes), len(precomputed))
	}
	for i := 1; i < len(sizes); i++ {
		if sizes[i] <= sizes[i-1] {
			t.Fatalf("sizes not sorted: %d <= %d at index %d", sizes[i], sizes[i-1], i)
		}
	}
	for _, n := range sizes {
		if _, ok := precomputed[n]; !ok {
			t.Errorf("PrecomputedSizes returned %d which is not in the map", n)
		}
	}
}

func checkBalance(t *testing.T, tags []string, maxCluster int) {
	t.Helper()
	slots := make([]int, len(tags))
	for i, tag := range tags {
		slots[i] = TagSlot(tag)
	}
	for k := 1; k <= maxCluster; k++ {
		counts := make([]int, k)
		for _, s := range slots {
			counts[SlotToNode(s, k)]++
		}
		mn, mx := counts[0], counts[0]
		for _, c := range counts[1:] {
			if c < mn {
				mn = c
			}
			if c > mx {
				mx = c
			}
		}
		if mx-mn > 1 {
			t.Errorf("cluster size %d: imbalanced [%d,%d], expected max-min <= 1",
				k, mn, mx)
		}
	}
}

// TestPrecomputedBalanced verifies all precomputed tag sets are balanced
// across every cluster size from 1 to numPartitions.
func TestPrecomputedBalanced(t *testing.T) {
	for n, tags := range precomputed {
		t.Run(fmt.Sprintf("p=%d", n), func(t *testing.T) {
			if len(tags) != n {
				t.Fatalf("precomputed has %d tags, want %d", len(tags), n)
			}
			checkBalance(t, tags, n)
		})
	}
}

// TestPrecomputedUnique verifies all precomputed tags map to unique slots.
func TestPrecomputedUnique(t *testing.T) {
	for n, tags := range precomputed {
		t.Run(fmt.Sprintf("p=%d", n), func(t *testing.T) {
			seenTags := make(map[string]bool)
			seenSlots := make(map[int]bool)
			for _, tag := range tags {
				if seenTags[tag] {
					t.Fatalf("duplicate tag: %s", tag)
				}
				seenTags[tag] = true
				slot := TagSlot(tag)
				if seenSlots[slot] {
					t.Fatalf("duplicate slot %d for tag %s", slot, tag)
				}
				seenSlots[slot] = true
			}
		})
	}
}

// TestPrecomputedSortedBySlot verifies precomputed tags are in slot order.
// Stable ordering matters because the broker maps a partition index to a
// tag by position; a reordering would shift every channel to a new slot.
func TestPrecomputedSortedBySlot(t *testing.T) {
	for n, tags := range precomputed {
		t.Run(fmt.Sprintf("p=%d", n), func(t *testing.T) {
			for i := 1; i < len(tags); i++ {
				if TagSlot(tags[i]) <= TagSlot(tags[i-1]) {
					t.Fatalf("not sorted at index %d: slot %d >= slot %d",
						i, TagSlot(tags[i-1]), TagSlot(tags[i]))
				}
			}
		})
	}
}

func TestCRC16MatchesRedis(t *testing.T) {
	// Reference values come from running CLUSTER KEYSLOT against a Redis
	// Cluster instance.
	tests := []struct {
		input string
		slot  int
	}{
		{"", 0},
		{"123456789", 12739},
		{"foo", 12182},
		{"bar", 5061},
		{"123", 5970},
	}
	for _, tt := range tests {
		got := int(crc16([]byte(tt.input))) % totalSlots
		if got != tt.slot {
			t.Errorf("crc16(%q) %% 16384 = %d, want %d", tt.input, got, tt.slot)
		}
	}
}

func TestSlotToNode(t *testing.T) {
	for numNodes := 1; numNodes <= 64; numNodes++ {
		seen := make(map[int]bool)
		for s := 0; s < totalSlots; s++ {
			n := SlotToNode(s, numNodes)
			if n < 0 || n >= numNodes {
				t.Fatalf("SlotToNode(%d, %d) = %d, out of range", s, numNodes, n)
			}
			seen[n] = true
		}
		if len(seen) != numNodes {
			t.Errorf("numNodes=%d: only %d nodes have slots", numNodes, len(seen))
		}
	}
}

func TestComputeTagsRejectsOutOfRange(t *testing.T) {
	for _, n := range []int{0, -1, 16385, 100000} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			_, err := ComputeTags(n)
			if err == nil {
				t.Fatalf("expected error for out-of-range n=%d", n)
			}
		})
	}
}

// TestComputeTagsDeterministic exercises the SA cold path and verifies
// that two independent runs produce byte-identical output. Uses a small
// partition count so the test stays fast.
func TestComputeTagsDeterministic(t *testing.T) {
	const n = 5
	tags1, err := ComputeTags(n)
	if err != nil {
		t.Fatal(err)
	}
	tags2, err := ComputeTags(n)
	if err != nil {
		t.Fatal(err)
	}
	if len(tags1) != n || len(tags2) != n {
		t.Fatalf("got lengths %d and %d, want %d", len(tags1), len(tags2), n)
	}
	for i := range tags1 {
		if tags1[i] != tags2[i] {
			t.Errorf("tags differ at index %d: %q vs %q", i, tags1[i], tags2[i])
		}
	}
	// Also check the produced tags balance across cluster sizes 1..n.
	checkBalance(t, tags1, n)
}

func TestSlotToNodeContiguous(t *testing.T) {
	for numNodes := 1; numNodes <= 64; numNodes++ {
		lastNode := -1
		transitions := 0
		for s := 0; s < totalSlots; s++ {
			n := SlotToNode(s, numNodes)
			if n != lastNode {
				transitions++
				lastNode = n
			}
		}
		if transitions != numNodes {
			t.Errorf("numNodes=%d: expected %d transitions, got %d", numNodes, numNodes, transitions)
		}
	}
}
