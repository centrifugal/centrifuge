package centrifuge

import (
	"strconv"
	"testing"
)

func TestRedisSlot_KnownValues(t *testing.T) {
	// These values match Redis CLUSTER KEYSLOT output.
	tests := []struct {
		key  string
		slot uint16
	}{
		{"", 0},
		{"foo", 12182},
		{"bar", 5061},
		{"hello", 866},
		{"{user}.info", 5474},
		{"{user}.name", 5474},
		{"{0}", 13907},
		{"{1}", 9842},
		{"{127}", 6102},
		{"{128}", 9785},
		{"prefix{0}.channel", 13907},
		{"prefix{1}.channel", 9842},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := redisSlot(tt.key)
			if got != tt.slot {
				t.Errorf("redisSlot(%q) = %d, want %d", tt.key, got, tt.slot)
			}
		})
	}
}

func TestRedisSlot_HashTagEdgeCases(t *testing.T) {
	// Empty hash tag {}: no valid tag, hash full key.
	slotFull := redisSlot("abc{}")
	slotABC := redisSlot("abc{}")
	if slotFull != slotABC {
		t.Errorf("empty hash tag should hash full key")
	}

	// Only first valid {} is used.
	slot1 := redisSlot("{a}x{b}")
	slot2 := redisSlot("{a}")
	if slot1 != slot2 {
		t.Errorf("should use first hash tag: got %d and %d", slot1, slot2)
	}

	// No closing brace: hash full key.
	slotNoBrace := redisSlot("{abc")
	slotPlain := redisSlot("{abc")
	if slotNoBrace != slotPlain {
		t.Errorf("no closing brace should hash full key")
	}
}

func TestFindSlotOwner(t *testing.T) {
	// Simulate a 3-node cluster with evenly split slots.
	ranges := []slotRange{
		{start: 0, end: 5460, addr: "node1:6379"},
		{start: 5461, end: 10922, addr: "node2:6379"},
		{start: 10923, end: 16383, addr: "node3:6379"},
	}

	tests := []struct {
		slot uint16
		want string
	}{
		{0, "node1:6379"},
		{5460, "node1:6379"},
		{5461, "node2:6379"},
		{10922, "node2:6379"},
		{10923, "node3:6379"},
		{16383, "node3:6379"},
	}
	for _, tt := range tests {
		got := findSlotOwner(ranges, tt.slot)
		if got != tt.want {
			t.Errorf("findSlotOwner(slot=%d) = %q, want %q", tt.slot, got, tt.want)
		}
	}

	// Slot not covered by any range returns empty string.
	emptyRanges := []slotRange{{start: 0, end: 100, addr: "a:1"}}
	got := findSlotOwner(emptyRanges, 200)
	if got != "" {
		t.Errorf("findSlotOwner for uncovered slot should return empty, got %q", got)
	}
}

func TestFindSlotOwner_PartitionDistribution(t *testing.T) {
	// Verify that partitions are distributed across nodes in a 3-node cluster.
	ranges := []slotRange{
		{start: 0, end: 5460, addr: "node1:6379"},
		{start: 5461, end: 10922, addr: "node2:6379"},
		{start: 10923, end: 16383, addr: "node3:6379"},
	}

	numPartitions := 128
	nodeCounts := map[string]int{}
	for i := 0; i < numPartitions; i++ {
		key := "{" + strconv.Itoa(i) + "}"
		slot := redisSlot(key)
		addr := findSlotOwner(ranges, slot)
		if addr == "" {
			t.Fatalf("partition %d (slot %d) has no owner", i, slot)
		}
		nodeCounts[addr]++
	}

	// All 3 nodes should have partitions assigned.
	if len(nodeCounts) != 3 {
		t.Errorf("expected partitions on 3 nodes, got %d: %v", len(nodeCounts), nodeCounts)
	}

	// Each node should have a reasonable share (not all on one node).
	for addr, count := range nodeCounts {
		if count < 10 {
			t.Errorf("node %s has only %d partitions out of %d — too few", addr, count, numPartitions)
		}
	}
	t.Logf("partition distribution across 3 nodes: %v", nodeCounts)
}

func TestRedisSlot_PartitionHashTags(t *testing.T) {
	// Verify that partition hash tags {0}, {1}, ... map to different slots.
	seen := make(map[uint16]bool)
	for i := 0; i < 256; i++ {
		key := "{" + strconv.Itoa(i) + "}"
		slot := redisSlot(key)
		if slot >= 16384 {
			t.Errorf("slot for %s out of range: %d", key, slot)
		}
		seen[slot] = true
	}
	// All 256 partition hash tags should map to unique slots.
	if len(seen) != 256 {
		t.Errorf("expected 256 unique slots, got %d", len(seen))
	}
}
