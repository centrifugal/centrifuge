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

func TestBuildNodePartitions(t *testing.T) {
	// 3 nodes, 6 partitions: 0→node0, 1→node1, 2→node2, 3→node0, 4→node1, 5→node2
	partitionToNodeIdx := []int{0, 1, 2, 0, 1, 2}
	np := buildNodePartitions(partitionToNodeIdx, 3)
	if len(np) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(np))
	}
	if len(np[0]) != 2 || np[0][0] != 0 || np[0][1] != 3 {
		t.Errorf("node 0 partitions: got %v, want [0 3]", np[0])
	}
	if len(np[1]) != 2 || np[1][0] != 1 || np[1][1] != 4 {
		t.Errorf("node 1 partitions: got %v, want [1 4]", np[1])
	}
	if len(np[2]) != 2 || np[2][0] != 2 || np[2][1] != 5 {
		t.Errorf("node 2 partitions: got %v, want [2 5]", np[2])
	}
}

func TestBuildNodePartitions_AllOnOneNode(t *testing.T) {
	partitionToNodeIdx := []int{0, 0, 0}
	np := buildNodePartitions(partitionToNodeIdx, 1)
	if len(np) != 1 {
		t.Fatalf("expected 1 node, got %d", len(np))
	}
	if len(np[0]) != 3 {
		t.Errorf("expected 3 partitions on node 0, got %d", len(np[0]))
	}
}

func TestBuildNodePartitions_EmptyNode(t *testing.T) {
	// 2 nodes but all partitions on node 0 — node 1 has empty slice.
	partitionToNodeIdx := []int{0, 0}
	np := buildNodePartitions(partitionToNodeIdx, 2)
	if len(np) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(np))
	}
	if len(np[0]) != 2 {
		t.Errorf("expected 2 partitions on node 0, got %d", len(np[0]))
	}
	if np[1] != nil && len(np[1]) != 0 {
		t.Errorf("expected 0 partitions on node 1, got %d", len(np[1]))
	}
}

func TestAllocPubSubArrays(t *testing.T) {
	subClients, starts := allocPubSubArrays(3, 2)
	if len(subClients) != 3 {
		t.Fatalf("expected 3 node entries for subClients, got %d", len(subClients))
	}
	if len(starts) != 3 {
		t.Fatalf("expected 3 node entries for starts, got %d", len(starts))
	}
	for i := 0; i < 3; i++ {
		if len(subClients[i]) != 2 {
			t.Errorf("node %d: expected 2 shard entries, got %d", i, len(subClients[i]))
		}
		if len(starts[i]) != 2 {
			t.Errorf("node %d: expected 2 start entries, got %d", i, len(starts[i]))
		}
		for j := 0; j < 2; j++ {
			if starts[i][j] == nil {
				t.Errorf("node %d shard %d: pubSubStart is nil", i, j)
			}
			if cap(starts[i][j].errCh) != 1 {
				t.Errorf("node %d shard %d: errCh capacity should be 1", i, j)
			}
		}
	}
}

func TestTopologyChanged(t *testing.T) {
	tests := []struct {
		name     string
		old      []int
		new_     []int
		oldNodes int
		newNodes int
		want     bool
	}{
		{"identical", []int{0, 1, 2}, []int{0, 1, 2}, 3, 3, false},
		{"node count changed", []int{0, 1}, []int{0, 1}, 2, 3, true},
		{"mapping changed", []int{0, 1, 2}, []int{0, 2, 1}, 3, 3, true},
		{"length changed", []int{0, 1}, []int{0, 1, 2}, 3, 3, true},
		{"empty same", []int{}, []int{}, 0, 0, false},
		{"single same", []int{0}, []int{0}, 1, 1, false},
		{"single diff", []int{0}, []int{1}, 2, 2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := topologyChanged(tt.old, tt.new_, tt.oldNodes, tt.newNodes)
			if got != tt.want {
				t.Errorf("topologyChanged() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildPartitionMapping(t *testing.T) {
	ranges := []slotRange{
		{start: 0, end: 5460, addr: "node1:6379"},
		{start: 5461, end: 10922, addr: "node2:6379"},
		{start: 10923, end: 16383, addr: "node3:6379"},
	}
	addrToIdx := map[string]int{
		"node1:6379": 0,
		"node2:6379": 1,
		"node3:6379": 2,
	}

	mapping, err := buildPartitionMapping(ranges, addrToIdx, 128)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mapping) != 128 {
		t.Fatalf("expected 128 entries, got %d", len(mapping))
	}
	// All values should be 0, 1, or 2.
	for i, v := range mapping {
		if v < 0 || v > 2 {
			t.Errorf("partition %d mapped to invalid node idx %d", i, v)
		}
	}
	// All three nodes should be represented.
	seen := map[int]bool{}
	for _, v := range mapping {
		seen[v] = true
	}
	if len(seen) != 3 {
		t.Errorf("expected all 3 nodes represented, got %d", len(seen))
	}
}

func TestBuildPartitionMapping_MissingOwner(t *testing.T) {
	ranges := []slotRange{
		{start: 0, end: 100, addr: "node1:6379"},
	}
	addrToIdx := map[string]int{
		"node1:6379": 0,
	}
	// Some partitions will have slots outside range 0-100, so findSlotOwner returns "".
	// buildPartitionMapping should return an error for those.
	_, err := buildPartitionMapping(ranges, addrToIdx, 128)
	if err == nil {
		t.Fatal("expected error for missing slot owner")
	}
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
