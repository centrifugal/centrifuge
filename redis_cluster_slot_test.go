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
