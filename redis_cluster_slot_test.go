package centrifuge

import (
	"strconv"
	"testing"
)

func TestRedisSlot_KnownValues(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	// Expected values match Redis CLUSTER KEYSLOT. Redis looks for the first
	// "{", then for the first "}" after it: if either is missing, or the tag
	// between them is empty, the whole key is hashed.
	tests := []struct {
		name string
		key  string
		slot uint16
	}{
		{"empty hash tag hashes full key", "abc{}", 16021},
		{"no closing brace hashes full key", "{abc", 444},
		{"closing brace before opening one hashes full key", "a}{b", 11640},
		{"first hash tag wins", "{a}x{b}", 15495},
		{"tag is found after a leading closing brace", "}{a}", 15495},
		{"empty first tag hashes full key even if a later tag is valid", "{}{a}", 13650},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redisSlot(tt.key); got != tt.slot {
				t.Errorf("redisSlot(%q) = %d, want %d", tt.key, got, tt.slot)
			}
		})
	}

	// Guard against the whole key accidentally being reduced to the brace-less
	// remainder: hashing "abc{}" or "{abc" must not give the same slot as "abc".
	plain := redisSlot("abc")
	for _, key := range []string{"abc{}", "{abc"} {
		if got := redisSlot(key); got == plain {
			t.Errorf("redisSlot(%q) = %d, must differ from redisSlot(\"abc\")", key, got)
		}
	}
}

func TestRedisSlot_PartitionHashTags(t *testing.T) {
	t.Parallel()
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
