package centrifuge

import (
	"testing"

	"github.com/centrifugal/centrifuge/internal/redispartition"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func helloMap(kv ...string) map[string]string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func TestParseHelloReply(t *testing.T) {
	cases := []struct {
		name        string
		hello       map[string]string
		gateCapable bool
		primary     bool
	}{
		{"redis 8", helloMap("server", "redis", "version", "8.2.11", "role", "master"), true, true},
		{"redis 7 replica", helloMap("server", "redis", "version", "7.4.0", "role", "replica"), true, false},
		{"valkey 7.2 answers redis", helloMap("server", "redis", "version", "7.2.15", "role", "master"), true, true},
		{"valkey 8", helloMap("server", "valkey", "version", "8.1.11", "role", "master"), true, true},
		{"redis 6", helloMap("server", "redis", "version", "6.2.24", "role", "master"), false, true},
		{"dragonfly", helloMap("server", "redis", "version", "7.4.0", "dragonfly_version", "df-v1.40.1", "role", "master"), false, true},
		{"unknown server", helloMap("server", "keydb", "version", "8.0.0"), false, false},
		{"no version", helloMap("server", "redis"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := parseHelloReply(tc.hello)
			require.Equal(t, tc.gateCapable, k.gateCapable)
			require.Equal(t, tc.primary, k.primary)
		})
	}
}

func TestParseMyselfSlots(t *testing.T) {
	nodes := "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:7002@17002 master - 0 1426238317239 2 connected 5461-10922\n" +
		"67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5 7 [8->-07c37dfeb235213a872192d90877d0cd55635b91]\n"
	id, slots, err := parseMyselfSlots(nodes)
	require.NoError(t, err)
	require.Equal(t, "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1", id)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 7}, slots)

	_, _, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 myself,slave 07c37dfe 0 0 1 connected\n")
	require.Error(t, err, "a replica serves no slots of its own")

	_, _, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 master - 0 0 1 connected 0-16383\n")
	require.Error(t, err, "no myself line")

	_, _, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 9-3\n")
	require.Error(t, err)

	lines := parseClusterNodes(nodes)
	require.Len(t, lines, 2)
	require.Equal(t, "127.0.0.1:7002", lines[0].addr)
	require.True(t, lines[0].primary)
	require.False(t, lines[0].myself)
}

func TestLineageTokenKeyForSlot(t *testing.T) {
	seen := map[string]struct{}{}
	for slot := 0; slot < 16384; slot++ {
		key := lineageTokenKeyForSlot(slot)
		require.Equal(t, uint16(slot), redisKeySlot(key), "token key must live in its slot")
		_, dup := seen[key]
		require.False(t, dup)
		seen[key] = struct{}{}
	}
}

func TestRedisKeySlot(t *testing.T) {
	require.Equal(t, redisKeySlot("{user1000}.following"), redisKeySlot("{user1000}.followers"))
	require.Equal(t, redisKeySlot("foo"), redisKeySlot("{foo}bar"))
	// An empty tag hashes the whole key, as Redis does.
	require.Equal(t, uint16(redispartition.TagSlot("foo{}{bar}")), redisKeySlot("foo{}{bar}"))
	require.Equal(t, uint16(redispartition.TagSlot("foo{bar")), redisKeySlot("foo{bar"))
}

func TestLineageScriptVariants(t *testing.T) {
	ls := newLineageScript("x", "return lineage_token(KEYS[1])", false)
	a, b := ls.gatedSource("tokA"), ls.gatedSource("tokB")
	require.NotEqual(t, sha1Hex(a), sha1Hex(b), "gated scripts are specific to a token")
	require.Contains(t, a, `"tokA"`)
	require.Contains(t, a, lineageGateLoadArg)
	require.NotEqual(t, lineageMarkerSource("tokA"), lineageMarkerSource("tokB"))
}

func TestLineageErrToken(t *testing.T) {
	tok, ok := lineageErrToken(&rueidis.RedisError{})
	require.False(t, ok)
	require.Empty(t, tok)
}

func TestClientInfoField(t *testing.T) {
	info := "id=7 addr=127.0.0.1:52418 laddr=10.0.0.5:6379 fd=8 name= age=0"
	require.Equal(t, "10.0.0.5:6379", clientInfoField(info, "laddr"))
	require.Equal(t, "", clientInfoField(info, "nope"))
}
