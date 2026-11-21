package centrifuge

import (
	"bufio"
	"context"
	"crypto/fips140"
	"errors"
	dto "github.com/prometheus/client_model/go"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
		{"connection id", helloMap("server", "redis", "version", "7.2.4", "role", "master", "id", "42"), true, true},
		// ElastiCache Serverless: a proxy keeping scripts itself.
		{"proxy without a connection id", helloMap("server", "redis", "version", "7.2", "role", "master", "id", "-1"), false, true},
		{"no version", helloMap("server", "redis"), false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := parseHelloReply(tc.hello)
			require.Equal(t, tc.gateCapable, k.gateCapable)
			require.Equal(t, tc.primary, k.primary)
			require.Equal(t, tc.hello["id"] == "-1", k.proxy)
		})
	}
}

func TestParseMyselfSlots(t *testing.T) {
	nodes := "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:7002@17002 master - 0 1426238317239 2 connected 5461-10922\n" +
		"67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 0-5 7 [8->-07c37dfeb235213a872192d90877d0cd55635b91]\n"
	slots, err := parseMyselfSlots(nodes)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 7}, slots)

	_, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 myself,slave 07c37dfe 0 0 1 connected\n")
	require.Error(t, err, "a replica serves no slots of its own")

	_, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 master - 0 0 1 connected 0-16383\n")
	require.Error(t, err, "no myself line")

	_, err = parseMyselfSlots("67ed2db8 127.0.0.1:7001@17001 myself,master - 0 0 1 connected 9-3\n")
	require.Error(t, err)

	lines := lineageParseClusterNodes(nodes)
	require.Len(t, lines, 2)
	require.Equal(t, "127.0.0.1:7002", lines[0].addr)
	require.True(t, lines[0].primary)
	require.False(t, lines[0].myself)
}

func TestLineageTokenKeyForSlot(t *testing.T) {
	for _, prefix := range []string{"centrifugo", "pre{fix}", "pre{"} {
		seen := map[string]struct{}{}
		for slot := 0; slot < 16384; slot++ {
			key := lineageKeyForSlot(prefix, slot)
			require.Equal(t, uint16(slot), redisKeySlot(key), "token key must live in its slot")
			_, dup := seen[key]
			require.False(t, dup)
			seen[key] = struct{}{}
		}
	}
	// The prefix comes first, so an ACL user limited to it may use the key.
	require.True(t, strings.HasPrefix(lineageKeyForSlot("centrifugo", 0), "centrifugo.lineage.state.{"))
}

func TestRedisKeySlot(t *testing.T) {
	require.Equal(t, redisKeySlot("{user1000}.following"), redisKeySlot("{user1000}.followers"))
	require.Equal(t, redisKeySlot("foo"), redisKeySlot("{foo}bar"))
	// An empty tag hashes the whole key, as Redis does.
	require.Equal(t, uint16(redispartition.TagSlot("foo{}{bar}")), redisKeySlot("foo{}{bar}"))
	require.Equal(t, uint16(redispartition.TagSlot("foo{bar")), redisKeySlot("foo{bar"))
}

func TestLineageScriptVariants(t *testing.T) {
	if fips140.Enforced() {
		t.Skip("compares SHA-1 digests")
	}
	ls := newLineageScript("x", "return lineage_token(KEYS[1])", false)
	a, b := ls.gatedSource("tokA"), ls.gatedSource("tokB")
	require.NotEqual(t, sha1Hex(a), sha1Hex(b), "gated scripts are specific to a token")
	require.Contains(t, a, `"tokA"`)
	require.Contains(t, a, lineageGateLoadArg)
	require.Contains(t, a, `LINEAGE_FIELD = "i"`)
	require.Contains(t, lineagePreludeInfo, `LINEAGE_FIELD = "j"`, "each mode stamps its own field")
	require.NotEqual(t, lineageMarkerSource("tokA"), lineageMarkerSource("tokB"))
}

// The marker tells a server whose lineage is intact apart from a new one: a
// new version must load the same marker, or every server it meets during a
// rolling upgrade would look new and reset all its streams. Never change it.
func TestLineageMarkerSourceIsStable(t *testing.T) {
	require.Equal(t, `return "centrifuge-lineage-v1:tok"`, lineageMarkerSource("tok"))
}

func TestLineageErrToken(t *testing.T) {
	_, _, ok := lineageErrToken(&rueidis.RedisError{})
	require.False(t, ok)

	parse := parseLineageErr
	v, preds, ok := parse("LINEAGE t=gnew p=gold,golder script: abc, on @user_script:12.")
	require.True(t, ok)
	require.Equal(t, "gnew", v)
	require.Equal(t, []string{"gold", "golder"}, preds)

	v, preds, ok = parse("LINEAGE t= p=")
	require.True(t, ok)
	require.Empty(t, v)
	require.Empty(t, preds)

	// Parts a later version adds are skipped.
	v, preds, ok = parse("LINEAGE x=1 t=gnew|ganc p=gold y=z")
	require.True(t, ok)
	require.Equal(t, "gnew|ganc", v)
	require.Equal(t, []string{"gold"}, preds)
}

func TestParseTokenValue(t *testing.T) {
	tok, anc := parseTokenValue("gnew")
	require.Equal(t, "gnew", tok)
	require.Empty(t, anc)
	tok, anc = parseTokenValue("gnew|gold")
	require.Equal(t, "gnew", tok)
	require.Equal(t, "gold", anc)
}

func TestLineageLeftTokens(t *testing.T) {
	l := newLineageLeftTokens()
	require.False(t, l.add())
	require.True(t, l.add("a", "b", ""))
	require.False(t, l.add("a"), "known already")
	require.True(t, l.has("a"))
	require.False(t, l.has(""))
	toks, _ := l.recent(10, 0)
	require.ElementsMatch(t, []string{"a", "b"}, toks)
	for i := 0; i < lineageLeftMax+10; i++ {
		l.add("t" + strconv.Itoa(i))
	}
	require.False(t, l.has("a"), "the oldest are dropped")
	require.True(t, l.has("t"+strconv.Itoa(lineageLeftMax+9)))
	toks, _ = l.recent(lineageLeftExchanged, 0)
	require.Len(t, toks, lineageLeftExchanged)
}

// Node info carries only tokens replaced within the window, the most recently
// replaced first; a token keeps the earliest time it was reported with.
func TestLineageLeftTokens_Window(t *testing.T) {
	now := time.Now().Unix()
	day := int64(24 * time.Hour / time.Second)
	l := newLineageLeftTokens()
	l.addAt(now-3*day, "old")
	l.addAt(now-60, "recent")
	l.addAt(now, "newest")
	toks, ats := l.recent(10, now-day)
	require.Equal(t, []string{"newest", "recent"}, toks)
	require.Equal(t, []int64{now, now - 60}, ats)
	require.True(t, l.has("old"), "older tokens stay known locally")

	require.False(t, l.addPairs([]string{"recent"}, []int64{now - 2*day}), "known already")
	toks, _ = l.recent(10, now-day)
	require.Equal(t, []string{"newest"}, toks, "the earlier time wins")

	// A missing or future time counts as now.
	require.True(t, l.addPairs([]string{"x", "y"}, []int64{now + day}))
	toks, _ = l.recent(10, now)
	require.ElementsMatch(t, []string{"newest", "x", "y"}, toks)
}

// Tokens read from a lineage key or a LINEAGE error were replaced when the
// token after them was minted, so a node reading them late gets the same times.
func TestLineageLeftTokens_Chain(t *testing.T) {
	mint := func(at time.Time) string {
		return "g" + strconv.FormatInt(at.UnixNano(), 36) + "r"
	}
	now := time.Now()
	t3 := mint(now.Add(-time.Hour))
	t2 := mint(now.Add(-48 * time.Hour))
	t1 := mint(now.Add(-72 * time.Hour))
	at, ok := lineageTokenMinted(t3)
	require.True(t, ok)
	require.Equal(t, now.Add(-time.Hour).Unix(), at)
	_, ok = lineageTokenMinted("rabc")
	require.False(t, ok)

	l := newLineageLeftTokens()
	require.True(t, l.addChain(t3+"|ancestor", []string{t2, t1}))
	toks, ats := l.recent(10, 0)
	require.Equal(t, []string{t2, t1}, toks)
	require.Equal(t, []int64{now.Add(-time.Hour).Unix(), now.Add(-48 * time.Hour).Unix()}, ats)
	toks, _ = l.recent(10, now.Add(-24*time.Hour).Unix())
	require.Equal(t, []string{t2}, toks, "t1 was replaced two days ago")
}

func TestPubsubEpochExtension(t *testing.T) {
	require.Equal(t, "Abc", pubsubEpoch("Abc"))
	require.Equal(t, "Abc", pubsubEpoch("Abc;anything=a later version adds"))
	// Parsers keep the extension for the lineage hook, which drops it.
	_, _, sp, _, _, ok := extractPushData([]byte("__p1:7:Abc;x=1__payload"))
	require.True(t, ok)
	require.Equal(t, StreamPosition{Offset: 7, Epoch: "Abc;x=1"}, sp)
	off, epoch, payload, _, _, err := parseMessage([]byte("7:Abc;x=1:payload"))
	require.NoError(t, err)
	require.Equal(t, uint64(7), off)
	require.Equal(t, "Abc;x=1", epoch)
	require.Equal(t, "payload", string(payload))
	l := newRedisLineage(&RedisShard{closeCh: make(chan struct{})}, "p")
	require.Equal(t, "Abc", l.epochExt("stream", "test", "k", "Abc;x=1"))
}

// A reply whose meta was dropped for a lineage mismatch carries the reset
// extension: the hook strips it and counts the reset by kind and detection.
func TestLineageEpochExtCountsResets(t *testing.T) {
	n, err := New(Config{})
	require.NoError(t, err)
	l := newRedisLineage(&RedisShard{closeCh: make(chan struct{})}, "p")
	l.setLogger(n, "test")
	resets := func(kind, detection string) float64 {
		m := &dto.Metric{}
		require.NoError(t, n.metrics.redisPotentialLostWritesEpochResets.WithLabelValues("test", kind, detection).Write(m))
		return m.GetCounter().GetValue()
	}
	gate, info := resets("stream", "script_cache"), resets("map", "info")

	require.Equal(t, "E", l.epochExt("stream", "test", "k", "E"))
	require.Equal(t, "E", l.epochExt("stream", "test", "k", "E;x=1"))
	require.Equal(t, gate, resets("stream", "script_cache"), "no reset extension")

	require.Equal(t, "E", l.epochExt("stream", "test", "k", "E;lr="+lineageStampGate))
	require.Equal(t, gate+1, resets("stream", "script_cache"))
	require.Equal(t, "E", l.epochExt("map", "test", "k", "E;x=1;lr="+lineageStampInfo))
	require.Equal(t, info+1, resets("map", "info"))
}

// A detection is labelled with the broker whose call found it; one no broker
// call triggered (the idle probe, tokens learned from other nodes) with the
// first broker that enabled detection on the shard and prefix.
func TestLineageBrokerName(t *testing.T) {
	n, err := New(Config{})
	require.NoError(t, err)
	l := newRedisLineage(&RedisShard{closeCh: make(chan struct{})}, "p")
	l.setLogger(n, "first")
	l.setLogger(n, "second")
	require.Equal(t, "first", l.brokerNameOf(context.Background()))
	require.Equal(t, "maps", l.brokerNameOf(withLineageBroker(context.Background(), "maps")))
	require.Equal(t, "", l.brokerNameOf(withLineageBroker(context.Background(), "")), "an unnamed broker stays unnamed")
}

func TestInfoRefusal(t *testing.T) {
	require.Contains(t, infoRefusal(errors.New("NOPERM User x has no permissions to run the 'info' command")), "@dangerous")
	// ElastiCache Serverless refuses INFO from scripts for every user.
	msg := infoRefusal(errors.New("ERR This Valkey command is not allowed from script"))
	require.NotContains(t, msg, "ACL")
	require.Contains(t, msg, "doesn't let scripts call INFO")
}

// Stopping the reports cancels the running ones and waits for them; a report
// started after the stop doesn't run.
func TestLineageReports_StopCancelsAndWaits(t *testing.T) {
	r := newLineageReports()
	var finished atomic.Bool
	started := make(chan struct{})
	r.start(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		finished.Store(true)
	})
	<-started
	r.stop()
	require.True(t, finished.Load(), "stop returns after the report")

	var ran atomic.Bool
	r.start(func(context.Context) { ran.Store(true) })
	r.stop()
	require.False(t, ran.Load(), "no report after the stop")
}

// startClusterDownRedis starts a server which speaks enough of the Redis
// protocol for a client to connect, replies to EXISTS with CLUSTERDOWN until
// up is closed, and to HELLO as Redis 8.
func startClusterDownRedis(tb testing.TB, up <-chan struct{}) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				r := bufio.NewReader(conn)
				for {
					command, err := readRedisCommand(r)
					if err != nil {
						return
					}
					reply := "+OK\r\n"
					switch {
					case strings.EqualFold(command[0], "EXISTS"):
						select {
						case <-up:
							reply = ":0\r\n"
						default:
							reply = "-CLUSTERDOWN The cluster is down\r\n"
						}
					case strings.EqualFold(command[0], "CLUSTER"):
						reply = "-ERR This instance has cluster support disabled\r\n"
					case strings.EqualFold(command[0], "HELLO"):
						fields := []string{"server", "redis", "version", "8.0.0", "id", "5", "role", "master"}
						var b strings.Builder
						b.WriteString("*" + strconv.Itoa(len(fields)) + "\r\n")
						for _, f := range fields {
							b.WriteString("$" + strconv.Itoa(len(f)) + "\r\n" + f + "\r\n")
						}
						reply = b.String()
					}
					if _, err := conn.Write([]byte(reply)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// A server which can't answer yet (CLUSTERDOWN during a failover) is not a
// server without the gate: the mode is an error then, not kept, and the gate
// is found once the server answers.
func TestRedisLineage_NotReadyServerIsNotKindWithoutGate(t *testing.T) {
	up := make(chan struct{})
	s, err := NewRedisShard(nil, RedisShardConfig{Address: startClusterDownRedis(t, up), IOTimeout: 2 * time.Second})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	l := s.lineageFor("not_ready")
	l.noProbeForTest = true

	_, err = l.mode(context.Background(), false)
	require.Error(t, err)
	var re *rueidis.RedisError
	require.ErrorAs(t, err, &re)
	require.True(t, re.IsClusterDown(), err)

	close(up)
	mode, err := l.mode(context.Background(), false)
	require.NoError(t, err)
	require.Equal(t, lineageGate, mode)
}
