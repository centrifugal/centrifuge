package centrifuge

import (
	"context"
	crand "crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/redispartition"

	"github.com/redis/rueidis"
)

// Lineage of a Redis server's data.
//
// Redis replication is asynchronous: a failover to a replica that missed the
// last writes, or a restart from an RDB or AOF file, keeps a stream's epoch while
// its top offset goes back, and the next publications reuse offsets clients
// already have (centrifugo#1319). To detect it, every meta records the lineage
// token of the data it was written in, and a meta script that finds a meta
// written under another token starts a new epoch.
//
// The token is a normal key (one per Redis server outside Redis Cluster, one
// per hash slot in Redis Cluster). It changes when a server's data may have lost
// writes, found by the script-cache gate. A server's script cache is never
// persisted, and on Redis 7+ and Valkey it never reaches replicas, so a
// restarted or promoted replica has none of our scripts. Meta scripts run only
// with EVALSHA, and the scripts and the gate's marker script are specific to a
// token: a node runs the scripts of the token it knows, and a script whose token
// isn't the server's fails with a LINEAGE error. A NOSCRIPT reply opens the
// gate, on one connection to the server: if the server is a primary and holds
// the marker of its current token, its lineage is intact and the scripts are
// loaded; otherwise the token of every slot the server serves is rotated first.
//
// A demoted primary keeps its script cache, through a resync too. Promoted again
// while its data still holds the old token, it would run its old scripts on data
// that may miss writes. Every Redis server's lineage is therefore also probed
// periodically: a demoted server the node can see that holds our scripts has
// its script cache flushed, and a server behind a connection that changed is
// rotated (see redisLineage.probe).
//
// Redis 6 and Dragonfly send scripts to replicas, so the gate can't be used
// there: no detection, as before. With UseInfoCommandForLineage the scripts
// read master_replid with INFO on every server instead of using the gate: exact
// (it changes on every restart and promotion), at the cost of INFO in every
// meta script and the @dangerous ACL category.

type lineageMode int

const (
	// lineageNone: no detection; meta scripts run as before (EVALSHA with EVAL
	// fallback) and stamp nothing.
	lineageNone lineageMode = iota
	// lineageGate: the script-cache gate.
	lineageGate
	// lineageInfo: scripts read master_replid with INFO.
	lineageInfo
)

func (m lineageMode) String() string {
	switch m {
	case lineageGate:
		return "gate"
	case lineageInfo:
		return "info"
	default:
		return "none"
	}
}

// lineageGateLoadArg as the only argument makes a gated script return at once.
const lineageGateLoadArg = "__centrifuge_lineage_load__"

// lineageTokenKeyBase is the name of the token key, the same for every prefix:
// a lineage is a property of the server's data.
const lineageTokenKeyBase = "centrifuge.lineage"

// lineageErrPrefix starts the error a gated script returns when the token key
// doesn't hold the token the script was made for: "LINEAGE <token>", with "-"
// for no token.
const lineageErrPrefix = "LINEAGE "

// lineageTwinErrPrefix starts the error a gated script returns when the
// server's write counter and its local twin differ: its data came from another
// server (see lineagePreludeGate). "LINEAGE_COPY <c> <l>".
const lineageTwinErrPrefix = "LINEAGE_COPY "

// lineagePreludeGate is the prelude of a gated script made for token. The token
// key holds "token" or, after a lineage break the gate proved lost nothing
// observed (ResyncOnlyLostSlots), "token|ancestor|count": metas stamped with the
// ancestor are adopted, not reset. lineage_token returns the token and the
// ancestor.
//
// With count (ResyncOnlyLostSlots), every write bumps a per-slot write counter
// (lineage_count) and every reply and PUB/SUB payload reporting an epoch adds
// ";count;token" to it (lineage_obs), so that every node knows the newest write
// it has observed: see redisLineage.observe.
//
// With twin, every write also bumps a local twin of that counter: the counter
// hash <token key>.n has field "c", replicated, and field "l", written with
// REPL_NONE, so never sent to replicas or the AOF. A server that executed every
// write in its data itself has c == l. A server whose data came from another
// server - a promoted replica, an old primary that took a full copy of the new
// one (a failback the script cache can't see), a restart from the AOF - has l
// behind c or missing: every gated script then refuses with LINEAGE_COPY and
// the gate rotates. Both fields live in one key, so an eviction removes both:
// a fresh start, not a false lineage break.
func lineagePreludeGate(token string, count, twin bool) string {
	countLua := "false"
	if count {
		countLua = "true"
	}
	twinLua := "false"
	if twin {
		twinLua = "true"
	}
	return `
local LINEAGE_TOKEN = "` + token + `"
local LINEAGE_COUNT = ` + countLua + `
local LINEAGE_TWIN = ` + twinLua + `
local function lineage_token(key)
    local value = redis.call("get", key)
    local token, ancestor = value, nil
    if value then
        local bar = string.find(value, "|", 1, true)
        if bar then
            token = string.sub(value, 1, bar - 1)
            local rest = string.sub(value, bar + 1)
            local bar2 = string.find(rest, "|", 1, true)
            if bar2 then
                ancestor = string.sub(rest, 1, bar2 - 1)
            else
                ancestor = rest
            end
        end
    end
    if token ~= LINEAGE_TOKEN then
        error({err = "` + lineageErrPrefix + `" .. (value or "-")})
    end
    if LINEAGE_TWIN then
        local t = redis.call("hmget", key .. ".n", "c", "l")
        if t[1] and t[1] ~= t[2] then
            error({err = "` + lineageTwinErrPrefix + `" .. t[1] .. " " .. (t[2] or "-")})
        end
    end
    return token, ancestor
end
local function lineage_count(key)
    if not LINEAGE_COUNT and not LINEAGE_TWIN then
        return false
    end
    local count = redis.call("hincrby", key .. ".n", "c", 1)
    if LINEAGE_TWIN then
        redis.set_repl(redis.REPL_NONE)
        redis.call("hincrby", key .. ".n", "l", 1)
        redis.set_repl(redis.REPL_ALL)
    end
    if not LINEAGE_COUNT then
        return false
    end
    return count
end
local function lineage_obs(count)
    if not LINEAGE_COUNT or not count then
        return ""
    end
    return ";" .. count .. ";" .. LINEAGE_TOKEN
end
if #ARGV == 1 and ARGV[1] == "` + lineageGateLoadArg + `" then
    return 0
end
`
}

func lineageMarkerSource(token string) string {
	return `return "centrifuge-lineage-v1:` + token + `"`
}

// lineagePreludeInfo defines lineage_token(key) from master_replid.
const lineagePreludeInfo = `
local LINEAGE_COUNT = false
local function lineage_token(key)
    local info = redis.call("info", "replication")
    local replid = string.match(info, "master_replid:(%w+)")
    if not replid then
        return ""
    end
    return "r" .. string.sub(replid, 1, 16), nil
end
local function lineage_count(key)
    return false
end
local function lineage_obs(count)
    return ""
end
`

// lineagePreludeNone defines lineage_token(key) as no token: nothing is
// stamped or compared.
const lineagePreludeNone = `
local LINEAGE_COUNT = false
local function lineage_token(key)
    return "", nil
end
local function lineage_count(key)
    return false
end
local function lineage_obs(count)
    return ""
end
`

// lineageRotateSource sets every token key in ARGV[2..] to ARGV[1]. It declares
// no keys so that it runs over the gate's connection in Redis Cluster; the
// server refuses a key it doesn't serve.
// The rotation is a compare-and-set: ARGV[1] is the token key of the gate's
// slot and ARGV[2] the value the gate read there. If another node rotated in
// between (it also found this server's marker missing), nothing is set and the
// value is returned: the gate adopts that rotation instead of making a second
// one, which would reset metas written under the first. ARGV[3] "1": the twin
// of each slot's write counter is set to the counter - from now on the server's
// own writes count (see lineagePreludeGate) - and the pairs start at ARGV[4].
const lineageRotateSourceCluster = `#!lua flags=allow-cross-slot-keys
local current = redis.call("get", ARGV[1]) or ""
if current ~= ARGV[2] then
    return current
end
for i = 4, #ARGV, 2 do
    redis.call("set", ARGV[i], ARGV[i + 1])
    if ARGV[3] == "1" then
        local c = redis.call("hget", ARGV[i] .. ".n", "c")
        if c then
            redis.set_repl(redis.REPL_NONE)
            redis.call("hset", ARGV[i] .. ".n", "l", c)
            redis.set_repl(redis.REPL_ALL)
        end
    end
end
return false
`

const lineageRotateSource = `
local current = redis.call("get", ARGV[1]) or ""
if current ~= ARGV[2] then
    return current
end
for i = 4, #ARGV, 2 do
    redis.call("set", ARGV[i], ARGV[i + 1])
    if ARGV[3] == "1" then
        local c = redis.call("hget", ARGV[i] .. ".n", "c")
        if c then
            redis.set_repl(redis.REPL_NONE)
            redis.call("hset", ARGV[i] .. ".n", "l", c)
            redis.set_repl(redis.REPL_ALL)
        end
    end
end
return false
`

// lineageSlotsSource returns the token key's value and the write counter of
// every token key in ARGV.
const lineageSlotsSourceCluster = `#!lua flags=allow-cross-slot-keys
local out = {}
for i = 1, #ARGV do
    out[#out + 1] = redis.call("get", ARGV[i]) or ""
    out[#out + 1] = redis.call("hget", ARGV[i] .. ".n", "c") or "0"
end
return out
`

const lineageSlotsSource = `
local out = {}
for i = 1, #ARGV do
    out[#out + 1] = redis.call("get", ARGV[i]) or ""
    out[#out + 1] = redis.call("hget", ARGV[i] .. ".n", "c") or "0"
end
return out
`

// lineageSha1Source returns the SHA-1 of a script source, for FIPS mode where
// it can't be computed here and SCRIPT LOAD would load the script.
const lineageSha1Source = `return redis.sha1hex(ARGV[1])`

// lineageMapHelpers are the lineage helpers of the map broker scripts.
//
// lineage_map_meta checks a map meta against the lineage token (see add_history
// in broker_history_add_stream_fn.lua). A meta written under another token is
// dropped with its change stream: the offsets start again in a new epoch, and
// clients reload the state. The keyed state itself stays - it is what the server
// has - and lineage_map_create rebinds it to the new epoch, so it isn't wiped as
// state of a dead epoch. Returns the epoch (false when there is none) and the
// epoch dropped (false when none was).
const lineageMapHelpers = `
local function lineage_map_meta(meta_key, stream_key, token, ancestor)
    if token == "" then
        return redis.call("hget", meta_key, "e"), false
    end
    local m = redis.call("hmget", meta_key, "e", "i")
    if m[1] == false or m[2] == token then
        return m[1], false
    end
    if m[2] == false or m[2] == ancestor then
        redis.call("hset", meta_key, "i", token)
        return m[1], false
    end
    redis.call("del", meta_key)
    if stream_key ~= "" then
        redis.call("del", stream_key)
    end
    return false, m[1]
end

-- lineage_map_obs returns the observation suffix of a map meta's epoch.
local function lineage_map_obs(meta_key)
    if not LINEAGE_COUNT then
        return ""
    end
    return lineage_obs(redis.call("hget", meta_key, "w"))
end

-- lineage_map_count bumps the write counter of a map write.
local function lineage_map_count(meta_key, token_key)
    local count = lineage_count(token_key)
    if count then
        redis.call("hset", meta_key, "w", count)
    end
    return lineage_obs(count)
end

local function lineage_map_create(meta_key, state_meta_key, epoch, token, dropped)
    if token ~= "" then
        redis.call("hset", meta_key, "e", epoch, "i", token)
    else
        redis.call("hset", meta_key, "e", epoch)
    end
    if dropped and state_meta_key ~= "" then
        if redis.call("hget", state_meta_key, "epoch") == dropped then
            redis.call("hset", state_meta_key, "epoch", epoch)
        end
    end
end
`

// lineageScript is a meta script whose source depends on the lineage mode and,
// with the gate, on the token.
type lineageScript struct {
	name     string
	body     string
	loadSHA1 bool
	none     *rueidis.Lua
	info     *rueidis.Lua
	shas     sync.Map // token -> SHA-1 of the gated source
}

func newLineageScript(name, body string, loadSHA1 bool) *lineageScript {
	return &lineageScript{
		name:     name,
		body:     body,
		loadSHA1: loadSHA1,
		none:     rueidis.NewLuaScript(lineagePreludeNone+body, rueidis.WithLoadSHA1(loadSHA1)),
		info:     rueidis.NewLuaScript(lineagePreludeInfo+body, rueidis.WithLoadSHA1(loadSHA1)),
	}
}

func (s *lineageScript) gatedSource(token string, count, twin bool) string {
	return lineagePreludeGate(token, count, twin) + s.body
}

func sha1Hex(src string) string {
	sum := sha1.Sum([]byte(src))
	return hex.EncodeToString(sum[:])
}

// redisServerKind is what the server answered to HELLO.
type redisServerKind struct {
	gateCapable bool   // Redis 7+ or Valkey, not Dragonfly
	primary     bool   // HELLO role is master
	desc        string // for logs
}

// helloFields keeps the string fields of a HELLO reply.
func helloFields(m map[string]rueidis.RedisMessage) map[string]string {
	fields := make(map[string]string, len(m))
	for k, v := range m {
		if str, err := v.ToString(); err == nil {
			fields[k] = str
		}
	}
	return fields
}

// parseHelloReply classifies a server by the string fields of its HELLO reply.
func parseHelloReply(fields map[string]string) redisServerKind {
	server := fields["server"]
	version := fields["version"]
	desc := server + " " + version
	primary := fields["role"] == "master"
	if dv, ok := fields["dragonfly_version"]; ok {
		return redisServerKind{primary: primary, desc: "dragonfly " + dv}
	}
	major := 0
	if dot := strings.IndexByte(version, '.'); dot > 0 {
		major, _ = strconv.Atoi(version[:dot])
	} else {
		major, _ = strconv.Atoi(version)
	}
	switch server {
	case "redis", "valkey":
		return redisServerKind{gateCapable: major >= 7, primary: primary, desc: desc}
	default:
		// An unknown server: no assumption about its script cache.
		return redisServerKind{primary: primary, desc: desc}
	}
}

// redisLineage is the lineage state of one RedisShard, shared by every broker
// on it.
type redisLineage struct {
	shard *RedisShard

	kindMu   sync.Mutex
	kindDone bool
	kind     redisServerKind

	infoMu      sync.Mutex
	infoProbed  bool
	infoAllowed bool

	openMu sync.Mutex

	scriptsMu sync.Mutex
	scripts   []*lineageScript // gated scripts, loaded whenever the gate opens
	loadSHA1  bool
	// selective: ResyncOnlyLostSlots - writes are counted, observations
	// tracked, and a lineage break resets only the slots where an observed
	// write is missing.
	selective bool
	node      *Node

	obsMu sync.Mutex
	// obs is the newest observed write per slot: its token and its count.
	obs map[int]lineageObs
	// adopted are slots kept through a lineage break: their ancestor token and
	// the count at the break. A later observation of the ancestor beyond it is
	// a write on the old server (a split brain): the slot is rotated.
	adopted map[int]lineageObs

	// tokens are the tokens this node runs the scripts of: one outside Redis
	// Cluster, one per slot in Redis Cluster.
	tokensMu sync.RWMutex
	tokens   map[int]string
	// known are the tokens this node has used, newest last: the markers a
	// demoted server may still hold.
	known []string
	// identity of the server the gate last opened on: CLIENT INFO laddr outside
	// Redis Cluster; in Redis Cluster the owner node ID of each slot.
	identity string
	owners   map[int]string

	// twin: the local twin of the write counter is in use (see
	// lineagePreludeGate); checked once, in gate mode, that scripts can use
	// redis.set_repl.
	twinMu     sync.Mutex
	twinProbed bool
	twin       atomic.Bool

	probeOnce sync.Once
	stats     lineageStats

	logFn func(level LogLevel, msg string, fields map[string]any)

	// disabledForTest turns detection off (tests showing the behavior without it).
	disabledForTest bool
	// twinDisabledForTest turns the twin counter off (tests showing what the
	// script cache alone detects).
	twinDisabledForTest bool
}

type lineageStats struct {
	mu         sync.Mutex
	rotations  int
	flushes    int
	keptSlots  int
	resetSlots int
}

func newRedisLineage(shard *RedisShard) *redisLineage {
	return &redisLineage{
		shard:   shard,
		tokens:  make(map[int]string),
		owners:  make(map[int]string),
		obs:     make(map[int]lineageObs),
		adopted: make(map[int]lineageObs),
	}
}

type lineageObs struct {
	token string
	count uint64
}

// splitObservation strips the observation suffix ";count;token" a counting
// script adds to an epoch.
func splitObservation(epoch string) (string, uint64, string, bool) {
	semi := strings.IndexByte(epoch, ';')
	if semi < 0 {
		return epoch, 0, "", false
	}
	rest := epoch[semi+1:]
	semi2 := strings.IndexByte(rest, ';')
	if semi2 < 0 {
		return epoch[:semi], 0, "", false
	}
	count, err := strconv.ParseUint(rest[:semi2], 10, 64)
	if err != nil {
		return epoch[:semi], 0, "", false
	}
	return epoch[:semi], count, rest[semi2+1:], true
}

// takeObservation records the observation an epoch from a reply or a PUB/SUB
// payload carries, for the slot of key, and returns the epoch without it. It
// must run before what was observed reaches anyone.
func (l *redisLineage) takeObservation(key, epoch string) string {
	plain, count, token, ok := splitObservation(epoch)
	if !ok {
		return plain
	}
	l.observe(l.slotIndex(key), token, count)
	return plain
}

func (l *redisLineage) observe(slot int, token string, count uint64) {
	l.obsMu.Lock()
	o := l.obs[slot]
	if o.token != token {
		// Another lineage: the newest observation stands for the slot. Counts of
		// different tokens don't compare (a lossy failover moves the counter
		// back); an old token seen late only makes the next survey reset.
		l.obs[slot] = lineageObs{token: token, count: count}
	} else if count > o.count {
		l.obs[slot] = lineageObs{token: token, count: count}
	}
	a := l.adopted[slot]
	splitBrain := a.token != "" && a.token == token && count > a.count
	if splitBrain {
		delete(l.adopted, slot)
	}
	l.obsMu.Unlock()
	if splitBrain {
		l.log(LogLevelWarn, "redis lineage: a write on a server of the ancestor lineage after the break (split brain), resetting the slot", map[string]any{"slot": slot})
		key := lineageTokenKeyBase
		if l.shard.isCluster {
			key = lineageTokenKeyForSlot(slot)
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = l.open(ctx, key, true, "")
		}()
	}
}

// observed returns the newest observed count of token per slot.
func (l *redisLineage) observed(slots []int, tokens []string) []uint64 {
	l.obsMu.Lock()
	defer l.obsMu.Unlock()
	out := make([]uint64, len(slots))
	for i, s := range slots {
		if o := l.obs[s]; o.token == tokens[i] {
			out[i] = o.count
		}
	}
	return out
}

// register adds a gated script loaded whenever the gate opens. loadSHA1 is the
// broker's FIPS setting.
func (l *redisLineage) register(s *lineageScript, loadSHA1, selective bool) {
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	if loadSHA1 {
		l.loadSHA1 = true
	}
	if selective {
		l.selective = true
	}
	for _, existing := range l.scripts {
		if existing == s {
			return
		}
	}
	l.scripts = append(l.scripts, s)
}

// setLogger logs lineage events with the node's logger (the first node wins).
func (l *redisLineage) setLogger(n *Node) {
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	if l.logFn != nil {
		return
	}
	l.node = n
	n.registerLineage(l)
	l.logFn = func(level LogLevel, msg string, fields map[string]any) {
		if n.logEnabled(level) {
			n.logger.log(newLogEntry(level, msg, fields))
		}
	}
}

func (l *redisLineage) log(level LogLevel, msg string, fields map[string]any) {
	l.scriptsMu.Lock()
	fn := l.logFn
	l.scriptsMu.Unlock()
	if fn != nil {
		fn(level, msg, fields)
	}
}

// serverKind asks the server with HELLO once.
func (l *redisLineage) serverKind(ctx context.Context) (redisServerKind, error) {
	l.kindMu.Lock()
	defer l.kindMu.Unlock()
	if l.kindDone {
		return l.kind, nil
	}
	m, err := l.shard.client.Do(ctx, l.shard.client.B().Hello().Build()).AsMap()
	if err != nil {
		return redisServerKind{}, fmt.Errorf("lineage: HELLO: %w", err)
	}
	l.kind = parseHelloReply(helloFields(m))
	l.kindDone = true
	if !l.kind.gateCapable {
		l.log(LogLevelWarn, "redis lineage: this server can't detect a failover or a restart that lost writes (Redis below 7, Dragonfly or an unknown server): use Redis 7+ or Valkey, or enable UseInfoCommandForLineage to detect it with INFO", map[string]any{"server": l.kind.desc})
	}
	return l.kind, nil
}

// probeInfo checks once whether scripts may call INFO.
func (l *redisLineage) probeInfo(ctx context.Context) bool {
	l.infoMu.Lock()
	defer l.infoMu.Unlock()
	if l.infoProbed {
		return l.infoAllowed
	}
	probe := rueidis.NewLuaScript(`local i = redis.call("info", "replication"); return string.match(i, "master_replid:(%w+)") or ""`,
		rueidis.WithLoadSHA1(l.loadSHA1))
	res, err := probe.Exec(ctx, l.shard.client, []string{l.anyTokenKey()}, nil).ToString()
	if err != nil {
		if _, ok := rueidis.IsRedisErr(err); ok {
			// A Redis error (an ACL refusal, for example): INFO isn't allowed.
			l.infoProbed = true
			l.infoAllowed = false
			l.log(LogLevelWarn, "redis lineage: UseInfoCommandForLineage is on but scripts can't call INFO (the ACL user needs the @dangerous category)", map[string]any{"error": err.Error()})
		}
		return false
	}
	l.infoProbed = true
	l.infoAllowed = res != ""
	return l.infoAllowed
}

func (l *redisLineage) twinOn() bool {
	return l.twin.Load()
}

// probeTwin checks once that scripts can write without replication
// (redis.set_repl): a managed service could take it away. Without it the twin
// counter is off, and every node finds the same, since it's a property of the
// server.
func (l *redisLineage) probeTwin(ctx context.Context) {
	l.twinMu.Lock()
	defer l.twinMu.Unlock()
	if l.twinProbed {
		return
	}
	probe := rueidis.NewLuaScript(`redis.set_repl(redis.REPL_NONE); redis.set_repl(redis.REPL_ALL); return 1`,
		rueidis.WithLoadSHA1(l.loadSHA1))
	err := probe.Exec(ctx, l.shard.client, []string{l.anyTokenKey()}, nil).Error()
	if err != nil {
		if _, ok := rueidis.IsRedisErr(err); !ok {
			return // a connection error: probe again with the next broker
		}
		l.log(LogLevelWarn, "redis lineage: scripts can't use redis.set_repl, a failback to a server that took a full copy of another can't be detected", map[string]any{"error": err.Error()})
	}
	l.twinProbed = true
	l.twin.Store(err == nil && !l.twinDisabledForTest)
}

// mode returns the lineage mode for a broker on this shard.
func (l *redisLineage) mode(ctx context.Context, useInfo bool) (lineageMode, error) {
	if l.disabledForTest {
		return lineageNone, nil
	}
	if useInfo && l.probeInfo(ctx) {
		return lineageInfo, nil
	}
	kind, err := l.serverKind(ctx)
	if err != nil {
		return lineageNone, err
	}
	if kind.gateCapable {
		l.probeTwin(ctx)
		l.startProbe()
		return lineageGate, nil
	}
	return lineageNone, nil
}

func (l *redisLineage) slotIndex(key string) int {
	if !l.shard.isCluster {
		return 0
	}
	return int(redisKeySlot(key))
}

// tokenKey returns the token key in the slot of key.
func (l *redisLineage) tokenKey(key string) string {
	if !l.shard.isCluster {
		return lineageTokenKeyBase
	}
	return lineageTokenKeyForSlot(int(redisKeySlot(key)))
}

func (l *redisLineage) anyTokenKey() string {
	if !l.shard.isCluster {
		return lineageTokenKeyBase
	}
	return lineageTokenKeyForSlot(0)
}

func (l *redisLineage) token(slot int) string {
	l.tokensMu.RLock()
	defer l.tokensMu.RUnlock()
	return l.tokens[slot]
}

// parseTokenValue splits a token key's value "token" or
// "token|ancestor|count".
func parseTokenValue(value string) (token string, ancestor lineageObs) {
	bar := strings.IndexByte(value, '|')
	if bar < 0 {
		return value, lineageObs{}
	}
	token = value[:bar]
	rest := value[bar+1:]
	bar2 := strings.IndexByte(rest, '|')
	if bar2 < 0 {
		return token, lineageObs{token: rest}
	}
	count, _ := strconv.ParseUint(rest[bar2+1:], 10, 64)
	return token, lineageObs{token: rest[:bar2], count: count}
}

// setToken sets the token of a slot from a token key's value.
func (l *redisLineage) setToken(slot int, value string) {
	token, ancestor := parseTokenValue(value)
	l.tokensMu.Lock()
	l.tokens[slot] = token
	l.rememberLocked(token)
	l.tokensMu.Unlock()
	if ancestor.token != "" {
		l.obsMu.Lock()
		if l.adopted[slot].token == "" {
			l.adopted[slot] = ancestor
		}
		l.obsMu.Unlock()
	}
}

func (l *redisLineage) rememberLocked(token string) {
	if token == "" {
		return
	}
	for _, t := range l.known {
		if t == token {
			return
		}
	}
	l.known = append(l.known, token)
	if len(l.known) > 16 {
		l.known = l.known[len(l.known)-16:]
	}
}

func (l *redisLineage) knownTokens() []string {
	l.tokensMu.RLock()
	defer l.tokensMu.RUnlock()
	return append([]string(nil), l.known...)
}

// errLineageGate is returned when the gate could not be opened.
var errLineageGate = errors.New("lineage gate")

// sha returns the SHA-1 of a source, asking the server in FIPS mode (without
// loading the script).
func (l *redisLineage) sha(ctx context.Context, c rueidis.CoreClient, cacheKey *sync.Map, token, src string) (string, error) {
	if v, ok := cacheKey.Load(token); ok {
		return v.(string), nil
	}
	l.scriptsMu.Lock()
	fips := l.loadSHA1
	l.scriptsMu.Unlock()
	var sha string
	if !fips {
		sha = sha1Hex(src)
	} else {
		// The helper is not gated: loading it opens nothing.
		res, err := c.Do(ctx, c.B().Eval().Script(lineageSha1Source).Numkeys(1).Key(l.anyTokenKey()).Arg(src).Build()).ToString()
		if err != nil {
			return "", err
		}
		sha = res
	}
	cacheKey.Store(token, sha)
	return sha, nil
}

var lineageMarkerShas sync.Map

// exec runs a gated script with EVALSHA, opening the gate on NOSCRIPT and
// following the server's token on a LINEAGE error.
func (l *redisLineage) exec(ctx context.Context, s *lineageScript, keys, args []string) rueidis.RedisResult {
	slot := l.slotIndex(keys[0])
	for attempt := 0; attempt < 5; attempt++ {
		token := l.token(slot)
		if token == "" {
			if err := l.open(ctx, keys[0], false, token); err != nil {
				return rueidis.NewErrorResult(err)
			}
			continue
		}
		twin := l.twinOn()
		sha, err := l.sha(ctx, l.shard.client, &s.shas, token+"|"+strconv.FormatBool(l.selective)+"|"+strconv.FormatBool(twin), s.gatedSource(token, l.selective, twin))
		if err != nil {
			return rueidis.NewErrorResult(err)
		}
		cmd := l.shard.client.B().Evalsha().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
		resp := l.shard.client.Do(ctx, cmd)
		err = resp.Error()
		if isNoScript(err) {
			if err := l.open(ctx, keys[0], false, token); err != nil {
				return rueidis.NewErrorResult(err)
			}
			continue
		}
		if t, ok := lineageErrToken(err); ok {
			// The server's token isn't ours: run the scripts of its token.
			l.setToken(slot, t)
			continue
		}
		if isLineageCopy(err) {
			// The server's data came from another server: the gate rotates.
			if err := l.open(ctx, keys[0], false, token); err != nil {
				return rueidis.NewErrorResult(err)
			}
			continue
		}
		l.shard.noteError(err)
		return resp
	}
	return rueidis.NewErrorResult(fmt.Errorf("%w: script %s: no stable lineage token", errLineageGate, s.name))
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	re, ok := rueidis.IsRedisErr(err)
	return ok && re.IsNoScript()
}

func isReadOnly(err error) bool {
	if err == nil {
		return false
	}
	re, ok := rueidis.IsRedisErr(err)
	return ok && strings.HasPrefix(re.Error(), "READONLY")
}

// isLineageCopy tells a LINEAGE_COPY error (see lineageTwinErrPrefix).
func isLineageCopy(err error) bool {
	if err == nil {
		return false
	}
	re, ok := rueidis.IsRedisErr(err)
	return ok && strings.HasPrefix(re.Error(), lineageTwinErrPrefix)
}

// lineageErrToken parses a LINEAGE error: the server's token ("" for none).
func lineageErrToken(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	re, ok := rueidis.IsRedisErr(err)
	if !ok {
		return "", false
	}
	msg := re.Error()
	if !strings.HasPrefix(msg, lineageErrPrefix) {
		return "", false
	}
	rest := msg[len(lineageErrPrefix):]
	if sp := strings.IndexByte(rest, ' '); sp >= 0 {
		rest = rest[:sp]
	}
	if rest == "-" {
		rest = ""
	}
	return rest, true
}

func (l *redisLineage) marker(ctx context.Context, c rueidis.CoreClient, token string) (string, error) {
	return l.sha(ctx, c, &lineageMarkerShas, token, lineageMarkerSource(token))
}

// open opens the gate of the server serving routeKey, all on one connection.
// With force, the token is rotated even if the server holds the marker of its
// token (a node saw a different server behind its connection). seen is the
// token the caller found missing or unusable: when another caller has replaced
// it meanwhile, the gate is open already and nothing is done.
func (l *redisLineage) open(ctx context.Context, routeKey string, force bool, seen string) error {
	l.openMu.Lock()
	defer l.openMu.Unlock()
	if !force && l.token(l.slotIndex(routeKey)) != seen {
		return nil
	}

	l.scriptsMu.Lock()
	scripts := append([]*lineageScript(nil), l.scripts...)
	l.scriptsMu.Unlock()

	slot := l.slotIndex(routeKey)
	tokenKey := l.tokenKey(routeKey)
	var (
		rotated       bool
		adoptedValue  string
		kept          int
		keptAncestors = map[int]lineageObs{}
		newToken      string
		slots         []int
		identity      string
		owner         string
		prevToken     = l.token(slot)
		copiedSeen    bool
	)
	err := l.shard.client.Dedicated(func(c rueidis.DedicatedClient) error {
		// The first command is keyed: it binds the connection to the server
		// serving routeKey.
		currentValue, err := c.Do(ctx, c.B().Get().Key(tokenKey).Build()).ToString()
		if err != nil && !rueidis.IsRedisNil(err) {
			return err
		}
		current, _ := parseTokenValue(currentValue)
		hello, err := c.Do(ctx, c.B().Hello().Build()).AsMap()
		if err != nil {
			return fmt.Errorf("HELLO: %w", err)
		}
		kind := parseHelloReply(helloFields(hello))
		if !kind.gateCapable {
			return fmt.Errorf("server %s can't use the gate", kind.desc)
		}
		if !kind.primary {
			// A demoted server keeps its script cache: never load anything on a
			// replica (centrifugo#1320).
			return errors.New("server is not a primary")
		}
		slots, identity, owner, err = l.serverSlots(ctx, c, routeKey)
		if err != nil {
			return err
		}
		// copied: the server's data came from another server (the twin of the
		// write counter differs, see lineagePreludeGate) - even if it holds
		// the marker, as an old primary does after a failback.
		copied := false
		if l.twinOn() {
			t, err := c.Do(ctx, c.B().Hmget().Key(tokenKey+".n").Field("c", "l").Build()).ToArray()
			if err != nil {
				return fmt.Errorf("twin: %w", err)
			}
			if len(t) == 2 {
				cv, cerr := t[0].ToString()
				lv, _ := t[1].ToString()
				copied = cerr == nil && cv != lv
			}
		}
		intact := false
		if current != "" && !force && !copied {
			msha, err := l.marker(ctx, c, current)
			if err != nil {
				return err
			}
			mresp := c.Do(ctx, c.B().Evalsha().Sha1(msha).Numkeys(1).Key(tokenKey).Build())
			if merr := mresp.Error(); merr == nil {
				intact = true
			} else if !isNoScript(merr) {
				return merr
			}
		}
		token := current
		if !intact && !force && !copied && l.shard.isCluster {
			// A slot whose token has no marker here may have been migrated in
			// from another server (MIGRATE moves its keys without loss) while
			// this server's own lineage is intact. Then only that slot is
			// adopted, under this server's token with its own as the ancestor,
			// instead of rotating - and resetting - every slot of the server.
			own, ok, err := l.intactServerToken(ctx, c, slots, slot)
			if err != nil {
				return err
			}
			if ok {
				adoptedValue, err = l.adoptSlot(ctx, c, tokenKey, currentValue, own)
				if err != nil {
					return err
				}
				token = own
				intact = true
			}
		}
		if !intact {
			// A new lineage (or the cache was flushed, or a different server):
			// rotate the token of every slot this server serves, then load.
			token = "g" + strconv.FormatInt(time.Now().UnixNano(), 36) + strconv.FormatUint(lineageRand(), 36)
			keys := make([]string, len(slots))
			for i, s := range slots {
				if l.shard.isCluster {
					keys[i] = lineageTokenKeyForSlot(s)
				} else {
					keys[i] = lineageTokenKeyBase
				}
			}
			values := make([]string, len(slots))
			for i := range values {
				values[i] = token
			}
			if l.selective {
				kept = l.keepSlots(ctx, c, slots, keys, token, values, keptAncestors)
			}
			args := make([]string, 0, 2*len(slots)+3)
			args = append(args, tokenKey, currentValue, boolToStr(l.twinOn()))
			for i := range keys {
				args = append(args, keys[i], values[i])
			}
			src := lineageRotateSource
			if l.shard.isCluster {
				src = lineageRotateSourceCluster
			}
			other, err := c.Do(ctx, c.B().Eval().Script(src).Numkeys(0).Arg(args...).Build()).ToString()
			if err != nil && !rueidis.IsRedisNil(err) {
				return fmt.Errorf("rotate: %w", err)
			}
			if err == nil {
				// Another node rotated first, for this same lineage break.
				token, _ = parseTokenValue(other)
				adoptedValue = other
				clear(keptAncestors)
				kept = 0
			} else {
				rotated = true
			}
		}
		for _, s := range scripts {
			if err := c.Do(ctx, c.B().ScriptLoad().Script(s.gatedSource(token, l.selective, l.twinOn())).Build()).Error(); err != nil {
				return fmt.Errorf("load %s: %w", s.name, err)
			}
		}
		if err := c.Do(ctx, c.B().ScriptLoad().Script(lineageMarkerSource(token)).Build()).Error(); err != nil {
			return fmt.Errorf("load marker: %w", err)
		}
		newToken = token
		copiedSeen = copied
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %v", errLineageGate, err)
	}

	if adoptedValue != "" {
		// The other node's rotation: this slot's token (and its ancestor, if
		// that node kept the slot).
		l.setToken(slot, adoptedValue)
	}
	l.tokensMu.Lock()
	if rotated {
		for _, s := range slots {
			l.tokens[s] = newToken
		}
		l.obsMu.Lock()
		for _, s := range slots {
			if a, ok := keptAncestors[s]; ok {
				l.adopted[s] = a
			} else {
				delete(l.adopted, s)
			}
		}
		l.obsMu.Unlock()
	} else {
		l.tokens[slot] = newToken
	}
	l.rememberLocked(newToken)
	if l.shard.isCluster {
		for _, s := range slots {
			l.owners[s] = owner
		}
	} else {
		l.identity = identity
	}
	l.tokensMu.Unlock()

	if rotated {
		l.stats.mu.Lock()
		l.stats.rotations++
		l.stats.keptSlots += kept
		l.stats.resetSlots += len(slots) - kept
		l.stats.mu.Unlock()
		l.log(LogLevelInfo, "redis lineage: new lineage token (a restart, a failover or SCRIPT FLUSH may have lost writes), streams and maps on the server start new epochs", map[string]any{
			"forced": force, "copied": copiedSeen, "previous_token": prevToken, "kept_slots": kept, "reset_slots": len(slots) - kept,
		})
	}
	return nil
}

// serverSlots returns the slots of the connected server (one slot outside
// Redis Cluster) and its identity: its CLIENT INFO laddr outside Redis Cluster,
// its node ID in Redis Cluster.
func (l *redisLineage) serverSlots(ctx context.Context, c rueidis.DedicatedClient, routeKey string) ([]int, string, string, error) {
	if !l.shard.isCluster {
		info, err := c.Do(ctx, c.B().ClientInfo().Build()).ToString()
		if err != nil {
			return nil, "", "", fmt.Errorf("CLIENT INFO: %w", err)
		}
		return []int{0}, clientInfoField(info, "laddr"), "", nil
	}
	nodes, err := c.Do(ctx, c.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return nil, "", "", fmt.Errorf("CLUSTER NODES: %w", err)
	}
	id, slots, err := parseMyselfSlots(nodes)
	if err != nil {
		return nil, "", "", err
	}
	want := int(redisKeySlot(routeKey))
	for _, s := range slots {
		if s == want {
			return slots, "", id, nil
		}
	}
	return nil, "", "", fmt.Errorf("server does not serve slot %d", want)
}

func clientInfoField(info, name string) string {
	for _, f := range strings.Fields(info) {
		if strings.HasPrefix(f, name+"=") {
			return f[len(name)+1:]
		}
	}
	return ""
}

// parseMyselfSlots returns the node ID and the slots of the "myself" line of
// CLUSTER NODES.
func parseMyselfSlots(nodes string) (string, []int, error) {
	for _, n := range lineageParseClusterNodes(nodes) {
		if !n.myself {
			continue
		}
		if !n.primary {
			return "", nil, errors.New("connected node is not a primary")
		}
		return n.id, n.slots, n.err
	}
	return "", nil, errors.New("no myself line in CLUSTER NODES")
}

type clusterNodeLine struct {
	id      string
	addr    string
	myself  bool
	primary bool
	failed  bool
	slots   []int
	err     error
}

func lineageParseClusterNodes(nodes string) []clusterNodeLine {
	var out []clusterNodeLine
	for _, line := range strings.Split(nodes, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		n := clusterNodeLine{id: fields[0], addr: fields[1]}
		if at := strings.IndexByte(n.addr, '@'); at >= 0 {
			n.addr = n.addr[:at]
		}
		flags := fields[2]
		n.myself = strings.Contains(flags, "myself")
		n.primary = strings.Contains(flags, "master")
		n.failed = strings.Contains(flags, "fail") && !strings.Contains(flags, "pfail")
		for _, f := range fields[8:] {
			if strings.HasPrefix(f, "[") {
				continue // a slot being migrated or imported
			}
			lo, hi := f, f
			if dash := strings.IndexByte(f, '-'); dash > 0 {
				lo, hi = f[:dash], f[dash+1:]
			}
			a, err1 := strconv.Atoi(lo)
			b, err2 := strconv.Atoi(hi)
			if err1 != nil || err2 != nil || a < 0 || b > 16383 || a > b {
				n.err = fmt.Errorf("bad slot range %q", f)
				break
			}
			for s := a; s <= b; s++ {
				n.slots = append(n.slots, s)
			}
		}
		out = append(out, n)
	}
	return out
}

// lineageProbeInterval is how often a node probes the lineage of its servers.
var lineageProbeInterval = time.Second

func (l *redisLineage) startProbe() {
	l.probeOnce.Do(func() {
		go l.runProbe()
	})
}

func (l *redisLineage) runProbe() {
	t := time.NewTicker(lineageProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-l.shard.closeCh:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			l.probe(ctx)
			cancel()
		}
	}
}

// probe opens the gate of a primary that started a new lineage before any
// traffic needs it, rotates the token of a server that changed behind this
// node's connection, and flushes the script cache of a demoted server holding
// our scripts (it keeps them through a resync, and promoted again would run
// them on data that may miss writes).
func (l *redisLineage) probe(ctx context.Context) {
	if l.shard.isCluster {
		l.probeCluster(ctx)
		return
	}
	l.probeSingle(ctx)
}

func (l *redisLineage) probeSingle(ctx context.Context) {
	c := l.shard.client
	hello, err := c.Do(ctx, c.B().Hello().Build()).AsMap()
	if err != nil {
		return
	}
	kind := parseHelloReply(helloFields(hello))
	if !kind.primary {
		// Our connection reaches a demoted server (centrifugo#1320): flush it.
		l.flushIfHolding(ctx, c)
		return
	}
	info, err := c.Do(ctx, c.B().ClientInfo().Build()).ToString()
	if err != nil {
		return
	}
	identity := clientInfoField(info, "laddr")
	l.tokensMu.RLock()
	prev := l.identity
	token := l.tokens[0]
	l.tokensMu.RUnlock()
	if prev != "" && identity != "" && identity != prev {
		// A different server behind the same address: its data may come from a
		// lineage our token doesn't describe.
		_ = l.open(ctx, lineageTokenKeyBase, true, "")
		return
	}
	if token == "" {
		return
	}
	if !l.holdsMarker(ctx, c, token) {
		_ = l.open(ctx, lineageTokenKeyBase, false, token)
	}
	l.flushReplicas(ctx)
}

func (l *redisLineage) probeCluster(ctx context.Context) {
	c := l.shard.client
	nodes, err := c.Do(ctx, c.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return
	}
	lines := lineageParseClusterNodes(nodes)
	clients := c.Nodes()
	for _, n := range lines {
		if n.err != nil || n.failed {
			continue
		}
		if !n.primary {
			if nc, ok := clients[n.addr]; ok {
				l.flushIfHolding(ctx, nc)
			}
			continue
		}
		if len(n.slots) == 0 {
			continue
		}
		l.tokensMu.RLock()
		changed := -1
		used := false
		for _, s := range n.slots {
			if l.tokens[s] != "" {
				used = true
			}
			if l.owners[s] != "" && l.owners[s] != n.id {
				changed = s
				break
			}
		}
		l.tokensMu.RUnlock()
		if changed >= 0 {
			// The slot is served by another node than when the gate opened: a
			// failover (the promoted replica has no scripts, its gate opens on
			// NOSCRIPT) or a migration (adopted by the gate). Nothing to force;
			// a demoted primary is flushed as a replica.
			l.tokensMu.Lock()
			for _, sl := range n.slots {
				delete(l.owners, sl)
			}
			l.tokensMu.Unlock()
		}
		if !used {
			continue
		}
		key := lineageTokenKeyForSlot(n.slots[0])
		current, err := c.Do(ctx, c.B().Get().Key(key).Build()).ToString()
		if err != nil || current == "" {
			continue
		}
		if !l.holdsMarkerKey(ctx, c, current, key) {
			_ = l.open(ctx, key, false, l.token(n.slots[0]))
		}
	}
}

// flushReplicas flushes demoted servers reachable outside Redis Cluster
// (the replica client of Sentinel setups).
func (l *redisLineage) flushReplicas(ctx context.Context) {
	if l.shard.replicaClient != nil {
		l.flushIfHolding(ctx, l.shard.replicaClient)
	}
}

func (l *redisLineage) holdsMarker(ctx context.Context, c rueidis.Client, token string) bool {
	return l.holdsMarkerKey(ctx, c, token, l.anyTokenKey())
}

func (l *redisLineage) holdsMarkerKey(ctx context.Context, c rueidis.Client, token, key string) bool {
	sha, err := l.marker(ctx, c, token)
	if err != nil {
		return true // unknown: don't open the gate from the probe
	}
	res, err := c.Do(ctx, c.B().ScriptExists().Sha1(sha).Build().SetSlot(key)).AsIntSlice()
	if err != nil || len(res) != 1 {
		return true
	}
	return res[0] == 1
}

// flushIfHolding flushes the script cache of a server that is not a primary and
// holds the marker of a token this node used.
func (l *redisLineage) flushIfHolding(ctx context.Context, c rueidis.Client) {
	tokens := l.knownTokens()
	if len(tokens) == 0 {
		return
	}
	shas := make([]string, 0, len(tokens))
	for _, t := range tokens {
		sha, err := l.marker(ctx, c, t)
		if err != nil {
			return
		}
		shas = append(shas, sha)
	}
	err := c.Dedicated(func(dc rueidis.DedicatedClient) error {
		hello, err := dc.Do(ctx, dc.B().Hello().Build()).AsMap()
		if err != nil {
			return err
		}
		if parseHelloReply(helloFields(hello)).primary {
			return nil
		}
		res, err := dc.Do(ctx, dc.B().ScriptExists().Sha1(shas...).Build()).AsIntSlice()
		if err != nil {
			return err
		}
		holds := false
		for _, v := range res {
			if v == 1 {
				holds = true
			}
		}
		if !holds {
			return nil
		}
		if err := dc.Do(ctx, dc.B().ScriptFlush().Build()).Error(); err != nil {
			return err
		}
		l.stats.mu.Lock()
		l.stats.flushes++
		l.stats.mu.Unlock()
		l.log(LogLevelInfo, "redis lineage: flushed the script cache of a demoted server", nil)
		return nil
	})
	if err != nil {
		l.log(LogLevelDebug, "redis lineage: probing a replica", map[string]any{"error": err.Error()})
	}
}

// onReadOnly handles a READONLY reply to a meta script: the server behind the
// connection was demoted.
func (l *redisLineage) onReadOnly(ctx context.Context) {
	if l.shard.isCluster {
		return
	}
	l.flushIfHolding(ctx, l.shard.client)
}

var (
	lineageSlotTagsOnce sync.Once
	lineageSlotTags     [16384]string
)

// lineageTokenKeyForSlot returns the token key living in a hash slot.
func lineageTokenKeyForSlot(slot int) string {
	lineageSlotTagsOnce.Do(func() {
		found := 0
		for i := 0; found < 16384; i++ {
			tag := strconv.FormatInt(int64(i), 36)
			s := redispartition.TagSlot(tag)
			if lineageSlotTags[s] == "" {
				lineageSlotTags[s] = tag
				found++
			}
		}
	})
	return lineageTokenKeyBase + ".{" + lineageSlotTags[slot] + "}"
}

// redisKeySlot returns the hash slot of a key, honouring hash tags.
func redisKeySlot(key string) uint16 {
	if start := strings.IndexByte(key, '{'); start >= 0 {
		if end := strings.IndexByte(key[start+1:], '}'); end > 0 {
			key = key[start+1 : start+1+end]
		}
	}
	return uint16(redispartition.TagSlot(key))
}

func lineageRand() uint64 {
	var b [8]byte
	_, _ = crand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

// keepSlots decides, for ResyncOnlyLostSlots, which slots keep their metas
// through a rotation: those where no node observed a write the server misses.
// A kept slot's token key gets "token|ancestor|count", so its metas stamped with
// the ancestor are adopted. Any doubt (a read error, a node that doesn't answer)
// keeps nothing.
func (l *redisLineage) keepSlots(ctx context.Context, c rueidis.DedicatedClient, slots []int, keys []string, token string, values []string, kept map[int]lineageObs) int {
	src := lineageSlotsSource
	if l.shard.isCluster {
		src = lineageSlotsSourceCluster
	}
	raw, err := c.Do(ctx, c.B().Eval().Script(src).Numkeys(0).Arg(keys...).Build()).AsStrSlice()
	if err != nil || len(raw) != 2*len(slots) {
		return 0
	}
	states := make([]lineageSlotState, len(slots))
	for i := range slots {
		tok, anc := parseTokenValue(raw[2*i])
		count, _ := strconv.ParseUint(raw[2*i+1], 10, 64)
		states[i] = lineageSlotState{token: tok, ancestor: anc, count: count}
	}
	tokens, counts, complete, err := l.surveyObserved(ctx, slots)
	if err != nil || !complete {
		l.log(LogLevelInfo, "redis lineage: not every node answered the lineage survey, resetting every slot", map[string]any{"error": fmt.Sprint(err)})
		return 0
	}
	n := 0
	for i, s := range slots {
		if slotKept(states[i], tokens, counts, i) {
			values[i] = token + "|" + states[i].token + "|" + strconv.FormatUint(states[i].count, 10)
			kept[s] = lineageObs{token: states[i].token, count: states[i].count}
			n++
		}
	}
	return n
}

// intactServerToken looks, on the gate's connection, for a token of another
// slot this server serves whose marker the server holds: the server's own
// lineage is then intact. It returns that token.
func (l *redisLineage) intactServerToken(ctx context.Context, c rueidis.DedicatedClient, slots []int, except int) (string, bool, error) {
	keys := make([]string, 0, 8)
	for _, s := range slots {
		if s == except {
			continue
		}
		keys = append(keys, lineageTokenKeyForSlot(s))
		if len(keys) == cap(keys) {
			break
		}
	}
	if len(keys) == 0 {
		return "", false, nil
	}
	raw, err := c.Do(ctx, c.B().Eval().Script(lineageSlotsSourceCluster).Numkeys(0).Arg(keys...).Build()).AsStrSlice()
	if err != nil {
		return "", false, err
	}
	seen := map[string]bool{}
	for i := 0; i+1 < len(raw); i += 2 {
		token, _ := parseTokenValue(raw[i])
		if token == "" || seen[token] {
			continue
		}
		seen[token] = true
		sha, err := l.marker(ctx, c, token)
		if err != nil {
			return "", false, err
		}
		res, err := c.Do(ctx, c.B().ScriptExists().Sha1(sha).Build()).AsIntSlice()
		if err != nil {
			return "", false, err
		}
		if len(res) == 1 && res[0] == 1 {
			return token, true, nil
		}
	}
	return "", false, nil
}

// adoptSlot gives a slot migrated in the server's token, with the slot's token
// as its ancestor (its metas are adopted) and its write counter, with a
// compare-and-set like the rotation. It returns the slot's new value.
func (l *redisLineage) adoptSlot(ctx context.Context, c rueidis.DedicatedClient, tokenKey, currentValue, own string) (string, error) {
	raw, err := c.Do(ctx, c.B().Eval().Script(lineageSlotsSourceCluster).Numkeys(0).Arg(tokenKey).Build()).AsStrSlice()
	if err != nil || len(raw) != 2 {
		return "", fmt.Errorf("read slot: %v", err)
	}
	value := own
	if prev, _ := parseTokenValue(currentValue); prev != "" && prev != own {
		value = own + "|" + prev + "|" + raw[1]
	}
	other, err := c.Do(ctx, c.B().Eval().Script(lineageRotateSourceCluster).Numkeys(0).
		Arg(tokenKey, currentValue, boolToStr(l.twinOn()), tokenKey, value).Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return "", fmt.Errorf("adopt: %w", err)
	}
	if err == nil {
		return other, nil // another node did it first
	}
	return value, nil
}
