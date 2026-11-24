package centrifuge

import (
	"context"
	crand "crypto/rand"
	"crypto/sha1" //nolint:gosec // Names Lua scripts for EVALSHA, as Redis does. Not security-sensitive.
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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
// The token lives in the lineage key (one per Redis server outside Redis
// Cluster, one per hash slot in Redis Cluster), with the tokens it replaced. It
// changes when a server's data may have lost writes:
//
//   - The script cache. It is never persisted, and on Redis 7+ and Valkey it
//     never reaches replicas, so a restarted or promoted replica has none of
//     our scripts. Meta scripts run only with EVALSHA, and they and the gate's
//     marker script are specific to a token: a node runs the scripts of the
//     token it knows, and a script whose token isn't the server's fails with a
//     LINEAGE error. NOSCRIPT opens the gate, on one connection to the server:
//     if the server is a primary and holds the marker of its token, its lineage
//     is intact and the scripts load; otherwise the token of every slot the
//     server serves rotates first.
//   - Tokens left behind. A node never goes back to a token it saw replaced: a
//     server presenting one is a copy that missed a rotation (a demoted primary
//     that took a full copy of the new one, promoted back with our scripts
//     still loaded). The lineage key lists the tokens its token replaced, so
//     every node reading it learns them, and nodes pass the replaced tokens
//     they know to each other with their node info, for a restarted node.
//   - The idle probe: a node that made no call to a server for a second calls
//     it, so a server promoted while no traffic reached it opens its gate (and
//     rotates) before a failback could hide it.
//   - Reused offsets: a subscriber seeing an offset a server already gave out
//     (see offsetWatermarks) rotates the server's lineage.
//
// Redis 6 and Dragonfly send scripts to replicas, so the gate can't be used
// there: no lineage, only the reused-offset check per channel. With
// DetectLostWritesWithInfo the scripts read master_replid with INFO on every
// server instead, at the cost of INFO in every meta script and the @dangerous
// ACL category. It changes on every restart and promotion, and also when
// nothing was lost: a primary creating its replication backlog (its first
// replica) or freeing it (no replicas for repl-backlog-ttl) takes a new ID, so
// every stream on it starts a new epoch.

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

// lineageKeySuffix ends the name of the lineage key: "<prefix>.lineage.state" outside
// Redis Cluster, "<prefix>.lineage.state.{<slot tag>}" in Redis Cluster (one per hash
// slot). The prefix comes first, as in every other key, so an ACL user limited
// to the prefix's keys may use it. A prefix with a "{" would move the hash tag:
// its keys are "{<slot tag>}.<prefix>.lineage.state" instead. A lineage belongs to a key
// prefix - a deployment, or a broker of one: deployments sharing a Redis server
// detect lineage breaks independently and never adopt each other's metas.
// It is a hash: "t" the token value ("token", or "token|ancestor" for a slot
// migrated in, whose metas stamped with the ancestor are adopted), "p" the
// tokens the token replaced, newest first, separated by spaces.
const lineageKeySuffix = ".lineage.state"

// lineagePredecessors is how many replaced tokens a lineage key keeps (a
// number in Lua source).
const lineagePredecessors = "8"

// lineageErrPrefix starts the error a gated script returns when the lineage key
// doesn't hold the token the script was made for: "LINEAGE t=<value>
// p=<replaced tokens, comma-separated>". Parts are key=value, separated by
// spaces; parts with other keys are ignored, so a later version can add some.
const lineageErrPrefix = "LINEAGE "

// The meta field holding a meta's lineage stamp, one per mode: each mode checks
// only its own and adopts a meta without it, so switching modes (or nodes in
// different modes during a rollout) resets nothing.
const (
	lineageStampGate = "i"
	lineageStampInfo = "j"
)

// lineagePreludeGate is the prelude of a gated script made for token.
// lineage_token returns the token and the ancestor (see lineageKeySuffix).
// lineage_count and lineage_obs are hooks of write observations, unused here.
func lineagePreludeGate(token string) string {
	return `
local LINEAGE_TOKEN = "` + token + `"
local LINEAGE_FIELD = "` + lineageStampGate + `"
local LINEAGE_COUNT = false
local function lineage_token(key)
    local m = redis.call("hmget", key, "t", "p")
    local value = m[1]
    local token, ancestor = value, nil
    if value then
        local bar = string.find(value, "|", 1, true)
        if bar then
            token = string.sub(value, 1, bar - 1)
            ancestor = string.sub(value, bar + 1)
        end
    end
    if token ~= LINEAGE_TOKEN then
        error({err = "` + lineageErrPrefix + `t=" .. (value or "") .. " p=" .. string.gsub(m[2] or "", " ", ",")})
    end
    return token, ancestor
end
local function lineage_count(key)
    return false
end
local function lineage_obs(count)
    return ""
end
local function lineage_reset_ext(reset)
    if reset then
        return ";lr=" .. LINEAGE_FIELD
    end
    return ""
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
local LINEAGE_FIELD = "` + lineageStampInfo + `"
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
local function lineage_reset_ext(reset)
    if reset then
        return ";lr=" .. LINEAGE_FIELD
    end
    return ""
end
`

// lineagePreludeNone defines lineage_token(key) as no token: nothing is
// stamped or compared.
const lineagePreludeNone = `
local LINEAGE_FIELD = "` + lineageStampGate + `"
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
local function lineage_reset_ext(reset)
    if reset then
        return ";lr=" .. LINEAGE_FIELD
    end
    return ""
end
`

// lineageRotateBody sets the lineage keys in ARGV[3], ARGV[5], ... to the
// values ARGV[4], ARGV[6], ...; each key's replaced token joins its list of
// replaced tokens. It declares no keys so that it runs over the gate's
// connection in Redis Cluster; the server refuses a key it doesn't serve.
// The rotation is a compare-and-set: ARGV[1] is the lineage key of the gate's
// slot and ARGV[2] the value the gate read there. If another node rotated in
// between, nothing is set and the value is returned: the gate adopts that
// rotation instead of making a second one, which would reset metas written
// under the first.
const lineageRotateBody = `
local current = redis.call("hget", ARGV[1], "t") or ""
if current ~= ARGV[2] then
    return current
end
for i = 3, #ARGV, 2 do
    local m = redis.call("hmget", ARGV[i], "t", "p")
    local preds = m[2] or ""
    if m[1] then
        local old = m[1]
        local bar = string.find(old, "|", 1, true)
        if bar then
            old = string.sub(old, 1, bar - 1)
        end
        if old ~= "" and old ~= ARGV[i + 1] then
            local list = {old}
            for t in string.gmatch(preds, "%S+") do
                if #list >= ` + lineagePredecessors + ` then
                    break
                end
                if t ~= old then
                    list[#list + 1] = t
                end
            end
            preds = table.concat(list, " ")
        end
    end
    redis.call("hset", ARGV[i], "t", ARGV[i + 1], "p", preds)
end
return false
`

const lineageRotateSourceCluster = "#!lua flags=allow-cross-slot-keys" + lineageRotateBody
const lineageRotateSource = lineageRotateBody

// lineageSlotsSourceCluster returns the token value of every lineage key in
// ARGV.
const lineageSlotsSourceCluster = `#!lua flags=allow-cross-slot-keys
local out = {}
for i = 1, #ARGV do
    out[#out + 1] = redis.call("hget", ARGV[i], "t") or ""
end
return out
`

// lineageSha1Source returns the SHA-1 of a script source, for FIPS mode where
// it can't be computed here and SCRIPT LOAD would load the script.
const lineageSha1Source = `return redis.sha1hex(ARGV[1])`

// lineagePingBody is the probe's call: the gated prelude alone.
const lineagePingBody = `
lineage_token(KEYS[1])
return 1
`

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
    local m = redis.call("hmget", meta_key, "e", LINEAGE_FIELD)
    if m[1] == false or m[2] == token then
        return m[1], false
    end
    if m[2] == false or m[2] == ancestor then
        redis.call("hset", meta_key, LINEAGE_FIELD, token)
        return m[1], false
    end
    redis.call("del", meta_key)
    if stream_key ~= "" then
        redis.call("del", stream_key)
    end
    return false, m[1]
end

-- lineage_map_obs and lineage_map_count are hooks of write observations.
local function lineage_map_obs(meta_key)
    return ""
end

local function lineage_map_count(meta_key, token_key)
    return ""
end

-- map_meta_expire sets the TTL of a map meta (milliseconds, 0: permanent).
-- With empty_ttl set, a meta whose stream has no publications (top 0) gets
-- the shorter of the two, and a permanent meta that got it while empty is
-- made permanent again once it has publications. below_half: the meta of a
-- map without publications with more than half its TTL left isn't extended,
-- one above it is shortened. A map with publications is always extended: its
-- reads extend its state, which an expired meta would make stale.
local function map_meta_expire(meta_key, meta_ttl, empty_ttl, below_half, top)
    meta_ttl = tonumber(meta_ttl) or 0
    empty_ttl = tonumber(empty_ttl) or 0
    local empty = (tonumber(top) or 0) == 0
    local target = meta_ttl
    if empty_ttl > 0 and empty and (meta_ttl == 0 or empty_ttl < meta_ttl) then
        target = empty_ttl
    end
    if target == 0 then
        if empty_ttl > 0 and redis.call("pttl", meta_key) > 0 then
            redis.call("persist", meta_key)
        end
        return
    end
    if below_half and empty then
        local ttl = redis.call("pttl", meta_key)
        if ttl >= 0 and ttl <= target and ttl * 2 >= target then
            return
        end
    end
    redis.call("pexpire", meta_key, target)
end

local function lineage_map_create(meta_key, state_meta_key, epoch, token, dropped)
    if token ~= "" then
        redis.call("hset", meta_key, "e", epoch, LINEAGE_FIELD, token)
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
	name string
	body string
	shas sync.Map // token -> SHA-1 of the gated source

	mu       sync.Mutex
	loadSHA1 bool
	none     *rueidis.Lua
	info     *rueidis.Lua
}

func newLineageScript(name, body string, loadSHA1 bool) *lineageScript {
	return &lineageScript{name: name, body: body, loadSHA1: loadSHA1}
}

// useLoadSHA1 makes the script get its digests from the server (FIPS mode).
func (s *lineageScript) useLoadSHA1() {
	s.mu.Lock()
	s.loadSHA1 = true
	s.mu.Unlock()
}

// lua returns the plain script of a mode without the gate (lineageInfo or
// lineageNone). It is made on first use: rueidis computes the SHA-1 of a
// script when it's made unless the digest comes from the server, and in
// FIPS-only mode computing SHA-1 panics.
func (s *lineageScript) lua(mode lineageMode) *rueidis.Lua {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mode == lineageInfo {
		if s.info == nil {
			s.info = rueidis.NewLuaScript(lineagePreludeInfo+s.body, rueidis.WithLoadSHA1(s.loadSHA1))
		}
		return s.info
	}
	if s.none == nil {
		s.none = rueidis.NewLuaScript(lineagePreludeNone+s.body, rueidis.WithLoadSHA1(s.loadSHA1))
	}
	return s.none
}

func (s *lineageScript) gatedSource(token string) string {
	return lineagePreludeGate(token) + s.body
}

func sha1Hex(src string) string {
	sum := sha1.Sum([]byte(src)) //nolint:gosec // Names Lua scripts for EVALSHA, as Redis does. Not security-sensitive.
	return hex.EncodeToString(sum[:])
}

// redisServerKind is what the server answered to HELLO.
type redisServerKind struct {
	gateCapable bool   // Redis 7+ or Valkey, not Dragonfly, not behind a proxy
	primary     bool   // HELLO role is master
	proxy       bool   // HELLO gives no connection id: a proxy answers
	desc        string // for logs
	// helloErr is the server's refusal of HELLO (a server or a proxy without
	// it): no assumption about its script cache.
	helloErr string
}

// helloFields keeps the string and integer fields of a HELLO reply.
func helloFields(m map[string]rueidis.RedisMessage) map[string]string {
	fields := make(map[string]string, len(m))
	for k, v := range m {
		if str, err := v.ToString(); err == nil {
			fields[k] = str
		} else if n, err := v.AsInt64(); err == nil {
			fields[k] = strconv.FormatInt(n, 10)
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
	if id, ok := fields["id"]; ok {
		if n, err := strconv.ParseInt(id, 10, 64); err != nil || n <= 0 {
			// A server gives every connection an id: a proxy without one
			// (ElastiCache Serverless answers -1) keeps scripts itself, and a
			// script missing behind it shows nothing about the servers.
			return redisServerKind{primary: primary, proxy: true, desc: desc}
		}
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
	shard  *RedisShard
	prefix string

	kindMu   sync.Mutex
	kindDone bool
	kind     redisServerKind

	infoMu      sync.Mutex
	infoProbed  bool
	infoAllowed bool

	modeMu sync.Mutex
	gate   bool // the shard runs in gate mode (some broker asked for it)

	openMu sync.Mutex

	// reported: the broker kinds that logged their environment.
	reported sync.Map

	scriptsMu sync.Mutex
	scripts   []*lineageScript // gated scripts, loaded whenever the gate opens
	loadSHA1  bool
	node      *Node
	ping      *lineageScript

	// tokens are the tokens this node runs the scripts of: one outside Redis
	// Cluster, one per slot in Redis Cluster.
	tokensMu sync.RWMutex
	tokens   map[int]string

	// left are the tokens this node saw replaced (shared with the node's other
	// shards: tokens are unique).
	left *lineageLeftTokens

	// lastCall is when this node last called the shard (UnixNano), for the
	// idle probe.
	lastCall atomic.Int64

	probeOnce sync.Once
	stats     lineageStats

	logFn func(level LogLevel, msg string, fields map[string]any)
	// brokerName names, in metrics and logs, what no broker call triggered (the
	// idle probe, replaced tokens learned from other nodes): the first broker
	// that enabled detection on the shard and prefix.
	brokerName string

	// disabledForTest turns detection off (tests showing the behavior without it).
	disabledForTest bool
	// noProbeForTest turns the idle probe off.
	noProbeForTest bool
}

type lineageStats struct {
	mu        sync.Mutex
	rotations int
	// stale: rotations of a server presenting a token this node saw replaced.
	stale int
	// reused: rotations after a subscriber saw a reused offset.
	reused int
}

func newRedisLineage(shard *RedisShard, prefix string) *redisLineage {
	l := &redisLineage{
		shard:  shard,
		prefix: prefix,
		tokens: make(map[int]string),
		left:   newLineageLeftTokens(),
	}
	l.ping = newLineageScript("lineage_ping", lineagePingBody, false)
	return l
}

// register adds a gated script loaded whenever the gate opens. loadSHA1 is the
// broker's FIPS setting.
func (l *redisLineage) register(s *lineageScript, loadSHA1 bool) {
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	if loadSHA1 {
		l.loadSHA1 = true
		s.useLoadSHA1()
	}
	for _, existing := range l.scripts {
		if existing == s {
			return
		}
	}
	l.scripts = append(l.scripts, s)
}

// setLogger logs lineage events with the node's logger (the first node wins)
// and shares the node's replaced tokens.
func (l *redisLineage) setLogger(n *Node, brokerName string) {
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	if l.logFn != nil {
		return
	}
	l.node = n
	l.brokerName = brokerName
	l.left = n.lineageLeft
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

func (l *redisLineage) leftTokens() *lineageLeftTokens {
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	return l.left
}

// serverKind asks the server with HELLO once.
func (l *redisLineage) serverKind(ctx context.Context) (redisServerKind, error) {
	l.kindMu.Lock()
	defer l.kindMu.Unlock()
	if l.kindDone {
		return l.kind, nil
	}
	kind, err := l.helloKind(ctx)
	if err != nil {
		return redisServerKind{}, err
	}
	l.kind = kind
	l.kindDone = true
	return l.kind, nil
}

// helloKind asks the server what it is with HELLO (not cached), on a
// connection to the primary serving the lineage key: in Redis Cluster a
// keyless command may go to a replica. A server refusing HELLO (Redis before
// 6, some proxies) is a kind without the gate, not an error. A reply telling
// that the server or the cluster can't answer now (CLUSTERDOWN during a
// failover, for example) is an error: it says nothing about the server, and
// serverKind would keep it for the life of the process.
func (l *redisLineage) helloKind(ctx context.Context) (redisServerKind, error) {
	var m map[string]rueidis.RedisMessage
	var helloErr error
	err := l.shard.client.Dedicated(func(c rueidis.DedicatedClient) error {
		// The first command is keyed: it binds the connection to the primary
		// serving the key.
		if err := c.Do(ctx, c.B().Exists().Key(l.anyTokenKey()).Build()).Error(); err != nil {
			return err
		}
		m, helloErr = c.Do(ctx, c.B().Hello().Build()).AsMap()
		return nil
	})
	if err == nil {
		err = helloErr
	}
	if err != nil {
		if re, ok := rueidis.IsRedisErr(err); ok && !isReadOnly(re) && !isRedisErrNotReady(re) {
			return redisServerKind{desc: "a server refusing HELLO", helloErr: re.Error()}, nil
		}
		return redisServerKind{}, fmt.Errorf("lineage: HELLO: %w", err)
	}
	return parseHelloReply(helloFields(m)), nil
}

// infoCheck runs a script calling INFO (not cached). redisErr tells that the
// server refused it (an ACL refusal, for example) rather than failing to answer.
//
// It is sent with EVAL: a script made with rueidis.NewLuaScript would compute
// its SHA-1 here, which panics in FIPS-only mode.
func (l *redisLineage) infoCheck(ctx context.Context) (allowed bool, redisErr bool, err error) {
	c := l.shard.client
	res, err := c.Do(ctx, c.B().Eval().Script(lineageInfoCheckScript).Numkeys(1).Key(l.anyTokenKey()).Build()).ToString()
	if err != nil {
		_, redisErr = rueidis.IsRedisErr(err)
		return false, redisErr, err
	}
	return res != "", false, nil
}

// lineageInfoCheckScript reads master_replid with INFO from a script.
const lineageInfoCheckScript = `local i = redis.call("info", "replication"); return string.match(i, "master_replid:(%w+)") or ""`

// probeInfo checks once whether scripts may call INFO.
func (l *redisLineage) probeInfo(ctx context.Context) bool {
	l.infoMu.Lock()
	defer l.infoMu.Unlock()
	if l.infoProbed {
		return l.infoAllowed
	}
	allowed, redisErr, err := l.infoCheck(ctx)
	if err != nil {
		if redisErr {
			// A Redis error (an ACL refusal, for example): INFO isn't allowed.
			l.infoProbed = true
			l.infoAllowed = false
			l.log(LogLevelWarn, "redis lineage: DetectLostWritesWithInfo is on but "+infoRefusal(err), nil)
		}
		return false
	}
	l.infoProbed = true
	l.infoAllowed = allowed
	return l.infoAllowed
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
		l.modeMu.Lock()
		l.gate = true
		l.modeMu.Unlock()
		l.startProbe()
		return lineageGate, nil
	}
	return lineageNone, nil
}

func (l *redisLineage) gateMode() bool {
	l.modeMu.Lock()
	defer l.modeMu.Unlock()
	return l.gate
}

func (l *redisLineage) slotIndex(key string) int {
	if !l.shard.isCluster {
		return 0
	}
	return int(redisKeySlot(key))
}

// tokenKey returns the lineage key in the slot of key.
func (l *redisLineage) tokenKey(key string) string {
	return l.keyForSlot(l.slotIndex(key))
}

func (l *redisLineage) anyTokenKey() string {
	return l.keyForSlot(0)
}

// keyForSlot returns the lineage key of a slot (slot 0 outside Redis Cluster).
func (l *redisLineage) keyForSlot(slot int) string {
	if !l.shard.isCluster {
		return l.prefix + lineageKeySuffix
	}
	return lineageKeyForSlot(l.prefix, slot)
}

func (l *redisLineage) token(slot int) string {
	l.tokensMu.RLock()
	defer l.tokensMu.RUnlock()
	return l.tokens[slot]
}

// parseTokenValue splits a lineage key's token value "token" or
// "token|ancestor".
func parseTokenValue(value string) (token, ancestor string) {
	bar := strings.IndexByte(value, '|')
	if bar < 0 {
		return value, ""
	}
	return value[:bar], value[bar+1:]
}

// errLineageGate wraps the errors of the lineage gate itself (it could
// not be opened or followed, or a script digest could not be had), as opposed
// to the errors of the gated script.
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
		// No key: the digest is the same on every node, and over a dedicated
		// connection in Redis Cluster a key of another slot is refused.
		res, err := c.Do(ctx, c.B().Eval().Script(lineageSha1Source).Numkeys(0).Arg(src).Build()).ToString()
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
// following the server's token on a LINEAGE error - unless it is a token this
// node saw replaced: then the server is a stale copy and the gate rotates.
func (l *redisLineage) exec(ctx context.Context, s *lineageScript, keys, args []string) rueidis.RedisResult {
	l.lastCall.Store(time.Now().UnixNano())
	if l.node != nil {
		l.node.awaitLineagePeers(ctx)
	}
	slot := l.slotIndex(keys[0])
	for attempt := 0; attempt < 5; attempt++ {
		token := l.token(slot)
		if token == "" {
			if err := l.open(ctx, keys[0], false, token, ""); err != nil {
				return rueidis.NewErrorResult(err)
			}
			continue
		}
		sha, err := l.sha(ctx, l.shard.client, &s.shas, token, s.gatedSource(token))
		if err != nil {
			return rueidis.NewErrorResult(fmt.Errorf("%w: script %s: digest: %w", errLineageGate, s.name, err))
		}
		cmd := l.shard.client.B().Evalsha().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build()
		resp := l.shard.client.Do(ctx, cmd)
		err = resp.Error()
		retry, gateErr := l.handleErr(ctx, keys[0], token, err)
		if gateErr != nil {
			return rueidis.NewErrorResult(gateErr)
		}
		if retry {
			continue
		}
		l.shard.noteError(err)
		return resp
	}
	return rueidis.NewErrorResult(fmt.Errorf("%w: script %s: no stable lineage token", errLineageGate, s.name))
}

// handleErr handles the error of a gated script run with the scripts of token
// on the slot of routeKey: NOSCRIPT opens the gate; a LINEAGE error follows
// the server's token - unless it is a token this node saw replaced, then the
// server is a stale copy and its lineage rotates. retry tells to send the call
// again (with the scripts of the slot's token now). Callers sending gated
// scripts themselves (batches) use it as exec does.
func (l *redisLineage) handleErr(ctx context.Context, routeKey, token string, err error) (bool, error) {
	if isNoScript(err) {
		return true, l.open(ctx, routeKey, false, token, "")
	}
	value, preds, ok := lineageErrToken(err)
	if !ok {
		return false, nil
	}
	left := l.leftTokens()
	left.addChain(value, preds)
	if t, _ := parseTokenValue(value); t != "" && left.has(t) {
		// A token this node saw replaced: a stale copy.
		return true, l.open(ctx, routeKey, true, token, value)
	}
	// The server's token isn't ours: run the scripts of its token.
	l.setToken(l.slotIndex(routeKey), value)
	return true, nil
}

func (l *redisLineage) setToken(slot int, value string) {
	token, _ := parseTokenValue(value)
	l.tokensMu.Lock()
	l.tokens[slot] = token
	l.tokensMu.Unlock()
}

func isNoScript(err error) bool {
	if err == nil {
		return false
	}
	re, ok := rueidis.IsRedisErr(err)
	return ok && re.IsNoScript()
}

// isRedisErrNotReady tells a Redis error reply meaning that the server or the
// cluster can't serve the command now, not that it refuses it: the cluster is
// down or the slot moving, the server loading its data, busy with a script, or
// a replica without a reachable primary.
func isRedisErrNotReady(re *rueidis.RedisError) bool {
	if _, ok := re.IsRedirect(); ok {
		return true
	}
	if re.IsClusterDown() || re.IsTryAgain() || re.IsLoading() {
		return true
	}
	msg := re.Error()
	return strings.HasPrefix(msg, "MASTERDOWN") || strings.HasPrefix(msg, "BUSY ")
}

func isReadOnly(err error) bool {
	if err == nil {
		return false
	}
	re, ok := rueidis.IsRedisErr(err)
	return ok && strings.HasPrefix(re.Error(), "READONLY")
}

// lineageErrToken parses a LINEAGE error: the server's token value ("" for
// none) and the tokens it replaced. Unknown parts are skipped (see
// lineageErrPrefix).
func lineageErrToken(err error) (string, []string, bool) {
	if err == nil {
		return "", nil, false
	}
	re, ok := rueidis.IsRedisErr(err)
	if !ok {
		return "", nil, false
	}
	return parseLineageErr(re.Error())
}

// parseLineageErr parses the message of a LINEAGE error.
func parseLineageErr(msg string) (string, []string, bool) {
	if !strings.HasPrefix(msg, lineageErrPrefix) {
		return "", nil, false
	}
	var value string
	var preds []string
	for _, part := range strings.Fields(msg[len(lineageErrPrefix):]) {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue // a script location Redis appends, for example
		}
		switch k {
		case "t":
			value = v
		case "p":
			for _, t := range strings.Split(v, ",") {
				if t != "" {
					preds = append(preds, t)
				}
			}
		}
	}
	return value, preds, true
}

func (l *redisLineage) marker(ctx context.Context, c rueidis.CoreClient, token string) (string, error) {
	return l.sha(ctx, c, &lineageMarkerShas, token, lineageMarkerSource(token))
}

// open opens the gate of the server serving routeKey, all on one connection.
// seen is the token the caller found missing or unusable: when another caller
// has replaced it meanwhile, the gate is open already and nothing is done. With
// force, the token rotates even if the server holds its marker (a stale copy,
// a reused offset), but only if the server's token value is still expect (""
// for any): a compare-and-set, so nodes noticing the same break rotate once.
func (l *redisLineage) open(ctx context.Context, routeKey string, force bool, seen, expect string) error {
	l.openMu.Lock()
	defer l.openMu.Unlock()
	slot := l.slotIndex(routeKey)
	if !force && l.token(slot) != seen {
		return nil
	}

	l.scriptsMu.Lock()
	scripts := append([]*lineageScript(nil), l.scripts...)
	scripts = append(scripts, l.ping)
	l.scriptsMu.Unlock()
	left := l.leftTokens()

	tokenKey := l.tokenKey(routeKey)
	var (
		rotated      bool
		staleSeen    bool
		firstSeen    bool // the server had no lineage yet
		adoptedValue string
		newToken     string
		slots        []int
		prevToken    = l.token(slot)
	)
	err := l.shard.client.Dedicated(func(c rueidis.DedicatedClient) error {
		// The first command is keyed: it binds the connection to the server
		// serving routeKey.
		state, err := c.Do(ctx, c.B().Hmget().Key(tokenKey).Field("t", "p").Build()).ToArray()
		if err != nil {
			return err
		}
		if len(state) != 2 {
			return fmt.Errorf("lineage key: %d fields", len(state))
		}
		currentValue, _ := state[0].ToString()
		predsValue, _ := state[1].ToString()
		left.addChain(currentValue, strings.Fields(predsValue))
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
		slots, err = l.serverSlots(ctx, c, routeKey)
		if err != nil {
			return err
		}
		if force && expect != "" && currentValue != expect {
			// Another node rotated already: follow it.
			force = false
		}
		stale := current != "" && left.has(current)
		intact := false
		if current != "" && !force && !stale {
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
		if !intact && !force && !stale && l.shard.isCluster {
			// A slot whose token has no marker here may have been migrated in
			// from another server (MIGRATE moves its keys without loss) while
			// this server's own lineage is intact. Then only that slot is
			// adopted, under this server's token with its own as the ancestor,
			// instead of rotating - and resetting - every slot of the server.
			// A server without an intact token of its own (no node used it
			// yet, like a node just added to the cluster) can't tell a
			// migration from a restart: the slot rotates and its streams start
			// new epochs. So does a slot whose token no other primary holds the
			// marker of: the server it came from may have lost writes (a
			// promotion or restart no node has called yet) before the slot
			// moved.
			own, ok, err := l.intactServerToken(ctx, c, slots, slot, left)
			if err != nil {
				return err
			}
			if ok && current != "" {
				ok, err = l.tokenServedElsewhere(ctx, c, current, slots)
				if err != nil {
					return err
				}
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
		loaded := ""
		load := func(token string) error {
			if token == loaded {
				return nil
			}
			for _, s := range scripts {
				if err := c.Do(ctx, c.B().ScriptLoad().Script(s.gatedSource(token)).Build()).Error(); err != nil {
					return fmt.Errorf("load %s: %w", s.name, err)
				}
			}
			if err := c.Do(ctx, c.B().ScriptLoad().Script(lineageMarkerSource(token)).Build()).Error(); err != nil {
				return fmt.Errorf("load marker: %w", err)
			}
			loaded = token
			return nil
		}
		if !intact {
			// A new lineage (a restart, a promotion, a stale copy, a flushed
			// cache): rotate the token of every slot this server serves.
			token = "g" + strconv.FormatInt(time.Now().UnixNano(), 36) + strconv.FormatUint(lineageRand(), 36)
			// Its marker and scripts first: a token in the lineage key must
			// have its marker here, or another node opening the gate between
			// the rotation and the loading finds no marker and rotates again,
			// starting new epochs for nothing.
			if err := load(token); err != nil {
				return err
			}
			args := make([]string, 0, 2*len(slots)+2)
			args = append(args, tokenKey, currentValue)
			for _, s := range slots {
				key := l.keyForSlot(s)
				args = append(args, key, token)
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
			} else {
				rotated = true
				staleSeen = stale
				firstSeen = current == "" && !force
				if current != "" {
					left.add(current)
				}
			}
		}
		if err := load(token); err != nil {
			return err
		}
		newToken = token
		return nil
	})
	if err != nil {
		return fmt.Errorf("%w: %v", errLineageGate, err)
	}

	l.tokensMu.Lock()
	if adoptedValue != "" {
		t, _ := parseTokenValue(adoptedValue)
		l.tokens[slot] = t
	} else {
		l.tokens[slot] = newToken
	}
	// The server's other slots most likely carry the same token: start with it
	// rather than opening the gate again on the first call to each of them (up
	// to 16384 in Redis Cluster). A slot with another token (migrated in)
	// answers LINEAGE with its token once.
	for _, s := range slots {
		if rotated || l.tokens[s] == "" {
			l.tokens[s] = newToken
		}
	}
	l.tokensMu.Unlock()

	if rotated {
		l.stats.mu.Lock()
		l.stats.rotations++
		if staleSeen {
			l.stats.stale++
		}
		l.stats.mu.Unlock()
		if firstSeen {
			// No lineage key: the server's first use, or the key was flushed,
			// evicted or lost in a restart without persistence. Whatever
			// removed it removed the metas too, or left metas whose stamp no
			// longer matches any token and so start new epochs: no write can
			// be lost unseen, and it isn't a detection.
			l.log(LogLevelInfo, "redis lineage: the server has no lineage yet (first use, or its lineage key was flushed or evicted), new lineage token", map[string]any{
				"slots": len(slots), "broker_name": l.brokerNameOf(ctx),
			})
			return nil
		}
		switch {
		case staleSeen:
			l.countDetection(ctx, "stale_copy")
		case force:
			l.countDetection(ctx, "reused_offset")
		default:
			l.countDetection(ctx, "restart_or_failover")
		}
		l.log(LogLevelInfo, "redis lineage: new lineage token (a restart, a failover or a stale copy may have lost writes), streams and maps on the server start new epochs", map[string]any{
			"previous_token": prevToken, "stale_copy": staleSeen, "forced": force, "slots": len(slots), "broker_name": l.brokerNameOf(ctx),
		})
	}
	return nil
}

// rotateIfToken rotates the lineage of the server serving routeKey if the
// node's token for that slot is still the server's: a subscriber saw the
// server give out an offset again (see offsetWatermarks), so it lost writes.
// Nodes noticing the same reuse rotate once (a compare-and-set on the token).
func (l *redisLineage) rotateIfToken(ctx context.Context, routeKey string) error {
	token := l.token(l.slotIndex(routeKey))
	if token == "" || l.leftTokens().has(token) {
		// Unknown, or already replaced: a rotation happened (many channels of a
		// server report the same loss).
		return nil
	}
	l.stats.mu.Lock()
	l.stats.reused++
	l.stats.mu.Unlock()
	return l.open(ctx, routeKey, true, token, token)
}

// reconcile rotates the servers whose current token this node now knows was
// replaced (learned from another node's info after it started using it).
func (l *redisLineage) reconcile(ctx context.Context) {
	left := l.leftTokens()
	l.tokensMu.RLock()
	stale := map[string]int{}
	for slot, t := range l.tokens {
		if left.has(t) {
			stale[t] = slot
		}
	}
	l.tokensMu.RUnlock()
	for t, slot := range stale {
		key := l.keyForSlot(slot)
		_ = l.open(ctx, key, true, t, t)
	}
}

// serverSlots returns the slots of the connected server (one slot outside
// Redis Cluster).
func (l *redisLineage) serverSlots(ctx context.Context, c rueidis.DedicatedClient, routeKey string) ([]int, error) {
	if !l.shard.isCluster {
		return []int{0}, nil
	}
	nodes, err := c.Do(ctx, c.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return nil, fmt.Errorf("CLUSTER NODES: %w", err)
	}
	slots, err := parseMyselfSlots(nodes)
	if err != nil {
		return nil, err
	}
	want := int(redisKeySlot(routeKey))
	for _, s := range slots {
		if s == want {
			return slots, nil
		}
	}
	return nil, fmt.Errorf("server does not serve slot %d", want)
}

// parseMyselfSlots returns the slots of the "myself" line of CLUSTER NODES.
func parseMyselfSlots(nodes string) ([]int, error) {
	for _, n := range lineageParseClusterNodes(nodes) {
		if !n.myself {
			continue
		}
		if !n.primary {
			return nil, errors.New("connected node is not a primary")
		}
		return n.slots, n.err
	}
	return nil, errors.New("no myself line in CLUSTER NODES")
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

// lineageProbeIntervalNs holds lineageProbeInterval. It is atomic: tests
// change it while probes of shards from earlier tests may still read it.
var lineageProbeIntervalNs atomic.Int64

func init() { lineageProbeIntervalNs.Store(int64(time.Second)) }

// lineageProbeInterval: a node that made no call to a shard for this long
// calls each of its primaries.
func lineageProbeInterval() time.Duration {
	return time.Duration(lineageProbeIntervalNs.Load())
}

// lineageProbeSlotsRefresh is how often the probe refreshes which slots the
// primaries of a Redis Cluster serve.
var lineageProbeSlotsRefresh = 30 * time.Second

func (l *redisLineage) startProbe() {
	if l.noProbeForTest {
		return
	}
	l.probeOnce.Do(func() {
		go l.runProbe()
	})
}

func (l *redisLineage) runProbe() {
	interval := lineageProbeInterval()
	t := time.NewTicker(interval)
	defer t.Stop()
	var keys []string
	var keysAt time.Time
	for {
		select {
		case <-l.shard.closeCh:
			return
		case <-t.C:
			if time.Since(time.Unix(0, l.lastCall.Load())) < interval {
				continue // the node calls the shard anyway
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if !l.shard.isCluster {
				keys = []string{l.keyForSlot(0)}
			} else if keys == nil || time.Since(keysAt) > lineageProbeSlotsRefresh {
				if k, err := l.primaryKeys(ctx); err == nil {
					keys, keysAt = k, time.Now()
				}
			}
			for _, key := range keys {
				_ = l.exec(ctx, l.ping, []string{key}, nil).Error()
			}
			cancel()
		}
	}
}

// primaryKeys returns a lineage key served by each primary of a Redis Cluster.
func (l *redisLineage) primaryKeys(ctx context.Context) ([]string, error) {
	c := l.shard.client
	slots, err := c.Do(ctx, c.B().ClusterSlots().Build()).ToArray()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(slots))
	seen := map[string]bool{}
	for _, r := range slots {
		e, err := r.ToArray()
		if err != nil || len(e) < 3 {
			continue
		}
		from, err := e[0].AsInt64()
		if err != nil {
			continue
		}
		m, err := e[2].ToArray()
		if err != nil || len(m) < 2 {
			continue
		}
		host, _ := m[0].ToString()
		port, _ := m[1].AsInt64()
		addr := host + ":" + strconv.FormatInt(port, 10)
		if seen[addr] {
			continue
		}
		seen[addr] = true
		keys = append(keys, l.keyForSlot(int(from)))
	}
	return keys, nil
}

var (
	lineageSlotTagsOnce sync.Once
	lineageSlotTags     [16384]string
)

// lineageKeyForSlot returns the lineage key of a prefix living in a hash slot.
func lineageKeyForSlot(prefix string, slot int) string {
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
	if strings.IndexByte(prefix, '{') >= 0 {
		// The prefix would hold the hash tag: the slot's tag goes first.
		return "{" + lineageSlotTags[slot] + "}." + prefix + lineageKeySuffix
	}
	return prefix + lineageKeySuffix + ".{" + lineageSlotTags[slot] + "}"
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

// intactServerToken looks, on the gate's connection, for a token of another
// slot this server serves whose marker the server holds (and which this node
// didn't see replaced): the server's own lineage is then intact. It returns
// that token.
func (l *redisLineage) intactServerToken(ctx context.Context, c rueidis.DedicatedClient, slots []int, except int, left *lineageLeftTokens) (string, bool, error) {
	keys := make([]string, 0, 8)
	for _, s := range slots {
		if s == except {
			continue
		}
		keys = append(keys, l.keyForSlot(s))
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
	for _, v := range raw {
		token, _ := parseTokenValue(v)
		if token == "" || seen[token] || left.has(token) {
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

// tokenServedElsewhere tells whether a primary serving none of slots (the
// gate's server) holds the marker of token: the lineage a migrated slot came
// from is still intact there. A primary that can't be asked counts as not
// holding it.
func (l *redisLineage) tokenServedElsewhere(ctx context.Context, c rueidis.DedicatedClient, token string, slots []int) (bool, error) {
	msha, err := l.marker(ctx, c, token)
	if err != nil {
		return false, err
	}
	keys, err := l.primaryKeys(ctx)
	if err != nil {
		return false, nil
	}
	mine := make(map[int]bool, len(slots))
	for _, s := range slots {
		mine[s] = true
	}
	client := l.shard.client
	for _, key := range keys {
		if mine[int(redisKeySlot(key))] {
			continue
		}
		// The marker script, keyed to reach that primary: it runs only where
		// it is loaded.
		if client.Do(ctx, client.B().Evalsha().Sha1(msha).Numkeys(1).Key(key).Build()).Error() == nil {
			return true, nil
		}
	}
	return false, nil
}

// adoptSlot gives a slot migrated in the server's token, with the slot's token
// as its ancestor (its metas are adopted), with a compare-and-set like the
// rotation. It returns the slot's new value.
func (l *redisLineage) adoptSlot(ctx context.Context, c rueidis.DedicatedClient, tokenKey, currentValue, own string) (string, error) {
	value := own
	if prev, _ := parseTokenValue(currentValue); prev != "" && prev != own {
		value = own + "|" + prev
	}
	other, err := c.Do(ctx, c.B().Eval().Script(lineageAdoptSource).Numkeys(0).
		Arg(tokenKey, currentValue, value).Build()).ToString()
	if err != nil && !rueidis.IsRedisNil(err) {
		return "", fmt.Errorf("adopt: %w", err)
	}
	if err == nil {
		return other, nil // another node did it first
	}
	return value, nil
}

// lineageAdoptSource sets a migrated slot's token value (ARGV[3]) if it is
// still ARGV[2]; its replaced tokens stay - its ancestor wasn't replaced.
const lineageAdoptSource = `#!lua flags=allow-cross-slot-keys
local current = redis.call("hget", ARGV[1], "t") or ""
if current ~= ARGV[2] then
    return current
end
redis.call("hset", ARGV[1], "t", ARGV[3])
return false
`

// lineageLeftTokens are tokens a node saw replaced, each with the time it was
// replaced (unix seconds), the newest kept (bounded).
type lineageLeftTokens struct {
	mu    sync.RWMutex
	at    map[string]int64
	order []string
}

// lineageLeftMax bounds the replaced tokens a node keeps.
const lineageLeftMax = 4096

func newLineageLeftTokens() *lineageLeftTokens {
	return &lineageLeftTokens{at: make(map[string]int64)}
}

// add records tokens replaced now; it returns whether any was new.
func (t *lineageLeftTokens) add(tokens ...string) bool {
	return t.addAt(time.Now().Unix(), tokens...)
}

// addAt records tokens replaced at the unix time at; it returns whether any
// was new. A token known already keeps the earlier of its two times.
func (t *lineageLeftTokens) addAt(at int64, tokens ...string) bool {
	if len(tokens) == 0 {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	added := false
	for _, tok := range tokens {
		added = t.put(tok, at) || added
	}
	t.trim()
	return added
}

// addChain records the tokens a lineage key's token value replaced, newest
// first, as the key and LINEAGE errors list them. Each was replaced when the
// token after it was minted, so every node gets the same times, however late
// it reads them.
func (t *lineageLeftTokens) addChain(value string, preds []string) bool {
	if len(preds) == 0 {
		return false
	}
	now := time.Now().Unix()
	replacer, _ := parseTokenValue(value)
	t.mu.Lock()
	defer t.mu.Unlock()
	added := false
	for _, p := range preds {
		at, ok := lineageTokenMinted(replacer)
		if !ok || at > now {
			at = now
		}
		added = t.put(p, at) || added
		replacer = p
	}
	t.trim()
	return added
}

// addPairs records tokens other nodes sent with the times they were replaced
// (now where a time is missing); it returns whether any was new.
func (t *lineageLeftTokens) addPairs(tokens []string, ats []int64) bool {
	if len(tokens) == 0 {
		return false
	}
	now := time.Now().Unix()
	t.mu.Lock()
	defer t.mu.Unlock()
	added := false
	for i, tok := range tokens {
		at := now
		if i < len(ats) && ats[i] > 0 && ats[i] < now {
			at = ats[i]
		}
		added = t.put(tok, at) || added
	}
	t.trim()
	return added
}

func (t *lineageLeftTokens) put(tok string, at int64) bool {
	if tok == "" {
		return false
	}
	if prev, ok := t.at[tok]; ok {
		if at < prev {
			t.at[tok] = at
		}
		return false
	}
	t.at[tok] = at
	t.order = append(t.order, tok)
	return true
}

func (t *lineageLeftTokens) trim() {
	for len(t.order) > lineageLeftMax {
		delete(t.at, t.order[0])
		t.order = t.order[1:]
	}
}

func (t *lineageLeftTokens) has(tok string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.at[tok]
	return ok
}

// recent returns up to n tokens replaced at or after the unix time since,
// the most recently replaced first, with their times.
func (t *lineageLeftTokens) recent(n int, since int64) ([]string, []int64) {
	t.mu.RLock()
	type entry struct {
		tok string
		at  int64
	}
	var entries []entry
	for tok, at := range t.at {
		if at >= since {
			entries = append(entries, entry{tok, at})
		}
	}
	t.mu.RUnlock()
	if len(entries) == 0 {
		return nil, nil
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].at != entries[j].at {
			return entries[i].at > entries[j].at
		}
		return entries[i].tok < entries[j].tok
	})
	if len(entries) > n {
		entries = entries[:n]
	}
	toks := make([]string, len(entries))
	ats := make([]int64, len(entries))
	for i, e := range entries {
		toks[i], ats[i] = e.tok, e.at
	}
	return toks, ats
}

// lineageTokenMinted returns when a gate token was minted (unix seconds): a
// gate token is "g", the mint time in nanoseconds in base 36 (12 digits), and
// a random part.
func lineageTokenMinted(tok string) (int64, bool) {
	if len(tok) < 13 || tok[0] != 'g' {
		return 0, false
	}
	ns, err := strconv.ParseInt(tok[1:13], 36, 64)
	if err != nil || ns <= 0 {
		return 0, false
	}
	return ns / int64(time.Second), true
}

// epochExt takes an epoch as a script reply or a PUB/SUB message has it and
// returns it without extensions (see pubsubEpochExt): the hook of data later
// versions add there. kind is "stream" or "map", key the meta key it belongs
// to. A reply whose meta was dropped for a lineage mismatch carries the reset
// extension (lineage_reset_ext in the preludes), counted here; PUB/SUB
// messages never carry it.
func (l *redisLineage) epochExt(kind, brokerName, key, epoch string) string {
	i := strings.IndexByte(epoch, pubsubEpochExt)
	if i < 0 {
		return epoch
	}
	for _, part := range strings.Split(epoch[i+1:], string(pubsubEpochExt)) {
		if field, ok := strings.CutPrefix(part, lineageResetExtPrefix); ok {
			l.countEpochReset(kind, brokerName, lineageDetectionName(field))
		}
	}
	return epoch[:i]
}

// lineageResetExtPrefix starts the epoch extension of a reply whose meta was
// dropped for a lineage mismatch: "lr=<the mode's stamp field>".
const lineageResetExtPrefix = "lr="

// lineageDetectionName names a mode by its stamp field, for metrics and logs.
func lineageDetectionName(field string) string {
	switch field {
	case lineageStampGate:
		return "script_cache"
	case lineageStampInfo:
		return "info"
	default:
		return "other"
	}
}

func (l *redisLineage) countEpochReset(kind, brokerName, detection string) {
	if m := l.metrics(); m != nil {
		m.incPotentialLostWritesEpochReset(brokerName, kind, detection)
	}
}

func (l *redisLineage) countDetection(ctx context.Context, reason string) {
	if m := l.metrics(); m != nil {
		m.incPotentialLostWritesDetection(l.brokerNameOf(ctx), reason)
	}
}

type lineageBrokerKey struct{}

// withLineageBroker tells the lineage which broker a call comes from, for the
// broker_name of metrics and logs.
func withLineageBroker(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, lineageBrokerKey{}, name)
}

// brokerNameOf returns the broker a call comes from (see withLineageBroker),
// or the first broker that enabled detection on the shard and prefix.
func (l *redisLineage) brokerNameOf(ctx context.Context) string {
	if name, ok := ctx.Value(lineageBrokerKey{}).(string); ok {
		return name
	}
	l.scriptsMu.Lock()
	defer l.scriptsMu.Unlock()
	return l.brokerName
}

func (l *redisLineage) metrics() *metrics {
	l.scriptsMu.Lock()
	n := l.node
	l.scriptsMu.Unlock()
	if n == nil {
		return nil
	}
	return n.metrics
}

// lineageCheckScript is a harmless script the startup report loads and runs
// to see that the server allows what the gate needs.
const lineageCheckScript = `return 1`

// lineageCheckCrossSlotScript checks that the server accepts the script flag
// the gate's rotation uses in Redis Cluster.
const lineageCheckCrossSlotScript = "#!lua flags=allow-cross-slot-keys\nreturn 1"

// reportEnvironment logs, once per broker kind and name on a shard and key
// prefix, the state of potential lost writes detection: whether the options
// are on, which detection is in use, and whether this server supports each
// detection (with the reasons when it doesn't). It changes nothing on the
// server: it doesn't open the gate, and its only writes are harmless scripts in
// the script cache. With detection off it caches nothing, so the shard's
// detection state stays untouched.
// It stops when parent is cancelled (the broker closing), and then logs
// nothing.
func (l *redisLineage) reportEnvironment(parent context.Context, n *Node, broker, brokerName string, enabled, useInfo bool) {
	if l.disabledForTest {
		return
	}
	if _, done := l.reported.LoadOrStore(broker+"/"+brokerName, true); done {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	var kind redisServerKind
	var kindErr error
	if enabled {
		kind, kindErr = l.serverKind(ctx)
	} else {
		kind, kindErr = l.helloKind(ctx)
	}
	scriptCache := l.scriptCacheSupport(ctx, kind, kindErr)
	var infoOK bool
	if enabled && useInfo {
		infoOK = l.probeInfo(ctx)
	}
	info := l.infoSupport(ctx)

	// What mode() picks. Without the gate or INFO, offsets a server gives out
	// again are still detected, per channel.
	detection := "off"
	if enabled {
		switch {
		case useInfo && infoOK:
			detection = "info"
		case kindErr == nil && kind.gateCapable:
			detection = "script_cache"
		default:
			detection = "reused_offsets"
		}
	}
	fields := map[string]any{
		"broker":                       broker,
		"broker_name":                  brokerName,
		"shard":                        l.shard.string(),
		"detect_lost_writes":           enabled,
		"detect_lost_writes_with_info": useInfo,
		"detection":                    detection,
		"script_cache":                 supportText(scriptCache),
		"info":                         supportText(info),
	}
	if kindErr == nil {
		fields["server"] = kind.desc
	}
	if parent.Err() != nil {
		return // the broker is closing: the checks above were cut short
	}
	level := LogLevelInfo
	if enabled && (detection == "reused_offsets" ||
		(detection == "script_cache" && len(scriptCache) > 0) ||
		(useInfo && len(info) > 0)) {
		level = LogLevelWarn
	}
	if n.logEnabled(level) {
		n.logger.log(newLogEntry(level, "redis potential lost writes detection", fields))
	}
}

// lineageReports runs a broker's startup lineage reports (reportEnvironment)
// in the background, and lets the broker's Close stop them and wait for them:
// a report holds a dedicated connection of the shard, which must be released
// before the shard closes.
type lineageReports struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func newLineageReports() *lineageReports {
	ctx, cancel := context.WithCancel(context.Background())
	return &lineageReports{ctx: ctx, cancel: cancel}
}

// start runs f in the background unless the reports were stopped.
func (r *lineageReports) start(f func(ctx context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f(r.ctx)
	}()
}

// stop cancels the running reports and waits for them. It waits whatever the
// caller's deadline: a cancelled report ends at once (its commands use its
// context), and the shard must not close under it.
func (r *lineageReports) stop() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
}

// supportText is "supported", or "not supported: " and the reasons.
func supportText(reasons []string) string {
	if len(reasons) == 0 {
		return "supported"
	}
	return "not supported: " + strings.Join(reasons, "; ")
}

// scriptCacheSupport returns why this server can't run detection with the
// script cache (none when it can): its kind, and the commands the gate needs,
// tried with harmless scripts.
func (l *redisLineage) scriptCacheSupport(ctx context.Context, kind redisServerKind, kindErr error) []string {
	if kindErr != nil {
		return []string{"HELLO failed: " + kindErr.Error()}
	}
	var reasons []string
	switch {
	case kind.helloErr != "":
		reasons = append(reasons, "HELLO failed: "+kind.helloErr)
	case kind.proxy:
		reasons = append(reasons, "a proxy answers (HELLO gives no connection id): it keeps scripts itself, so its script cache can't show a failover or a restart behind it")
	case !kind.gateCapable:
		reasons = append(reasons, kind.desc+" is not Redis 7+ or Valkey: its script cache can't show a failover or a restart")
	}
	if !kind.primary {
		reasons = append(reasons, "the server is not a primary")
	}
	c := l.shard.client
	key := l.anyTokenKey()
	// One connection to the primary serving the key, as the gate uses: in
	// Redis Cluster a keyless SCRIPT LOAD may reach another node than the
	// EVALSHA after it.
	err := c.Dedicated(func(dc rueidis.DedicatedClient) error {
		if err := dc.Do(ctx, dc.B().Exists().Key(key).Build()).Error(); err != nil {
			return err
		}
		sha, err := dc.Do(ctx, dc.B().ScriptLoad().Script(lineageCheckScript).Build()).ToString()
		if err != nil {
			reasons = append(reasons, "SCRIPT LOAD failed: "+err.Error())
		} else if err := dc.Do(ctx, dc.B().Evalsha().Sha1(sha).Numkeys(1).Key(key).Build()).Error(); err != nil {
			reasons = append(reasons, "EVALSHA failed: "+err.Error())
		}
		if l.shard.isCluster {
			if err := dc.Do(ctx, dc.B().ClusterNodes().Build()).Error(); err != nil {
				reasons = append(reasons, "CLUSTER NODES failed: "+err.Error())
			}
			if err := dc.Do(ctx, dc.B().Eval().Script(lineageCheckCrossSlotScript).Numkeys(1).Key(key).Build()).Error(); err != nil {
				reasons = append(reasons, "a script with allow-cross-slot-keys failed: "+err.Error())
			}
		}
		return nil
	})
	if err != nil {
		reasons = append(reasons, "the check failed: "+err.Error())
	}
	return reasons
}

// infoRefusal tells why scripts can't call INFO: the ACL user's permissions
// (NOPERM), or the server refusing INFO from scripts for everyone (ElastiCache
// Serverless).
func infoRefusal(err error) string {
	if strings.HasPrefix(err.Error(), "NOPERM") {
		return "scripts can't call INFO (the ACL user needs the @dangerous category): " + err.Error()
	}
	return "the server doesn't let scripts call INFO: " + err.Error()
}

// infoSupport returns why this server can't run detection with INFO (none
// when it can), from a harmless script reading INFO replication.
func (l *redisLineage) infoSupport(ctx context.Context) []string {
	allowed, redisErr, err := l.infoCheck(ctx)
	switch {
	case err != nil && redisErr:
		return []string{infoRefusal(err)}
	case err != nil:
		return []string{"the INFO check failed: " + err.Error()}
	case !allowed:
		return []string{"INFO replication has no master_replid"}
	}
	return nil
}
