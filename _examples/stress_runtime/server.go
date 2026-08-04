package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
)

// ---------------------------------------------------------------------------
// Channel behaviour is selected by name prefix so that a single OnSubscribe
// handler can serve every scenario. Scenarios build channel names from these.
// ---------------------------------------------------------------------------

const (
	chRecov  = "recov:"  // history + recovery + positioning (+ presence)
	chDelta  = "delta:"  // history + recovery + positioning + fossil delta (no presence)
	chPos    = "pos:"    // positioning only
	chCache  = "cache:"  // recovery in cache mode with auto cache recover
	chTags   = "tags:"   // client-requested tags filter allowed
	chSTags  = "stags:"  // server-enforced tags filter
	chPres   = "pres:"   // presence + join/leave only
	chPlain  = "plain:"  // nothing at all — the cheapest channel
	chDeny   = "deny:"   // subscribe is rejected with ErrorPermissionDenied
	chNoPub  = "nopub:"  // client publish is rejected with ErrorPermissionDenied
	chSubExp = "subexp:" // subscription with a short TTL (exercises sub refresh)
)

// User (token) prefixes select per-connection behaviour in OnConnecting.
const (
	userRefresh = "refresh:" // short connection TTL, kept alive by refresh
	userExpire  = "expire:"  // short TTL whose refresh always reports expired
	userPing    = "ping:"    // fast ping/pong cycle
	userSSub    = "ssub:"    // server-side subscriptions, channels encoded in the id
)

// subOptions returns SubscribeOptions derived from a channel-name prefix.
func subOptions(channel string) centrifuge.SubscribeOptions {
	o := centrifuge.SubscribeOptions{}
	switch {
	case strings.HasPrefix(channel, chRecov), strings.HasPrefix(channel, chDelta):
		o.EnableRecovery = true
		o.EnablePositioning = true
	case strings.HasPrefix(channel, chCache):
		// Cache recovery: (re)subscribe delivers the latest publication only.
		o.EnableRecovery = true
		o.EnablePositioning = true
		o.RecoveryMode = centrifuge.RecoveryModeCache
		o.AutoCacheRecover = true
	case strings.HasPrefix(channel, chPos):
		o.EnablePositioning = true
	case strings.HasPrefix(channel, chSubExp):
		o.EnablePositioning = true
		// Deliberately short: the connection must keep it alive through the
		// server-side sub refresh handler or it gets closed with DisconnectSubExpired.
		o.ExpireAt = time.Now().Add(2 * time.Second).Unix()
	case strings.HasPrefix(channel, chSTags):
		o.ServerTagsFilter = &centrifuge.FilterNode{Key: "team", Cmp: "eq", Val: "eng"}
	case strings.HasPrefix(channel, chTags):
		o.AllowTagsFilter = true
	}
	if strings.HasPrefix(channel, chDelta) {
		o.AllowedDeltaTypes = []centrifuge.DeltaType{centrifuge.DeltaTypeFossil}
	}
	// Presence + join/leave everywhere they don't conflict — cheap and lets any
	// channel be inspected. Kept off delta channels to avoid mixing concerns, and
	// off "plain:" channels used by scenarios that subscribe to thousands of them.
	if !strings.HasPrefix(channel, chDelta) && !strings.HasPrefix(channel, chPlain) {
		o.EmitPresence = true
		o.EmitJoinLeave = true
		o.PushJoinLeave = true
	}
	return o
}

// historyPublishOptions gives history-backed channels a history stream so that
// client-published messages are recoverable too.
func historyPublishOptions(channel string) centrifuge.PublishOptions {
	var opts centrifuge.PublishOptions
	if strings.HasPrefix(channel, chRecov) || strings.HasPrefix(channel, chDelta) ||
		strings.HasPrefix(channel, chCache) || strings.HasPrefix(channel, chPos) {
		opts.HistorySize = 500
		opts.HistoryTTL = time.Minute
	}
	return opts
}

// nodeConfig is the tuning that differs between the nodes the suite runs.
type nodeConfig struct {
	name string
	cfg  centrifuge.Config
	ws   centrifuge.WebsocketConfig
	// storage builds the broker and presence manager. Nil means in-memory. The
	// returned closer releases whatever the storage holds (Redis clients).
	storage func(*centrifuge.Node) (centrifuge.Broker, centrifuge.PresenceManager, func(), error)
}

// mainNodeConfig is generous on purpose: bursty concurrent scenarios must not
// trip slow-client disconnects or per-client caps (this is a stress harness,
// not a production sizing).
func mainNodeConfig() nodeConfig {
	return nodeConfig{
		name: "main",
		cfg: centrifuge.Config{
			LogLevel:           centrifuge.LogLevelError,
			ClientQueueMaxSize: 128 * 1024 * 1024,
			ClientChannelLimit: 100000,
		},
		ws: centrifuge.WebsocketConfig{
			UseWriteBufferPool: true,
			// Large enough for the megabyte-scale client publishes in large_payloads.
			MessageSizeLimit: 4 * 1024 * 1024,
		},
	}
}

// strictNodeConfig is the adversarial node: every limit is small enough that a
// misbehaving client trips it quickly. Scenarios that assert on limit
// enforcement (slow client, channel limit, stale/expired connections, oversized
// frames) run against this node, so the tight limits never leak into the
// throughput scenarios.
func strictNodeConfig() nodeConfig {
	return nodeConfig{
		name: "strict",
		cfg: centrifuge.Config{
			LogLevel: centrifuge.LogLevelError,
			// Small queue: a client that stops reading must be dropped as slow.
			ClientQueueMaxSize: 64 * 1024,
			ClientChannelLimit: 8,
			// A connection that never sends connect must be closed quickly.
			ClientStaleCloseDelay: time.Second,
			// Almost no grace after expiration.
			ClientExpiredCloseDelay:    500 * time.Millisecond,
			ClientExpiredSubCloseDelay: 500 * time.Millisecond,
		},
		ws: centrifuge.WebsocketConfig{
			MessageSizeLimit: 4096,
		},
	}
}

// redisNodeConfig describes one of the two Redis-backed nodes. Both share a
// Redis instance and a key prefix, so they form a real two-node cluster: PUB/SUB
// fanout, history, presence and the control plane all go through Redis, which is
// the code path the memory-backed nodes never touch.
func redisNodeConfig(name, address, prefix string) nodeConfig {
	return nodeConfig{
		name: name,
		cfg: centrifuge.Config{
			Name:               name,
			LogLevel:           centrifuge.LogLevelError,
			ClientQueueMaxSize: 128 * 1024 * 1024,
			ClientChannelLimit: 100000,
			// Keep every key this run creates short-lived: the suite uses a fresh
			// prefix per run and never deletes anything, so Redis must expire it.
			HistoryMetaTTL: 2 * time.Minute,
		},
		ws: centrifuge.WebsocketConfig{
			UseWriteBufferPool: true,
			MessageSizeLimit:   4 * 1024 * 1024,
		},
		storage: func(node *centrifuge.Node) (centrifuge.Broker, centrifuge.PresenceManager, func(), error) {
			shard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{Address: address})
			if err != nil {
				return nil, nil, nil, fmt.Errorf("redis shard: %w", err)
			}
			shards := []*centrifuge.RedisShard{shard}
			broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
				Prefix: prefix,
				Shards: shards,
			})
			if err != nil {
				shard.Close()
				return nil, nil, nil, fmt.Errorf("redis broker: %w", err)
			}
			pm, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
				Prefix:      prefix,
				Shards:      shards,
				PresenceTTL: time.Minute,
			})
			if err != nil {
				_ = broker.Close(context.Background())
				shard.Close()
				return nil, nil, nil, fmt.Errorf("redis presence manager: %w", err)
			}
			closer := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = broker.Close(ctx)
				_ = pm.Close(ctx)
				shard.Close()
			}
			return broker, pm, closer, nil
		},
	}
}

// buildNode wires a node with its storage and handlers and runs it. The returned
// closer shuts the node down and releases its storage.
func buildNode(nc nodeConfig) (*centrifuge.Node, func(), error) {
	node, err := centrifuge.New(nc.cfg)
	if err != nil {
		return nil, nil, err
	}
	storage := nc.storage
	if storage == nil {
		storage = memoryStorage
	}
	broker, pm, closeStorage, err := storage(node)
	if err != nil {
		return nil, nil, err
	}
	node.SetBroker(broker)
	node.SetPresenceManager(pm)

	// Survey/notification handlers must be installed before Run.
	node.OnSurvey(func(e centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		cb(centrifuge.SurveyReply{Data: []byte(fmt.Sprintf(`{"op":%q,"echo":%s}`, e.Op, string(e.Data)))})
	})
	node.OnNotification(func(e centrifuge.NotificationEvent) {
		notifications.record(e.Op, e.Data)
	})

	installHandlers(node)

	if err := node.Run(); err != nil {
		closeStorage()
		return nil, nil, err
	}
	closer := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = node.Shutdown(ctx)
		closeStorage()
	}
	return node, closer, nil
}

func memoryStorage(node *centrifuge.Node) (centrifuge.Broker, centrifuge.PresenceManager, func(), error) {
	broker, err := centrifuge.NewMemoryBroker(node, centrifuge.MemoryBrokerConfig{})
	if err != nil {
		return nil, nil, nil, err
	}
	pm, err := centrifuge.NewMemoryPresenceManager(node, centrifuge.MemoryPresenceManagerConfig{})
	if err != nil {
		return nil, nil, nil, err
	}
	return broker, pm, func() {}, nil
}

func installHandlers(node *centrifuge.Node) {
	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		user := e.Token // the harness carries the user id in the token field.
		if user == "" {
			user = "anon"
		}
		cred := &centrifuge.Credentials{UserID: user}
		reply := centrifuge.ConnectReply{Credentials: cred}

		switch {
		case strings.HasPrefix(user, userRefresh):
			// Must refresh to stay connected.
			cred.ExpireAt = time.Now().Add(2 * time.Second).Unix()
		case strings.HasPrefix(user, userExpire):
			// Refresh handler reports expired — the server must drop the connection.
			cred.ExpireAt = time.Now().Add(time.Second).Unix()
		case strings.HasPrefix(user, userPing):
			// Fast per-connection ping so the ping/pong scenario observes many cycles
			// in a short window. If pongs weren't processed the server would drop the
			// connection after PongTimeout.
			// PongTimeout must stay below PingInterval: the pong deadline is rearmed
			// on every ping, so a pong timeout longer than the ping interval would
			// never be reached and a silent client would live forever.
			reply.PingPongConfig = &centrifuge.PingPongConfig{
				PingInterval: 2 * time.Second,
				PongTimeout:  time.Second,
			}
		case strings.HasPrefix(user, userSSub):
			// Channels are encoded in the user id: "ssub:<ch>[,<ch>...]".
			reply.Subscriptions = map[string]centrifuge.SubscribeOptions{}
			for _, ch := range strings.Split(strings.TrimPrefix(user, userSSub), ",") {
				if ch != "" {
					reply.Subscriptions[ch] = subOptions(ch)
				}
			}
		}
		return reply, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if strings.HasPrefix(e.Channel, chDeny) {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{Options: subOptions(e.Channel)}, nil)
		})
		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			if strings.HasPrefix(e.Channel, chNoPub) {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			// Library auto-publishes when Result is nil.
			cb(centrifuge.PublishReply{Options: historyPublishOptions(e.Channel)}, nil)
		})
		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			switch e.Method {
			case "deny":
				cb(centrifuge.RPCReply{}, centrifuge.ErrorPermissionDenied)
				return
			case "boom":
				// A non-protocol error must surface to the client as error 100.
				cb(centrifuge.RPCReply{}, errors.New("handler exploded"))
				return
			}
			// Echo method + data back as a valid JSON object (the JSON client
			// requires reply data to be valid JSON).
			data := e.Data
			if len(data) == 0 {
				data = []byte("null")
			}
			cb(centrifuge.RPCReply{Data: []byte(fmt.Sprintf(`{"method":%q,"data":%s}`, e.Method, data))}, nil)
		})
		client.OnMessage(func(e centrifuge.MessageEvent) {
			// Async send: echo back as a message so the client can observe it.
			_ = client.Send(e.Data)
		})
		// Presence/history on a "deny:" channel is rejected — the scenarios use it
		// to check that application errors surface to the client with the right
		// code and leave the connection usable.
		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			if strings.HasPrefix(e.Channel, chDeny) {
				cb(centrifuge.PresenceReply{}, centrifuge.ErrorNotAvailable)
				return
			}
			cb(centrifuge.PresenceReply{}, nil)
		})
		client.OnPresenceStats(func(e centrifuge.PresenceStatsEvent, cb centrifuge.PresenceStatsCallback) {
			if strings.HasPrefix(e.Channel, chDeny) {
				cb(centrifuge.PresenceStatsReply{}, centrifuge.ErrorNotAvailable)
				return
			}
			cb(centrifuge.PresenceStatsReply{}, nil)
		})
		client.OnHistory(func(e centrifuge.HistoryEvent, cb centrifuge.HistoryCallback) {
			if strings.HasPrefix(e.Channel, chDeny) {
				cb(centrifuge.HistoryReply{}, centrifuge.ErrorNotAvailable)
				return
			}
			cb(centrifuge.HistoryReply{}, nil)
		})
		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			if strings.HasPrefix(client.UserID(), userExpire) {
				cb(centrifuge.RefreshReply{Expired: true}, nil)
				return
			}
			cb(centrifuge.RefreshReply{ExpireAt: time.Now().Add(2 * time.Second).Unix()}, nil)
		})
		client.OnSubRefresh(func(e centrifuge.SubRefreshEvent, cb centrifuge.SubRefreshCallback) {
			cb(centrifuge.SubRefreshReply{ExpireAt: time.Now().Add(2 * time.Second).Unix()}, nil)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			if debugDisc && e.Code != centrifuge.DisconnectConnectionClosed.Code {
				fmt.Fprintf(os.Stderr, "[srv-disconnect] user=%s code=%d reason=%q\n", client.UserID(), e.Code, e.Reason)
			}
		})
	})
}

// nodeMux mounts every transport the scenarios use.
func nodeMux(node *centrifuge.Node, ws centrifuge.WebsocketConfig) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", centrifuge.NewWebsocketHandler(node, ws))
	mux.Handle("/connection/sse", centrifuge.NewSSEHandler(node, centrifuge.SSEConfig{}))
	mux.Handle("/connection/http_stream", centrifuge.NewHTTPStreamHandler(node, centrifuge.HTTPStreamConfig{}))
	mux.Handle("/emulation", centrifuge.NewEmulationHandler(node, centrifuge.EmulationConfig{}))
	return mux
}
