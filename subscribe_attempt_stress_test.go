package centrifuge

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// attemptTracker is the application side of the SubscribeHandler /
// UnsubscribeHandler contract for one client: it counts what the handler
// allowed (before invoking the callback, as an application allocating per
// subscription state would) and what UnsubscribeHandler released.
type attemptTracker struct {
	mu           sync.Mutex
	allowed      map[string]int
	released     map[string]int
	overReleased []string
	outOfOrder   []string
	// serverUnsubscribing counts server unsubscribes in progress per channel: a
	// subscribe may go ahead of their UnsubscribeHandler calls (only attempt ends
	// are ordered before the next SubscribeHandler).
	serverUnsubscribing map[string]int
	afterDisconnect     []string
	disconnects         int
	async               sync.WaitGroup
	kinds               map[string]int // Event kinds, to show what a run covered.
}

func newAttemptTracker() *attemptTracker {
	return &attemptTracker{allowed: map[string]int{}, released: map[string]int{}, kinds: map[string]int{}, serverUnsubscribing: map[string]int{}}
}

func (tr *attemptTracker) allow(ch string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.allowed[ch]++
}

func (tr *attemptTracker) onUnsubscribe(e UnsubscribeEvent) {
	if e.ServerSide {
		return // Not from SubscribeHandler.
	}
	if rand.IntN(4) == 0 {
		// A slow handler (a database write, say).
		time.Sleep(time.Duration(rand.IntN(1500)) * time.Microsecond)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.released[e.Channel]++
	if tr.disconnects > 0 {
		tr.afterDisconnect = append(tr.afterDisconnect, e.Channel)
	}
	tr.kinds[fmt.Sprintf("%s subscribed=%v code=%d", strings.SplitN(e.Channel, ":", 2)[0], e.Subscribed, e.Code)]++
	desc := fmt.Sprintf("%s (code %d, subscribed %v)", e.Channel, e.Code, e.Subscribed)
	if tr.released[e.Channel] > tr.allowed[e.Channel] {
		tr.overReleased = append(tr.overReleased, desc)
	}
}

// serverUnsubscribe unsubscribes the client server-side, recording it in
// progress.
func (tr *attemptTracker) serverUnsubscribe(client *Client, ch string) {
	tr.mu.Lock()
	tr.serverUnsubscribing[ch]++
	tr.mu.Unlock()
	client.Unsubscribe(ch)
	tr.mu.Lock()
	tr.serverUnsubscribing[ch]--
	tr.mu.Unlock()
}

func (tr *attemptTracker) onDisconnect(DisconnectEvent) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.disconnects++
}

// onSubscribe answers synchronously or from another goroutine, allows or denies,
// and sometimes allows with options Centrifuge then refuses.
func (tr *attemptTracker) onSubscribe(e SubscribeEvent, cb SubscribeCallback) {
	tr.mu.Lock()
	if tr.allowed[e.Channel] != tr.released[e.Channel] && tr.serverUnsubscribing[e.Channel] == 0 {
		// A previous attempt on the channel did not get its UnsubscribeHandler
		// call yet.
		tr.outOfOrder = append(tr.outOfOrder, e.Channel)
	}
	tr.mu.Unlock()
	reply := SubscribeReply{Options: SubscribeOptions{Type: e.Type}}
	if e.Type == SubscriptionTypeSharedPoll {
		reply.Options.Type = 0
		reply.Options.ExpireAt = time.Now().Unix() + 3600
		reply.ClientSideRefresh = true
	} else if e.Type == SubscriptionTypeStream {
		reply.Options.EnableRecovery = rand.IntN(2) == 0
		reply.Options.EmitPresence = rand.IntN(2) == 0
		reply.Options.EmitJoinLeave = rand.IntN(2) == 0
	}
	var err error
	switch r := rand.IntN(20); {
	case r < 3:
		err = ErrorPermissionDenied
	case r < 4:
		reply.Options.ExpireAt = time.Now().Unix() - 10 // Refused by Centrifuge.
	case r < 5 && e.Type == SubscriptionTypeMap:
		reply.Options.Type = SubscriptionTypeMapClients // Refused by Centrifuge.
	}
	answer := func() {
		if err == nil {
			tr.allow(e.Channel)
		}
		cb(reply, err)
	}
	if rand.IntN(2) == 0 {
		answer()
		return
	}
	tr.async.Add(1)
	go func() {
		defer tr.async.Done()
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		answer()
	}()
}

// stressReplyWriter delivers the replies of one request, from whichever
// goroutine writes them.
func stressReplyWriter() (*replyWriter, chan *protocol.Reply) {
	ch := make(chan *protocol.Reply, 4)
	return &replyWriter{write: func(rep *protocol.Reply) {
		d, _ := rep.MarshalCF()
		var r protocol.Reply
		_ = r.UnmarshalCF(d)
		ch <- &r
	}}, ch
}

// jitterHistoryBroker delays history reads randomly, so subscribes commit at
// random moments relative to unsubscribes and disconnects.
type jitterHistoryBroker struct {
	*MemoryBroker
}

func (b *jitterHistoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	time.Sleep(time.Duration(rand.IntN(1000)) * time.Microsecond)
	return b.MemoryBroker.History(ch, opts)
}

func newStressNode(t *testing.T) (*Node, *MemoryMapBroker) {
	node, err := New(Config{
		LogLevel:   LogLevelTrace,
		LogHandler: func(LogEntry) {},
		Map: MapConfig{
			GetMapChannelOptions: func(string) MapChannelOptions {
				// A short catch-up timeout: loads paused between pages expire, and
				// are swept by later map subscribes.
				return MapChannelOptions{Mode: MapModeRecoverable, KeyTTL: time.Minute, MinPageSize: 1, SubscribeCatchUpTimeout: 15 * time.Millisecond}
			},
		},
		SharedPoll: SharedPollConfig{
			GetSharedPollChannelOptions: func(channel string) (SharedPollChannelOptions, bool) {
				return SharedPollChannelOptions{
					RefreshInterval: 100 * time.Millisecond, RefreshBatchSize: 100, MaxKeysPerConnection: 100,
				}, strings.HasPrefix(channel, "poll:")
			},
		},
	})
	require.NoError(t, err)
	broker, err := NewMemoryBroker(node, MemoryBrokerConfig{})
	require.NoError(t, err)
	node.SetBroker(&jitterHistoryBroker{MemoryBroker: broker})
	mapBroker, err := NewMemoryMapBroker(node, MemoryMapBrokerConfig{})
	require.NoError(t, err)
	require.NoError(t, mapBroker.RegisterEventHandler(nil))
	node.SetMapBroker(mapBroker)
	node.OnSharedPoll(func(context.Context, SharedPollEvent) (SharedPollResult, error) {
		return SharedPollResult{}, nil
	})
	require.NoError(t, node.Run())
	t.Cleanup(func() { _ = node.Shutdown(context.Background()) })
	return node, mapBroker
}

var (
	stressRegularChannels = []string{"ch:0", "ch:1", "ch:2"}
	stressMapChannels     = []string{"map:0", "map:1"}
	stressPollChannels    = []string{"poll:0", "poll:1"}
)

func stressAllChannels() []string {
	var all []string
	all = append(all, stressRegularChannels...)
	all = append(all, stressMapChannels...)
	return append(all, stressPollChannels...)
}

// stressClientCommands sends random subscribe and unsubscribe commands, one at a
// time as a connection does, until the client closes or ops run out.
func stressClientCommands(client *Client, mapPositions *sync.Map, ops int) {
	await := func(replies chan *protocol.Reply) *protocol.Reply {
		if rand.IntN(3) == 0 {
			return nil // Pipeline: send the next command without waiting for the reply.
		}
		select {
		case r := <-replies:
			return r
		case <-time.After(50 * time.Millisecond):
			return nil // Disconnected, or the reply was dropped on a closed client.
		}
	}
	for i := 0; i < ops; i++ {
		client.mu.RLock()
		closed := client.status == statusClosed
		client.mu.RUnlock()
		if closed && rand.IntN(4) != 0 {
			return // Sometimes keep sending to a closed client.
		}
		cmd := &protocol.Command{Id: uint32(i + 1)}
		switch op := rand.IntN(10); {
		case op < 3:
			rw, replies := stressReplyWriter()
			ch := stressRegularChannels[rand.IntN(len(stressRegularChannels))]
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Recover: rand.IntN(2) == 0}, cmd, time.Now(), rw)
			await(replies)
		case op < 6:
			ch := stressMapChannels[rand.IntN(len(stressMapChannels))]
			req := &protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeMap), Phase: MapPhaseState, Limit: 2}
			if rand.IntN(4) == 0 {
				// Direct-to-live or stream recovery from a recent position.
				if v, ok := mapPositions.Load(ch); ok && v.(StreamPosition).Offset > 2 {
					pos := v.(StreamPosition)
					req.Recover = true
					req.Epoch = pos.Epoch
					req.Offset = pos.Offset - uint64(rand.IntN(3))
					req.Phase = MapPhaseLive
					if rand.IntN(2) == 0 {
						req.Phase = MapPhaseStream
						req.Offset = 1
						req.Limit = 1
					}
				}
			}
			// Paginate, sometimes abandoning the load halfway.
			for page := 0; page < 20; page++ {
				rw, replies := stressReplyWriter()
				_ = client.handleSubscribe(req, cmd, time.Now(), rw)
				r := await(replies)
				if r == nil || r.Error != nil || r.Subscribe == nil || r.Subscribe.Phase == MapPhaseLive || rand.IntN(6) == 0 {
					break
				}
				next := &protocol.SubscribeRequest{
					Channel: ch, Type: req.Type, Phase: r.Subscribe.Phase, Limit: req.Limit,
					Cursor: r.Subscribe.Cursor, Offset: r.Subscribe.Offset, Epoch: r.Subscribe.Epoch,
				}
				if r.Subscribe.Phase == MapPhaseStream {
					next.Recover = req.Recover
				}
				if rand.IntN(8) == 0 {
					time.Sleep(20 * time.Millisecond) // Longer than the catch-up timeout.
				}
				req = next
			}
		case op < 7:
			rw, replies := stressReplyWriter()
			ch := stressPollChannels[rand.IntN(len(stressPollChannels))]
			_ = client.handleSubscribe(&protocol.SubscribeRequest{Channel: ch, Type: int32(SubscriptionTypeSharedPoll)}, cmd, time.Now(), rw)
			await(replies)
		default:
			ch := stressAllChannels()[rand.IntN(len(stressAllChannels()))]
			_ = client.handleUnsubscribe(&protocol.UnsubscribeRequest{Channel: ch}, cmd, time.Now(), nil)
		}
	}
}

// TestSubscribeAttempt_Stress runs clients which subscribe (regular, map and
// shared poll), paginate, unsubscribe and get unsubscribed and disconnected by
// the server concurrently, with SubscribeHandler answering synchronously or
// asynchronously. Every allowed attempt must get exactly one UnsubscribeHandler
// call, never before it was allowed, a channel's SubscribeHandler call must come
// after the UnsubscribeHandler calls for its previous attempts, and nothing of
// the client may remain. SUBSCRIBE_ATTEMPT_STRESS_ROUNDS raises the
// number of rounds.
func TestSubscribeAttempt_Stress(t *testing.T) {
	t.Parallel()
	rounds := 3
	if v, err := strconv.Atoi(os.Getenv("SUBSCRIBE_ATTEMPT_STRESS_ROUNDS")); err == nil {
		rounds = v
	}
	node, mapBroker := newStressNode(t)
	ctx := context.Background()
	var mapPositions sync.Map
	for _, ch := range stressMapChannels {
		for i := 0; i < 6; i++ {
			res, err := mapBroker.Publish(ctx, ch, "k"+strconv.Itoa(i), MapPublishOptions{Data: []byte(`{}`)})
			require.NoError(t, err)
			mapPositions.Store(ch, res.Position)
		}
	}

	stopPublishing := make(chan struct{})
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		for i := 0; ; i++ {
			select {
			case <-stopPublishing:
				return
			case <-time.After(200 * time.Microsecond):
			}
			ch := stressRegularChannels[i%len(stressRegularChannels)]
			_, _ = node.Publish(ch, []byte(`{}`), WithHistory(10, time.Minute))
			mch := stressMapChannels[i%len(stressMapChannels)]
			if res, err := mapBroker.Publish(ctx, mch, "k"+strconv.Itoa(i%8), MapPublishOptions{Data: []byte(`{}`)}); err == nil {
				mapPositions.Store(mch, res.Position)
			}
		}
	}()
	defer func() {
		close(stopPublishing)
		<-publisherDone
	}()

	trackers := map[*Client]*attemptTracker{}
	var trackersMu sync.Mutex
	node.OnConnect(func(client *Client) {
		tr := newAttemptTracker()
		trackersMu.Lock()
		trackers[client] = tr
		trackersMu.Unlock()
		client.OnSubscribe(tr.onSubscribe)
		client.OnUnsubscribe(tr.onUnsubscribe)
		client.OnDisconnect(tr.onDisconnect)
	})

	const clientsPerRound = 40
	for round := 0; round < rounds; round++ {
		var wg sync.WaitGroup
		for i := 0; i < clientsPerRound; i++ {
			client := newTestConnectedClientV2(t, node, "user"+strconv.Itoa(i))
			trackersMu.Lock()
			tr := trackers[client]
			trackersMu.Unlock()
			commandsDone := make(chan struct{})
			wg.Add(2)
			go func() {
				defer wg.Done()
				defer close(commandsDone)
				stressClientCommands(client, &mapPositions, 30)
			}()
			go func() {
				defer wg.Done()
				// The server unsubscribes, subscribes and disconnects at random moments.
				deadline := time.After(time.Duration(rand.IntN(40)) * time.Millisecond)
				for {
					select {
					case <-deadline:
					case <-commandsDone:
					case <-time.After(time.Duration(rand.IntN(3000)) * time.Microsecond):
						switch rand.IntN(6) {
						case 0, 1:
							ch := stressAllChannels()[rand.IntN(len(stressAllChannels()))]
							go tr.serverUnsubscribe(client, ch)
						case 2:
							ch := stressRegularChannels[rand.IntN(len(stressRegularChannels))]
							go func() { _ = client.Subscribe(ch) }()
						}
						continue
					}
					break
				}
				disconnect := []Disconnect{DisconnectForceNoReconnect, DisconnectConnectionClosed, DisconnectServerError}[rand.IntN(3)]
				_ = client.close(disconnect)
			}()
		}
		wg.Wait()
	}

	// close() waited for every SubscribeCallback, so nothing is pending. Give the
	// asynchronous server unsubscribes (now no-ops) a moment to finish.
	time.Sleep(100 * time.Millisecond)
	trackersMu.Lock()
	defer trackersMu.Unlock()
	require.Len(t, trackers, rounds*clientsPerRound)
	var allowedTotal int
	kinds := map[string]int{}
	for client, tr := range trackers {
		tr.async.Wait()
		// UnsubscribeHandler calls for attempts ended after close() may still be
		// on their way.
		require.Eventually(t, func() bool {
			client.mu.RLock()
			defer client.mu.RUnlock()
			return client.pendingUnsubscribes == 0
		}, 10*time.Second, time.Millisecond, "client %s", client.uid)
		tr.mu.Lock()
		require.Equal(t, 1, tr.disconnects, "client %s", client.uid)
		require.Empty(t, tr.overReleased, "client %s: UnsubscribeHandler without an allowed attempt", client.uid)
		require.Empty(t, tr.outOfOrder, "client %s: SubscribeHandler before the UnsubscribeHandler of a previous attempt on the channel", client.uid)
		require.Empty(t, tr.afterDisconnect, "client %s: UnsubscribeHandler after DisconnectHandler", client.uid)
		for ch, n := range tr.allowed {
			require.Equal(t, n, tr.released[ch], "client %s channel %s: allowed %d attempts, released %d", client.uid, ch, n, tr.released[ch])
			allowedTotal += n
		}
		for k, n := range tr.kinds {
			kinds[k] += n
		}
		tr.mu.Unlock()
		client.mu.RLock()
		require.Empty(t, client.channels, "client %s", client.uid)
		require.Empty(t, client.mapSubscribing, "client %s", client.uid)
		require.Zero(t, client.pendingUnsubscribes, "client %s", client.uid)
		client.mu.RUnlock()
	}
	for _, ch := range stressAllChannels() {
		require.Zero(t, node.hub.NumSubscribers(ch), "channel %s", ch)
	}
	t.Logf("%d clients, %d allowed attempts, all released once, in order per channel", len(trackers), allowedTotal)
	for k, n := range kinds {
		t.Logf("  %s: %d", k, n)
	}
}
