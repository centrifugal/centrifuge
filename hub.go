package centrifuge

import (
	"context"
	"io"
	"sync"

	"github.com/centrifugal/centrifuge/internal/clientproto"
	"github.com/centrifugal/centrifuge/internal/prepared"
	"github.com/centrifugal/centrifuge/internal/recovery"

	"github.com/centrifugal/protocol"
)

const numHubShards = 64

// Hub tracks Client connections on the current Node.
type Hub struct {
	connShards [numHubShards]*connShard
	subShards  [numHubShards]*subShard
}

// newHub initializes Hub.
func newHub() *Hub {
	h := &Hub{}
	for i := 0; i < numHubShards; i++ {
		h.connShards[i] = newConnShard()
		h.subShards[i] = newSubShard()
	}
	return h
}

// shutdown unsubscribes users from all channels and disconnects them.
func (h *Hub) shutdown(ctx context.Context) error {
	// Limit concurrency here to prevent resource usage burst on shutdown.
	sem := make(chan struct{}, hubShutdownSemaphoreSize)

	var errMu sync.Mutex
	var shutdownErr error

	var wg sync.WaitGroup
	wg.Add(numHubShards)
	for i := 0; i < numHubShards; i++ {
		go func(i int) {
			defer wg.Done()
			err := h.connShards[i].shutdown(ctx, sem)
			if err != nil {
				errMu.Lock()
				if shutdownErr == nil {
					shutdownErr = err
				}
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return shutdownErr
}

// add adds connection into clientHub connections registry.
func (h *Hub) add(c *Client) error {
	return h.connShards[index(c.UserID(), numHubShards)].add(c)
}

// Remove removes connection from clientHub connections registry.
func (h *Hub) remove(c *Client) error {
	return h.connShards[index(c.UserID(), numHubShards)].remove(c)
}

// userConnections returns all user connections to the current Node.
func (h *Hub) userConnections(userID string) map[string]*Client {
	return h.connShards[index(userID, numHubShards)].userConnections(userID)
}

func (h *Hub) disconnect(userID string, disconnect *Disconnect, whitelist []string) error {
	return h.connShards[index(userID, numHubShards)].disconnect(userID, disconnect, whitelist)
}

func (h *Hub) unsubscribe(userID string, ch string, clientID string) error {
	return h.connShards[index(userID, numHubShards)].unsubscribe(userID, ch, clientID)
}

func (h *Hub) subscribe(userID string, ch string, clientID string, opts ...SubscribeOption) error {
	return h.connShards[index(userID, numHubShards)].subscribe(userID, ch, clientID, opts...)
}

func (h *Hub) addSub(ch string, c *Client) (bool, error) {
	return h.subShards[index(ch, numHubShards)].addSub(ch, c)
}

// removeSub removes connection from clientHub subscriptions registry.
func (h *Hub) removeSub(ch string, c *Client) (bool, error) {
	return h.subShards[index(ch, numHubShards)].removeSub(ch, c)
}

// broadcastPublication sends message to all clients subscribed on channel.
func (h *Hub) broadcastPublication(ch string, pub *protocol.Publication, sp StreamPosition) error {
	return h.subShards[index(ch, numHubShards)].broadcastPublication(ch, pub, sp)
}

// broadcastJoin sends message to all clients subscribed on channel.
func (h *Hub) broadcastJoin(ch string, join *protocol.Join) error {
	return h.subShards[index(ch, numHubShards)].broadcastJoin(ch, join)
}

func (h *Hub) broadcastLeave(ch string, leave *protocol.Leave) error {
	return h.subShards[index(ch, numHubShards)].broadcastLeave(ch, leave)
}

// NumSubscribers returns number of current subscribers for a given channel.
func (h *Hub) NumSubscribers(ch string) int {
	return h.subShards[index(ch, numHubShards)].NumSubscribers(ch)
}

// Channels returns a slice of all active channels.
func (h *Hub) Channels() []string {
	channels := make([]string, 0, h.NumChannels())
	for i := 0; i < numHubShards; i++ {
		channels = append(channels, h.subShards[i].Channels()...)
	}
	return channels
}

// NumClients returns total number of client connections.
func (h *Hub) NumClients() int {
	var total int
	for i := 0; i < numHubShards; i++ {
		total += h.connShards[i].NumClients()
	}
	return total
}

// NumUsers returns a number of unique users connected.
func (h *Hub) NumUsers() int {
	var total int
	for i := 0; i < numHubShards; i++ {
		// users do not overlap among shards.
		total += h.connShards[i].NumUsers()
	}
	return total
}

// NumChannels returns a total number of different channels.
func (h *Hub) NumChannels() int {
	var total int
	for i := 0; i < numHubShards; i++ {
		// channels do not overlap among shards.
		total += h.subShards[i].NumChannels()
	}
	return total
}

type connShard struct {
	mu sync.RWMutex
	// match client ID with actual client connection.
	conns map[string]*Client
	// registry to hold active client connections grouped by user.
	users map[string]map[string]struct{}
}

func newConnShard() *connShard {
	return &connShard{
		conns: make(map[string]*Client),
		users: make(map[string]map[string]struct{}),
	}
}

const (
	// hubShutdownSemaphoreSize limits graceful disconnects concurrency
	// on node shutdown.
	hubShutdownSemaphoreSize = 128
)

// shutdown unsubscribes users from all channels and disconnects them.
func (h *connShard) shutdown(ctx context.Context, sem chan struct{}) error {
	advice := DisconnectShutdown

	h.mu.RLock()
	// At this moment node won't accept new client connections so we can
	// safely copy existing clients and release lock.
	clients := make([]*Client, 0, len(h.conns))
	for _, client := range h.conns {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	closeFinishedCh := make(chan struct{}, len(clients))
	finished := 0

	if len(clients) == 0 {
		return nil
	}

	for _, client := range clients {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		go func(cc *Client) {
			defer func() { <-sem }()
			defer func() { closeFinishedCh <- struct{}{} }()
			_ = cc.close(advice)
		}(client)
	}

	for {
		select {
		case <-closeFinishedCh:
			finished++
			if finished == len(clients) {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func stringInSlice(str string, slice []string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (h *connShard) subscribe(user string, ch string, clientID string, opts ...SubscribeOption) error {
	userConnections := h.userConnections(user)

	var firstErr error
	var errMu sync.Mutex

	var wg sync.WaitGroup
	for _, c := range userConnections {
		if clientID != "" && c.ID() != clientID {
			continue
		}
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			err := c.Subscribe(ch, opts...)
			errMu.Lock()
			defer errMu.Unlock()
			if err != nil && err != io.EOF && firstErr == nil {
				firstErr = err
			}
		}(c)
	}
	wg.Wait()
	return firstErr
}

func (h *connShard) unsubscribe(user string, ch string, clientID string) error {
	userConnections := h.userConnections(user)

	var firstErr error
	var errMu sync.Mutex

	var wg sync.WaitGroup
	for _, c := range userConnections {
		if clientID != "" && c.ID() != clientID {
			continue
		}
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			err := c.Unsubscribe(ch)
			errMu.Lock()
			defer errMu.Unlock()
			if err != nil && err != io.EOF && firstErr == nil {
				firstErr = err
			}
		}(c)
	}
	wg.Wait()
	return firstErr
}

func (h *connShard) disconnect(user string, disconnect *Disconnect, whitelist []string) error {
	userConnections := h.userConnections(user)

	var firstErr error
	var errMu sync.Mutex

	var wg sync.WaitGroup
	for _, c := range userConnections {
		if stringInSlice(c.ID(), whitelist) {
			continue
		}
		wg.Add(1)
		go func(cc *Client) {
			defer wg.Done()
			err := cc.close(disconnect)
			errMu.Lock()
			defer errMu.Unlock()
			if err != nil && err != io.EOF && firstErr == nil {
				firstErr = err
			}
		}(c)
	}
	wg.Wait()
	return firstErr
}

// userConnections returns all connections of user with specified UserID.
func (h *connShard) userConnections(userID string) map[string]*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	userConnections, ok := h.users[userID]
	if !ok {
		return map[string]*Client{}
	}

	conns := make(map[string]*Client, len(userConnections))
	for uid := range userConnections {
		c, ok := h.conns[uid]
		if !ok {
			continue
		}
		conns[uid] = c
	}

	return conns
}

// add adds connection into clientHub connections registry.
func (h *connShard) add(c *Client) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	uid := c.ID()
	user := c.UserID()

	h.conns[uid] = c

	if _, ok := h.users[user]; !ok {
		h.users[user] = make(map[string]struct{})
	}
	h.users[user][uid] = struct{}{}
	return nil
}

// Remove removes connection from clientHub connections registry.
func (h *connShard) remove(c *Client) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	uid := c.ID()
	user := c.UserID()

	delete(h.conns, uid)

	// try to find connection to delete, return early if not found.
	if _, ok := h.users[user]; !ok {
		return nil
	}
	if _, ok := h.users[user][uid]; !ok {
		return nil
	}

	// actually remove connection from hub.
	delete(h.users[user], uid)

	// clean up users map if it's needed.
	if len(h.users[user]) == 0 {
		delete(h.users, user)
	}

	return nil
}

// NumClients returns total number of client connections.
func (h *connShard) NumClients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	total := 0
	for _, clientConnections := range h.users {
		total += len(clientConnections)
	}
	return total
}

// NumUsers returns a number of unique users connected.
func (h *connShard) NumUsers() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.users)
}

type subShard struct {
	mu sync.RWMutex
	// registry to hold active subscriptions of clients to channels.
	subs map[string]map[string]*Client
}

func newSubShard() *subShard {
	return &subShard{
		subs: make(map[string]map[string]*Client),
	}
}

// addSub adds connection into clientHub subscriptions registry.
func (h *subShard) addSub(ch string, c *Client) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	uid := c.ID()

	_, ok := h.subs[ch]
	if !ok {
		h.subs[ch] = make(map[string]*Client)
	}
	h.subs[ch][uid] = c
	if !ok {
		return true, nil
	}
	return false, nil
}

// removeSub removes connection from clientHub subscriptions registry.
func (h *subShard) removeSub(ch string, c *Client) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	uid := c.ID()

	// try to find subscription to delete, return early if not found.
	if _, ok := h.subs[ch]; !ok {
		return true, nil
	}
	if _, ok := h.subs[ch][uid]; !ok {
		return true, nil
	}

	// actually remove subscription from hub.
	delete(h.subs[ch], uid)

	// clean up subs map if it's needed.
	if len(h.subs[ch]) == 0 {
		delete(h.subs, ch)
		return true, nil
	}

	return false, nil
}

// broadcastPublication sends message to all clients subscribed on channel.
func (h *subShard) broadcastPublication(channel string, pub *protocol.Publication, sp StreamPosition) error {
	useSeqGen := hasFlag(CompatibilityFlags, UseSeqGen)
	if useSeqGen {
		pub.Seq, pub.Gen = recovery.UnpackUint64(pub.Offset)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	// get connections currently subscribed on channel.
	channelSubscriptions, ok := h.subs[channel]
	if !ok {
		return nil
	}

	var jsonPublicationReply *prepared.Reply
	var protobufPublicationReply *prepared.Reply
	// Iterate over channel subscribers and send message.
	for _, c := range channelSubscriptions {
		protoType := c.Transport().Protocol().toProto()
		if protoType == protocol.TypeJSON {
			if jsonPublicationReply == nil {
				// Do not send offset to clients for now.
				var offset uint64
				if useSeqGen {
					offset = pub.Offset
					pub.Offset = 0
				}
				data, err := protocol.GetPushEncoder(protoType).EncodePublication(pub)
				if err != nil {
					return err
				}
				if useSeqGen {
					pub.Offset = offset
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewPublicationPush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				jsonPublicationReply = prepared.NewReply(reply, protocol.TypeJSON)
			}
			_ = c.writePublication(channel, pub, jsonPublicationReply, sp)
		} else if protoType == protocol.TypeProtobuf {
			if protobufPublicationReply == nil {
				// Do not send offset to clients for now.
				var offset uint64
				if useSeqGen {
					offset = pub.Offset
					pub.Offset = 0
				}
				data, err := protocol.GetPushEncoder(protoType).EncodePublication(pub)
				if err != nil {
					return err
				}
				if useSeqGen {
					pub.Offset = offset
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewPublicationPush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				protobufPublicationReply = prepared.NewReply(reply, protocol.TypeProtobuf)
			}
			_ = c.writePublication(channel, pub, protobufPublicationReply, sp)
		}
	}
	return nil
}

// broadcastJoin sends message to all clients subscribed on channel.
func (h *subShard) broadcastJoin(channel string, join *protocol.Join) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	channelSubscriptions, ok := h.subs[channel]
	if !ok {
		return nil
	}

	var (
		jsonReply     *prepared.Reply
		protobufReply *prepared.Reply
	)

	for _, c := range channelSubscriptions {
		protoType := c.Transport().Protocol().toProto()
		if protoType == protocol.TypeJSON {
			if jsonReply == nil {
				data, err := protocol.GetPushEncoder(protoType).EncodeJoin(join)
				if err != nil {
					return err
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewJoinPush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				jsonReply = prepared.NewReply(reply, protocol.TypeJSON)
			}
			_ = c.writeJoin(channel, jsonReply)
		} else if protoType == protocol.TypeProtobuf {
			if protobufReply == nil {
				data, err := protocol.GetPushEncoder(protoType).EncodeJoin(join)
				if err != nil {
					return err
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewJoinPush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				protobufReply = prepared.NewReply(reply, protocol.TypeProtobuf)
			}
			_ = c.writeJoin(channel, protobufReply)
		}
	}
	return nil
}

// broadcastLeave sends message to all clients subscribed on channel.
func (h *subShard) broadcastLeave(channel string, leave *protocol.Leave) error {
	h.mu.RLock()
	defer h.mu.RUnlock()

	channelSubscriptions, ok := h.subs[channel]
	if !ok {
		return nil
	}

	var (
		jsonReply     *prepared.Reply
		protobufReply *prepared.Reply
	)

	for _, c := range channelSubscriptions {
		protoType := c.Transport().Protocol().toProto()
		if protoType == protocol.TypeJSON {
			if jsonReply == nil {
				data, err := protocol.GetPushEncoder(protoType).EncodeLeave(leave)
				if err != nil {
					return err
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewLeavePush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				jsonReply = prepared.NewReply(reply, protocol.TypeJSON)
			}
			_ = c.writeLeave(channel, jsonReply)
		} else if protoType == protocol.TypeProtobuf {
			if protobufReply == nil {
				data, err := protocol.GetPushEncoder(protoType).EncodeLeave(leave)
				if err != nil {
					return err
				}
				messageBytes, err := protocol.GetPushEncoder(protoType).Encode(clientproto.NewLeavePush(channel, data))
				if err != nil {
					return err
				}
				reply := &protocol.Reply{
					Result: messageBytes,
				}
				protobufReply = prepared.NewReply(reply, protocol.TypeProtobuf)
			}
			_ = c.writeLeave(channel, protobufReply)
		}
	}
	return nil
}

// NumChannels returns a total number of different channels.
func (h *subShard) NumChannels() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// Channels returns a slice of all active channels.
func (h *subShard) Channels() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	channels := make([]string, len(h.subs))
	i := 0
	for ch := range h.subs {
		channels[i] = ch
		i++
	}
	return channels
}

// NumSubscribers returns number of current subscribers for a given channel.
func (h *subShard) NumSubscribers(ch string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	conns, ok := h.subs[ch]
	if !ok {
		return 0
	}
	return len(conns)
}
