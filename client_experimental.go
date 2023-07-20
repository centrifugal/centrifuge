package centrifuge

import (
	"errors"

	"github.com/centrifugal/protocol"
)

var errNoSubscription = errors.New("no subscription to a channel")

// WritePublication allows sending publications to Client subscription directly
// without HUB and Broker semantics. The possible use case is to turn subscription
// to a channel into an individual data stream.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) WritePublication(channel string, publication *Publication, sp StreamPosition) error {
	if !c.IsSubscribed(channel) {
		return errNoSubscription
	}

	pub := pubToProto(publication)
	protoType := c.transport.Protocol().toProto()

	if protoType == protocol.TypeJSON {
		if c.transport.Unidirectional() {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			jsonPush, err := protocol.DefaultJsonPushEncoder.Encode(push)
			if err != nil {
				go func(c *Client) { c.Disconnect(DisconnectInappropriateProtocol) }(c)
				return err
			}
			return c.writePublication(channel, pub, jsonPush, sp)
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			jsonReply, err := protocol.DefaultJsonReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				go func(c *Client) { c.Disconnect(DisconnectInappropriateProtocol) }(c)
				return err
			}
			return c.writePublication(channel, pub, jsonReply, sp)
		}
	} else if protoType == protocol.TypeProtobuf {
		if c.transport.Unidirectional() {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufPush, err := protocol.DefaultProtobufPushEncoder.Encode(push)
			if err != nil {
				return err
			}
			return c.writePublication(channel, pub, protobufPush, sp)
		} else {
			push := &protocol.Push{Channel: channel, Pub: pub}
			var err error
			protobufReply, err := protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{Push: push})
			if err != nil {
				return err
			}
			return c.writePublication(channel, pub, protobufReply, sp)
		}
	}

	return errors.New("unknown protocol type")
}

// AcquireStorage returns an attached connection storage (a map) and a function to be
// called when the application finished working with the storage map. Be accurate when
// using this API – avoid acquiring storage for a long time - i.e. on the time of IO operations.
// Do the work fast and release with the updated map. The API designed this way to allow
// reading, modifying or fully overriding storage map and avoid making deep copies each time.
// Note, that if storage map has not been initialized yet - i.e. if it's nil - then it will
// be initialized to an empty map and then returned – so you never receive nil map when
// acquiring. The purpose of this map is to simplify handling user-defined state during the
// lifetime of connection. Try to keep this map reasonably small.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) AcquireStorage() (map[string]any, func(map[string]any)) {
	c.storageMu.Lock()
	if c.storage == nil {
		c.storage = map[string]any{}
	}
	return c.storage, func(updatedStorage map[string]any) {
		c.storage = updatedStorage
		c.storageMu.Unlock()
	}
}

// OnStateSnapshot allows settings StateSnapshotHandler.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) OnStateSnapshot(h StateSnapshotHandler) {
	c.eventHub.stateSnapshotHandler = h
}

// StateSnapshot allows collecting current state copy.
// Mostly useful for connection introspection from the outside.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) StateSnapshot() (any, error) {
	if c.eventHub.stateSnapshotHandler != nil {
		return c.eventHub.stateSnapshotHandler()
	}
	return nil, nil
}
