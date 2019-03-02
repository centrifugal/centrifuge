package natsengine

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/centrifugal/centrifuge/protocent"

	"github.com/nats-io/nats"
)

type (
	// channelID is unique channel identificator in Nats.
	channelID string
)

// Config ...
type Config struct {
	Prefix string
}

// NatsEngine ...
type NatsEngine struct {
	node         *centrifuge.Node
	config       Config
	eventHandler centrifuge.EngineEventHandler

	nc     *nats.Conn
	subsMu sync.Mutex
	subs   map[channelID]*nats.Subscription

	pushEncoder protocent.PushEncoder
	pushDecoder protocent.PushDecoder
}

// New ...
func New(n *centrifuge.Node, conf Config) (*NatsEngine, error) {
	e := &NatsEngine{
		node:        n,
		config:      conf,
		subs:        make(map[channelID]*nats.Subscription),
		pushEncoder: protocent.NewProtobufPushEncoder(),
		pushDecoder: protocent.NewProtobufPushDecoder(),
	}
	return e, nil
}

func (e *NatsEngine) controlChannel() channelID {
	return channelID(e.config.Prefix + "." + "control")
}

func (e *NatsEngine) clientChannel(ch string) channelID {
	return channelID(e.config.Prefix + ".client." + ch)
}

// Run runs engine after node initialized.
func (e *NatsEngine) Run(h centrifuge.EngineEventHandler) error {
	e.eventHandler = h
	nc, err := nats.Connect(nats.DefaultURL, nats.ReconnectBufSize(-1), nats.MaxReconnects(math.MaxInt64))
	if err != nil {
		return err
	}
	_, err = nc.Subscribe(string(e.controlChannel()), e.handleControl)
	if err != nil {
		return err
	}
	e.nc = nc
	return nil
}

// Shutdown ...
func (e *NatsEngine) Shutdown(ctx context.Context) error {
	return nil
}

// Publish - see engine interface description.
func (e *NatsEngine) Publish(ch string, pub *centrifuge.Publication, opts *centrifuge.ChannelOptions) <-chan error {
	eChan := make(chan error, 1)
	data, err := e.pushEncoder.EncodePublication(pub)
	if err != nil {
		eChan <- err
		return eChan
	}
	byteMessage, err := e.pushEncoder.Encode(protocent.NewPublicationPush(ch, data))
	if err != nil {
		eChan <- err
		return eChan
	}
	eChan <- e.nc.Publish(string(e.clientChannel(ch)), byteMessage)
	return eChan
}

// PublishJoin - see engine interface description.
func (e *NatsEngine) PublishJoin(ch string, join *centrifuge.Join, opts *centrifuge.ChannelOptions) <-chan error {
	eChan := make(chan error, 1)
	data, err := e.pushEncoder.EncodeJoin(join)
	if err != nil {
		eChan <- err
		return eChan
	}
	byteMessage, err := e.pushEncoder.Encode(protocent.NewJoinPush(ch, data))
	if err != nil {
		eChan <- err
		return eChan
	}
	eChan <- e.nc.Publish(string(e.clientChannel(ch)), byteMessage)
	return eChan
}

// PublishLeave - see engine interface description.
func (e *NatsEngine) PublishLeave(ch string, leave *centrifuge.Leave, opts *centrifuge.ChannelOptions) <-chan error {
	eChan := make(chan error, 1)
	data, err := e.pushEncoder.EncodeLeave(leave)
	if err != nil {
		eChan <- err
		return eChan
	}
	byteMessage, err := e.pushEncoder.Encode(protocent.NewLeavePush(ch, data))
	if err != nil {
		eChan <- err
		return eChan
	}
	eChan <- e.nc.Publish(string(e.clientChannel(ch)), byteMessage)
	return eChan
}

// PublishControl - see engine interface description.
func (e *NatsEngine) PublishControl(data []byte) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.nc.Publish(string(e.controlChannel()), data)
	return eChan
}

func (e *NatsEngine) handleClientMessage(chID channelID, data []byte) error {
	var push protocent.Push
	err := push.Unmarshal(data)
	if err != nil {
		return err
	}
	switch push.Type {
	case protocent.PushTypePublication:
		pub, err := e.pushDecoder.DecodePublication(push.Data)
		if err != nil {
			return err
		}
		e.eventHandler.HandlePublication(push.Channel, pub)
	case protocent.PushTypeJoin:
		join, err := e.pushDecoder.DecodeJoin(push.Data)
		if err != nil {
			return err
		}
		e.eventHandler.HandleJoin(push.Channel, join)
	case protocent.PushTypeLeave:
		leave, err := e.pushDecoder.DecodeLeave(push.Data)
		if err != nil {
			return err
		}
		e.eventHandler.HandleLeave(push.Channel, leave)
	default:
	}
	return nil
}

func (e *NatsEngine) handleClient(m *nats.Msg) {
	e.handleClientMessage(channelID(m.Subject), m.Data)
}

func (e *NatsEngine) handleControl(m *nats.Msg) {
	e.eventHandler.HandleControl(m.Data)
}

// Subscribe ...
func (e *NatsEngine) Subscribe(ch string) error {
	e.subsMu.Lock()
	defer e.subsMu.Unlock()
	subClient, err := e.nc.Subscribe(string(e.clientChannel(ch)), e.handleClient)
	if err != nil {
		return err
	}
	e.subs[e.clientChannel(ch)] = subClient
	return nil
}

// Unsubscribe ...
func (e *NatsEngine) Unsubscribe(ch string) error {
	e.subsMu.Lock()
	defer e.subsMu.Unlock()
	if sub, ok := e.subs[e.clientChannel(ch)]; ok {
		sub.Unsubscribe()
		delete(e.subs, e.clientChannel(ch))
	}
	return nil
}

// AddPresence - see engine interface description.
func (e *NatsEngine) AddPresence(ch string, uid string, info *centrifuge.ClientInfo, exp time.Duration) error {
	return nil
}

// RemovePresence - see engine interface description.
func (e *NatsEngine) RemovePresence(ch string, uid string) error {
	return nil
}

// Presence - see engine interface description.
func (e *NatsEngine) Presence(ch string) (map[string]*centrifuge.ClientInfo, error) {
	return nil, nil
}

// PresenceStats - see engine interface description.
func (e *NatsEngine) PresenceStats(ch string) (centrifuge.PresenceStats, error) {
	return centrifuge.PresenceStats{}, nil
}

// History - see engine interface description.
func (e *NatsEngine) History(ch string, limit int) ([]*centrifuge.Publication, error) {
	return nil, nil
}

// RecoverHistory - see engine interface description.
func (e *NatsEngine) RecoverHistory(ch string, since *centrifuge.Recovery) ([]*centrifuge.Publication, bool, centrifuge.Recovery, error) {
	return nil, false, centrifuge.Recovery{}, nil
}

// RemoveHistory - see engine interface description.
func (e *NatsEngine) RemoveHistory(ch string) error {
	return nil
}

// Channels - see engine interface description.
func (e *NatsEngine) Channels() ([]string, error) {
	return nil, nil
}
