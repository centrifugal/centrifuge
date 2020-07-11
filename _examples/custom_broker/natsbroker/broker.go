// Package natsbroker defines custom Nats Broker for Centrifuge library.
package natsbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/centrifugal/centrifuge"
	"github.com/nats-io/nats.go"
)

type (
	// channelID is unique channel identifier in Nats.
	channelID string
)

// Config of NatsEngine.
type Config struct {
	Servers string
	Prefix  string
}

// NatsBroker is a broker on top of Nats messaging system.
type NatsBroker struct {
	node   *centrifuge.Node
	config Config

	nc           *nats.Conn
	subsMu       sync.Mutex
	subs         map[channelID]*nats.Subscription
	eventHandler centrifuge.BrokerEventHandler
}

// New creates NatsEngine.
func New(n *centrifuge.Node, conf Config) (*NatsBroker, error) {
	b := &NatsBroker{
		node:   n,
		config: conf,
		subs:   make(map[channelID]*nats.Subscription),
	}
	return b, nil
}

func (b *NatsBroker) controlChannel() channelID {
	return channelID(b.config.Prefix + ".control")
}

func (b *NatsBroker) clientChannel(ch string) channelID {
	return channelID(b.config.Prefix + ".client." + ch)
}

func (b *NatsBroker) extractChannel(subject string) string {
	return strings.TrimPrefix(subject, b.config.Prefix+".client.")
}

// Run runs engine after node initialized.
func (b *NatsBroker) Run(h centrifuge.BrokerEventHandler) error {
	b.eventHandler = h
	servers := b.config.Servers
	if servers == "" {
		servers = nats.DefaultURL
	}
	nc, err := nats.Connect(servers, nats.ReconnectBufSize(-1), nats.MaxReconnects(math.MaxInt64))
	if err != nil {
		return err
	}
	_, err = nc.Subscribe(string(b.controlChannel()), b.handleControl)
	if err != nil {
		return err
	}
	b.nc = nc
	b.node.Log(centrifuge.NewLogEntry(centrifuge.LogLevelInfo, fmt.Sprintf("Nats Broker connected to: %s", servers)))
	return nil
}

// Close is not implemented.
func (b *NatsBroker) Close(_ context.Context) error {
	return nil
}

type pushType int

const (
	pubPushType   pushType = 0
	joinPushType  pushType = 1
	leavePushType pushType = 2
)

type push struct {
	Type pushType        `json:"type,omitempty"`
	Data json.RawMessage `json:"data"`
}

// Publish - see Engine interface description.
func (b *NatsBroker) Publish(ch string, pub *centrifuge.Publication, _ *centrifuge.ChannelOptions) error {
	data, err := json.Marshal(pub)
	if err != nil {
		return err
	}
	byteMessage, err := json.Marshal(push{
		Type: pubPushType,
		Data: data,
	})
	if err != nil {
		return err
	}
	return b.nc.Publish(string(b.clientChannel(ch)), byteMessage)
}

// PublishJoin - see Engine interface description.
func (b *NatsBroker) PublishJoin(ch string, info *centrifuge.ClientInfo, _ *centrifuge.ChannelOptions) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	byteMessage, err := json.Marshal(push{
		Type: joinPushType,
		Data: data,
	})
	if err != nil {
		return err
	}
	return b.nc.Publish(string(b.clientChannel(ch)), byteMessage)
}

// PublishLeave - see Engine interface description.
func (b *NatsBroker) PublishLeave(ch string, info *centrifuge.ClientInfo, _ *centrifuge.ChannelOptions) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	byteMessage, err := json.Marshal(push{
		Type: leavePushType,
		Data: data,
	})
	if err != nil {
		return err
	}
	return b.nc.Publish(string(b.clientChannel(ch)), byteMessage)
}

// PublishControl - see Engine interface description.
func (b *NatsBroker) PublishControl(data []byte) error {
	return b.nc.Publish(string(b.controlChannel()), data)
}

func (b *NatsBroker) handleClientMessage(subject string, data []byte) error {
	var p push
	err := json.Unmarshal(data, &p)
	if err != nil {
		return err
	}
	channel := b.extractChannel(subject)
	switch p.Type {
	case pubPushType:
		var pub centrifuge.Publication
		err := json.Unmarshal(p.Data, &pub)
		if err != nil {
			return err
		}
		_ = b.eventHandler.HandlePublication(channel, &pub)
	case joinPushType:
		var info centrifuge.ClientInfo
		err := json.Unmarshal(p.Data, &info)
		if err != nil {
			return err
		}
		_ = b.eventHandler.HandleJoin(channel, &info)
	case leavePushType:
		var info centrifuge.ClientInfo
		err := json.Unmarshal(p.Data, &info)
		if err != nil {
			return err
		}
		_ = b.eventHandler.HandleLeave(channel, &info)
	default:
	}
	return nil
}

func (b *NatsBroker) handleClient(m *nats.Msg) {
	_ = b.handleClientMessage(m.Subject, m.Data)
}

func (b *NatsBroker) handleControl(m *nats.Msg) {
	_ = b.eventHandler.HandleControl(m.Data)
}

// Subscribe - see Engine interface description.
func (b *NatsBroker) Subscribe(ch string) error {
	if strings.Contains(ch, "*") || strings.Contains(ch, ">") {
		// Do not support wildcard subscriptions.
		return centrifuge.ErrorBadRequest
	}
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	clientChannel := b.clientChannel(ch)
	if _, ok := b.subs[clientChannel]; ok {
		return nil
	}
	subClient, err := b.nc.Subscribe(string(b.clientChannel(ch)), b.handleClient)
	if err != nil {
		return err
	}
	b.subs[clientChannel] = subClient
	return nil
}

// Unsubscribe - see Engine interface description.
func (b *NatsBroker) Unsubscribe(ch string) error {
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	if sub, ok := b.subs[b.clientChannel(ch)]; ok {
		_ = sub.Unsubscribe()
		delete(b.subs, b.clientChannel(ch))
	}
	return nil
}

// Channels - see Engine interface description.
func (b *NatsBroker) Channels() ([]string, error) {
	return nil, nil
}
