package centrifuge

import (
	"context"
)

// ConnectEvent contains fields related to connecting event.
type ConnectEvent struct {
	// ClientID that was generated by library for client connection.
	ClientID string
	// Token received from client as part of Connect Command.
	Token string
	// Data received from client as part of Connect Command.
	Data []byte
	// Transport contains information about transport used by client.
	Transport TransportInfo
}

// ConnectReply contains fields determining the reaction on auth event.
type ConnectReply struct {
	// Context allows to return modified context.
	Context context.Context
	// Credentials should be set if app wants to authenticate connection.
	// This field still optional as auth could be provided through HTTP
	// middleware or via JWT token.
	Credentials *Credentials
	// Data allows to set custom data in connect reply.
	Data []byte
	// Channels slice contains channels to subscribe connection to on server-side.
	Channels []string
	// ClientSideRefresh tells library to use client-side refresh logic:
	// i.e. send refresh commands with new connection token. If not set
	// then server-side refresh mechanism will be used.
	ClientSideRefresh bool
	// Events mask to be called for connection. Zero value means all events for
	// all client event handlers set to Node.
	Events Event
}

// ConnectingHandler called when new client authenticates on server.
type ConnectingHandler func(context.Context, ConnectEvent) (ConnectReply, error)

// ConnectHandler called when client connected to server and ready to communicate.
type ConnectHandler func(*Client)

// RefreshEvent contains fields related to refresh event.
type RefreshEvent struct {
	// ClientSideRefresh is true for refresh initiated by client-side refresh workflow.
	ClientSideRefresh bool
	// Token will only be set in case of using client-side refresh mechanism.
	Token string
}

// RefreshReply contains fields determining the reaction on refresh event.
type RefreshReply struct {
	// Expired tells Centrifuge that connection expired. In this case connection will be
	// closed with DisconnectExpired.
	Expired bool
	// ExpireAt defines time in future when connection should expire,
	// zero value means no expiration.
	ExpireAt int64
	// Info allows to modify connection information,
	// zero value means no modification of current connection Info.
	Info []byte
}

// RefreshHandler called when it's time to validate client connection and
// update it's expiration time if it's still actual.
//
// Centrifuge library supports two ways of refreshing connection: client-side
// and server-side.
//
// The default mechanism is server-side, this means that as soon refresh handler
// set and connection expiration time happens (by timer) – refresh handler will
// be called.
//
// If ClientSideRefresh in ConnectReply inside ConnectingHandler set to true then
// library uses client-side refresh mechanism. In this case library relies on
// Refresh commands sent from client periodically to refresh connection. Refresh
// command contains updated connection token.
type RefreshHandler func(*Client, RefreshEvent) (RefreshReply, error)

// AliveHandler called periodically while connection alive. This is a helper
// to do periodic things which can tolerate some approximation in time. This
// callback will run every ClientPresenceUpdateInterval and can save you a timer.
type AliveHandler func(*Client)

// DisconnectEvent contains fields related to disconnect event.
type DisconnectEvent struct {
	// Disconnect can optionally contain a custom disconnect object that
	// was sent from server to client with closing handshake. If this field
	// exists then client connection was closed from server. If this field
	// is nil then this means that client disconnected normally and connection
	// closing was initiated by client side.
	Disconnect *Disconnect
}

// DisconnectHandler called when client disconnects from server. The important
// thing to remember is that you should not rely entirely on this handler to
// clean up non-expiring resources (in your database for example). Why? Because
// in case of any non-graceful node shutdown (kill -9, process crash, machine lost)
// disconnect handler will never be called (obviously) so you can have stale data.
type DisconnectHandler func(*Client, DisconnectEvent)

// SubscribeEvent contains fields related to subscribe event.
type SubscribeEvent struct {
	// Channel client wants to subscribe to.
	Channel string
	// Token will only be set for token channels. This is a task of application
	// to check that subscription to a channel has valid token.
	Token string
}

// SubscribeReply contains fields determining the reaction on subscribe event.
type SubscribeReply struct {
	// ExpireAt defines time in future when subscription should expire,
	// zero value means no expiration.
	ExpireAt int64
	// ChannelInfo defines custom channel information, zero value means no channel information.
	ChannelInfo []byte
	// ClientSideRefresh tells library to use client-side refresh logic: i.e. send
	// SubRefresh commands with new Subscription Token. If not set then server-side
	// SubRefresh handler will be used.
	ClientSideRefresh bool
}

// SubscribeHandler called when client wants to subscribe on channel.
type SubscribeHandler func(*Client, SubscribeEvent) (SubscribeReply, error)

// UnsubscribeEvent contains fields related to unsubscribe event.
type UnsubscribeEvent struct {
	// Channel client unsubscribed from.
	Channel string
}

// UnsubscribeHandler called when client unsubscribed from channel.
type UnsubscribeHandler func(*Client, UnsubscribeEvent)

// PublishEvent contains fields related to publish event. Note that this event
// called before actual publish to Engine so handler has an option to reject this
// publication returning an error.
type PublishEvent struct {
	// Channel client wants to publish data to.
	Channel string
	// Data client wants to publish.
	Data []byte
	// Info about client connection.
	Info *ClientInfo
}

// PublishReply contains fields determining the result on publish.
type PublishReply struct {
	// Result if set will tell Centrifuge that message already published to
	// channel by handler code. In this case Centrifuge won't try to publish
	// into channel again after handler returned PublishReply. This can be
	// useful if you need to know new Publication offset in your code or you
	// want to make sure message successfully published to Engine on server
	// side (otherwise only client will get an error).
	Result *PublishResult
}

// PublishHandler called when client publishes into channel.
type PublishHandler func(*Client, PublishEvent) (PublishReply, error)

// SubRefreshEvent contains fields related to subscription refresh event.
type SubRefreshEvent struct {
	// ClientSideRefresh is true for refresh initiated by client-side subscription
	// refresh workflow.
	ClientSideRefresh bool
	// Channel to which SubRefreshEvent belongs to.
	Channel string
	// Token will only be set in case of using client-side subscription refresh mechanism.
	Token string
}

// SubRefreshReply contains fields determining the reaction on
// subscription refresh event.
type SubRefreshReply struct {
	// Expired tells Centrifuge that subscription expired. In this case connection will be
	// closed with DisconnectExpired.
	Expired bool
	// ExpireAt is a new Unix time of expiration. Zero value means no expiration.
	ExpireAt int64
	// Info is a new channel-scope info. Zero value means do not change previous one.
	Info []byte
}

// SubRefreshHandler called when it's time to validate client subscription to channel and
// update it's state if needed.
//
// If ClientSideRefresh in SubscribeReply inside SubscribeHandler set to true then
// library uses client-side subscription refresh mechanism. In this case library relies on
// SubRefresh commands sent from client periodically to refresh subscription. SubRefresh
// command contains updated subscription token.
type SubRefreshHandler func(*Client, SubRefreshEvent) (SubRefreshReply, error)

// RPCEvent contains fields related to rpc request.
type RPCEvent struct {
	// Method is an optional string that contains RPC method name client wants to call.
	// This is an optional field, by default clients send RPC without any method set.
	Method string
	// Data contains RPC untouched payload.
	Data []byte
}

// RPCReply contains fields determining the reaction on rpc request.
type RPCReply struct {
	// Data to return in RPC reply to client.
	Data []byte
}

// RPCHandler must handle incoming command from client.
type RPCHandler func(*Client, RPCEvent) (RPCReply, error)

// MessageEvent contains fields related to message request.
type MessageEvent struct {
	// Data contains message untouched payload.
	Data []byte
}

// MessageHandler must handle incoming async message from client.
type MessageHandler func(*Client, MessageEvent)

// PresenceEvent has channel operation called for.
type PresenceEvent struct {
	Channel string
}

// PresenceReply contains fields determining the reaction on presence request.
type PresenceReply struct{}

// PresenceHandler called when presence request received from client.
type PresenceHandler func(*Client, PresenceEvent) (PresenceReply, error)

// PresenceStatsEvent has channel operation called for.
type PresenceStatsEvent struct {
	Channel string
}

// PresenceStatsReply contains fields determining the reaction on presence request.
type PresenceStatsReply struct{}

// PresenceStatsHandler must handle incoming command from client.
type PresenceStatsHandler func(*Client, PresenceStatsEvent) (PresenceStatsReply, error)

// HistoryEvent has channel operation called for.
type HistoryEvent struct {
	Channel string
}

// HistoryReply contains fields determining the reaction on history request.
type HistoryReply struct{}

// HistoryHandler must handle incoming command from client.
type HistoryHandler func(*Client, HistoryEvent) (HistoryReply, error)
