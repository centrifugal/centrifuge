[![Join the chat at https://t.me/joinchat/ABFVWBE0AhkyyhREoaboXQ](https://img.shields.io/badge/Telegram-Group-blue.svg)](https://t.me/joinchat/ABFVWBE0AhkyyhREoaboXQ)
[![Build Status](https://github.com/centrifugal/centrifuge/workflows/build/badge.svg?branch=master)](https://github.com/centrifugal/centrifuge/actions)
[![codecov.io](https://codecov.io/gh/centrifugal/centrifuge/branch/master/graphs/badge.svg)](https://codecov.io/github/centrifugal/centrifuge?branch=master)
[![GoDoc](https://pkg.go.dev/badge/centrifugal/centrifuge)](https://pkg.go.dev/github.com/centrifugal/centrifuge)

**This library has no v1 release, API still evolves. Use with strict versioning.** Before v1 release patch version updates only have backwards compatible changes and fixes, minor version updates can have backwards-incompatible API changes. For all breaking changes we provide a detailed changelog. Master branch can have unreleased code. Only two last Go minor versions are officially supported by this library.

Centrifuge library is a real-time core of [Centrifugo](https://github.com/centrifugal/centrifugo) server. It's also supposed to be a general purpose real-time messaging library for Go programming language. The library built on top of strict client-server protocol schema and exposes various real-time oriented primitives for a developer. Centrifuge solves several problems a developer may come across when building complex real-time applications – like scalability (millions of connections), proper persistent connection management and invalidation, fast reconnect with message recovery, WebSocket fallback option.

Library highlights:

* Fast and optimized for low-latency communication with millions of client connections. See [test stand with 1 million connections in Kubernetes](https://centrifugal.dev/blog/2020/02/10/million-connections-with-centrifugo)
* Builtin bidirectional transports: WebSocket (JSON or binary Protobuf) and SockJS (JSON only)
* Possibility to use unidirectional transports without using custom Centrifuge client library: see examples for [GRPC](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_grpc), [EventSource(SSE)](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_sse), [Fetch Streams](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_fetch_stream), [Unidirectional WebSocket](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_ws)  
* Built-in horizontal scalability with Redis PUB/SUB, consistent Redis sharding, Sentinel and Redis Cluster for HA
* Native authentication over HTTP middleware or custom token-based
* Channel concept to broadcast message to all active subscribers
* Client-side and server-side channel subscriptions
* Bidirectional asynchronous message communication and RPC calls
* Presence information for channels (show all active clients in a channel)
* History information for channels (ephemeral streams with size and TTL retention)
* Join/leave events for channels (aka client goes online/offline)
* Possibility to register a custom PUB/SUB Broker and Presence Manager implementations
* Message recovery mechanism for channels to survive PUB/SUB delivery problems, short network disconnects or node restart
* Prometheus instrumentation
* Client libraries for main application environments (see below)

For **bidirectional** communication between a client and a Centrifuge-based server we have a bunch of client libraries:

* [centrifuge-js](https://github.com/centrifugal/centrifuge-js) – for a browser, NodeJS and React Native
* [centrifuge-go](https://github.com/centrifugal/centrifuge-go) - for Go language
* [centrifuge-dart](https://github.com/centrifugal/centrifuge-dart) - for Dart and Flutter
* [centrifuge-swift](https://github.com/centrifugal/centrifuge-swift) – for native iOS development
* [centrifuge-java](https://github.com/centrifugal/centrifuge-java) – for native Android development and general Java

If you opt for a **unidirectional** communication then you may leverage Centrifuge possibilities without any specific library on client-side - simply by using native browser API or GRPC-generated code. See examples of unidirectional communication over [GRPC](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_grpc), [EventSource(SSE)](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_sse), [Fetch Streams](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_fetch_stream), [WebSocket](https://github.com/centrifugal/centrifuge/tree/master/_examples/unidirectional_ws).

### Explore Centrifuge

* see Centrifuge [Documentation on pkg.go.dev](https://pkg.go.dev/github.com/centrifugal/centrifuge)
* read [Centrifuge introduction post](https://centrifugal.dev/blog/2021/01/15/centrifuge-intro) in Centrifugo blog   
* you can also consider [Centrifugo server documentation](https://centrifugal.dev/) as extra doc for this package (since Centrifugo is built on top of Centrifuge library, though keep in mind that Centrifugo adds some things on top, Centrifugo source code is also a good place to learn from)
* read this README to the end for [installation](#installation) details, [quick example](#quick-example) and [tips and tricks](#tips-and-tricks) section
* check out [examples](https://github.com/centrifugal/centrifuge/tree/master/_examples) folder

### Installation

To install use:

```bash
go get github.com/centrifugal/centrifuge
```

`go mod` is a recommended way of adding this library to your project dependencies.

### Quick example

Let's take a look on how to build the simplest real-time chat with Centrifuge library. Clients will be able to connect to a server over Websocket, send a message into a channel and this message will be instantly delivered to all active channel subscribers. On a server side we will accept all connections and will work as a simple PUB/SUB proxy without worrying too much about permissions. In this example we will use Centrifuge Javascript client on a frontend.

Create file `main.go` with the following code:

```go
package main

import (
	"log"
	"net/http"

	// Import this library.
	"github.com/centrifugal/centrifuge"
)

// Authentication middleware example. Centrifuge expects Credentials
// with current user ID set. Without provided Credentials client
// connection won't be accepted. Another way to authenticate connection
// is reacting to node.OnConnecting event where you may authenticate
// connection based on a custom token sent by a client in first protocol
// frame. See _examples folder in repo to find real-life auth samples
// (OAuth2, Gin sessions, JWT etc).
func auth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Put authentication Credentials into request Context.
		// Since we don't have any session backend here we simply
		// set user ID as empty string. Users with empty ID called
		// anonymous users, in real app you should decide whether
		// anonymous users allowed to connect to your server or not.
		cred := &centrifuge.Credentials{
			UserID: "",
		}
		newCtx := centrifuge.SetCredentials(ctx, cred)
		r = r.WithContext(newCtx)
		h.ServeHTTP(w, r)
	})
}

func main() {
	// Node is the core object in Centrifuge library responsible for
	// many useful things. For example Node allows publishing messages
	// into channels with its Publish method. Here we initialize Node
	// with Config which has reasonable defaults for zero values.
	node, err := centrifuge.New(centrifuge.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Set ConnectHandler called when client successfully connected to Node.
	// Your code inside a handler must be synchronized since it will be called
	// concurrently from different goroutines (belonging to different client
	// connections). See information about connection life cycle in library readme.
	// This handler should not block – so do minimal work here, set required
	// connection event handlers and return.
	node.OnConnect(func(client *centrifuge.Client) {
		// In our example transport will always be Websocket but it can also be SockJS.
		transportName := client.Transport().Name()
		// In our example clients connect with JSON protocol but it can also be Protobuf.
		transportProto := client.Transport().Protocol()
		log.Printf("client connected via %s (%s)", transportName, transportProto)

		// Set SubscribeHandler to react on every channel subscription attempt
		// initiated by a client. Here you can theoretically return an error or
		// disconnect a client from a server if needed. But here we just accept
		// all subscriptions to all channels. In real life you may use a more
		// complex permission check here. The reason why we use callback style
		// inside client event handlers is that it gives a possibility to control
		// operation concurrency to developer and still control order of events.
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("client subscribes on channel %s", e.Channel)
			cb(centrifuge.SubscribeReply{}, nil)
		})

		// By default, clients can not publish messages into channels. By setting
		// PublishHandler we tell Centrifuge that publish from a client-side is
		// possible. Now each time client calls publish method this handler will be
		// called and you have a possibility to validate publication request. After
		// returning from this handler Publication will be published to a channel and
		// reach active subscribers with at most once delivery guarantee. In our simple
		// chat app we allow everyone to publish into any channel but in real case
		// you may have more validation.
		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("client publishes into channel %s: %s", e.Channel, string(e.Data))
			cb(centrifuge.PublishReply{}, nil)
		})

		// Set Disconnect handler to react on client disconnect events.
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("client disconnected")
		})
	})

	// Run node. This method does not block. See also node.Shutdown method
	// to finish application gracefully.
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Now configure HTTP routes.

	// Serve Websocket connections using WebsocketHandler.
	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})
	http.Handle("/connection/websocket", auth(wsHandler))

	// The second route is for serving index.html file.
	http.Handle("/", http.FileServer(http.Dir("./")))

	log.Printf("Starting server, visit http://localhost:8000")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatal(err)
	}
}
```

Also create file `index.html` near `main.go` with content:

```html
<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <script type="text/javascript" src="https://unpkg.com/centrifuge@3.0.0/dist/centrifuge.js"></script>
        <title>Centrifuge library chat example</title>
    </head>
    <body>
        <input type="text" id="input" />
        <script type="text/javascript">
            function drawText(text) {
                const div = document.createElement('div');
                div.innerHTML = text + '<br>';
                document.body.appendChild(div);
            }
            
            // Create Centrifuge object with Websocket endpoint address set in main.go
            const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');

            centrifuge.on('connected', function(ctx){
                drawText('Connected over ' + ctx.transport);
            });
            
            const sub = centrifuge.newSubscription("chat");
            sub.on('publication', function(ctx) {
                drawText(JSON.stringify(ctx.data));
            });
            // Move subscription to subscribing state.
            sub.subscribe();
            
            const input = document.getElementById("input");
            input.addEventListener('keyup', function(e) {
                if (e.keyCode === 13) {
                    sub.publish(this.value);
                    input.value = '';
                }
            });
            // After setting event handlers – initiate actual connection with server.
            centrifuge.connect();
        </script>
    </body>
</html>
```

Then run as usual:

```bash
go run main.go
```

Open several browser tabs with http://localhost:8000 and see chat in action.

While this example is only the top of an iceberg, it should give you a good insight on library API. Check out [examples](https://github.com/centrifugal/centrifuge/tree/master/_examples) folder for more.

Keep in mind that Centrifuge library is not a framework to build chat applications. It's a **general purpose real-time transport** for your messages with some helpful primitives. You can build many kinds of real-time apps on top of this library including chats but depending on application you may need to write business logic yourself.

### Tips and tricks

Some useful advices about library here.

#### Connection life cycle

Let's describe some aspects related to connection life cycle and event handling in Centrifuge:

* If you set middleware for transport handlers (`WebsocketHandler`, `SockjsHandler`) – then it will be called first before a client sent any command to a server and handler had a chance to start working. Just like a regular HTTP middleware. You can put `Credentials` to `Context` to authenticate connection.
* `node.OnConnecting` called as soon as client sent `Connect` command to server. At this point no `Client` instance exists. You have incoming `Context` and `Transport` information. You still can authenticate Client at this point (based on string token sent from client side or any other way). Also, you can add extra data to context and return modified context to Centrifuge. Context cancelled as soon as client connection closes. This handler is synchronous and connection read loop can't proceed until you return `ConnectReply`. 
* `node.OnConnect` then called (after a reply to `Connect` command already written to connection). Inside `OnConnect` closure you have a possibility to define per-connection event handlers. If particular handler not set then client will get `ErrorNotAvailable` errors requesting it. Remember that none of event handlers available in Centrifuge should block forever – do minimal work, start separate goroutines if you need blocking code.
* Client initiated request handlers called one by one from connection reading goroutine. This includes `OnSubscribe`, `OnPublish`, `OnPresence`, `OnPresenceStats`, `OnHistory`, client-side `OnRefresh`, client-side `OnSubRefresh`.
* Other handlers like `OnAlive`, `OnDisconnect`, server-side `OnSubRefresh`, server-side `OnRefresh` called from separate internal goroutines.
* `OnAlive` handler must not be called after `OnDisconnect`.
* Client initiated request handlers can be processed asynchronously in goroutines to manage operation concurrency. This is achieved using callback functions. See [concurrency](https://github.com/centrifugal/centrifuge/tree/master/_examples/concurrency) example for more details.

#### Channel history stream

Centrifuge `Broker` interface supports saving `Publication` to history stream on publish. Depending on Broker implementation this feature can be missing though. Builtin Memory and Redis brokers support keeping Publication stream.

When using default `MemoryBroker` Publication stream kept in process memory and lost as soon as process restarts. `RedisBroker` keeps Publication stream in Redis LIST or STREAM data structures – reliability inherited from Redis configuration in this case.

Centrifuge library publication stream not meant to be used as the only source of missed Publications for a client. It mostly exists to help many clients reconnect at once (load balancer reload, application deploy) without creating a massive spike in load on your main application database. So application database still required in idiomatic use case.

Centrifuge message recovery protocol feature designed to be used together with reasonably small Publication stream size as all missed publications sent towards client in one protocol frame on resubscribe to channel.

#### Logging

Centrifuge library exposes logs with different log level. In your app you can set special function to handle these log entries in a way you want.

```go
// Function to handle Centrifuge internal logs.
func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

cfg := centrifuge.DefaultConfig
cfg.LogLevel = centrifuge.LogLevelDebug
cfg.LogHandler = handleLog
```
