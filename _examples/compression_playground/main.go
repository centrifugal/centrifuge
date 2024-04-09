package main

import (
	"context"
	"html/template"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge/_examples/compression_playground/apppb"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func simulateMatch(ctx context.Context, num int32, node *centrifuge.Node, useProtobufPayload bool) {
	// Predefined lists of player names for each team.
	playerNamesTeamA := []string{"John Doe", "Jane Smith", "Alex Johnson", "Chris Lee", "Pat Kim", "Sam Morgan", "Jamie Brown", "Casey Davis", "Morgan Garcia", "Taylor White", "Jordan Martinez"}
	playerNamesTeamB := []string{"Robin Wilson", "Drew Taylor", "Jessie Bailey", "Casey Flores", "Jordan Walker", "Charlie Green", "Alex Adams", "Morgan Thompson", "Taylor Clark", "Jordan Hernandez", "Jamie Lewis"}

	// Example setup
	match := &apppb.Match{
		Id: num,
		HomeTeam: &apppb.Team{
			Name:    "Real Madrid",
			Players: assignNamesToPlayers(playerNamesTeamA),
		},
		AwayTeam: &apppb.Team{
			Name:    "Barcelona",
			Players: assignNamesToPlayers(playerNamesTeamB),
		},
	}

	totalSimulationTime := 1                                             // Total time for the simulation in seconds
	totalEvents := 20                                                    // Total number of events to simulate
	eventInterval := float64(totalSimulationTime) / float64(totalEvents) // Time between events

	r := rand.New(rand.NewSource(27))

	for i := 0; i < totalEvents; i++ {
		// Sleep between events
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(eventInterval*1000) * time.Millisecond):
		}

		// Calculate minute based on event occurrence.
		minute := int(float64(i) * eventInterval / float64(totalSimulationTime) * 90)
		eventType := chooseRandomEventType(r)
		team := chooseRandomTeam(r, match)
		playerIndex := r.Intn(11) // Choose one of the 11 players randomly

		event := &apppb.Event{Type: eventType, Minute: int32(minute)}
		team.Players[playerIndex].Events = append(team.Players[playerIndex].Events, event)

		if eventType == apppb.EventType_GOAL {
			team.Score++
		}

		var data []byte
		var err error

		if useProtobufPayload {
			data, err = proto.Marshal(match)
		} else {
			data, err = protojson.MarshalOptions{
				UseProtoNames: false,
			}.Marshal(match)
		}
		if err != nil {
			log.Fatal(err)

		}
		ch := "match:js:1"
		if useProtobufPayload {
			ch = "match:pb:1"
		}
		_, err = node.Publish(
			ch, data,
			centrifuge.WithDelta(true),
			centrifuge.WithHistory(10, time.Minute),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func chooseRandomEventType(r *rand.Rand) apppb.EventType {
	events := []apppb.EventType{
		apppb.EventType_GOAL, apppb.EventType_YELLOW_CARD, apppb.EventType_RED_CARD, apppb.EventType_SUBSTITUTE}
	return events[r.Intn(len(events))]
}

func chooseRandomTeam(r *rand.Rand, match *apppb.Match) *apppb.Team {
	if r.Intn(2) == 0 {
		return match.HomeTeam
	}
	return match.AwayTeam
}

// Helper function to create players with names from a given list
func assignNamesToPlayers(names []string) []*apppb.Player {
	var players [11]*apppb.Player
	for i, name := range names {
		players[i] = &apppb.Player{Name: name}
	}
	return players[:]
}

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
			Info:   []byte(r.URL.RawQuery), // This is a hack for the playground.
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
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel: centrifuge.LogLevelDebug,
		LogHandler: func(entry centrifuge.LogEntry) {
			log.Println(entry.Message, entry.Fields)
		},
		AllowedDeltaTypes: []centrifuge.DeltaType{centrifuge.DeltaTypeFossil},
		GetChannelCacheOptions: func(channel string) (centrifuge.ChannelCacheOptions, bool) {
			return centrifuge.ChannelCacheOptions{
				Delay: 400 * time.Millisecond,
			}, true
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		cred, _ := centrifuge.GetCredentials(ctx)
		reply := centrifuge.ConnectReply{}
		if strings.Contains(string(cred.Info), "delay") {
			reply.MaxMessagesInFrame = -1
			reply.WriteDelay = 200 * time.Millisecond
			reply.ReplyWithoutQueue = true
		}
		return reply, nil
	})

	// Set ConnectHandler called when client successfully connected to Node.
	// Your code inside a handler must be synchronized since it will be called
	// concurrently from different goroutines (belonging to different client
	// connections). See information about connection life cycle in library readme.
	// This handler should not block – so do minimal work here, set required
	// connection event handlers and return.
	node.OnConnect(func(client *centrifuge.Client) {
		// In our example transport will always be Websocket but it can be different.
		transportName := client.Transport().Name()
		// In our example clients connect with JSON protocol but it can also be Protobuf.
		transportProto := client.Transport().Protocol()
		log.Printf("client connected via %s (%s)", transportName, transportProto)

		var useProtobufPayload bool
		if strings.Contains(string(client.Info()), "protobuf") {
			useProtobufPayload = true
		}

		go func() {
			log.Printf("using protobuf payload: %v", useProtobufPayload)
			simulateMatch(client.Context(), 0, node, useProtobufPayload)
		}()

		//client.OnCacheEmpty(func(event centrifuge.CacheEmptyEvent) centrifuge.CacheEmptyReply {
		//	simulateMatch(context.Background(), 0, node)
		//	//go func() {
		//	//	num := 0
		//	//	for {
		//	//
		//	//		num++
		//	//		time.Sleep(5 * time.Second)
		//	//	}
		//	//}()
		//	fmt.Println("simulated")
		//	return centrifuge.CacheEmptyReply{Populated: true}
		//})

		// Set SubscribeHandler to react on every channel subscription attempt
		// initiated by a client. Here you can theoretically return an error or
		// disconnect a client from a server if needed. But here we just accept
		// all subscriptions to all channels. In real life you may use a more
		// complex permission check here. The reason why we use callback style
		// inside client event handlers is that it gives a possibility to control
		// operation concurrency to developer and still control order of events.
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("client subscribes on channel %s", e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableRecovery: true,
					RecoveryMode:   centrifuge.RecoveryModeCache,
				},
			}, nil)
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
			log.Printf("client disconnected: %d (%s)", e.Code, e.Reason)
		})
	})

	// Run node. This method does not block. See also node.Shutdown method
	// to finish application gracefully.
	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			simulateMatch(context.Background(), 0, node, false)
		}
	}()

	// Now configure HTTP routes.

	http.Handle("/connection/websocket/no_compression", auth(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))

	http.Handle("/connection/websocket/with_compression", auth(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		Compression:        true,
		CompressionMinSize: 1,
		CompressionLevel:   1,
	})))

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/json", serveJsonApp)
	http.HandleFunc("/protobuf", serveProtobufApp)

	// Serve static files from the /static folder
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	log.Printf("Starting server, visit http://localhost:8000")
	if err := http.ListenAndServe("127.0.0.1:8000", nil); err != nil {
		log.Fatal(err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("templates/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = t.Execute(w, nil)
}

func serveJsonApp(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("templates/json.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = t.Execute(w, nil)
}

func serveProtobufApp(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("templates/protobuf.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = t.Execute(w, nil)
}
