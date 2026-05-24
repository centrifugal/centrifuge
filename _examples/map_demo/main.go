package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
)

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

var (
	port      = flag.String("port", "3000", "HTTP server port")
	redisAddr = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory broker.")
)

func main() {
	flag.Parse()

	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
		GetChannelMediumOptions: func(channel string) centrifuge.ChannelMediumOptions {
			return centrifuge.ChannelMediumOptions{
				SharedPositionSync: true,
			}
		},
		Map: centrifuge.MapConfig{
			// Configure channel options per channel.
			// Each channel must specify Mode explicitly.
			GetMapChannelOptions: func(channel string) centrifuge.MapChannelOptions {
				if channel == "inventory" {
					return centrifuge.MapChannelOptions{
						Mode: centrifuge.MapModePersistent,
					}
				}
				if channel == "scoreboard" {
					return centrifuge.MapChannelOptions{
						Mode: centrifuge.MapModePersistent,
					}
				}
				if channel == "visualizer" {
					return centrifuge.MapChannelOptions{
						Mode:               centrifuge.MapModePersistent,
						MinPageSize: 1, // Set to 1 for demo purposes, default is 100.
					}
				}
				if strings.HasPrefix(channel, "clients:") {
					return centrifuge.MapChannelOptions{
						Mode:   centrifuge.MapModeEphemeral,
						KeyTTL: 60 * time.Second,
					}
				}
				if strings.HasPrefix(channel, "users:") {
					return centrifuge.MapChannelOptions{
						Mode:   centrifuge.MapModeEphemeral,
						KeyTTL: 60 * time.Second,
					}
				}
				// Cursors, games, tickers — all ephemeral (default).
				return centrifuge.MapChannelOptions{
					Mode:   centrifuge.MapModeEphemeral,
					KeyTTL: 1 * time.Minute,
				}
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Set up map broker (memory or Redis based on flags).
	mapBroker, err := setupMapBroker(node, *redisAddr)
	if err != nil {
		log.Fatal(err)
	}
	node.SetMapBroker(mapBroker)

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// Use client name as user ID for simplicity.
		userID := e.Name
		if userID == "" {
			userID = "anonymous"
		}
		return centrifuge.ConnectReply{
			Credentials: &centrifuge.Credentials{
				UserID: userID,
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		log.Printf("client connected: %s (user: %s)", client.ID(), client.UserID())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("client %s subscribing to %s (type: %v)", client.ID(), e.Channel, e.Type)
			if e.Type == centrifuge.SubscriptionTypeStream {
				// We expect only map subscriptions here.
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}

			opts := centrifuge.SubscribeOptions{
				Type: e.Type,
			}

			// Enable delta compression for scoreboard channel.
			if e.Channel == "scoreboard" {
				opts.AllowedDeltaTypes = []centrifuge.DeltaType{centrifuge.DeltaTypeFossil}
			}

			// Enable map presence for games and individual game channels.
			if e.Channel == "games" || strings.HasPrefix(e.Channel, "game:") {
				opts.MapClientPresenceChannel = "clients:" + e.Channel
				opts.MapUserPresenceChannel = "users:" + e.Channel
			}

			// Enable automatic cleanup for cursors channel - removes key=clientID on unsubscribe/disconnect.
			if e.Channel == "cursors" {
				opts.MapRemoveClientOnUnsubscribe = true
			}

			// Tickers channel: enable tags filter for sector-based filtering.
			if e.Channel == "tickers" {
				opts.AllowTagsFilter = true
			}

			cb(centrifuge.SubscribeReply{Options: opts}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("client %s unsubscribed from channel %s", client.ID(), e.Channel)
		})

		client.OnMapPublish(func(e centrifuge.MapPublishEvent, cb centrifuge.MapPublishCallback) {
			// For cursors channel: server assigns key to client ID, auto-cleanup TTL.
			// Removal is handled automatically via MapRemoveClientOnUnsubscribe.
			if e.Channel == "cursors" {
				cb(centrifuge.MapPublishReply{
					Key: client.ID(),
				}, nil)
				return
			}
			cb(centrifuge.MapPublishReply{}, centrifuge.ErrorPermissionDenied)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			switch e.Method {
			case "game:create":
				handleGameCreate(client, node, e.Data, cb)
			case "game:join":
				handleGameJoin(client, node, e.Data, cb)
			case "game:leave":
				handleGameLeave(client, node, e.Data, cb)
			case "inventory:buy":
				handleInventoryBuy(client, node, e.Data, cb)
			case "inventory:restock":
				handleInventoryRestock(client, node, e.Data, cb)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("client disconnected: %s", client.ID())
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Initialize inventory items on startup.
	initInventory(node)

	// Start publishing ticker data every second.
	go publishTickerData(node)

	// Start publishing scoreboard data (6 live matches with delta compression).
	go publishScoreboardData(context.Background(), node)

	// Serve static files.
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// WebSocket handler.
	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})
	http.Handle("/connection/websocket", wsHandler)

	http.HandleFunc("/api/viz/populate", handleVizPopulate(node))
	http.HandleFunc("/api/viz/publish", handleVizPublish(node))
	http.HandleFunc("/api/viz/remove", handleVizRemove(node))
	http.HandleFunc("/api/viz/clear", handleVizClear(node))
	http.HandleFunc("/api/viz/stats", handleVizStats(node))

	server := &http.Server{Addr: ":" + *port}

	go func() {
		log.Printf("Starting server on http://localhost:%s", *port)
		log.Printf("  - Cursors demo:    http://localhost:%s/cursors.html", *port)
		log.Printf("  - Lobby demo:      http://localhost:%s/lobby.html", *port)
		log.Printf("  - Inventory demo:  http://localhost:%s/inventory.html", *port)
		log.Printf("  - Tickers demo:    http://localhost:%s/tickers.html", *port)
		log.Printf("  - Scoreboard:      http://localhost:%s/scoreboard.html", *port)
		log.Printf("  - Visualizer:      http://localhost:%s/visualizer.html", *port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	// Wait for interrupt signal.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	_ = node.Shutdown(ctx)
}
