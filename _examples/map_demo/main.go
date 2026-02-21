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
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQL pool for native leaderboard operations.
var pgPool *pgxpool.Pool

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

var (
	port         = flag.String("port", "3000", "HTTP server port")
	redisAddr    = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory broker.")
	postgresAddr = flag.String("postgres", "", "PostgreSQL connection string (e.g., postgres://user:pass@localhost:5432/db?sslmode=disable)")
	enableCache  = flag.Bool("cache", false, "Enable memory cache layer for Redis/Postgres brokers (provides read-your-own-writes and low-latency reads)")
	replicas     = flag.String("replicas", "", "Comma-separated PostgreSQL read replica connection strings")
)

func main() {
	flag.Parse()

	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
		// Map subscription sync protocol configs.
		MapMinStreamPaginationLimit:  100,  // Minimum limit for stream pagination to prevent excessive round trips.
		MapMaxImmediateJoinStateSize: 1000, // Max state entries for immediate join (Scenario B).
		MapStateToLiveEnabled:        true,
		// Configure channel options per channel.
		// Each channel must specify SyncMode and RetentionMode explicitly.
		GetMapChannelOptions: func(channel string) centrifuge.MapChannelOptions {
			if channel == "inventory" || channel == "leaderboard" {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncConverging,
					RetentionMode: centrifuge.MapRetentionPermanent,
				}
			}
			if channel == "scoreboard" {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncConverging,
					RetentionMode: centrifuge.MapRetentionPermanent,
				}
			}
			if channel == "board" {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncConverging,
					RetentionMode: centrifuge.MapRetentionPermanent,
					Ordered:       true,
				}
			}
			// Poll channels need streams for proper state recovery after reconnect.
			if strings.HasPrefix(channel, "poll:") {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncConverging,
					RetentionMode: centrifuge.MapRetentionPermanent,
				}
			}
			if channel == "visualizer" {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncConverging,
					RetentionMode: centrifuge.MapRetentionPermanent,
				}
			}
			if strings.HasPrefix(channel, "clients:") {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncEphemeral,
					RetentionMode: centrifuge.MapRetentionExpiring,
					KeyTTL:        60 * time.Second,
				}
			}
			if strings.HasPrefix(channel, "users:") {
				return centrifuge.MapChannelOptions{
					SyncMode:      centrifuge.MapSyncEphemeral,
					RetentionMode: centrifuge.MapRetentionExpiring,
					KeyTTL:        60 * time.Second,
				}
			}
			// Cursors, games — all ephemeral with expiring keys (default).
			return centrifuge.MapChannelOptions{
				SyncMode:      centrifuge.MapSyncEphemeral,
				RetentionMode: centrifuge.MapRetentionExpiring,
				KeyTTL:        1 * time.Minute,
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Set up map broker (memory, Redis, or PostgreSQL based on flags).
	// When -cache flag is set, wraps Redis/Postgres brokers with CachedMapBroker.
	mapBroker, err := setupMapBroker(node, *redisAddr, *postgresAddr, *replicas, *enableCache)
	if err != nil {
		log.Fatal(err)
	}
	// Wrap with debouncing for cursors channel — coalesces rapid cursor updates
	// on the server side before forwarding to the backend.
	mapBroker = centrifuge.NewDebouncingMapBroker(node, mapBroker, centrifuge.DebouncingMapBrokerConfig{
		Debounce: func(channel string) time.Duration {
			if channel == "cursors" {
				return 200 * time.Millisecond
			}
			return 0
		},
	})
	node.SetMapBroker(mapBroker)

	// Set up PostgreSQL pool for native leaderboard operations.
	if *postgresAddr != "" {
		pool, err := pgxpool.New(context.Background(), *postgresAddr)
		if err != nil {
			log.Fatal(err)
		}
		pgPool = pool
		log.Printf("PostgreSQL pool initialized for native leaderboard and polls operations")
	}

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
				opts.MapClientPresenceChannelPrefix = "clients:"
				opts.MapUserPresenceChannelPrefix = "users:"
			}

			// Enable automatic cleanup for cursors channel - removes key=clientID on unsubscribe/disconnect.
			if e.Channel == "cursors" {
				opts.MapRemoveOnUnsubscribe = true
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
			// Removal is handled automatically via MapRemoveOnUnsubscribe.
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
			// Cursor cleanup is now automatic via MapRemoveOnUnsubscribe option.
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
	go publishScoreboardData(node)

	// Start poll manager goroutine.
	go runPollManager()

	// Serve static files.
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// WebSocket handler.
	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})
	http.Handle("/connection/websocket", wsHandler)

	// Native PostgreSQL leaderboard HTTP endpoints.
	http.HandleFunc("/api/leaderboard/join", handleLeaderboardJoinHTTP)
	http.HandleFunc("/api/leaderboard/click", handleLeaderboardClickHTTP)
	http.HandleFunc("/api/leaderboard/leave", handleLeaderboardLeaveHTTP)
	http.HandleFunc("/api/poll/vote", handlePollVoteHTTP)
	http.HandleFunc("/api/board/create", handleBoardCreateHTTP)
	http.HandleFunc("/api/board/update", handleBoardUpdateHTTP)
	http.HandleFunc("/api/board/delete", handleBoardDeleteHTTP)

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
		log.Printf("  - Leaderboard:     http://localhost:%s/leaderboard.html", *port)
		log.Printf("  - Inventory demo:  http://localhost:%s/inventory.html", *port)
		log.Printf("  - Tickers demo:    http://localhost:%s/tickers.html", *port)
		log.Printf("  - Live Polls demo: http://localhost:%s/polls.html", *port)
		log.Printf("  - Sprint Board:    http://localhost:%s/board.html", *port)
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
	if pgPool != nil {
		pgPool.Close()
	}
}
