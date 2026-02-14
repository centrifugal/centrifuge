package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
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

type LeaderboardEntry struct {
	UserID string `json:"userId"`
	Name   string `json:"name"`
	Score  int64  `json:"score"`
	Color  string `json:"color"`
}

type GameInfo struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	CreatedBy  string    `json:"createdBy"`
	CreatedAt  time.Time `json:"createdAt"`
	MaxPlayers int       `json:"maxPlayers"`
}

type GamePlayer struct {
	UserID   string `json:"userId"`
	Name     string `json:"name"`
	ClientID string `json:"clientId"`
	Slot     int    `json:"slot"`
}

// Inventory items (in-memory for demo).
var inventoryItems = map[string]*InventoryItem{
	"golden_ticket": {ID: "golden_ticket", Name: "Golden Ticket", Price: 100, Stock: 3, Emoji: "🎫"},
	"rare_potion":   {ID: "rare_potion", Name: "Rare Potion", Price: 50, Stock: 5, Emoji: "🧪"},
	"dragon_egg":    {ID: "dragon_egg", Name: "Dragon Egg", Price: 500, Stock: 1, Emoji: "🥚"},
}

type InventoryItem struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
	Stock int    `json:"stock"`
	Emoji string `json:"emoji"`
}

// Poll types for live polls demo — PG-native with cf_map_publish.
// Uses three channels:
//   - "poll:meta"    — poll question, options, timing (key = pollId)
//   - "poll:results" — vote counts per option (key = optionId, score = votes)
//   - "poll:votes"   — per-user vote dedup (key = userId, KeyModeIfNew)
type PollMeta struct {
	PollID    string          `json:"pollId"`
	Question  string          `json:"question"`
	Options   []PollOptionDef `json:"options"`
	StartTime int64           `json:"startTime"`
	EndTime   int64           `json:"endTime"`
	Status    string          `json:"status"` // "active" or "closed"
}

// PollOptionDef defines a poll option's display properties.
type PollOptionDef struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Color string `json:"color"`
}

// pollQuestions is a pool of fun dev-themed poll questions.
var pollQuestions = []struct {
	Question string
	Options  []PollOptionDef
}{
	{
		Question: "Best programming language?",
		Options: []PollOptionDef{
			{ID: "go", Label: "Go", Color: "#00D9FF"},
			{ID: "rust", Label: "Rust", Color: "#FF8C42"},
			{ID: "python", Label: "Python", Color: "#4B8BBE"},
			{ID: "typescript", Label: "TypeScript", Color: "#3178C6"},
		},
	},
	{
		Question: "Preferred code editor?",
		Options: []PollOptionDef{
			{ID: "vscode", Label: "VS Code", Color: "#0098FF"},
			{ID: "vim", Label: "Vim/Neovim", Color: "#00C853"},
			{ID: "jetbrains", Label: "JetBrains", Color: "#FF4081"},
			{ID: "zed", Label: "Zed", Color: "#FFB74D"},
		},
	},
	{
		Question: "Favorite database?",
		Options: []PollOptionDef{
			{ID: "postgres", Label: "PostgreSQL", Color: "#4A90E2"},
			{ID: "redis", Label: "Redis", Color: "#EF5350"},
			{ID: "sqlite", Label: "SQLite", Color: "#42A5F5"},
			{ID: "mongo", Label: "MongoDB", Color: "#66BB6A"},
		},
	},
	{
		Question: "Preferred deployment target?",
		Options: []PollOptionDef{
			{ID: "k8s", Label: "Kubernetes", Color: "#4A90E2"},
			{ID: "serverless", Label: "Serverless", Color: "#FF9800"},
			{ID: "vps", Label: "Plain VPS", Color: "#9E9E9E"},
			{ID: "docker", Label: "Docker Compose", Color: "#29B6F6"},
		},
	},
	{
		Question: "Tabs or spaces?",
		Options: []PollOptionDef{
			{ID: "tabs", Label: "Tabs", Color: "#EC407A"},
			{ID: "spaces2", Label: "2 Spaces", Color: "#AB47BC"},
			{ID: "spaces4", Label: "4 Spaces", Color: "#7E57C2"},
			{ID: "mixed", Label: "Mixed chaos", Color: "#FF7043"},
		},
	},
	{
		Question: "Best time to code?",
		Options: []PollOptionDef{
			{ID: "morning", Label: "Early morning", Color: "#FFA726"},
			{ID: "afternoon", Label: "Afternoon", Color: "#66BB6A"},
			{ID: "night", Label: "Late night", Color: "#5C6BC0"},
			{ID: "allday", Label: "All day!", Color: "#EF5350"},
		},
	},
}

type InventoryTransaction struct {
	Action   string `json:"action"` // "purchase" or "restock"
	ItemID   string `json:"itemId"`
	Quantity int    `json:"quantity"`
	BuyerID  string `json:"buyerId,omitempty"`
	Message  string `json:"message"`
}

type InventoryPayload struct {
	Item        InventoryItem         `json:"item"`
	Transaction *InventoryTransaction `json:"transaction,omitempty"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

var (
	port         = flag.String("port", "3000", "HTTP server port")
	redisAddr    = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory broker.")
	postgresAddr = flag.String("postgres", "", "PostgreSQL connection string (e.g., postgres://user:pass@localhost:5432/db?sslmode=disable)")
	enableCache  = flag.Bool("cache", false, "Enable memory cache layer for Redis/Postgres brokers (provides read-your-own-writes and low-latency reads)")
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
		// Most channels use streamless defaults (no stream history).
		// Only inventory needs stream for CAS (Compare-And-Swap) operations.
		GetMapChannelOptions: func(channel string) centrifuge.MapChannelOptions {
			if channel == "inventory" || channel == "leaderboard" {
				return centrifuge.MapChannelOptions{
					StreamSize: 1000,
					StreamTTL:  time.Hour,
					MetaTTL:    24 * time.Hour,
				}
			}
			// Cursors, games — all streamless (default).
			return centrifuge.DefaultMapChannelOptions()
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Set up map broker (memory, Redis, or PostgreSQL based on flags).
	// When -cache flag is set, wraps Redis/Postgres brokers with CachedMapBroker.
	mapBroker, err := setupMapBroker(node, *redisAddr, *postgresAddr, *enableCache)
	if err != nil {
		log.Fatal(err)
	}
	// Wrap with debouncing for cursors channel — coalesces rapid cursor updates
	// on the server side before forwarding to the backend.
	mapBroker = centrifuge.NewDebouncingMapBroker(mapBroker, centrifuge.DebouncingMapBrokerConfig{
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

			// Inventory and leaderboard use streams — enable positioned mode with recovery.
			if e.Channel == "inventory" || e.Channel == "leaderboard" {
				opts.EnablePositioning = true
				opts.EnableRecovery = true
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
					Options: centrifuge.MapPublishOptions{
						KeyTTL: 10 * time.Second,
					},
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

	server := &http.Server{Addr: ":" + *port}

	go func() {
		log.Printf("Starting server on http://localhost:%s", *port)
		log.Printf("  - Cursors demo:    http://localhost:%s/cursors.html", *port)
		log.Printf("  - Lobby demo:      http://localhost:%s/lobby.html", *port)
		log.Printf("  - Leaderboard:     http://localhost:%s/leaderboard.html", *port)
		log.Printf("  - Inventory demo:  http://localhost:%s/inventory.html", *port)
		log.Printf("  - Tickers demo:    http://localhost:%s/tickers.html", *port)
		log.Printf("  - Live Polls demo: http://localhost:%s/polls.html", *port)
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

// Game handlers.
func handleGameCreate(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	var req struct {
		Name       string `json:"name"`
		MaxPlayers int    `json:"maxPlayers"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	if req.MaxPlayers < 2 {
		req.MaxPlayers = 2
	}
	if req.MaxPlayers > 4 {
		req.MaxPlayers = 4
	}

	// Generate unique game ID using timestamp + random suffix.
	gameID := fmt.Sprintf("game_%d_%s", time.Now().UnixNano(), client.ID()[:8])
	game := &GameInfo{
		ID:         gameID,
		Name:       req.Name,
		CreatedBy:  client.UserID(),
		CreatedAt:  time.Now(),
		MaxPlayers: req.MaxPlayers,
	}

	// Publish game to games list channel (single source of truth).
	gameData, _ := json.Marshal(game)
	_, err := node.MapPublish(context.Background(), "games", gameID, centrifuge.MapPublishOptions{
		Data:   gameData,
		KeyTTL: 10 * time.Minute, // Games expire after 10 minutes of inactivity.
	})
	if err != nil {
		log.Printf("game create error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{Data: gameData}, nil)
}

func handleGameJoin(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	var req struct {
		GameID string `json:"gameId"`
		Name   string `json:"name"`
		Slot   int    `json:"slot"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	// Read game info from map broker (single source of truth).
	stateResult, err := node.MapStateRead(context.Background(), "games", centrifuge.MapReadStateOptions{
		Key: req.GameID,
	})
	if err != nil {
		log.Printf("game lookup error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}
	if len(stateResult.Publications) == 0 {
		cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "game not found"})
		return
	}

	var game GameInfo
	if err := json.Unmarshal(stateResult.Publications[0].Data, &game); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	if req.Slot < 1 || req.Slot > game.MaxPlayers {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	channel := "game:" + req.GameID
	key := fmt.Sprintf("slot_%d", req.Slot)

	player := GamePlayer{
		UserID:   client.UserID(),
		Name:     req.Name,
		ClientID: client.ID(),
		Slot:     req.Slot,
	}
	playerData, _ := json.Marshal(player)

	// Use KeyModeIfNew to prevent slot stealing.
	result, err := node.MapPublish(context.Background(), channel, key, centrifuge.MapPublishOptions{
		Data:    playerData,
		KeyMode: centrifuge.KeyModeIfNew,
		KeyTTL:  5 * time.Minute,
	})
	if err != nil {
		log.Printf("game join error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	if result.Suppressed {
		cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4001, Message: "slot already taken"})
		return
	}

	cb(centrifuge.RPCReply{Data: playerData}, nil)

	// Check if game is full.
	go checkGameFull(node, req.GameID, game.MaxPlayers)
}

func handleGameLeave(_ *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	var req struct {
		GameID string `json:"gameId"`
		Slot   int    `json:"slot"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	channel := "game:" + req.GameID
	key := fmt.Sprintf("slot_%d", req.Slot)

	_, err := node.MapRemove(context.Background(), channel, key, centrifuge.MapRemoveOptions{})
	if err != nil {
		log.Printf("game leave error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, nil)
}

func checkGameFull(node *centrifuge.Node, gameID string, maxPlayers int) {
	channel := "game:" + gameID

	// Read state - by default reads fresh data from backend (safe for CAS operations).
	stateResult, err := node.MapStateRead(context.Background(), channel, centrifuge.MapReadStateOptions{
		Limit: 10,
	})
	if err != nil {
		return
	}

	// Count players (slots).
	playerCount := 0
	var players []GamePlayer
	for _, pub := range stateResult.Publications {
		if strings.HasPrefix(pub.Key, "slot_") {
			playerCount++
			var player GamePlayer
			if err := json.Unmarshal(pub.Data, &player); err == nil {
				players = append(players, player)
			}
		}
	}

	if playerCount >= maxPlayers {
		// Game is full - publish game start event.
		gameData, _ := json.Marshal(map[string]any{
			"event":   "game_start",
			"gameId":  gameID,
			"players": players,
			"message": "Game is starting!",
		})
		_, _ = node.MapPublish(context.Background(), channel, "game_event", centrifuge.MapPublishOptions{
			Data: gameData,
		})

		// Remove game from games list and clear game channel after delay.
		time.AfterFunc(3*time.Second, func() {
			// Remove from games list.
			_, _ = node.MapRemove(context.Background(), "games", gameID, centrifuge.MapRemoveOptions{})

			// Clear all slots and event.
			for i := 1; i <= maxPlayers; i++ {
				key := fmt.Sprintf("slot_%d", i)
				_, _ = node.MapRemove(context.Background(), channel, key, centrifuge.MapRemoveOptions{})
			}
			_, _ = node.MapRemove(context.Background(), channel, "game_event", centrifuge.MapRemoveOptions{})
		})
	}
}

// Native PostgreSQL leaderboard handlers - using cf_map_publish directly.

func handleLeaderboardJoinHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		UserID string `json:"userId"`
		Name   string `json:"name"`
		Color  string `json:"color"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	entry := LeaderboardEntry{
		UserID: req.UserID,
		Name:   req.Name,
		Score:  0,
		Color:  req.Color,
	}
	entryData, _ := json.Marshal(entry)

	// Call cf_map_publish directly on PostgreSQL.
	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	err := pgPool.QueryRow(r.Context(), `
		SELECT * FROM cf_map_publish(
			p_channel => $1,
			p_key => $2,
			p_data => $3,
			p_score => $4,
			p_stream_size => 1000,
			p_stream_ttl => '1 hour'::interval,
			p_meta_ttl => '24 hours'::interval
		)
	`, "leaderboard", req.UserID, entryData, entry.Score).Scan(
		&resultID, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset,
	)

	if err != nil {
		log.Printf("leaderboard join error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"entry":   entry,
		"offset":  channelOffset,
		"epoch":   epoch,
	})
}

func handleLeaderboardClickHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Read current score from snapshot.
	var currentData []byte
	var currentOffset int64
	err := pgPool.QueryRow(ctx, `
		SELECT data, key_offset FROM cf_map_state
		WHERE channel = 'leaderboard' AND key = $1
	`, req.UserID).Scan(&currentData, &currentOffset)

	if err != nil {
		http.Error(w, "Player not found - join first", http.StatusNotFound)
		return
	}

	var entry LeaderboardEntry
	if err := json.Unmarshal(currentData, &entry); err != nil {
		http.Error(w, "Invalid player data", http.StatusInternalServerError)
		return
	}

	// Increment score.
	entry.Score++
	newData, _ := json.Marshal(entry)

	// Update using cf_map_publish with new score.
	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var returnedData []byte
	var returnedOffset *int64

	err = pgPool.QueryRow(ctx, `
		SELECT * FROM cf_map_publish(
			p_channel => 'leaderboard',
			p_key => $1,
			p_data => $2,
			p_score => $3,
			p_stream_size => 1000,
			p_stream_ttl => '1 hour'::interval,
			p_meta_ttl => '24 hours'::interval
		)
	`, req.UserID, newData, entry.Score).Scan(
		&resultID, &channelOffset, &epoch, &suppressed, &suppressReason, &returnedData, &returnedOffset,
	)

	if err != nil {
		log.Printf("leaderboard click error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"entry":   entry,
		"offset":  channelOffset,
	})
}

func handleLeaderboardLeaveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		UserID string `json:"userId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Call cf_map_remove directly.
	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string

	err := pgPool.QueryRow(r.Context(), `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason
		FROM cf_map_remove(
			'leaderboard',
			$1,   -- key (userId)
			NULL, -- client_id
			$1,   -- user_id
			'1 hour'::interval,    -- stream_ttl
			NULL,                  -- idempotency_key
			NULL,                  -- idempotency_ttl
			'24 hours'::interval   -- meta_ttl
		)
	`, req.UserID).Scan(&resultID, &channelOffset, &epoch, &suppressed, &suppressReason)

	if err != nil {
		log.Printf("leaderboard leave error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": !suppressed,
		"offset":  channelOffset,
	})
}

// Inventory handlers - demonstrates CAS (Compare-And-Swap) for preventing overselling.

// initInventory initializes inventory items in the map broker on startup.
func initInventory(node *centrifuge.Node) {
	ctx := context.Background()
	for _, item := range inventoryItems {
		payload, _ := json.Marshal(InventoryPayload{Item: *item})
		// Note: StreamSize/TTL/MetaTTL are configured via GetMapChannelOptions in node config.
		_, _ = node.MapPublish(ctx, "inventory", item.ID, centrifuge.MapPublishOptions{
			Data:    payload,
			KeyMode: centrifuge.KeyModeIfNew, // Only set if item doesn't exist yet.
		})
	}
	log.Printf("Inventory initialized with %d items", len(inventoryItems))
}

// handleInventoryBuy handles purchase requests with CAS to prevent overselling.
func handleInventoryBuy(client *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	var req struct {
		ItemID   string `json:"itemId"`
		Quantity int    `json:"quantity"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	ctx := context.Background()

	// Add delay to make it easier to test concurrent purchases from UI.
	time.Sleep(2 * time.Second)

	// CAS retry loop - keeps trying until success or terminal failure.
	maxRetries := 5
	for attempt := range maxRetries {
		time.Sleep(100 * time.Millisecond)
		// Step 1: Read current state using Key filter (single key lookup).
		// By default reads fresh data from backend (safe for CAS operations).
		stateResult, err := node.MapStateRead(ctx, "inventory", centrifuge.MapReadStateOptions{
			Key: req.ItemID,
		})
		if err != nil {
			log.Printf("inventory read error: %v", err)
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}
		if len(stateResult.Publications) == 0 {
			cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "item not found"})
			return
		}

		pubs := stateResult.Publications
		pos := stateResult.Position

		// Parse current item state.
		var current InventoryPayload
		if err := json.Unmarshal(pubs[0].Data, &current); err != nil {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}
		item := current.Item

		// Check stock.
		if item.Stock < req.Quantity {
			// Not enough stock - return error with current state.
			msg := "Out of stock"
			if item.Stock > 0 {
				msg = fmt.Sprintf("Only %d left in stock", item.Stock)
			}
			result, _ := json.Marshal(map[string]any{
				"success":      false,
				"error":        "insufficient_stock",
				"message":      msg,
				"currentStock": item.Stock,
			})
			cb(centrifuge.RPCReply{Data: result}, nil)
			return
		}

		// Step 2: Prepare new state (decrement stock).
		newStock := item.Stock - req.Quantity
		item.Stock = newStock

		// Combined payload with item state and transaction info.
		transaction := InventoryTransaction{
			Action:   "purchase",
			ItemID:   req.ItemID,
			Quantity: req.Quantity,
			BuyerID:  client.UserID(),
			Message:  fmt.Sprintf("%s bought %d x %s", client.UserID(), req.Quantity, item.Name),
		}
		payload, _ := json.Marshal(InventoryPayload{
			Item:        item,
			Transaction: &transaction,
		})

		// Step 3: CAS write - only succeeds if position matches what we read.
		expectedPos := centrifuge.StreamPosition{
			Offset: pubs[0].Offset,
			Epoch:  pos.Epoch,
		}

		// Note: StreamSize/TTL/MetaTTL are configured via GetMapChannelOptions in node config.
		result, err := node.MapPublish(ctx, "inventory", req.ItemID, centrifuge.MapPublishOptions{
			Data:             payload,
			ExpectedPosition: &expectedPos,
		})
		if err != nil {
			log.Printf("inventory CAS error: %v", err)
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}

		// Check if CAS succeeded.
		if !result.Suppressed {
			// Success! Purchase completed.
			successResult, _ := json.Marshal(map[string]any{
				"success":     true,
				"message":     fmt.Sprintf("Purchased %d x %s!", req.Quantity, item.Name),
				"newStock":    newStock,
				"transaction": transaction,
				"attempts":    attempt + 1,
			})
			log.Printf("Purchase success: %s bought %d x %s (attempts: %d)", client.UserID(), req.Quantity, item.Name, attempt+1)
			cb(centrifuge.RPCReply{Data: successResult}, nil)
			return
		}

		// CAS failed - someone else modified the item.
		if result.SuppressReason == centrifuge.SuppressReasonPositionMismatch {
			log.Printf("CAS conflict for %s (attempt %d), retrying with current state...", req.ItemID, attempt+1)

			// Use CurrentPublication for immediate retry (no extra read needed).
			if result.CurrentPublication != nil {
				// Update pubs[0] with current state for next iteration.
				pubs[0] = result.CurrentPublication
				pos = result.Position
			}
			// Continue to next retry attempt.
			continue
		}

		// Unknown suppression reason.
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	// Exhausted retries.
	result, _ := json.Marshal(map[string]any{
		"success": false,
		"error":   "too_many_conflicts",
		"message": "Too many concurrent purchases, please try again",
	})
	cb(centrifuge.RPCReply{Data: result}, nil)
}

// handleInventoryRestock adds stock to an item (admin action).
func handleInventoryRestock(_ *centrifuge.Client, node *centrifuge.Node, data []byte, cb centrifuge.RPCCallback) {
	var req struct {
		ItemID   string `json:"itemId"`
		Quantity int    `json:"quantity"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	ctx := context.Background()

	// CAS retry loop for restock.
	maxRetries := 5
	for range maxRetries {
		// By default reads fresh data from backend (safe for CAS operations).
		stateResult, err := node.MapStateRead(ctx, "inventory", centrifuge.MapReadStateOptions{
			Key: req.ItemID,
		})
		if err != nil || len(stateResult.Publications) == 0 {
			cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "item not found"})
			return
		}

		pubs := stateResult.Publications
		pos := stateResult.Position

		var current InventoryPayload
		if err := json.Unmarshal(pubs[0].Data, &current); err != nil {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}
		item := current.Item

		// Prepare new state.
		item.Stock += req.Quantity

		transaction := InventoryTransaction{
			Action:   "restock",
			ItemID:   req.ItemID,
			Quantity: req.Quantity,
			Message:  fmt.Sprintf("Restocked %d x %s", req.Quantity, item.Name),
		}
		payload, _ := json.Marshal(InventoryPayload{
			Item:        item,
			Transaction: &transaction,
		})

		expectedPos := centrifuge.StreamPosition{
			Offset: pubs[0].Offset,
			Epoch:  pos.Epoch,
		}

		// Note: StreamSize/TTL/MetaTTL are configured via GetMapChannelOptions in node config.
		result, err := node.MapPublish(ctx, "inventory", req.ItemID, centrifuge.MapPublishOptions{
			Data:             payload,
			ExpectedPosition: &expectedPos,
		})
		if err != nil {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}

		if !result.Suppressed {
			successResult, _ := json.Marshal(map[string]any{
				"success":  true,
				"message":  fmt.Sprintf("Restocked %d x %s", req.Quantity, item.Name),
				"newStock": item.Stock,
			})
			cb(centrifuge.RPCReply{Data: successResult}, nil)
			return
		}

		if result.SuppressReason == centrifuge.SuppressReasonPositionMismatch && result.CurrentPublication != nil {
			pubs[0] = result.CurrentPublication
			pos = result.Position
			continue
		}

		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4002, Message: "too many conflicts"})
}

// pgPublish calls cf_map_publish directly on PostgreSQL.
func pgPublish(ctx context.Context, channel, key string, data []byte, score *int64) error {
	var resultID *int64
	return pgPool.QueryRow(ctx, `
		SELECT result_id FROM cf_map_publish(
			p_channel => $1, p_key => $2, p_data => $3, p_score => $4
		)
	`, channel, key, data, score).Scan(&resultID)
}

// pgRemove calls cf_map_remove directly on PostgreSQL.
func pgRemove(ctx context.Context, channel, key string) error {
	var resultID *int64
	return pgPool.QueryRow(ctx, `
		SELECT result_id FROM cf_map_remove(p_channel => $1, p_key => $2)
	`, channel, key).Scan(&resultID)
}

// recordPollVote atomically records a vote in a PG transaction:
//  1. If userID is provided, writes to "poll:votes" with KeyModeIfNew (dedup).
//  2. Reads current score from "poll:results" with FOR UPDATE lock.
//  3. Increments score via cf_map_publish on "poll:results".
func recordPollVote(ctx context.Context, optionID string, userID *string) (bool, string, error) {
	tx, err := pgPool.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx)

	// Dedup check for real users.
	if userID != nil {
		voteData, _ := json.Marshal(map[string]string{"optionId": optionID})
		var suppressed bool
		err = tx.QueryRow(ctx, `
			SELECT suppressed FROM cf_map_publish(
				p_channel => 'poll:votes',
				p_key => $1,
				p_data => $2,
				p_key_mode => 'if_new'
			)
		`, *userID, voteData).Scan(&suppressed)
		if err != nil {
			return false, "", err
		}
		if suppressed {
			return false, "Already voted", nil
		}
	}

	// Read current score and data with FOR UPDATE lock.
	var currentScore int64
	var currentData []byte
	err = tx.QueryRow(ctx, `
		SELECT COALESCE(score, 0), data FROM cf_map_state
		WHERE channel = 'poll:results' AND key = $1 FOR UPDATE
	`, optionID).Scan(&currentScore, &currentData)
	if err != nil {
		return false, "Poll not active", nil
	}

	// Increment score, keep data the same.
	var suppressed bool
	err = tx.QueryRow(ctx, `
		SELECT suppressed FROM cf_map_publish(
			p_channel => 'poll:results',
			p_key => $1,
			p_data => $2,
			p_score => $3
		)
	`, optionID, currentData, currentScore+1).Scan(&suppressed)
	if err != nil {
		return false, "", err
	}

	if err = tx.Commit(ctx); err != nil {
		return false, "", err
	}
	return true, "Vote recorded", nil
}

// cleanupPollVotes removes all vote records between polls.
func cleanupPollVotes(ctx context.Context) {
	rows, err := pgPool.Query(ctx, `SELECT key FROM cf_map_state WHERE channel = 'poll:votes'`)
	if err != nil {
		return
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if rows.Scan(&key) == nil {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		_ = pgRemove(ctx, "poll:votes", key)
	}
}

// Poll manager — PG-native goroutine using cf_map_publish directly.
// Uses three channels: poll:meta (question/timing), poll:results (scores), poll:votes (dedup).
func runPollManager() {
	if pgPool == nil {
		log.Println("Polls demo requires -postgres flag, skipping poll manager")
		return
	}

	ctx := context.Background()
	questionIdx := 0

	for {
		q := pollQuestions[questionIdx%len(pollQuestions)]
		questionIdx++

		pollID := fmt.Sprintf("poll_%d", time.Now().UnixMilli())
		activeDuration := time.Duration(20+rand.Intn(5)) * time.Second
		startTime := time.Now()
		endTime := startTime.Add(activeDuration)

		meta := PollMeta{
			PollID:    pollID,
			Question:  q.Question,
			Options:   q.Options,
			StartTime: startTime.UnixMilli(),
			EndTime:   endTime.UnixMilli(),
			Status:    "active",
		}

		// Publish poll metadata.
		metaData, _ := json.Marshal(meta)
		if err := pgPublish(ctx, "poll:meta", pollID, metaData, nil); err != nil {
			log.Printf("poll meta publish error: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}

		// Create initial options with score=0.
		score0 := int64(0)
		for _, opt := range q.Options {
			optData, _ := json.Marshal(PollOptionDef{ID: opt.ID, Label: opt.Label, Color: opt.Color})
			if err := pgPublish(ctx, "poll:results", opt.ID, optData, &score0); err != nil {
				log.Printf("poll option publish error: %v", err)
			}
		}
		log.Printf("Poll started: %s — %s", pollID, meta.Question)

		// Bot votes during active phase.
		deadline := time.After(time.Until(endTime))
		botTick := time.NewTimer(time.Duration(2000+rand.Intn(2000)) * time.Millisecond)
	active:
		for {
			select {
			case <-deadline:
				botTick.Stop()
				break active
			case <-botTick.C:
				optIdx := rand.Intn(len(q.Options))
				_, _, _ = recordPollVote(ctx, q.Options[optIdx].ID, nil)
				botTick.Reset(time.Duration(2000+rand.Intn(2000)) * time.Millisecond)
			}
		}

		// Close poll.
		meta.Status = "closed"
		metaData, _ = json.Marshal(meta)
		_ = pgPublish(ctx, "poll:meta", pollID, metaData, nil)
		log.Printf("Poll closed: %s", pollID)

		// Display results.
		time.Sleep(5 * time.Second)

		// Cleanup: remove options, votes, and metadata.
		for _, opt := range q.Options {
			_ = pgRemove(ctx, "poll:results", opt.ID)
		}
		cleanupPollVotes(ctx)
		_ = pgRemove(ctx, "poll:meta", pollID)

		// Gap before next poll.
		time.Sleep(3 * time.Second)
	}
}

// handlePollVoteHTTP handles POST /api/poll/vote — records a vote via PG transaction.
func handlePollVoteHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured — start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		UserID   string `json:"userId"`
		OptionID string `json:"optionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.UserID == "" || req.OptionID == "" {
		http.Error(w, "Missing userId or optionId", http.StatusBadRequest)
		return
	}

	success, message, err := recordPollVote(r.Context(), req.OptionID, &req.UserID)
	if err != nil {
		log.Printf("poll vote error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": success,
		"message": message,
	})
}

// Ticker sector assignments.
var tickerSectors = map[string]string{
	"AAPL": "tech", "GOOG": "tech", "MSFT": "tech", "META": "tech", "NVDA": "tech",
	"AMZN": "ecommerce",
	"TSLA": "auto",
	"NFLX": "media",
	"ORCL": "enterprise", "CRM": "enterprise",
}

func roundToTwoDecimals(f float64) float64 {
	return math.Round(f*100) / 100
}

// publishTickerData publishes random stock prices to the "tickers" map channel.
// Each tick updates 2-4 randomly chosen tickers (not all at once), which is more
// realistic and produces visually distinct highlights on the client.
func publishTickerData(node *centrifuge.Node) {
	tc := time.NewTicker(500 * time.Millisecond)
	defer tc.Stop()

	tickers := []string{"AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "NFLX", "ORCL", "CRM"}
	for range tc.C {
		// Pick 2-4 random tickers to update this tick.
		count := 2 + rand.Intn(3)
		perm := rand.Perm(len(tickers))
		for i := range count {
			ticker := tickers[perm[i]]
			basePrice := 100.0 + float64(len(ticker))*10.0
			bid := roundToTwoDecimals(basePrice + (rand.Float64()-0.5)*10.0)
			ask := roundToTwoDecimals(bid + rand.Float64()*2.0)

			data := map[string]any{
				"ticker": ticker,
				"bid":    bid,
				"ask":    ask,
				"time":   time.Now().UnixMilli(),
			}
			jsonData, err := json.Marshal(data)
			if err != nil {
				continue
			}
			_, err = node.MapPublish(context.Background(), "tickers", ticker, centrifuge.MapPublishOptions{
				Data:   jsonData,
				Tags:   map[string]string{"sector": tickerSectors[ticker]},
				KeyTTL: -1, // No expiration — tickers are permanent.
			})
			if err != nil {
				log.Printf("Failed to publish ticker %s: %v", ticker, err)
			}
		}
	}
}

// setupMapBroker creates either a memory, Redis, or PostgreSQL map broker.
// When enableCache is true, wraps Redis/Postgres brokers with CachedMapBroker
// for read-your-own-writes consistency and low-latency reads.
func setupMapBroker(node *centrifuge.Node, redisAddr, postgresAddr string, enableCache bool) (centrifuge.MapBroker, error) {
	var backend centrifuge.MapBroker

	// PostgreSQL takes priority if specified
	if postgresAddr != "" {
		pgConfig := centrifuge.PostgresMapBrokerConfig{
			ConnString: postgresAddr,
		}

		// If Redis is also specified, use it as broker for multi-node fan-out
		if redisAddr != "" {
			log.Printf("Using PostgreSQL map broker with Redis broker for fan-out")
			redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
				Address: redisAddr,
			})
			if err != nil {
				return nil, fmt.Errorf("error creating Redis shard for broker: %w", err)
			}

			broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
				Shards: []*centrifuge.RedisShard{redisShard},
				Prefix: "map_demo",
			})
			if err != nil {
				return nil, fmt.Errorf("error creating Redis broker: %w", err)
			}
			pgConfig.Broker = broker
		} else {
			log.Printf("Using PostgreSQL map broker (single-node, local delivery only)")
		}

		broker, err := centrifuge.NewPostgresMapBroker(node, pgConfig)
		if err != nil {
			return nil, fmt.Errorf("error creating PostgreSQL map broker: %w", err)
		}
		backend = broker
	} else if redisAddr != "" {
		// Redis if specified
		log.Printf("Using Redis map broker at %s", redisAddr)

		redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
			Address: redisAddr,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating Redis shard: %w", err)
		}

		broker, err := centrifuge.NewRedisMapBroker(node, centrifuge.RedisMapBrokerConfig{
			Shards: []*centrifuge.RedisShard{redisShard},
			Prefix: "map_demo",
		})
		if err != nil {
			return nil, err
		}
		backend = broker
	} else {
		// Default to memory - cache not applicable (already in-memory)
		log.Println("Using in-memory map broker")
		broker, err := centrifuge.NewMemoryMapBroker(node, centrifuge.MemoryMapBrokerConfig{})
		if err != nil {
			return nil, err
		}
		return broker, nil
	}

	// Wrap with cache layer if enabled (only for Redis/Postgres)
	if enableCache {
		log.Println("Enabling memory cache layer")
		cached, err := centrifuge.NewCachedMapBroker(node, backend, centrifuge.CachedMapBrokerConfig{
			Cache: centrifuge.MapCacheConfig{
				MaxChannels:        10000,
				ChannelIdleTimeout: 5 * time.Minute,
				StreamSize:         1000,
			},
			SyncInterval:  10000 * time.Millisecond,
			SyncBatchSize: 1000,
		})
		if err != nil {
			return nil, fmt.Errorf("error creating cached map broker: %w", err)
		}
		return cached, nil
	}

	return backend, nil
}
