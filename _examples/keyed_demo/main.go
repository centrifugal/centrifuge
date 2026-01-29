package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
)

// Store for leaderboard scores (in-memory for demo).
var (
	leaderboardMu     sync.RWMutex
	leaderboardScores = make(map[string]*LeaderboardEntry)
)

// Store for games (in-memory for demo).
var (
	gamesMu     sync.RWMutex
	games       = make(map[string]*GameInfo)
	gameCounter int
)

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

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

var (
	redisAddr = flag.String("redis", "", "Redis address (e.g., localhost:6379). If empty, uses in-memory engine.")
)

func main() {
	flag.Parse()

	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Set up keyed engine (memory or Redis based on flag).
	keyedEngine, registerHandler, err := setupKeyedEngine(node, *redisAddr)
	if err != nil {
		log.Fatal(err)
	}
	node.SetKeyedEngine(keyedEngine)

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
			log.Printf("client %s subscribing to %s (keyed: %v)", client.ID(), e.Channel, e.Keyed)

			opts := centrifuge.SubscribeOptions{
				EnableKeyed: true,
			}

			// Enable keyed presence for lobby and game channels.
			// - :clients tracks each connection (key=clientId, full ClientInfo)
			// - :users tracks unique users (key=userId, TTL-based leave for debounce)
			if e.Channel == "lobby" || strings.HasPrefix(e.Channel, "game:") {
				opts.EmitKeyedClientPresence = true
				opts.EmitKeyedUserPresence = true
			}

			cb(centrifuge.SubscribeReply{Options: opts}, nil)
		})

		// Handle presence subscriptions (channels ending with :clients or :users).
		// This is a separate permission scope - watching who's online.
		client.OnPresenceSubscribe(func(e centrifuge.PresenceSubscribeEvent, cb centrifuge.PresenceSubscribeCallback) {
			log.Printf("client %s presence subscribing to %s", client.ID(), e.Channel)
			// e.Channel is the base channel (without :clients or :users suffix)
			cb(centrifuge.PresenceSubscribeReply{}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("client %s unsubscribed from %s", client.ID(), e.Channel)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			switch e.Method {
			case "cursor:update":
				handleCursorUpdate(client, node.KeyedEngine(), e.Data, cb)
			case "game:create":
				handleGameCreate(client, node.KeyedEngine(), e.Data, cb)
			case "game:join":
				handleGameJoin(client, node.KeyedEngine(), e.Data, cb)
			case "game:leave":
				handleGameLeave(client, node.KeyedEngine(), e.Data, cb)
			case "leaderboard:click":
				handleLeaderboardClick(client, node.KeyedEngine(), e.Data, cb)
			case "leaderboard:join":
				handleLeaderboardJoin(client, node.KeyedEngine(), e.Data, cb)
			case "leaderboard:leave":
				handleLeaderboardLeave(client, node.KeyedEngine(), e.Data, cb)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("client disconnected: %s", client.ID())
			// Clean up cursor on disconnect.
			if node.KeyedEngine() != nil {
				_, _ = node.KeyedEngine().Unpublish(context.Background(), "cursors", client.ID(), centrifuge.KeyedUnpublishOptions{
					Publish:       true,
					StreamSize:    1000,
					StreamTTL:     300 * time.Second,
					StreamMetaTTL: time.Hour,
				})
			}
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Register broker event handler for the keyed engine.
	if err := registerHandler(node); err != nil {
		log.Fatal(err)
	}

	// Serve static files.
	http.Handle("/", http.FileServer(http.Dir("./static")))

	// WebSocket handler.
	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})
	http.Handle("/connection/websocket", wsHandler)

	server := &http.Server{Addr: ":3000"}

	go func() {
		log.Printf("Starting server on http://localhost:3000")
		log.Printf("  - Cursors demo:    http://localhost:3000/cursors.html")
		log.Printf("  - Lobby demo:      http://localhost:3000/lobby.html")
		log.Printf("  - Leaderboard:     http://localhost:3000/leaderboard.html")
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

// Cursor update handler.
func handleCursorUpdate(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	_, err := ke.Publish(context.Background(), "cursors", client.ID(), centrifuge.KeyedPublishOptions{
		Publish:       true,
		Data:          data,
		KeyTTL:        5 * time.Second, // Auto-expire if client stops sending updates.
		StreamSize:    1000,
		StreamTTL:     300 * time.Second,
		StreamMetaTTL: time.Hour,
	})
	if err != nil {
		log.Printf("cursor update error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, nil)
}

// Game handlers.
func handleGameCreate(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

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

	gamesMu.Lock()
	gameCounter++
	gameID := fmt.Sprintf("game_%d", gameCounter)
	game := &GameInfo{
		ID:         gameID,
		Name:       req.Name,
		CreatedBy:  client.UserID(),
		CreatedAt:  time.Now(),
		MaxPlayers: req.MaxPlayers,
	}
	games[gameID] = game
	gamesMu.Unlock()

	// Publish game to games list channel.
	gameData, _ := json.Marshal(game)
	_, err := ke.Publish(context.Background(), "games", gameID, centrifuge.KeyedPublishOptions{
		Publish: true,
		Data:    gameData,
		KeyTTL:  10 * time.Minute, // Games expire after 10 minutes of inactivity.
	})
	if err != nil {
		log.Printf("game create error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{Data: gameData}, nil)
}

func handleGameJoin(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	var req struct {
		GameID string `json:"gameId"`
		Name   string `json:"name"`
		Slot   int    `json:"slot"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	gamesMu.RLock()
	game, exists := games[req.GameID]
	gamesMu.RUnlock()

	if !exists {
		cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "game not found"})
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
	result, err := ke.Publish(context.Background(), channel, key, centrifuge.KeyedPublishOptions{
		Publish: true,
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
	go checkGameFull(ke, req.GameID, game.MaxPlayers)
}

func handleGameLeave(_ *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

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

	_, err := ke.Unpublish(context.Background(), channel, key, centrifuge.KeyedUnpublishOptions{
		Publish: true,
	})
	if err != nil {
		log.Printf("game leave error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, nil)
}

func checkGameFull(ke centrifuge.KeyedEngine, gameID string, maxPlayers int) {
	channel := "game:" + gameID

	pubs, _, _, err := ke.ReadSnapshot(context.Background(), channel, centrifuge.KeyedReadSnapshotOptions{
		Limit: 10,
	})
	if err != nil {
		return
	}

	// Count players (slots).
	playerCount := 0
	var players []GamePlayer
	for _, pub := range pubs {
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
		_, _ = ke.Publish(context.Background(), channel, "game_event", centrifuge.KeyedPublishOptions{
			Publish: true,
			Data:    gameData,
		})

		// Remove game from games list and clear game channel after delay.
		time.AfterFunc(3*time.Second, func() {
			// Remove from games list.
			_, _ = ke.Unpublish(context.Background(), "games", gameID, centrifuge.KeyedUnpublishOptions{
				Publish: true,
			})

			// Clear all slots and event.
			for i := 1; i <= maxPlayers; i++ {
				key := fmt.Sprintf("slot_%d", i)
				_, _ = ke.Unpublish(context.Background(), channel, key, centrifuge.KeyedUnpublishOptions{Publish: true})
			}
			_, _ = ke.Unpublish(context.Background(), channel, "game_event", centrifuge.KeyedUnpublishOptions{Publish: true})

			// Remove from in-memory store.
			gamesMu.Lock()
			delete(games, gameID)
			gamesMu.Unlock()
		})
	}
}

// Leaderboard handlers.
func handleLeaderboardJoin(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	var req struct {
		Name  string `json:"name"`
		Color string `json:"color"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	entry := &LeaderboardEntry{
		UserID: client.UserID(),
		Name:   req.Name,
		Score:  0,
		Color:  req.Color,
	}

	leaderboardMu.Lock()
	leaderboardScores[client.UserID()] = entry
	leaderboardMu.Unlock()

	entryData, _ := json.Marshal(entry)
	_, err := ke.Publish(context.Background(), "leaderboard", client.UserID(), centrifuge.KeyedPublishOptions{
		Publish: true,
		Data:    entryData,
		Score:   entry.Score,
		Ordered: true,
	})
	if err != nil {
		log.Printf("leaderboard join error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{Data: entryData}, nil)
}

func handleLeaderboardLeave(client *centrifuge.Client, ke centrifuge.KeyedEngine, _ []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	leaderboardMu.Lock()
	delete(leaderboardScores, client.UserID())
	leaderboardMu.Unlock()

	_, err := ke.Unpublish(context.Background(), "leaderboard", client.UserID(), centrifuge.KeyedUnpublishOptions{
		Publish: true,
	})
	if err != nil {
		log.Printf("leaderboard leave error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}
	cb(centrifuge.RPCReply{}, nil)
}

func handleLeaderboardClick(client *centrifuge.Client, ke centrifuge.KeyedEngine, _ []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	leaderboardMu.Lock()
	entry, ok := leaderboardScores[client.UserID()]
	if !ok {
		leaderboardMu.Unlock()
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}
	entry.Score++
	entryCopy := *entry
	leaderboardMu.Unlock()

	entryData, _ := json.Marshal(entryCopy)
	_, err := ke.Publish(context.Background(), "leaderboard", client.UserID(), centrifuge.KeyedPublishOptions{
		Publish: true,
		Data:    entryData,
		Score:   entryCopy.Score,
		Ordered: true,
	})
	if err != nil {
		log.Printf("leaderboard click error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{Data: entryData}, nil)
}

// setupKeyedEngine creates either a memory or Redis keyed engine.
// Returns the engine and a function to register the broker event handler.
func setupKeyedEngine(node *centrifuge.Node, redisAddr string) (centrifuge.KeyedEngine, func(centrifuge.BrokerEventHandler) error, error) {
	if redisAddr == "" {
		log.Println("Using in-memory keyed engine")
		engine, err := centrifuge.NewMemoryKeyedEngine(node, centrifuge.MemoryKeyedEngineConfig{})
		if err != nil {
			return nil, nil, err
		}
		return engine, engine.RegisterBrokerEventHandler, nil
	}

	log.Printf("Using Redis keyed engine at %s", redisAddr)

	redisShard, err := centrifuge.NewRedisShard(node, centrifuge.RedisShardConfig{
		Address: redisAddr,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating Redis shard: %w", err)
	}

	engine, err := centrifuge.NewRedisKeyedEngine(node, centrifuge.RedisKeyedEngineConfig{
		Shards: []*centrifuge.RedisShard{redisShard},
		Prefix: "keyed_demo",
	})
	if err != nil {
		return nil, nil, err
	}
	return engine, engine.RegisterEventHandler, nil
}
