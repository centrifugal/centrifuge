package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
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

type LeaderboardEntry struct {
	UserID string `json:"userId"`
	Name   string `json:"name"`
	Score  int64  `json:"score"`
	Color  string `json:"color"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

func main() {
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Set up in-memory keyed engine.
	keyedEngine, err := centrifuge.NewMemoryKeyedEngine(node, centrifuge.MemoryKeyedEngineConfig{})
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
			log.Printf("client %s subscribing to %s (keyed: %v)",
				client.ID(), e.Channel, e.Keyed)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EnableKeyed: true,
				},
			}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("client %s unsubscribed from %s", client.ID(), e.Channel)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			switch e.Method {
			case "cursor:update":
				handleCursorUpdate(client, node.KeyedEngine(), e.Data, cb)
			case "lobby:join":
				handleLobbyJoin(client, node.KeyedEngine(), e.Data, cb)
			case "lobby:leave":
				handleLobbyLeave(client, node.KeyedEngine(), e.Data, cb)
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
					Publish: true,
				})
			}
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	// Register broker event handler for the keyed engine.
	if err := keyedEngine.RegisterBrokerEventHandler(node); err != nil {
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

	_, err := ke.Publish(context.Background(), "cursors", client.ID(), data, centrifuge.KeyedPublishOptions{
		Publish: true,
		KeyTTL:  30 * time.Second, // Auto-expire if client stops sending updates.
	})
	if err != nil {
		log.Printf("cursor update error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, nil)
}

// Lobby handlers.
type LobbySlot struct {
	Slot   int    `json:"slot"`
	UserID string `json:"userId"`
	Name   string `json:"name"`
	Ready  bool   `json:"ready"`
}

func handleLobbyJoin(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	var req struct {
		Slot int    `json:"slot"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	if req.Slot < 1 || req.Slot > 2 {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	key := "slot_" + string(rune('0'+req.Slot))
	slot := LobbySlot{
		Slot:   req.Slot,
		UserID: client.UserID(),
		Name:   req.Name,
		Ready:  true,
	}

	slotData, _ := json.Marshal(slot)

	// Use KeyModeIfNew to prevent slot stealing.
	result, err := ke.Publish(context.Background(), "lobby", key, slotData, centrifuge.KeyedPublishOptions{
		Publish: true,
		KeyMode: centrifuge.KeyModeIfNew,
	})
	if err != nil {
		log.Printf("lobby join error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	if !result.Applied {
		// Slot already taken.
		cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4001, Message: "slot already taken"})
		return
	}

	cb(centrifuge.RPCReply{Data: slotData}, nil)

	// Check if both slots are filled.
	go checkLobbyFull(ke)
}

func handleLobbyLeave(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

	var req struct {
		Slot int `json:"slot"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorBadRequest)
		return
	}

	key := "slot_" + string(rune('0'+req.Slot))
	_, err := ke.Unpublish(context.Background(), "lobby", key, centrifuge.KeyedUnpublishOptions{
		Publish: true,
	})
	if err != nil {
		log.Printf("lobby leave error: %v", err)
		cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
		return
	}

	cb(centrifuge.RPCReply{}, nil)
}

func checkLobbyFull(ke centrifuge.KeyedEngine) {
	pubs, _, _, err := ke.ReadSnapshot(context.Background(), "lobby", centrifuge.KeyedReadSnapshotOptions{
		Limit: 10,
	})
	if err != nil {
		return
	}

	// Count actual player slots (not game_event).
	playerCount := 0
	for _, pub := range pubs {
		if pub.Key != "game_event" {
			playerCount++
		}
	}

	if playerCount >= 2 {
		// Both slots filled - publish game start event.
		gameData, _ := json.Marshal(map[string]any{
			"event":   "game_start",
			"message": "Game is starting!",
		})
		_, _ = ke.Publish(context.Background(), "lobby", "game_event", gameData, centrifuge.KeyedPublishOptions{
			Publish: true,
		})

		// Clear lobby after a short delay.
		time.AfterFunc(3*time.Second, func() {
			_, _ = ke.Unpublish(context.Background(), "lobby", "slot_1", centrifuge.KeyedUnpublishOptions{Publish: true})
			_, _ = ke.Unpublish(context.Background(), "lobby", "slot_2", centrifuge.KeyedUnpublishOptions{Publish: true})
			_, _ = ke.Unpublish(context.Background(), "lobby", "game_event", centrifuge.KeyedUnpublishOptions{Publish: true})
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
	_, err := ke.Publish(context.Background(), "leaderboard", client.UserID(), entryData, centrifuge.KeyedPublishOptions{
		Publish: true,
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

func handleLeaderboardLeave(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
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

func handleLeaderboardClick(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
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
	_, err := ke.Publish(context.Background(), "leaderboard", client.UserID(), entryData, centrifuge.KeyedPublishOptions{
		Publish: true,
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
