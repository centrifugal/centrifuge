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

// Inventory items (in-memory for demo).
var (
	inventoryMu    sync.RWMutex
	inventoryItems = map[string]*InventoryItem{
		"golden_ticket": {ID: "golden_ticket", Name: "Golden Ticket", Price: 100, Stock: 3, Emoji: "🎫"},
		"rare_potion":   {ID: "rare_potion", Name: "Rare Potion", Price: 50, Stock: 5, Emoji: "🧪"},
		"dragon_egg":    {ID: "dragon_egg", Name: "Dragon Egg", Price: 500, Stock: 1, Emoji: "🥚"},
	}
)

type InventoryItem struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Price int    `json:"price"`
	Stock int    `json:"stock"`
	Emoji string `json:"emoji"`
}

type InventoryTransaction struct {
	Action   string `json:"action"` // "purchase" or "restock"
	ItemID   string `json:"itemId"`
	Quantity int    `json:"quantity"`
	BuyerID  string `json:"buyerId,omitempty"`
	Message  string `json:"message"`
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

			// Enable keyed presence for games and individual game channels.
			// - :clients tracks each connection (key=clientId, full ClientInfo)
			// - :users tracks unique users (key=userId, TTL-based leave for debounce)
			if e.Channel == "games" || strings.HasPrefix(e.Channel, "game:") {
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
			case "inventory:buy":
				handleInventoryBuy(client, node.KeyedEngine(), e.Data, cb)
			case "inventory:restock":
				handleInventoryRestock(client, node.KeyedEngine(), e.Data, cb)
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

	// Initialize inventory items on startup.
	initInventory(keyedEngine)

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
		log.Printf("  - Inventory demo:  http://localhost:3000/inventory.html")
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

// Inventory handlers - demonstrates CAS (Compare-And-Swap) for preventing overselling.

// initInventory initializes inventory items in the keyed engine on startup.
func initInventory(ke centrifuge.KeyedEngine) {
	ctx := context.Background()
	for _, item := range inventoryItems {
		itemData, _ := json.Marshal(item)
		_, _ = ke.Publish(ctx, "inventory", item.ID, centrifuge.KeyedPublishOptions{
			Data:       itemData,
			KeyMode:    centrifuge.KeyModeIfNew, // Only set if item doesn't exist yet.
			StreamSize: 100,
			StreamTTL:  time.Hour,
		})
	}
	log.Printf("Inventory initialized with %d items", len(inventoryItems))
}

// handleInventoryBuy handles purchase requests with CAS to prevent overselling.
func handleInventoryBuy(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

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
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Step 1: Read current state using Key filter (single key lookup).
		pubs, pos, _, err := ke.ReadSnapshot(ctx, "inventory", centrifuge.KeyedReadSnapshotOptions{
			Key: req.ItemID,
		})
		if err != nil {
			log.Printf("inventory read error: %v", err)
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}
		if len(pubs) == 0 {
			cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "item not found"})
			return
		}

		// Parse current item state.
		var item InventoryItem
		if err := json.Unmarshal(pubs[0].Data, &item); err != nil {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}

		// Check stock.
		if item.Stock < req.Quantity {
			// Not enough stock - return error with current state.
			result, _ := json.Marshal(map[string]any{
				"success":      false,
				"error":        "insufficient_stock",
				"message":      fmt.Sprintf("Only %d left in stock", item.Stock),
				"currentStock": item.Stock,
			})
			cb(centrifuge.RPCReply{Data: result}, nil)
			return
		}

		// Step 2: Prepare new state (decrement stock).
		newStock := item.Stock - req.Quantity
		item.Stock = newStock
		newItemData, _ := json.Marshal(item)

		// Prepare transaction record for stream (incremental update).
		transaction := InventoryTransaction{
			Action:   "purchase",
			ItemID:   req.ItemID,
			Quantity: req.Quantity,
			BuyerID:  client.UserID(),
			Message:  fmt.Sprintf("%s bought %d x %s", client.UserID(), req.Quantity, item.Name),
		}
		transactionData, _ := json.Marshal(transaction)

		// Step 3: CAS write - only succeeds if position matches what we read.
		expectedPos := centrifuge.StreamPosition{
			Offset: pubs[0].Offset,
			Epoch:  pos.Epoch,
		}

		result, err := ke.Publish(ctx, "inventory", req.ItemID, centrifuge.KeyedPublishOptions{
			Publish:          true,
			Data:             newItemData,     // Full state -> snapshot
			StreamData:       transactionData, // Transaction -> stream
			ExpectedPosition: &expectedPos,
			StreamSize:       100,
			StreamTTL:        time.Hour,
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
func handleInventoryRestock(client *centrifuge.Client, ke centrifuge.KeyedEngine, data []byte, cb centrifuge.RPCCallback) {
	if ke == nil {
		cb(centrifuge.RPCReply{}, centrifuge.ErrorNotAvailable)
		return
	}

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
	for attempt := 0; attempt < maxRetries; attempt++ {
		pubs, pos, _, err := ke.ReadSnapshot(ctx, "inventory", centrifuge.KeyedReadSnapshotOptions{
			Key: req.ItemID,
		})
		if err != nil || len(pubs) == 0 {
			cb(centrifuge.RPCReply{}, &centrifuge.Error{Code: 4004, Message: "item not found"})
			return
		}

		var item InventoryItem
		if err := json.Unmarshal(pubs[0].Data, &item); err != nil {
			cb(centrifuge.RPCReply{}, centrifuge.ErrorInternal)
			return
		}

		// Prepare new state.
		item.Stock += req.Quantity
		newItemData, _ := json.Marshal(item)

		transaction := InventoryTransaction{
			Action:   "restock",
			ItemID:   req.ItemID,
			Quantity: req.Quantity,
			Message:  fmt.Sprintf("Restocked %d x %s", req.Quantity, item.Name),
		}
		transactionData, _ := json.Marshal(transaction)

		expectedPos := centrifuge.StreamPosition{
			Offset: pubs[0].Offset,
			Epoch:  pos.Epoch,
		}

		result, err := ke.Publish(ctx, "inventory", req.ItemID, centrifuge.KeyedPublishOptions{
			Publish:          true,
			Data:             newItemData,
			StreamData:       transactionData,
			ExpectedPosition: &expectedPos,
			StreamSize:       100,
			StreamTTL:        time.Hour,
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
