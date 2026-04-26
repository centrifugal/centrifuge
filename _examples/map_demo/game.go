package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/centrifugal/centrifuge"
)

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
		Data: gameData,
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
