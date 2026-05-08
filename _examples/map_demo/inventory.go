package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/centrifugal/centrifuge"
)

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

// initInventory initializes inventory items in the map broker on startup.
func initInventory(node *centrifuge.Node) {
	ctx := context.Background()
	for _, item := range inventoryItems {
		payload, _ := json.Marshal(InventoryPayload{Item: *item})
		// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.
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

		// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.
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

		// Note: SyncMode/RetentionMode are configured via GetMapChannelOptions in node config.
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
