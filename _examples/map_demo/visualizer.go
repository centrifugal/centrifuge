package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/centrifugal/centrifuge"
)

func handleVizPopulate(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Count int `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		if req.Count <= 0 || req.Count > 100000 {
			http.Error(w, "count must be 1-100000", http.StatusBadRequest)
			return
		}
		ctx := context.Background()
		for i := range req.Count {
			key := fmt.Sprintf("item_%d", i)
			data, _ := json.Marshal(map[string]any{
				"id":    key,
				"value": i,
				"ts":    time.Now().UnixMilli(),
			})
			_, err := node.MapPublish(ctx, "visualizer", key, centrifuge.MapPublishOptions{
				Data: data,
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "count": req.Count})
	}
}

func handleVizPublish(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value any    `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		if req.Key == "" {
			req.Key = fmt.Sprintf("key_%d", time.Now().UnixMilli())
		}
		if req.Value == nil {
			req.Value = map[string]any{"ts": time.Now().UnixMilli()}
		}
		data, _ := json.Marshal(req.Value)
		_, err := node.MapPublish(context.Background(), "visualizer", req.Key, centrifuge.MapPublishOptions{
			Data: data,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "key": req.Key})
	}
}

func handleVizRemove(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, "Invalid request: key required", http.StatusBadRequest)
			return
		}
		_, err := node.MapRemove(context.Background(), "visualizer", req.Key, centrifuge.MapRemoveOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func handleVizClear(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		err := node.MapClear(context.Background(), "visualizer", centrifuge.MapClearOptions{})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func handleVizStats(node *centrifuge.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		result, err := node.MapStats(context.Background(), "visualizer")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"numKeys": result.NumKeys})
	}
}
