package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
)

func handleBoardCreateHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		Title    string `json:"title"`
		Status   string `json:"status"`
		Priority string `json:"priority"`
		Author   string `json:"author"`
		Color    string `json:"color"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	taskID := "task:" + uuid.New().String()
	score := timeNowMs()

	taskData, _ := json.Marshal(map[string]any{
		"title":    req.Title,
		"status":   req.Status,
		"priority": req.Priority,
		"assignee": "",
		"author":   req.Author,
		"color":    req.Color,
		"created":  score,
	})

	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	err := pgPool.QueryRow(r.Context(), `
		SELECT * FROM cf_map_publish(
			p_channel => 'board',
			p_key => $1,
			p_data => $2,
			p_score => $3
		)
	`, taskID, taskData, score).Scan(
		&resultID, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset,
	)

	if err != nil {
		log.Printf("board create error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"taskId":  taskID,
	})
}

func handleBoardUpdateHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		TaskID   string `json:"taskId"`
		Title    string `json:"title"`
		Status   string `json:"status"`
		Priority string `json:"priority"`
		Assignee string `json:"assignee"`
		Author   string `json:"author"`
		Color    string `json:"color"`
		Created  int64  `json:"created"`
		Score    int64  `json:"score"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	taskData, _ := json.Marshal(map[string]any{
		"title":    req.Title,
		"status":   req.Status,
		"priority": req.Priority,
		"assignee": req.Assignee,
		"author":   req.Author,
		"color":    req.Color,
		"created":  req.Created,
	})

	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string
	var currentData []byte
	var currentOffset *int64

	err := pgPool.QueryRow(r.Context(), `
		SELECT * FROM cf_map_publish(
			p_channel => 'board',
			p_key => $1,
			p_data => $2,
			p_score => $3
		)
	`, req.TaskID, taskData, req.Score).Scan(
		&resultID, &channelOffset, &epoch, &suppressed, &suppressReason, &currentData, &currentOffset,
	)

	if err != nil {
		log.Printf("board update error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": true,
	})
}

func handleBoardDeleteHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if pgPool == nil {
		http.Error(w, "PostgreSQL not configured - start with -postgres flag", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		TaskID string `json:"taskId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	var resultID *int64
	var channelOffset int64
	var epoch string
	var suppressed bool
	var suppressReason *string

	err := pgPool.QueryRow(r.Context(), `
		SELECT result_id, channel_offset, epoch, suppressed, suppress_reason
		FROM cf_map_remove(
			'board',
			$1,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL
		)
	`, req.TaskID).Scan(&resultID, &channelOffset, &epoch, &suppressed, &suppressReason)

	if err != nil {
		log.Printf("board delete error: %v", err)
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": !suppressed,
	})
}

func timeNowMs() int64 {
	return time.Now().UnixMilli()
}
