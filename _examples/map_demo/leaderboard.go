package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type LeaderboardEntry struct {
	UserID string `json:"userId"`
	Name   string `json:"name"`
	Score  int64  `json:"score"`
	Color  string `json:"color"`
}

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
			p_channel => 'leaderboard',
			p_key => $1,
			p_user_id => $1,
			p_meta_ttl => '24 hours'::interval
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
