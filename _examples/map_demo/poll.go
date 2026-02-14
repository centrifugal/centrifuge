package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"
)

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

// cleanupStalePollData removes any leftover poll data from previous runs that
// doesn't belong to the current poll. Called AFTER publishing the new poll so
// the client always has fresh data to display (no empty-state flash).
func cleanupStalePollData(ctx context.Context, currentPollID string, currentOptionIDs map[string]bool) {
	// Remove stale poll metadata (keys other than the current poll).
	rows, err := pgPool.Query(ctx, `SELECT key FROM cf_map_state WHERE channel = 'poll:meta' AND key != $1`, currentPollID)
	if err != nil {
		return
	}
	var staleMetaKeys []string
	for rows.Next() {
		var key string
		if rows.Scan(&key) == nil {
			staleMetaKeys = append(staleMetaKeys, key)
		}
	}
	rows.Close()
	for _, key := range staleMetaKeys {
		_ = pgRemove(ctx, "poll:meta", key)
	}

	// Remove stale poll results (option keys not in the current poll).
	rows, err = pgPool.Query(ctx, `SELECT key FROM cf_map_state WHERE channel = 'poll:results'`)
	if err != nil {
		return
	}
	var staleResultKeys []string
	for rows.Next() {
		var key string
		if rows.Scan(&key) == nil {
			if !currentOptionIDs[key] {
				staleResultKeys = append(staleResultKeys, key)
			}
		}
	}
	rows.Close()
	for _, key := range staleResultKeys {
		_ = pgRemove(ctx, "poll:results", key)
	}

	// Remove stale vote records from previous polls.
	cleanupPollVotes(ctx)

	if len(staleMetaKeys) > 0 || len(staleResultKeys) > 0 {
		log.Printf("Cleaned up stale poll data: %d meta keys, %d result keys", len(staleMetaKeys), len(staleResultKeys))
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
		currentOptionIDs := make(map[string]bool, len(q.Options))
		for _, opt := range q.Options {
			currentOptionIDs[opt.ID] = true
			optData, _ := json.Marshal(PollOptionDef{ID: opt.ID, Label: opt.Label, Color: opt.Color})
			if err := pgPublish(ctx, "poll:results", opt.ID, optData, &score0); err != nil {
				log.Printf("poll option publish error: %v", err)
			}
		}
		log.Printf("Poll started: %s — %s", pollID, meta.Question)

		// Clean up any stale data from previous runs AFTER publishing the new poll,
		// so clients always have fresh data to display (no empty-state loader flash).
		cleanupStalePollData(ctx, pollID, currentOptionIDs)

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
