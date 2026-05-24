package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/centrifugal/centrifuge"
)

const (
	// signatureSecret is the HMAC-SHA256 key used to sign and verify track signatures.
	// In production this MUST come from a secure secret store, not be hardcoded.
	signatureSecret = "feature-flags-demo-secret"
	// signatureTTLSeconds controls how long a track signature stays valid. After expiry the
	// client must obtain a fresh signature via getSignature callback — this is the
	// mechanism that enforces periodic re-authorization. Keep it short (30-120s) so
	// revoked access takes effect quickly.
	signatureTTLSeconds = 30

	flagsChannel = "feature_flags"
)

// Flag represents a feature flag.
type Flag struct {
	Key         string `json:"key"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	AdminOnly   bool   `json:"admin_only"`
}

var (
	flagsMu   sync.RWMutex
	flagOrder = []string{
		"dark_mode", "new_dashboard", "maintenance_banner",
		"beta_api", "debug_logging", "ai_assistant",
	}
	flags = map[string]*Flag{
		"dark_mode":          {Key: "dark_mode", Name: "Dark Mode", Description: "Enable dark theme across the application", Enabled: true},
		"new_dashboard":      {Key: "new_dashboard", Name: "New Dashboard", Description: "Redesigned analytics dashboard layout", Enabled: false},
		"maintenance_banner": {Key: "maintenance_banner", Name: "Maintenance Banner", Description: "Show scheduled maintenance notification", Enabled: false},
		"beta_api":           {Key: "beta_api", Name: "Beta API", Description: "Enable experimental v2 API endpoints", Enabled: false, AdminOnly: true},
		"debug_logging":      {Key: "debug_logging", Name: "Debug Logging", Description: "Verbose server-side logging for troubleshooting", Enabled: true, AdminOnly: true},
		"ai_assistant":       {Key: "ai_assistant", Name: "AI Assistant", Description: "AI-powered code review suggestions", Enabled: false, AdminOnly: true},
	}
)

// makeSignature creates an HMAC-SHA256 track signature.
//
// Signature format: "issuedAt:expireAt:role:hmacHex"
// HMAC payload: "issuedAt\0expireAt\0role\0channel\0sha256(sorted_keys)"
//
// The signature binds together: time window, role, channel, and exact set of keys.
// Changing any of these invalidates the HMAC. The role is embedded in the signature
// so the server can extract it during verification without needing to look up the
// connection's role separately.
//
// Inner payload fields are NUL-separated (matching Centrifugo's standard track
// signature format) so colons inside role or channel can't shift between fields
// and yield a colliding HMAC for a different (role, channel) tuple. The outer
// signature string itself stays ':'-separated because its non-role fields are
// colon-free by construction; role here is constrained to {"admin","viewer"}
// so it's safe to keep in the outer string.
func makeSignature(ch string, keys []string, role string, ttl int) string {
	now := time.Now().Unix()
	expireAt := now + int64(ttl)
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	keysHash := sha256.Sum256([]byte(strings.Join(sorted, "\x00")))
	payload := fmt.Sprintf("%d\x00%d\x00%s\x00%s\x00%x", now, expireAt, role, ch, keysHash)
	mac := hmac.New(sha256.New, []byte(signatureSecret))
	mac.Write([]byte(payload))
	return fmt.Sprintf("%d:%d:%s:%x", now, expireAt, role, mac.Sum(nil))
}

// verifySignature checks the HMAC signature and returns the embedded role and expireAt.
func verifySignature(sig, ch string, keys []string) (role string, expireAt int64, err error) {
	parts := strings.SplitN(sig, ":", 4)
	if len(parts) != 4 {
		return "", 0, errors.New("invalid signature format")
	}
	issuedAt, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", 0, errors.New("invalid issued_at")
	}
	expireAt, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return "", 0, errors.New("invalid expire_at")
	}
	role = parts[2]
	macHex := parts[3]

	if time.Now().Unix() > expireAt {
		return "", 0, errors.New("signature expired")
	}

	// Reconstruct the expected HMAC from the claimed parameters. Must mirror
	// the NUL-separated payload built in makeSignature.
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	keysHash := sha256.Sum256([]byte(strings.Join(sorted, "\x00")))
	payload := fmt.Sprintf("%d\x00%d\x00%s\x00%s\x00%x", issuedAt, expireAt, role, ch, keysHash)
	mac := hmac.New(sha256.New, []byte(signatureSecret))
	mac.Write([]byte(payload))
	expectedMAC := fmt.Sprintf("%x", mac.Sum(nil))

	if !hmac.Equal([]byte(macHex), []byte(expectedMAC)) {
		return "", 0, errors.New("HMAC mismatch")
	}

	return role, expireAt, nil
}

// flagsForRole returns a copy of flags visible to the given role.
func flagsForRole(role string) []Flag {
	flagsMu.RLock()
	defer flagsMu.RUnlock()
	var result []Flag
	for _, key := range flagOrder {
		f := flags[key]
		if f.AdminOnly && role != "admin" {
			continue
		}
		result = append(result, *f)
	}
	return result
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

// authMiddleware reads role from query parameter and sets Centrifuge credentials.
//
// IMPORTANT: In a real application, you would authenticate the user via JWT,
// session cookie, or OAuth — NOT via a query parameter. The role must come from
// a trusted source (your auth system), never from client-provided input.
func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		role := r.URL.Query().Get("role")
		if role != "admin" {
			role = "viewer"
		}
		ctx := centrifuge.SetCredentials(r.Context(), &centrifuge.Credentials{
			UserID: fmt.Sprintf("%s_%d", role, time.Now().UnixNano()%100000),
			Info:   []byte(fmt.Sprintf(`{"role":"%s"}`, role)),
		})
		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}

func main() {
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelInfo,
		LogHandler: handleLog,
		SharedPoll: centrifuge.SharedPollConfig{
			GetSharedPollChannelOptions: func(ch string) (centrifuge.SharedPollChannelOptions, bool) {
				if ch == flagsChannel {
					return centrifuge.SharedPollChannelOptions{
						RefreshInterval: 5 * time.Second,
					}, true
				}
				return centrifuge.SharedPollChannelOptions{}, false
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// OnSharedPoll is called by the shared poll refresh worker to fetch current
	// flag data. This is the "single backend call per refresh cycle" that all
	// connected clients share — the key efficiency win of shared polling.
	node.OnSharedPoll(func(ctx context.Context, event centrifuge.SharedPollEvent) (centrifuge.SharedPollResult, error) {
		flagsMu.RLock()
		defer flagsMu.RUnlock()
		var items []centrifuge.SharedPollRefreshItem
		for _, item := range event.Items {
			f, ok := flags[item.Key]
			if !ok {
				continue
			}
			data, _ := json.Marshal(f)
			items = append(items, centrifuge.SharedPollRefreshItem{
				Key:  item.Key,
				Data: data,
			})
		}
		return centrifuge.SharedPollResult{Items: items}, nil
	})

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		log.Printf("[user %s] connected", client.UserID())

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			if e.Channel != flagsChannel {
				cb(centrifuge.SubscribeReply{}, centrifuge.ErrorPermissionDenied)
				return
			}
			cb(centrifuge.SubscribeReply{}, nil)
		})

		// OnTrack is the SOLE authorization gate for shared poll subscriptions.
		// The handler MUST verify every batch's signature — returning nil error
		// without validation lets any client subscribe to arbitrary keys.
		//
		// A single track request can carry multiple signed batches (the SDK
		// replays its cached signature library after reconnect). We verify each
		// batch independently and reject the whole request on any failure.
		client.OnTrack(func(e centrifuge.TrackEvent, cb centrifuge.TrackCallback) {
			batchReplies := make([]centrifuge.TrackBatchReply, len(e.Batches))
			totalKeys := 0
			for i, b := range e.Batches {
				keys := make([]string, len(b.Items))
				for j, item := range b.Items {
					keys[j] = item.Key
				}

				// Step 1: verify HMAC signature for this batch — proves the
				// backend authorized this exact set of keys for the embedded role.
				role, expireAt, err := verifySignature(b.Signature, e.Channel, keys)
				if err != nil {
					log.Printf("[user %s] track rejected: %v", client.UserID(), err)
					cb(centrifuge.TrackReply{}, centrifuge.ErrorPermissionDenied)
					return
				}

				// Step 2: defense-in-depth — verify each key is accessible by the
				// role extracted from the (already-verified) signature. This catches
				// bugs where the signature-issuing endpoint accidentally includes
				// keys the role shouldn't see.
				flagsMu.RLock()
				for _, item := range b.Items {
					f, ok := flags[item.Key]
					if ok && f.AdminOnly && role != "admin" {
						flagsMu.RUnlock()
						log.Printf("[user %s] track rejected: %s requires admin role", client.UserID(), item.Key)
						cb(centrifuge.TrackReply{}, centrifuge.ErrorPermissionDenied)
						return
					}
				}
				flagsMu.RUnlock()

				batchReplies[i] = centrifuge.TrackBatchReply{ExpireAt: expireAt}
				totalKeys += len(keys)
			}

			log.Printf("[user %s] track approved: %d keys across %d batch(es)", client.UserID(), totalKeys, len(e.Batches))

			// Per-batch ExpireAt forces the client to re-authorize each batch
			// independently after its signature TTL. This is how you enforce
			// access revocation: when the client re-tracks with a fresh signature,
			// the backend can refuse to sign keys the user no longer has access to.
			cb(centrifuge.TrackReply{Batches: batchReplies}, nil)
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("[user %s] disconnected: %s", client.UserID(), e.Reason)
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()

	// GET /api/flags?role=admin|viewer — returns flags visible to the role
	// along with a pre-signed track signature.
	//
	// IMPORTANT: In production, authenticate the request (JWT, session) and
	// derive the role from your auth system — not from a query parameter.
	mux.HandleFunc("/api/flags", func(w http.ResponseWriter, r *http.Request) {
		role := r.URL.Query().Get("role")
		if role != "admin" {
			role = "viewer"
		}
		ff := flagsForRole(role)
		keys := make([]string, len(ff))
		for i, f := range ff {
			keys[i] = f.Key
		}
		sig := makeSignature(flagsChannel, keys, role, signatureTTLSeconds)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"flags":     ff,
			"keys":      keys,
			"signature": sig,
		})
	})

	// POST /api/flags/{key}/toggle?role=admin — toggle a flag (admin only).
	mux.HandleFunc("/api/flags/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		role := r.URL.Query().Get("role")
		if role != "admin" {
			http.Error(w, "admin only", http.StatusForbidden)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/api/flags/")
		key = strings.TrimSuffix(key, "/toggle")

		flagsMu.Lock()
		f, ok := flags[key]
		if !ok {
			flagsMu.Unlock()
			http.Error(w, "flag not found", http.StatusNotFound)
			return
		}
		f.Enabled = !f.Enabled
		cp := *f
		flagsMu.Unlock()

		// Notify shared poll — triggers an immediate backend poll for this key
		// so all watchers see the change within milliseconds, not at the next
		// refresh cycle (which could be up to RefreshInterval away).
		node.SharedPollNotify([]centrifuge.SharedPollNotificationItem{
			{Channel: flagsChannel, Key: key},
		})

		log.Printf("flag %q toggled to %v by %s", key, cp.Enabled, role)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cp)
	})

	// POST /api/track_refresh — signature refresh endpoint called by the
	// centrifuge-js getSignature callback when the previous signature expires.
	// Filters out keys the role can't access (revoking them on the client).
	mux.HandleFunc("/api/track_refresh", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Keys []string `json:"keys"`
			Role string   `json:"role"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.Role != "admin" {
			req.Role = "viewer"
		}

		// Filter keys the role can still access — any removed keys will
		// trigger a 'removed' update event on the client.
		validKeys := make([]string, 0, len(req.Keys))
		flagsMu.RLock()
		for _, key := range req.Keys {
			f, ok := flags[key]
			if !ok {
				continue
			}
			if f.AdminOnly && req.Role != "admin" {
				continue
			}
			validKeys = append(validKeys, key)
		}
		flagsMu.RUnlock()

		sig := makeSignature(flagsChannel, validKeys, req.Role, signatureTTLSeconds)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys":      validKeys,
			"signature": sig,
		})
	})

	wsHandler := centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{
		ReadBufferSize:     1024,
		UseWriteBufferPool: true,
	})
	mux.Handle("/connection/websocket", authMiddleware(wsHandler))

	mux.Handle("/", http.FileServer(http.Dir("./")))

	server := &http.Server{
		Addr:         ":8000",
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("Feature Flags Dashboard: http://localhost:8000")
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = node.Shutdown(ctx)
	_ = server.Shutdown(ctx)
}
