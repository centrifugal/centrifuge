package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ---------------------------------------------------------------------------
// Raw client protocol types. Scenarios that need to speak the wire protocol
// directly (malformed frames, unidirectional transports, the emulation layer)
// use these instead of centrifuge-go, because the SDK by construction never
// sends anything invalid.
// ---------------------------------------------------------------------------

type protoError struct {
	Code      uint32 `json:"code"`
	Message   string `json:"message"`
	Temporary bool   `json:"temporary"`
}

type connectResult struct {
	Client  string `json:"client"`
	Version string `json:"version"`
	Session string `json:"session"`
	Node    string `json:"node"`
}

type pubPush struct {
	Data    json.RawMessage `json:"data"`
	Offset  uint64          `json:"offset"`
	Key     string          `json:"key"`
	Removed bool            `json:"removed"`
	Version uint64          `json:"version"`
}

type pushBody struct {
	Channel     string           `json:"channel"`
	Pub         *pubPush         `json:"pub"`
	Subscribe   *json.RawMessage `json:"subscribe"`
	Unsubscribe *json.RawMessage `json:"unsubscribe"`
	Message     *json.RawMessage `json:"message"`
	Disconnect  *json.RawMessage `json:"disconnect"`
}

type reply struct {
	ID        uint32           `json:"id"`
	Error     *protoError      `json:"error"`
	Connect   *connectResult   `json:"connect"`
	Subscribe *json.RawMessage `json:"subscribe"`
	Publish   *json.RawMessage `json:"publish"`
	RPC       *struct {
		Data json.RawMessage `json:"data"`
	} `json:"rpc"`
	Push *pushBody `json:"push"`
}

// isPing reports whether the reply is a server ping (an empty object).
func (r *reply) isPing() bool {
	return r.ID == 0 && r.Error == nil && r.Connect == nil && r.Subscribe == nil &&
		r.Publish == nil && r.RPC == nil && r.Push == nil
}

// command is the subset of the client protocol the raw clients send.
type command struct {
	ID          uint32          `json:"id,omitempty"`
	Connect     *connectCmd     `json:"connect,omitempty"`
	Subscribe   *subscribeCmd   `json:"subscribe,omitempty"`
	Unsubscribe *unsubscribeCmd `json:"unsubscribe,omitempty"`
	Publish     *publishCmd     `json:"publish,omitempty"`
	RPC         *rpcCmd         `json:"rpc,omitempty"`
	SubRefresh  *subRefreshCmd  `json:"sub_refresh,omitempty"`
}

type connectCmd struct {
	Token string `json:"token,omitempty"`
	Name  string `json:"name,omitempty"`
}

type subscribeCmd struct {
	Channel string `json:"channel"`
	Recover bool   `json:"recover,omitempty"`
	Offset  uint64 `json:"offset,omitempty"`
	Epoch   string `json:"epoch,omitempty"`
	// Keyed subscriptions: type selects map (1) or shared poll (2), phase picks
	// the map handshake stage, and cursor/limit page through it.
	Type   int32  `json:"type,omitempty"`
	Phase  int32  `json:"phase,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	Limit  int32  `json:"limit,omitempty"`
	Asc    bool   `json:"asc,omitempty"`
}

// subRefreshCmd doubles as the track/untrack command for shared poll: type 1
// tracks the items, type 2 untracks the listed keys.
type subRefreshCmd struct {
	Channel string       `json:"channel"`
	Type    int32        `json:"type,omitempty"`
	Track   []trackBatch `json:"track,omitempty"`
	Untrack []string     `json:"untrack,omitempty"`
}

type trackBatch struct {
	Items []keyedItem `json:"items"`
}

type keyedItem struct {
	Key     string `json:"key"`
	Version uint64 `json:"version,omitempty"`
}

// subscribeResult is the part of a subscribe reply the keyed scenarios read.
type subscribeResult struct {
	Type         int32     `json:"type"`
	Phase        int32     `json:"phase"`
	Cursor       string    `json:"cursor"`
	Epoch        string    `json:"epoch"`
	Offset       uint64    `json:"offset"`
	State        []pubPush `json:"state"`
	Publications []pubPush `json:"publications"`
}

type unsubscribeCmd struct {
	Channel string `json:"channel"`
}

type publishCmd struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

type rpcCmd struct {
	Method string          `json:"method,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// splitReplies splits a protocol frame into individual newline-delimited
// replies. The JSON encoder never emits a raw newline inside a reply, so this
// is exact.
func splitReplies(frame []byte) [][]byte {
	parts := bytes.Split(frame, []byte("\n"))
	out := make([][]byte, 0, len(parts))
	for _, p := range parts {
		p = bytes.TrimSpace(p)
		if len(p) > 0 {
			out = append(out, p)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Raw WebSocket client.
// ---------------------------------------------------------------------------

type rawWS struct {
	ws      *websocket.Conn
	pending [][]byte
	// failed records that a read already errored. gorilla panics on a second
	// read after a failure, and a read deadline that expires is such a failure,
	// so every helper here must stop reading once one has.
	failed error
}

func dialRaw(wsURL string) (*rawWS, error) {
	d := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	ws, _, err := d.Dial(wsURL, nil)
	if err != nil {
		return nil, err
	}
	return &rawWS{ws: ws}, nil
}

func (r *rawWS) close() { _ = r.ws.Close() }

func (r *rawWS) sendJSON(cmd any) error {
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return r.sendBytes(b)
}

func (r *rawWS) sendBytes(b []byte) error {
	_ = r.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return r.ws.WriteMessage(websocket.TextMessage, b)
}

// readReply returns the next protocol reply, transparently answering server
// pings so the connection is not dropped while a scenario waits.
func (r *rawWS) readReply(timeout time.Duration) (*reply, error) {
	if r.failed != nil {
		return nil, r.failed
	}
	deadline := time.Now().Add(timeout)
	for {
		for len(r.pending) > 0 {
			raw := r.pending[0]
			r.pending = r.pending[1:]
			var rep reply
			if err := json.Unmarshal(raw, &rep); err != nil {
				return nil, fmt.Errorf("decode reply %q: %w", raw, err)
			}
			if rep.isPing() {
				_ = r.sendBytes([]byte("{}"))
				continue
			}
			return &rep, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, errReadTimeout
		}
		_ = r.ws.SetReadDeadline(time.Now().Add(remaining))
		_, frame, err := r.ws.ReadMessage()
		if err != nil {
			r.failed = err
			return nil, err
		}
		r.pending = splitReplies(frame)
	}
}

var errReadTimeout = fmt.Errorf("raw read timeout")

// waitReplyID waits for the reply to one command, returning every push that
// arrived while waiting. Keyed subscriptions interleave pushes with replies, so
// a scenario that dropped them would lose the very updates it is asserting on.
func (r *rawWS) waitReplyID(id uint32, timeout time.Duration) (*reply, []*pushBody, error) {
	deadline := time.Now().Add(timeout)
	var pushes []*pushBody
	for {
		rep, err := r.readReply(time.Until(deadline))
		if err != nil {
			return nil, pushes, err
		}
		if rep.Push != nil {
			pushes = append(pushes, rep.Push)
			continue
		}
		if rep.ID == id {
			return rep, pushes, nil
		}
	}
}

// drainUntil reads pushes until enough returns true or the window elapses.
//
// It stops on the caller's condition rather than on a read deadline: letting a
// deadline expire fails the connection for good (gorilla panics on the next
// read), so a scenario that still has commands to send must never wait past
// what it needs.
func (r *rawWS) drainUntil(window time.Duration, enough func(*pushBody) bool) ([]*pushBody, error) {
	deadline := time.Now().Add(window)
	var pushes []*pushBody
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return pushes, nil
		}
		rep, err := r.readReply(remaining)
		if err != nil {
			var netErr net.Error
			if errors.Is(err, errReadTimeout) || (errors.As(err, &netErr) && netErr.Timeout()) {
				return pushes, nil
			}
			return pushes, err
		}
		if rep.Push == nil {
			continue
		}
		pushes = append(pushes, rep.Push)
		if enough != nil && enough(rep.Push) {
			return pushes, nil
		}
	}
}

// drainPushes collects every push in the window. It leaves the connection
// unusable for further reads, so callers must not send commands afterwards.
func (r *rawWS) drainPushes(window time.Duration) ([]*pushBody, error) {
	return r.drainUntil(window, nil)
}

// mapSubscribe sends one map subscribe command and decodes its result.
func (r *rawWS) mapSubscribe(id uint32, channel string, phase int32, cursor string, limit int32) (*subscribeResult, []*pushBody, error) {
	cmd := command{ID: id, Subscribe: &subscribeCmd{
		Channel: channel,
		Type:    subTypeMap,
		Phase:   phase,
		Cursor:  cursor,
		Limit:   limit,
	}}
	if err := r.sendJSON(cmd); err != nil {
		return nil, nil, err
	}
	rep, pushes, err := r.waitReplyID(id, 10*time.Second)
	if err != nil {
		return nil, pushes, err
	}
	if rep.Error != nil {
		return nil, pushes, fmt.Errorf("map subscribe %s: error %d %s", channel, rep.Error.Code, rep.Error.Message)
	}
	if rep.Subscribe == nil {
		return nil, pushes, fmt.Errorf("map subscribe %s: empty result", channel)
	}
	var res subscribeResult
	if err := json.Unmarshal(*rep.Subscribe, &res); err != nil {
		return nil, pushes, fmt.Errorf("decode map subscribe result: %w", err)
	}
	return &res, pushes, nil
}

// pollSubscribe subscribes to a shared poll channel.
func (r *rawWS) pollSubscribe(id uint32, channel string) (*subscribeResult, error) {
	cmd := command{ID: id, Subscribe: &subscribeCmd{Channel: channel, Type: subTypeSharedPoll}}
	if err := r.sendJSON(cmd); err != nil {
		return nil, err
	}
	rep, _, err := r.waitReplyID(id, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if rep.Error != nil {
		return nil, fmt.Errorf("poll subscribe %s: error %d %s", channel, rep.Error.Code, rep.Error.Message)
	}
	var res subscribeResult
	if rep.Subscribe != nil {
		_ = json.Unmarshal(*rep.Subscribe, &res)
	}
	return &res, nil
}

// track and untrack drive the shared poll key set for this connection.
func (r *rawWS) track(id uint32, channel string, keys []string) error {
	items := make([]keyedItem, 0, len(keys))
	for _, k := range keys {
		items = append(items, keyedItem{Key: k})
	}
	cmd := command{ID: id, SubRefresh: &subRefreshCmd{
		Channel: channel, Type: trackTypeTrack, Track: []trackBatch{{Items: items}},
	}}
	if err := r.sendJSON(cmd); err != nil {
		return err
	}
	rep, _, err := r.waitReplyID(id, 10*time.Second)
	if err != nil {
		return err
	}
	if rep.Error != nil {
		return fmt.Errorf("track %s: error %d %s", channel, rep.Error.Code, rep.Error.Message)
	}
	return nil
}

func (r *rawWS) untrack(id uint32, channel string, keys []string) error {
	cmd := command{ID: id, SubRefresh: &subRefreshCmd{
		Channel: channel, Type: trackTypeUntrack, Untrack: keys,
	}}
	if err := r.sendJSON(cmd); err != nil {
		return err
	}
	rep, _, err := r.waitReplyID(id, 10*time.Second)
	if err != nil {
		return err
	}
	if rep.Error != nil {
		return fmt.Errorf("untrack %s: error %d %s", channel, rep.Error.Code, rep.Error.Message)
	}
	return nil
}

// Protocol constants the keyed scenarios send on the wire.
const (
	subTypeMap        int32 = 1
	subTypeSharedPoll int32 = 4

	trackTypeTrack   int32 = 1
	trackTypeUntrack int32 = 2

	mapPhaseLive   int32 = 0
	mapPhaseStream int32 = 1
	mapPhaseState  int32 = 2
)

// connect performs the handshake and returns the connect result.
func (r *rawWS) connect(token string) (*reply, error) {
	if err := r.sendJSON(command{ID: 1, Connect: &connectCmd{Token: token, Name: "stress-raw"}}); err != nil {
		return nil, err
	}
	return r.readReply(10 * time.Second)
}

// expectClosed waits until the server closes the connection, returning the
// websocket close code it used.
func (r *rawWS) expectClosed(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, fmt.Errorf("connection still open after %s", timeout)
		}
		_ = r.ws.SetReadDeadline(time.Now().Add(remaining))
		_, frame, err := r.ws.ReadMessage()
		if err != nil {
			var ce *websocket.CloseError
			if e, ok := err.(*websocket.CloseError); ok {
				ce = e
				return ce.Code, nil
			}
			return 0, err
		}
		// Answer pings so the server does not close us for the wrong reason.
		for _, raw := range splitReplies(frame) {
			var rep reply
			if json.Unmarshal(raw, &rep) == nil && rep.isPing() {
				_ = r.sendBytes([]byte("{}"))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Streaming (SSE / HTTP-stream) client with an emulation uplink.
// ---------------------------------------------------------------------------

// streamClient reads replies from a unidirectional-style HTTP transport and can
// send commands back through the emulation endpoint.
type streamClient struct {
	cancel  context.CancelFunc
	body    io.ReadCloser
	emuURL  string
	node    string
	session string
	client  string

	mu       sync.Mutex
	replies  []*reply
	closed   bool
	readErr  error
	replyCh  chan struct{}
	httpClnt *http.Client
}

// dialSSE connects over SSE. The connect command travels in the URL, so the
// connection's server-side subscriptions must be configured via the token.
func dialSSE(baseURL, token string) (*streamClient, error) {
	cmd, err := json.Marshal(command{ID: 1, Connect: &connectCmd{Token: token, Name: "stress-sse"}})
	if err != nil {
		return nil, err
	}
	u := baseURL + "/connection/sse?cf_connect=" + url.QueryEscape(string(cmd))
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := streamHTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("sse status %d", resp.StatusCode)
	}
	sc := newStreamClient(cancel, resp.Body, baseURL+"/emulation")
	go sc.read(true)
	return sc, nil
}

// dialHTTPStream connects over HTTP streaming. It is bidirectional in practice:
// the downlink is the streamed response, the uplink is the emulation endpoint.
func dialHTTPStream(baseURL, token string) (*streamClient, error) {
	cmd, err := json.Marshal(command{ID: 1, Connect: &connectCmd{Token: token, Name: "stress-stream"}})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/connection/http_stream", bytes.NewReader(cmd))
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := streamHTTPClient.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("http_stream status %d", resp.StatusCode)
	}
	sc := newStreamClient(cancel, resp.Body, baseURL+"/emulation")
	go sc.read(false)
	return sc, nil
}

var streamHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		DisableCompression:  true,
	},
}

func newStreamClient(cancel context.CancelFunc, body io.ReadCloser, emuURL string) *streamClient {
	return &streamClient{
		cancel:   cancel,
		body:     body,
		emuURL:   emuURL,
		replyCh:  make(chan struct{}, 1),
		httpClnt: streamHTTPClient,
	}
}

func (s *streamClient) read(sse bool) {
	scanner := bufio.NewScanner(s.body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if sse {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			line = strings.TrimPrefix(line, "data: ")
		}
		for _, raw := range splitReplies([]byte(line)) {
			var rep reply
			if err := json.Unmarshal(raw, &rep); err != nil {
				continue
			}
			if rep.isPing() {
				continue
			}
			s.mu.Lock()
			if rep.Connect != nil {
				s.node, s.session, s.client = rep.Connect.Node, rep.Connect.Session, rep.Connect.Client
			}
			s.replies = append(s.replies, &rep)
			s.mu.Unlock()
			select {
			case s.replyCh <- struct{}{}:
			default:
			}
		}
	}
	s.mu.Lock()
	s.closed = true
	s.readErr = scanner.Err()
	s.mu.Unlock()
	select {
	case s.replyCh <- struct{}{}:
	default:
	}
}

func (s *streamClient) close() {
	s.cancel()
	_ = s.body.Close()
}

// waitConnected waits for the connect reply that carries node/session ids.
func (s *streamClient) waitConnected(timeout time.Duration) error {
	ok := waitFor(timeout, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.session != ""
	})
	if !ok {
		return fmt.Errorf("no connect reply within %s", timeout)
	}
	return nil
}

// publications returns the payloads pushed to channel so far, in arrival order.
func (s *streamClient) publications(channel string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, r := range s.replies {
		if r.Push != nil && r.Push.Pub != nil && (channel == "" || r.Push.Channel == channel) {
			out = append(out, string(r.Push.Pub.Data))
		}
	}
	return out
}

// replyWithID returns the command reply with the given id, if it arrived.
func (s *streamClient) replyWithID(id uint32) *reply {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.replies {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (s *streamClient) waitReplyWithID(id uint32, timeout time.Duration) (*reply, error) {
	var rep *reply
	ok := waitFor(timeout, func() bool {
		rep = s.replyWithID(id)
		return rep != nil
	})
	if !ok {
		return nil, fmt.Errorf("no reply with id %d within %s", id, timeout)
	}
	return rep, nil
}

// emulate sends a command up through the emulation endpoint.
func (s *streamClient) emulate(cmd command) error {
	s.mu.Lock()
	node, session := s.node, s.session
	s.mu.Unlock()
	if session == "" {
		return fmt.Errorf("emulate before connect reply")
	}
	inner, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	// EmulationRequest.Data is protocol Raw: for JSON it must be a quoted string
	// holding the encoded command.
	quoted, err := json.Marshal(string(inner))
	if err != nil {
		return err
	}
	body := fmt.Sprintf(`{"node":%q,"session":%q,"data":%s}`, node, session, quoted)
	req, err := http.NewRequest(http.MethodPost, s.emuURL, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClnt.Do(req)
	if err != nil {
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("emulation status %d", resp.StatusCode)
	}
	return nil
}
