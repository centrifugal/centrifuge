package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	Data   json.RawMessage `json:"data"`
	Offset uint64          `json:"offset"`
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
			return nil, err
		}
		r.pending = splitReplies(frame)
	}
}

var errReadTimeout = fmt.Errorf("raw read timeout")

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
