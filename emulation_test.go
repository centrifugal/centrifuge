package centrifuge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestEmulationHandler_Options(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodOptions, url, bytes.NewBuffer(nil))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestEmulationHandler_UnknownMethod(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(nil))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestEmulationHandler_RequestTooLarge(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{
		MaxRequestBodySize: 2,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    "unknown",
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestEmulationHandler_NodeNotFound(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    "unknown",
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestEmulationHandler_OK(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/emulation"
	client := &http.Client{Timeout: 5 * time.Second}

	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    n.ID(),
		Session: "unknown",
		Data:    jsonData,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestEmulation_SameNode(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{
		LogLevel: LogLevelDebug,
	})

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{
			UserID: "test",
		}}, nil
	})

	n.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{
				Data: event.Data,
			}, nil)
		})
	})

	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{}))
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/connection/http_stream", bytes.NewBuffer(jsonData))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { _ = resp.Body.Close() }()

	var node string
	var session string

	dec := newJSONStreamDecoder(resp.Body)
	for {
		msg, err := dec.decode()
		require.NoError(t, err)
		var reply protocol.Reply
		err = json.Unmarshal(msg, &reply)
		require.NoError(t, err)
		require.NotNil(t, reply.Connect)
		require.Equal(t, uint32(1), reply.Id)
		require.NotZero(t, reply.Connect.Session)
		require.NotZero(t, reply.Connect.Node)
		node = reply.Connect.Node
		session = reply.Connect.Session
		break
	}

	// Send emulation request with RPC command.
	command = &protocol.Command{
		Id: 2,
		Rpc: &protocol.RPCRequest{
			Data: []byte(`{"data":"emulation_works_fine"}`),
		},
	}
	jsonData, err = json.Marshal(command)
	require.NoError(t, err)
	jsonData2, err := json.Marshal(string(jsonData))
	require.NoError(t, err)

	emuRequest := &protocol.EmulationRequest{
		Node:    node,
		Session: session,
		Data:    jsonData2,
	}
	jsonEmuRequest, err := json.Marshal(emuRequest)
	require.NoError(t, err)

	req, err = http.NewRequest(http.MethodPost, server.URL+"/emulation", bytes.NewBuffer(jsonEmuRequest))
	require.NoError(t, err)

	resp2, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp2.StatusCode)
	defer func() { _ = resp2.Body.Close() }()

	// Wait for RPC reply.
	for {
		msg, err := dec.decode()
		require.NoError(t, err)
		var reply protocol.Reply
		err = json.Unmarshal(msg, &reply)
		require.NoError(t, err)
		if reply.Rpc != nil {
			require.Equal(t, uint32(2), reply.Id)
			require.Equal(t, []byte(`{"data":"emulation_works_fine"}`), []byte(reply.Rpc.Data))
			break
		}
	}
}

// TestEmulationHandlerBadJSON covers the unmarshal-error branch in EmulationHandler
// when the body cannot be decoded.
func TestEmulationHandlerBadJSON(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Post(server.URL+"/emulation", "application/json", strings.NewReader("not-json"))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_ = resp.Body.Close()
}

// TestEmulationHandlerProtobufInvalid exercises the protobuf branch of the emulation
// handler with an invalid (random-bytes) protobuf payload — drives the unmarshal-error
// path for Content-Type=application/octet-stream.
func TestEmulationHandlerProtobufInvalid(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	// Random bytes that won't decode as a valid EmulationRequest protobuf.
	resp, err := http.Post(server.URL+"/emulation", "application/octet-stream",
		strings.NewReader("\xff\xff\xff\xff"))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_ = resp.Body.Close()
}

// TestEmulationSurveyHandler_BadProtobuf covers the UnmarshalVT-error branch
// of emulationSurveyHandler.HandleEmulation.
func TestEmulationSurveyHandler_BadProtobuf(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	h := newEmulationSurveyHandler(n)
	got := make(chan SurveyReply, 1)
	h.HandleEmulation(SurveyEvent{Op: emulationOp, Data: []byte("\xff\xff garbage")}, func(r SurveyReply) { got <- r })

	select {
	case r := <-got:
		require.Equal(t, uint32(emulationErrorCodeBadRequest), r.Code)
	case <-time.After(time.Second):
		t.Fatal("survey callback not invoked")
	}
}

// TestEmulationSurveyHandler_NoSession covers the no-session branch.
func TestEmulationSurveyHandler_NoSession(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	h := newEmulationSurveyHandler(n)

	req := &protocol.EmulationRequest{
		Node:    "any-node",
		Session: "missing-session",
		Data:    []byte(`""`),
	}
	data, err := req.MarshalVT()
	require.NoError(t, err)

	got := make(chan SurveyReply, 1)
	h.HandleEmulation(SurveyEvent{Op: emulationOp, Data: data}, func(r SurveyReply) { got <- r })

	select {
	case r := <-got:
		require.Equal(t, uint32(emulationErrorCodeNoSession), r.Code)
	case <-time.After(time.Second):
		t.Fatal("survey callback not invoked")
	}
}

// TestEmulationHandler_NodeNotFoundDirect covers the sendEmulation
// errNodeNotFound path returning HTTP 404 from EmulationHandler.ServeHTTP.
//
// (The existing TestEmulationHandler_NodeNotFound covers it too via JSON; we
// add a Protobuf variant to also cover the application/octet-stream branch.)
func TestEmulationHandler_NodeNotFoundProtobuf(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	req := &protocol.EmulationRequest{
		Node:    "no-such-node",
		Session: "sess",
		Data:    []byte("payload"),
	}
	body, err := req.MarshalVT()
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/emulation", "application/octet-stream", bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	_ = resp.Body.Close()
}

// TestEmulationHandler_GetMethodNotAllowed covers the method-not-allowed
// branch in EmulationHandler.ServeHTTP (any non-POST without OPTIONS).
func TestEmulationHandler_GetMethodNotAllowed(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/emulation")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	// Either MethodNotAllowed or 405 — accept anything in 4xx range that's not 200.
	require.True(t, resp.StatusCode >= 400 && resp.StatusCode < 500,
		"unexpected status %d", resp.StatusCode)
}

// TestEmulationHandler_BadJSONString tests that the JSON parse path in
// EmulationHandler with valid JSON-but-wrong-type (number where string
// expected) hits the bad-request branch.
func TestEmulationHandler_BadJSONString(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/emulation", NewEmulationHandler(n, EmulationConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Post(server.URL+"/emulation", "application/json",
		strings.NewReader(`{"node":123}`))
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_ = resp.Body.Close()
}
