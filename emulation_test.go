package centrifuge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
