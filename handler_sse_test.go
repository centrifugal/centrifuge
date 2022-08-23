package centrifuge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

func TestSSEHandler_GET(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{
		LogLevel: LogLevelDebug,
	})

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{
			UserID: "test",
		}}, nil
	})

	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/sse", NewSSEHandler(n, SSEConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	netURL, err := url.Parse(server.URL + "/connection/sse")
	require.NoError(t, err)

	// Test bad request without connect param.
	req, err := http.NewRequest(http.MethodGet, netURL.String(), nil)
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// And OK with connect param.
	values := netURL.Query()
	values.Add(connectUrlParam, string(jsonData))
	netURL.RawQuery = values.Encode()

	req, err = http.NewRequest(http.MethodGet, netURL.String(), nil)
	require.NoError(t, err)

	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { _ = resp.Body.Close() }()

	dec := newSSEStreamDecoder(resp.Body)
	for {
		msg, err := dec.decode()
		require.NoError(t, err)
		if len(msg.Data) == 0 {
			continue
		}
		var reply protocol.Reply
		err = json.Unmarshal(msg.Data, &reply)
		require.NoError(t, err)
		require.NotNil(t, reply.Connect)
		require.Equal(t, uint32(1), reply.Id)
		require.NotZero(t, reply.Connect.Session)
		require.NotZero(t, reply.Connect.Node)
		break
	}
}

func TestSSEHandler_POST(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{
			UserID: "test",
		}}, nil
	})

	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/sse", NewSSEHandler(n, SSEConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/connection/sse", bytes.NewBuffer(jsonData))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { _ = resp.Body.Close() }()

	dec := newSSEStreamDecoder(resp.Body)
	for {
		msg, err := dec.decode()
		require.NoError(t, err)
		if len(msg.Data) == 0 {
			continue
		}
		var reply protocol.Reply
		err = json.Unmarshal(msg.Data, &reply)
		require.NoError(t, err)
		require.NotNil(t, reply.Connect)
		require.Equal(t, uint32(1), reply.Id)
		require.NotZero(t, reply.Connect.Session)
		require.NotZero(t, reply.Connect.Node)
		break
	}
}

func TestSSEHandler_RequestTooLarge(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/sse", NewSSEHandler(n, SSEConfig{
		MaxRequestBodySize: 2,
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/connection/sse", bytes.NewBuffer(jsonData))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func newSSEStreamDecoder(body io.Reader) *sseStreamDecoder {
	return &sseStreamDecoder{
		r: bufio.NewReader(body),
	}
}

type sseStreamDecoder struct {
	r *bufio.Reader
}

type sseMessage struct {
	Data []byte
}

func (d *sseStreamDecoder) decode() (sseMessage, error) {
	// Very naive parser for SSE in general, but works for our case.
	line, _, err := d.r.ReadLine()
	if bytes.HasPrefix(line, []byte("data: ")) {
		line = line[6:]
	}
	return sseMessage{
		Data: line,
	}, err
}
