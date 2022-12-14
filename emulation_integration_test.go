//go:build integration

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

func TestEmulation_DifferentNodes(t *testing.T) {
	t.Parallel()
	n1, _ := New(Config{
		LogLevel: LogLevelDebug,
	})

	n1.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{
			UserID: "test",
		}}, nil
	})

	n1.OnConnect(func(client *Client) {
		client.OnRPC(func(event RPCEvent, callback RPCCallback) {
			callback(RPCReply{
				Data: event.Data,
			}, nil)
		})
	})

	redisConf := testRedisConf()

	s, err := NewRedisShard(n1, redisConf)
	require.NoError(t, err)

	prefix := getUniquePrefix()

	b1, _ := NewRedisBroker(n1, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s},
	})
	defer stopRedisBroker(b1)

	n1.SetBroker(b1)

	require.NoError(t, n1.Run())
	defer func() { _ = n1.Shutdown(context.Background()) }()

	mux1 := http.NewServeMux()
	mux1.Handle("/connection/http_stream", NewHTTPStreamHandler(n1, HTTPStreamConfig{}))
	server1 := httptest.NewServer(mux1)
	defer server1.Close()

	n2, _ := New(Config{
		LogLevel: LogLevelDebug,
	})

	s2, err := NewRedisShard(n2, redisConf)
	require.NoError(t, err)

	b2, _ := NewRedisBroker(n2, RedisBrokerConfig{
		Prefix: prefix,
		Shards: []*RedisShard{s2},
	})
	n2.SetBroker(b2)

	require.NoError(t, n2.Run())
	defer func() { _ = n2.Shutdown(context.Background()) }()
	defer stopRedisBroker(b2)

	mux2 := http.NewServeMux()
	mux2.Handle("/emulation", NewEmulationHandler(n2, EmulationConfig{}))
	server2 := httptest.NewServer(mux2)
	defer server2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		info, err := n1.Info()
		require.NoError(t, err)
		if len(info.Nodes) == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("timeout waiting for two nodes running")
		case <-time.After(time.Second):
		}
	}

	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, server1.URL+"/connection/http_stream", bytes.NewBuffer(jsonData))
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

	req, err = http.NewRequest(http.MethodPost, server2.URL+"/emulation", bytes.NewBuffer(jsonEmuRequest))
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
