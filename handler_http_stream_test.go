package centrifuge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/centrifugal/protocol"

	"github.com/stretchr/testify/require"
)

func TestHTTPStreamHandler(t *testing.T) {
	n, _ := New(Config{
		//LogLevel: LogLevelTrace,
		//LogHandler: func(entry LogEntry) {
		//	fmt.Println(entry)
		//},
	})

	n.OnConnecting(func(ctx context.Context, event ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{
			UserID: "test",
		}}, nil
	})

	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/connection/http_stream"
	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	jsonData, err := json.Marshal(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(jsonData))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { _ = resp.Body.Close() }()

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
		break
	}
}

func newJSONStreamDecoder(body io.Reader) *jsonStreamDecoder {
	return &jsonStreamDecoder{
		r: bufio.NewReader(body),
	}
}

type jsonStreamDecoder struct {
	r *bufio.Reader
}

func (d *jsonStreamDecoder) decode() ([]byte, error) {
	line, _, err := d.r.ReadLine()
	return line, err
}
