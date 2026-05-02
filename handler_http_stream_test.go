package centrifuge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

// nonFlusherResponseWriter is an http.ResponseWriter that does NOT implement
// http.Flusher — used to drive the "not a Flusher" error path in HTTP/SSE handlers.
type nonFlusherResponseWriter struct {
	headers http.Header
	body    []byte
	status  int
}

func newNonFlusherWriter() *nonFlusherResponseWriter {
	return &nonFlusherResponseWriter{headers: http.Header{}, status: http.StatusOK}
}

func (w *nonFlusherResponseWriter) Header() http.Header         { return w.headers }
func (w *nonFlusherResponseWriter) Write(b []byte) (int, error) { w.body = append(w.body, b...); return len(b), nil }
func (w *nonFlusherResponseWriter) WriteHeader(status int)      { w.status = status }

func TestHTTPStreamHandler(t *testing.T) {
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

func TestHTTPStreamHandler_Protobuf(t *testing.T) {
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
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/connection/http_stream"
	client := &http.Client{Timeout: 5 * time.Second}
	command := &protocol.Command{
		Id:      1,
		Connect: &protocol.ConnectRequest{},
	}
	enc := protocol.NewProtobufCommandEncoder()
	protoData, err := enc.Encode(command)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(protoData))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	defer func() { _ = resp.Body.Close() }()

	dec := newProtobufStreamCommandDecoder(resp.Body)
	for {
		reply, _, err := dec.decode()
		require.NoError(t, err)
		require.NotNil(t, reply.Connect)
		require.Equal(t, uint32(1), reply.Id)
		require.NotZero(t, reply.Connect.Session)
		require.NotZero(t, reply.Connect.Node)
		break
	}
}

func TestHTTPStreamHandler_RequestTooLarge(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{
		MaxRequestBodySize: 2,
	}))
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
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestHTTPStreamHandler_Options(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/connection/http_stream"
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodOptions, url, bytes.NewBuffer(nil))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	_ = resp.Body.Close()
}

func TestHTTPStreamHandler_UnknownMethod(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()
	mux := http.NewServeMux()
	mux.Handle("/connection/http_stream", NewHTTPStreamHandler(n, HTTPStreamConfig{}))
	server := httptest.NewServer(mux)
	defer server.Close()

	url := server.URL + "/connection/http_stream"
	client := &http.Client{Timeout: 5 * time.Second}

	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(nil))
	require.NoError(t, err)

	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	_ = resp.Body.Close()
}

// TestHTTPStreamHandlerNonFlusher verifies the handler returns 500 when the
// ResponseWriter does not implement http.Flusher.
func TestHTTPStreamHandlerNonFlusher(t *testing.T) {
	t.Parallel()
	n, _ := New(Config{LogLevel: LogLevelInfo, LogHandler: func(LogEntry) {}})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	h := NewHTTPStreamHandler(n, HTTPStreamConfig{})
	req := httptest.NewRequest(http.MethodPost, "/connection/http_stream", strings.NewReader("{}"))
	w := newNonFlusherWriter()
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusInternalServerError, w.status)
}

// TestHTTPStreamTransportGetters verifies the simple getters of the http stream transport.
// These reflect protocol/HTTP version state derived from the request, so they're worth covering
// as they're part of the Transport contract used by metrics and push handling.
func TestHTTPStreamTransportGetters(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/connection/http_stream", strings.NewReader(""))
	req.ProtoMajor = 2
	transport := newHTTPStreamTransport(req, httpStreamTransportConfig{
		protocolType: ProtocolTypeJSON,
		protoMajor:   uint8(req.ProtoMajor),
		pingPong: PingPongConfig{
			PingInterval: 5 * time.Second,
			PongTimeout:  3 * time.Second,
		},
	}, make(chan struct{}))

	require.Equal(t, transportHTTPStream, transport.Name())
	require.Equal(t, "h2", transport.AcceptProtocol())
	require.Equal(t, ProtocolVersion2, transport.ProtocolVersion())
	require.Equal(t, ProtocolTypeJSON, transport.Protocol())
	require.False(t, transport.Unidirectional())
	require.True(t, transport.Emulation())
	require.EqualValues(t, 0, transport.DisabledPushFlags())
	require.Equal(t, 5*time.Second, transport.PingPongConfig().PingInterval)
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

type protobufStreamCommandDecoder struct {
	reader *bufio.Reader
}

func newProtobufStreamCommandDecoder(reader io.Reader) *protobufStreamCommandDecoder {
	return &protobufStreamCommandDecoder{reader: bufio.NewReader(reader)}
}

func (d *protobufStreamCommandDecoder) decode() (*protocol.Reply, int, error) {
	msgLength, err := binary.ReadUvarint(d.reader)
	if err != nil {
		return nil, 0, err
	}

	b := make([]byte, msgLength)
	n, err := io.ReadFull(d.reader, b)
	if err != nil {
		return nil, 0, err
	}
	if uint64(n) != msgLength {
		return nil, 0, io.ErrShortBuffer
	}
	var c protocol.Reply
	err = c.UnmarshalVT(b[:int(msgLength)])
	if err != nil {
		return nil, 0, err
	}
	return &c, int(msgLength) + 8, nil
}
