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
	"net/url"
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

func (w *nonFlusherResponseWriter) Header() http.Header { return w.headers }
func (w *nonFlusherResponseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}
func (w *nonFlusherResponseWriter) WriteHeader(status int) { w.status = status }

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
	err = c.UnmarshalCF(b[:int(msgLength)])
	if err != nil {
		return nil, 0, err
	}
	return &c, int(msgLength) + 8, nil
}

type unwrappingResponseWriter struct {
	http.ResponseWriter
}

func (w unwrappingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

type flushErrorResponseWriter struct {
	http.ResponseWriter
}

func (w flushErrorResponseWriter) FlushError() error {
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func streamingResponseWriterWrappers() []struct {
	name string
	wrap func(http.ResponseWriter) http.ResponseWriter
} {
	return []struct {
		name string
		wrap func(http.ResponseWriter) http.ResponseWriter
	}{
		{"original", func(w http.ResponseWriter) http.ResponseWriter { return w }},
		{"unwrap", func(w http.ResponseWriter) http.ResponseWriter { return unwrappingResponseWriter{w} }},
		{"nested_unwrap", func(w http.ResponseWriter) http.ResponseWriter {
			return unwrappingResponseWriter{unwrappingResponseWriter{w}}
		}},
		{"flush_error", func(w http.ResponseWriter) http.ResponseWriter { return flushErrorResponseWriter{w} }},
		{"unwrap_flush_error", func(w http.ResponseWriter) http.ResponseWriter {
			return unwrappingResponseWriter{flushErrorResponseWriter{w}}
		}},
	}
}

func TestStreamingHandlers_WrappedResponseWriter(t *testing.T) {
	t.Parallel()
	n, err := New(Config{})
	require.NoError(t, err)
	n.OnConnecting(func(context.Context, ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "test"}}, nil
	})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	for _, transport := range []struct {
		name     string
		method   string
		handler  http.Handler
		protobuf bool
	}{
		{"sse_get", http.MethodGet, NewSSEHandler(n, SSEConfig{}), false},
		{"sse_post", http.MethodPost, NewSSEHandler(n, SSEConfig{}), false},
		{"http_stream_json", http.MethodPost, NewHTTPStreamHandler(n, HTTPStreamConfig{}), false},
		{"http_stream_protobuf", http.MethodPost, NewHTTPStreamHandler(n, HTTPStreamConfig{}), true},
	} {
		for _, wrapper := range streamingResponseWriterWrappers() {
			t.Run(transport.name+"/"+wrapper.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					transport.handler.ServeHTTP(wrapper.wrap(w), r)
				}))
				defer server.Close()

				command := &protocol.Command{Id: 1, Connect: &protocol.ConnectRequest{}}
				var data []byte
				var err error
				if transport.protobuf {
					data, err = protocol.NewProtobufCommandEncoder().Encode(command)
				} else {
					data, err = json.Marshal(command)
				}
				require.NoError(t, err)
				address := server.URL
				if transport.method == http.MethodGet {
					values := url.Values{connectUrlParam: {string(data)}}
					address += "?" + values.Encode()
				}
				request, err := http.NewRequest(transport.method, address, bytes.NewReader(data))
				require.NoError(t, err)
				if transport.protobuf {
					request.Header.Set("Content-Type", "application/octet-stream")
				}
				client := &http.Client{Timeout: 5 * time.Second}
				response, err := client.Do(request)
				require.NoError(t, err)
				defer func() { _ = response.Body.Close() }()
				require.Equal(t, http.StatusOK, response.StatusCode)

				reply := new(protocol.Reply)
				if transport.protobuf {
					decoded, _, err := newProtobufStreamCommandDecoder(response.Body).decode()
					require.NoError(t, err)
					reply = decoded
				} else if strings.HasPrefix(transport.name, "sse") {
					decoder := newSSEStreamDecoder(response.Body)
					for {
						message, err := decoder.decode()
						require.NoError(t, err)
						if len(message.Data) == 0 {
							continue
						}
						require.NoError(t, json.Unmarshal(message.Data, reply))
						break
					}
				} else {
					message, err := newJSONStreamDecoder(response.Body).decode()
					require.NoError(t, err)
					require.NoError(t, json.Unmarshal(message, reply))
				}
				require.Equal(t, uint32(1), reply.Id)
				require.NotNil(t, reply.Connect)
				require.NotEmpty(t, reply.Connect.Session)
				require.NotEmpty(t, reply.Connect.Node)
			})
		}
	}
}

func TestStreamingHandlers_WrappedResponseWriterValidation(t *testing.T) {
	t.Parallel()
	n, err := New(Config{})
	require.NoError(t, err)
	for _, transport := range []struct {
		name    string
		method  string
		handler http.Handler
		body    string
		status  int
	}{
		{"sse_get_missing_connect", http.MethodGet, NewSSEHandler(n, SSEConfig{}), "", http.StatusBadRequest},
		{"sse_post_too_large", http.MethodPost, NewSSEHandler(n, SSEConfig{MaxRequestBodySize: 2}), "large", http.StatusRequestEntityTooLarge},
		{"http_stream_too_large", http.MethodPost, NewHTTPStreamHandler(n, HTTPStreamConfig{MaxRequestBodySize: 2}), "large", http.StatusRequestEntityTooLarge},
	} {
		for _, wrapper := range streamingResponseWriterWrappers() {
			t.Run(transport.name+"/"+wrapper.name, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(transport.method, "/", strings.NewReader(transport.body))
				transport.handler.ServeHTTP(wrapper.wrap(recorder), request)
				require.Equal(t, transport.status, recorder.Code)
				require.False(t, recorder.Flushed)
			})
		}
	}
}
