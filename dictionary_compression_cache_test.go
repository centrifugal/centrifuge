package centrifuge

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/centrifugal/centrifuge/internal/websocket"
	"github.com/centrifugal/protocol"
	"github.com/stretchr/testify/require"
)

type warmConn struct {
	net.Conn
	read *int64
}

func (c *warmConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	atomic.AddInt64(c.read, int64(n))
	return n, err
}

// Cold and warm are the two populations that matter: a client connecting for the
// first time has to be sent the structure dictionary, one that kept it from an
// earlier connection gets it back for the price of an id. This measures both
// over a real socket.
func TestWarmVersusColdClient(t *testing.T) {
	const channel = "odds:board"
	n, _ := New(Config{
		DictionaryCompression: NewDictionaryCompressionEngine(DictionaryCompressionConfig{
			MinSamples: 32, DictionarySize: 4096, MaxChannelDictionaries: 8,
			UseChannelDictionary: func(ch string) bool { return ch == channel },
		}),
	})
	n.OnConnecting(func(ctx context.Context, e ConnectEvent) (ConnectReply, error) {
		return ConnectReply{Credentials: &Credentials{UserID: "u"}}, nil
	})
	n.OnConnect(func(c *Client) {
		c.OnSubscribe(func(e SubscribeEvent, cb SubscribeCallback) { cb(SubscribeReply{}, nil) })
	})
	require.NoError(t, n.Run())
	defer func() { _ = n.Shutdown(context.Background()) }()

	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", NewWebsocketHandler(n, WebsocketConfig{
		PingPongConfig: PingPongConfig{PingInterval: 10 * time.Minute, PongTimeout: time.Minute},
	}))
	server := httptest.NewServer(mux)
	defer server.Close()

	run := func(held []string, msgs int) (wire int64, structureID string) {
		var read int64
		d := websocket.Dialer{NetDial: func(network, addr string) (net.Conn, error) {
			c, err := net.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			return &warmConn{Conn: c, read: &read}, nil
		}}
		conn, _, _, err := d.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/connection/websocket", nil)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()

		send := func(cmd *protocol.Command) {
			data, err := protocol.NewJSONCommandEncoder().Encode(cmd)
			require.NoError(t, err)
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, data))
		}
		req := &protocol.ConnectRequest{Flag: ConnectionFlagDictionaryCompression}
		if len(held) > 0 {
			req.State = &protocol.ClientState{DictionaryIds: held}
		}
		send(&protocol.Command{Id: 1, Connect: req})
		send(&protocol.Command{Id: 2, Subscribe: &protocol.SubscribeRequest{Channel: channel}})

		var codec, pending *protocol.DeflateFrameCodec
		got, subscribed, published := 0, false, false
		deadline := time.Now().Add(20 * time.Second)
		for got < msgs && time.Now().Before(deadline) {
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, data, err := conn.ReadMessage()
			require.NoError(t, err)
			if codec != nil {
				data, err = codec.Decompress(nil, data, 1<<20)
				require.NoError(t, err)
			}
			for _, line := range bytes.Split(bytes.TrimRight(data, "\n"), []byte("\n")) {
				r, derr := protocol.NewJSONReplyDecoder(line).Decode()
				require.NoError(t, derr)
				if r.Push != nil && r.Push.State != nil && r.Push.State.Dictionary != nil {
					dd := r.Push.State.Dictionary
					if dd.DataB64 != "" {
						raw, e2 := base64.StdEncoding.DecodeString(dd.DataB64)
						require.NoError(t, e2)
						if dd.Flags&protocol.DictionaryFlagDeflate != 0 {
							raw, e2 = protocol.InflateDictionary(raw, 1<<20)
							require.NoError(t, e2)
						}
						pending = protocol.NewDeflateFrameCodec(dd.Id, raw)
						if structureID == "" {
							structureID = dd.Id
						}
					} else {
						// Named, not sent: the client already has these bytes.
						pending = protocol.StructureFrameCodec()
						require.Equal(t, protocol.StructureDictionaryID, dd.Id)
					}
				}
				if r.Id == 2 {
					subscribed = true
				}
				if r.Push != nil && r.Push.Pub != nil {
					got++
				}
			}
			if pending != nil {
				codec, pending = pending, nil
			}
			if subscribed && !published {
				published = true
				go func() {
					for i := 0; i < msgs; i++ {
						_, _ = n.Publish(channel, []byte(fmt.Sprintf(
							`{"eventId":"evt-%06d","market":"1x2","home":"Arsenal","odds":{"h":%d.15}}`,
							100000+i, 1+i%9)))
					}
				}()
			}
		}
		require.Equal(t, msgs, got)
		return atomic.LoadInt64(&read), structureID
	}

	const msgs = 400
	cold, structureID := run(nil, msgs)
	require.Equal(t, protocol.StructureDictionaryID, structureID)
	warm, _ := run([]string{protocol.StructureDictionaryID}, msgs)

	t.Logf("cold client: %d B for %d messages", cold, msgs)
	t.Logf("warm client: %d B for %d messages (%.1f%% less)",
		warm, msgs, 100*(1-float64(warm)/float64(cold)))
	require.Less(t, warm, cold, "a returning client must not pay for the structure dictionary again")
}
