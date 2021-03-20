package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge/_examples/unidirectional_grpc/clientproto"

	"github.com/centrifugal/centrifuge"
	"google.golang.org/grpc"
)

var (
	httpPort = flag.Int("http_port", 8000, "Port to bind HTTP server to")
	grpcPort = flag.Int("grpc_port", 10000, "Port to bind GRPC server to")
	redis    = flag.Bool("redis", false, "Use Redis")
)

func grpcAuthInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// You probably want to authenticate user by information included in stream metadata.
	// meta, ok := metadata.FromIncomingContext(ss.Context())
	// But here we skip it for simplicity and just always authenticate user with ID 42.
	ctx := ss.Context()
	newCtx := centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
		UserID: "42",
	})

	// GRPC has no builtin method to add data to context so here we use small
	// wrapper over ServerStream.
	wrapped := WrapServerStream(ss)
	wrapped.WrappedContext = newCtx
	return handler(srv, wrapped)
}

// WrappedServerStream is a thin wrapper around grpc.ServerStream that allows modifying context.
// This can be replaced by analogue from github.com/grpc-ecosystem/go-grpc-middleware
// package - https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/wrappers.go.
// You most probably will have dependency to it in your application as it has lots of
// useful features to deal with GRPC.
type WrappedServerStream struct {
	grpc.ServerStream
	// WrappedContext is the wrapper's own Context. You can assign it.
	WrappedContext context.Context
}

// Context returns the wrapper's WrappedContext, overwriting the nested grpc.ServerStream.Context()
func (w *WrappedServerStream) Context() context.Context {
	return w.WrappedContext
}

// WrapServerStream returns a ServerStream that has the ability to overwrite context.
func WrapServerStream(stream grpc.ServerStream) *WrappedServerStream {
	if existing, ok := stream.(*WrappedServerStream); ok {
		return existing
	}
	return &WrappedServerStream{ServerStream: stream, WrappedContext: stream.Context()}
}

func waitExitSignal(n *centrifuge.Node, server *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = n.Shutdown(context.Background())
		server.GracefulStop()
		done <- true
	}()
	<-done
}

// RegisterGRPCServerClient ...
func RegisterGRPCServerClient(n *centrifuge.Node, server *grpc.Server, config GRPCClientServiceConfig) error {
	clientproto.RegisterCentrifugeUniServer(server, newGRPCClientService(n, config))
	return nil
}

// GRPCClientServiceConfig for GRPC client Service.
type GRPCClientServiceConfig struct{}

// GRPCClientService can work with client GRPC connections.
type grpcClientService struct {
	clientproto.UnimplementedCentrifugeUniServer
	config GRPCClientServiceConfig
	node   *centrifuge.Node
}

// newGRPCClientService creates new Service.
func newGRPCClientService(n *centrifuge.Node, c GRPCClientServiceConfig) *grpcClientService {
	return &grpcClientService{
		config: c,
		node:   n,
	}
}

// Consume is a unidirectional server->client stream wit real-time data.
func (s *grpcClientService) Consume(req *clientproto.ConnectRequest, stream clientproto.CentrifugeUni_ConsumeServer) error {
	streamDataCh := make(chan *clientproto.StreamData)
	transport := newGRPCTransport(stream, streamDataCh)

	connectRequest := centrifuge.ConnectRequest{
		Token:   req.Token,
		Data:    req.Data,
		Name:    req.Name,
		Version: req.Version,
	}
	if req.Subs != nil {
		subs := make(map[string]centrifuge.SubscribeRequest)
		for k, v := range connectRequest.Subs {
			subs[k] = centrifuge.SubscribeRequest{
				Recover: v.Recover,
				Offset:  v.Offset,
				Epoch:   v.Epoch,
			}
		}
	}
	c, closeFn, err := centrifuge.NewClient(stream.Context(), s.node, transport)
	if err != nil {
		log.Printf("client create error: %v", err)
		return err
	}
	defer func() { _ = closeFn() }()

	log.Printf("client connected (id %s)", c.ID())
	defer func(started time.Time) {
		log.Printf("client disconnected (id %s, duration %s)", c.ID(), time.Since(started))
	}(time.Now())

	err = c.Connect(connectRequest)
	if err != nil {
		log.Printf("client connect error: %v", err)
		return err
	}

	for {
		select {
		case streamData := <-streamDataCh:
			err := stream.Send(streamData)
			if err != nil {
				log.Printf("stream send error: %v", err)
				return err
			}
		case <-transport.closeCh:
			return nil
		}
	}
}

// grpcTransport wraps a stream.
type grpcTransport struct {
	mu           sync.Mutex
	stream       clientproto.CentrifugeUni_ConsumeServer
	closed       bool
	closeCh      chan struct{}
	streamDataCh chan *clientproto.StreamData
}

func newGRPCTransport(stream clientproto.CentrifugeUni_ConsumeServer, streamDataCh chan *clientproto.StreamData) *grpcTransport {
	return &grpcTransport{
		stream:       stream,
		streamDataCh: streamDataCh,
		closeCh:      make(chan struct{}),
	}
}

func (t *grpcTransport) Name() string {
	return "grpc"
}

func (t *grpcTransport) Protocol() centrifuge.ProtocolType {
	return centrifuge.ProtocolTypeProtobuf
}

func (t *grpcTransport) Encoding() centrifuge.EncodingType {
	return centrifuge.EncodingTypeBinary
}

// Unidirectional returns whether transport is unidirectional.
func (t *grpcTransport) Unidirectional() bool {
	return true
}

// DisabledPushFlags ...
func (t *grpcTransport) DisabledPushFlags() uint64 {
	return 0
}

func (t *grpcTransport) Write(messages ...[]byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	for i := 0; i < len(messages); i++ {
		select {
		case t.streamDataCh <- &clientproto.StreamData{
			Data: messages[i],
		}:
		case <-t.closeCh:
			return nil
		}
	}
	return nil
}

func (t *grpcTransport) Close(_ *centrifuge.Disconnect) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	close(t.closeCh)
	return nil
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("%s: %v", e.Message, e.Fields)
}

var exampleChannel = "unidirectional"

func main() {
	flag.Parse()

	cfg := centrifuge.DefaultConfig
	cfg.LogLevel = centrifuge.LogLevelDebug
	cfg.LogHandler = handleLog

	node, _ := centrifuge.New(cfg)

	if *redis {
		redisShardConfigs := []centrifuge.RedisShardConfig{
			{Address: "localhost:6379"},
		}
		var redisShards []*centrifuge.RedisShard
		for _, redisConf := range redisShardConfigs {
			redisShard, err := centrifuge.NewRedisShard(node, redisConf)
			if err != nil {
				log.Fatal(err)
			}
			redisShards = append(redisShards, redisShard)
		}
		// Using Redis Broker here to scale nodes.
		broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetBroker(broker)

		presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
			Shards: redisShards,
		})
		if err != nil {
			log.Fatal(err)
		}
		node.SetPresenceManager(presenceManager)
	}

	node.OnConnecting(func(ctx context.Context, e centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		return centrifuge.ConnectReply{
			Subscriptions: map[string]centrifuge.SubscribeOptions{
				exampleChannel: {},
			},
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})
		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
		transport := client.Transport()
		log.Printf("user %s connected via %s", client.UserID(), transport.Name())
	})

	// Publish to a channel periodically.
	go func() {
		for {
			currentTime := strconv.FormatInt(time.Now().Unix(), 10)
			_, err := node.Publish(exampleChannel, []byte(`{"server_time": "`+currentTime+`"}`))
			if err != nil {
				log.Println(err.Error())
			}
			time.Sleep(5 * time.Second)
		}
	}()

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpcAuthInterceptor),
	)
	err := RegisterGRPCServerClient(node, grpcServer, GRPCClientServiceConfig{})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		log.Println("starting GRPC server on :" + strconv.Itoa(*grpcPort))
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(*grpcPort))
		if err != nil {
			log.Fatal(err)
		}
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Serve GRPC: %v", err)
		}
	}()

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node, grpcServer)
	log.Println("bye!")
}
