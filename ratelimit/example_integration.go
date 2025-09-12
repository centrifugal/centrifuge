package ratelimit

import (
	"context"
	"net/http"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/channelwill/cw2-live-chat-common/pkg/zaplog"
	"github.com/redis/go-redis/v9"
)

// InitializeRateLimitedNode 初始化带限流功能的 Centrifuge Node
// 这是一个示例函数，展示如何集成限流功能到 Centrifuge
func InitializeRateLimitedNode(redisClient redis.UniversalClient, config *Config, logger zaplog.LiveChatLogger) (*NodeWrapper, error) {
	// 1. 创建标准的 Centrifuge Node
	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:       centrifuge.LogLevelInfo,
		HistoryMetaTTL: 24 * time.Hour,
	})
	if err != nil {
		return nil, err
	}

	// 2. 创建限流管理器
	rateLimiter, err := NewManager(redisClient, config, logger)
	if err != nil {
		return nil, err
	}

	// 3. 创建 NodeWrapper
	wrapper := NewNodeWrapper(node, rateLimiter, logger)

	// 4. 设置节点级别的连接处理器
	node.OnConnect(func(client *centrifuge.Client) {
		// 为每个新连接设置限流处理器
		wrapper.SetupRateLimitHandlers(client)

		// 记录连接信息
		transport := client.Transport()
		logger.InfoWithCtx(context.Background(),
			"用户连接成功",
			"user_id", client.UserID(),
			"transport", transport.Name(),
			"protocol", transport.Protocol())
	})

	return wrapper, nil
}

// ExampleHTTPHandler 示例：创建带限流的 HTTP 处理器
func ExampleHTTPHandler(wrapper *NodeWrapper) http.Handler {
	// 使用 wrapper 的 WebsocketHandler 方法，它包含了限流逻辑
	wsHandler := wrapper.WebsocketHandler(centrifuge.WebsocketConfig{
		ReadBufferSize:   1024,
		WriteBufferSize:  1024,
		CheckOrigin:      func(r *http.Request) bool { return true },
		WriteTimeout:     10 * time.Second,
		CompressionLevel: 1,
	})

	// 创建路由
	mux := http.NewServeMux()
	mux.Handle("/connection/websocket", wsHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return mux
}

// ExampleUsage 完整的使用示例
func ExampleUsage() {
	// 1. 初始化 Redis 客户端
	redisOpts := &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}
	redisClient := redis.NewClient(redisOpts)

	// 2. 配置限流参数（支持B/C端用户差异化限流）
	rateLimitConfig := &Config{
		Enabled: true,

		// B端用户（客服）配置
		BUser: UserRateLimitConfig{
			ConnectionsPerUser:     0,   // 不限制连接数
			MessagesPerMinute:      100, // 每分钟100条消息
			MessageBurst:           20,
			SubscriptionsPerMinute: 30, // 每分钟30个订阅
			MaxSubscriptions:       100,
			BroadcastsPerMinute:    60, // 每分钟60个广播
			BroadcastBurst:         10,
		},

		// C端用户（客户）配置
		CUser: UserRateLimitConfig{
			ConnectionsPerUser:     3,  // 严格连接限制
			MessagesPerMinute:      50, // 每分钟50条消息
			MessageBurst:           10,
			SubscriptionsPerMinute: 20, // 每分钟20个订阅
			MaxSubscriptions:       50,
			BroadcastsPerMinute:    10, // 每分钟10个广播
			BroadcastBurst:         5,
		},

		// 时间窗口配置 - 统一使用分钟级别
		ConnectionWindow:   time.Minute,
		MessageWindow:      time.Minute,
		SubscriptionWindow: time.Minute,
		BroadcastWindow:    time.Minute,

		// 其他配置
		WhitelistUsers: []string{},
		RedisTTL:       60 * time.Second,
	}

	// 3. 创建日志记录器
	logger := zaplog.GetGlobalLogger()

	// 4. 初始化带限流的 Node
	wrapper, err := InitializeRateLimitedNode(redisClient, rateLimitConfig, logger)
	if err != nil {
		logger.ErrorWithCtx(context.Background(), err, "初始化 Centrifuge Node 失败")
		return
	}

	// 5. 启动 Node
	if err := wrapper.GetNode().Run(); err != nil {
		logger.ErrorWithCtx(context.Background(), err, "启动 Centrifuge Node 失败")
		return
	}

	// 6. 创建 HTTP 服务器
	handler := ExampleHTTPHandler(wrapper)
	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// 7. 启动 HTTP 服务器
	logger.InfoWithCtx(context.Background(), "Centrifuge 服务器启动，监听端口 :8080")
	if err := server.ListenAndServe(); err != nil {
		logger.ErrorWithCtx(context.Background(), err, "HTTP 服务器错误")
	}
}

// ExampleWithExistingNode 示例：为现有的 Node 添加限流功能
func ExampleWithExistingNode(existingNode *centrifuge.Node, redisClient redis.UniversalClient) *NodeWrapper {
	// 创建限流配置（默认配置）
	config := DefaultConfig()
	config.Enabled = true

	// 创建限流管理器
	logger := zaplog.GetGlobalLogger()
	rateLimiter, err := NewManager(redisClient, config, logger)
	if err != nil {
		logger.ErrorWithCtx(context.Background(), err, "创建限流管理器失败")
		return nil
	}

	// 包装现有的 Node
	wrapper := NewNodeWrapper(existingNode, rateLimiter, logger)

	// 修改现有的 OnConnect 处理器
	existingNode.OnConnect(func(client *centrifuge.Client) {
		// 设置限流处理器
		wrapper.SetupRateLimitHandlers(client)

		// 其他现有的连接处理逻辑...
	})

	return wrapper
}
