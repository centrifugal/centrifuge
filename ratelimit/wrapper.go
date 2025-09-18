package ratelimit

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/centrifugal/centrifuge"
	"github.com/channelwill/cw2-live-chat-common/pkg/zaplog"
)

// NodeWrapper 包装 Centrifuge Node，添加限流功能
// 这是一个装饰器模式的实现，遵循 KISS 原则
type NodeWrapper struct {
	node           *centrifuge.Node
	rateLimiter    *Manager
	logger         zaplog.LiveChatLogger
	connectionInfo map[string]*ConnectionInfo // clientID -> ConnectionInfo
}

// NewNodeWrapper 创建带限流功能的 Node 包装器
func NewNodeWrapper(node *centrifuge.Node, rateLimiter *Manager, logger zaplog.LiveChatLogger) *NodeWrapper {
	return &NodeWrapper{
		node:           node,
		rateLimiter:    rateLimiter,
		logger:         logger,
		connectionInfo: make(map[string]*ConnectionInfo),
	}
}

// SetupRateLimitHandlers 设置限流处理器
// 这个方法在 OnConnect 回调中调用，此时 Client 已经认证完成
func (w *NodeWrapper) SetupRateLimitHandlers(client *centrifuge.Client) {
	// 获取用户信息（复用现有的认证信息，遵循 DRY 原则）
	userID := client.UserID()
	clientID := client.ID()

	// 从客户端上下文中提取用户类型
	userType := w.extractUserType(client)

	// 创建连接信息
	info := &ConnectionInfo{
		ClientID: clientID,
		UserID:   userID,
		UserType: userType,
	}

	// 检查连接限流（基于用户ID和用户类型）
	ctx := context.Background()
	if !w.rateLimiter.CheckConnectionLimit(ctx, userID, userType) {
		w.logger.WarnWithCtx(ctx, "连接被限流拒绝",
			"user_id", userID,
			"user_type", userType,
			"client_id", clientID)

		// 断开连接
		disconnectReason := centrifuge.DisconnectConnectionLimit
		w.logger.InfoWithCtx(ctx, "用户连接数超限，断开连接",
			"user_id", userID, "user_type", userType)
		client.Disconnect(disconnectReason)
		return
	}

	// 增加连接计数
	if err := w.rateLimiter.IncrementConnectionCount(ctx, userID); err != nil {
		w.logger.ErrorWithCtx(ctx, err, "增加连接计数失败")
	}

	// 保存连接信息
	w.connectionInfo[clientID] = info

	// 包装 OnMessage 处理器 - 添加消息限流
	client.OnMessage(func(e centrifuge.MessageEvent) {
		// 检查消息限流（使用用户类型）
		if !w.rateLimiter.CheckMessageLimit(ctx, clientID, userType) {
			w.logger.WarnWithCtx(ctx, "消息被限流",
				"client_id", clientID,
				"user_type", userType)
			// 消息限流时不返回错误，只是丢弃消息
			// 避免断开连接
			return
		}

		// 注意：由于我们在 OnConnect 中设置处理器，无法调用原始处理器
		// 原始处理器需要在调用 SetupRateLimitHandlers 之前设置
	})

	// 包装 OnSubscribe 处理器 - 添加订阅限流
	client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
		// 检查订阅限流（使用用户类型）
		if !w.rateLimiter.CheckSubscriptionLimit(ctx, clientID, userType) {
			w.logger.WarnWithCtx(ctx, "订阅被限流",
				"client_id", clientID,
				"user_type", userType,
				"channel", e.Channel)
			cb(centrifuge.SubscribeReply{}, centrifuge.ErrorLimitExceeded)
			return
		}

		// 允许订阅
		cb(centrifuge.SubscribeReply{
			Options: centrifuge.SubscribeOptions{
				EnableRecovery:    true,
				EnablePositioning: true,
			},
		}, nil)
	})

	// 包装 OnDisconnect 处理器 - 清理连接计数
	client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
		// 获取连接信息
		if info, ok := w.connectionInfo[clientID]; ok {
			// 减少连接计数
			if err := w.rateLimiter.DecrementConnectionCount(ctx, info.UserID); err != nil {
				w.logger.ErrorWithCtx(ctx, err, "减少连接计数失败")
			}

			// 删除连接信息
			delete(w.connectionInfo, clientID)
		}
	})
}

// GetNode 获取原始的 Centrifuge Node
func (w *NodeWrapper) GetNode() *centrifuge.Node {
	return w.node
}

// extractUserType 从客户端上下文中提取用户类型
// 根据业务需要从 token 或其他上下文信息中提取用户类型
func (w *NodeWrapper) extractUserType(client *centrifuge.Client) UserType {
	// 方法 1: 从 client context 中获取
	if ctx := client.Context(); ctx != nil {
		if userTypeValue := ctx.Value("user_type"); userTypeValue != nil {
			if userTypeStr, ok := userTypeValue.(string); ok {
				return ParseUserType(userTypeStr)
			}
		}
	}

	// 方法 2: 从 client.Info() 中解析用户类型（从 Gateway 传递的 userContext）
	if info := client.Info(); len(info) > 0 {
		// 解析完整的 authpb.UserContext 结构（从 Gateway 传递）
		var userContext struct {
			UserId   string `json:"user_id"`
			UserType string `json:"user_type"`
			OrgId    string `json:"org_id"`
			BrandId  string `json:"brand_id"`
			ShopId   string `json:"shop_id"`
			// 其他字段...
		}
		if err := json.Unmarshal(info, &userContext); err == nil && userContext.UserType != "" {
			// 成功解析用户上下文，提取用户类型
			w.logger.DebugFormat(context.Background(), "从用户上下文解析用户类型成功: user_id=%s, user_type=%s",
				userContext.UserId, userContext.UserType)
			return ParseUserType(userContext.UserType)
		}

		// 调试信息：记录无法解析的原始数据
		previewLen := len(info)
		if previewLen > 100 {
			previewLen = 100
		}
		w.logger.WarnFormat(context.Background(), "无法解析用户上下文中的用户类型: raw_info_size=%d, raw_info_preview=%s",
			len(info), string(info[:previewLen]))
	}

	// 无法从可信来源确定用户类型，记录错误并拒绝连接
	// 不应该基于用户ID格式进行猜测，这是不安全的
	userID := client.UserID()
	w.logger.ErrorWithCtx(context.Background(), nil, "无法从可信来源确定用户类型，连接将被拒绝", "user_id", userID)

	// 返回一个特殊的未知类型，让调用者处理拒绝逻辑
	// 而不是默认给予任何权限
	return Unknown
}

// WebsocketHandler 创建带限流的 WebSocket 处理器（简化版，专注于用户ID和类型限流）
func (w *NodeWrapper) WebsocketHandler(config centrifuge.WebsocketConfig) http.Handler {
	handler := centrifuge.NewWebsocketHandler(w.node, config)

	// 返回原始处理器，实际的用户限流在SetupRateLimitHandlers中处理
	// 这样遵循KISS原则，避免在两个地方重复处理限流
	return handler
}
