package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/channelwill/cw2-live-chat-common/configx"
	"github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis"
	"github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis/keys"
	"github.com/channelwill/cw2-live-chat-common/pkg/zaplog"
	"github.com/redis/go-redis/v9"
)

// UserType 用户类型枚举
type UserType string

const (
	// BUser B端用户（客服）- 使用 configx 定义的值
	BUser UserType = UserType(configx.BUSER_TYPE)
	// CUser C端用户（客户）- 使用 configx 定义的值
	CUser UserType = UserType(configx.CUSER_TYPE)
	// Unknown 未知用户类型（应该拒绝连接）
	Unknown UserType = UserType(configx.UNKNOWN_TYPE)
)

// Manager 限流管理器
// 统一管理所有限流器，提供简单的接口供Centrifuge使用
type Manager struct {
	redisClient redis.UniversalClient
	config      *Config
	limiters    sync.Map // 缓存限流器实例，避免重复创建
	keyManager  *keys.LiveChatKeys
	logger      zaplog.LiveChatLogger
}

// NewManager 创建限流管理器
func NewManager(redisClient redis.UniversalClient, config *Config, logger zaplog.LiveChatLogger) (*Manager, error) {
	if redisClient == nil {
		return nil, fmt.Errorf("redis client不能为空")
	}

	if config == nil {
		config = DefaultConfig()
	}

	// 验证配置
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return &Manager{
		redisClient: redisClient,
		config:      config,
		keyManager:  keys.NewLiveChatKeys(),
		logger:      logger,
	}, nil
}

// CheckConnectionLimit 检查连接限制（基于用户ID和用户类型）
// 返回true表示允许连接，false表示拒绝
func (m *Manager) CheckConnectionLimit(ctx context.Context, userID string, userType UserType) bool {
	// 如果限流未启用，直接放行
	if !m.config.Enabled {
		return true
	}

	// 检查白名单
	if m.config.IsUserWhitelisted(userID) {
		return true
	}

	// 检查用户维度的连接限制
	if userID != "" && !m.checkUserConnectionLimit(ctx, userID, userType) {
		userConfig := m.getUserRateLimitConfig(userType)
		m.logger.WarnWithCtx(ctx, "用户连接数超限",
			"user_id", userID,
			"user_type", userType,
			"limit", userConfig.ConnectionsPerUser)
		return false
	}

	return true
}

// CheckMessageLimit 检查消息限流（基于用户类型）
// 返回true表示允许发送消息，false表示拒绝
func (m *Manager) CheckMessageLimit(ctx context.Context, clientID string, userType UserType) bool {
	// 如果限流未启用，直接放行
	if !m.config.Enabled {
		return true
	}

	// 获取用户类型对应的配置
	userConfig := m.getUserRateLimitConfig(userType)

	// 获取或创建消息限流器
	limiter, err := m.getOrCreateLimiter(
		m.keyManager.CentrifugeMessageLimit(clientID),
		userConfig.MessagesPerMinute,
		m.config.MessageWindow,
	)
	if err != nil {
		m.logger.ErrorWithCtx(ctx, fmt.Errorf("创建消息限流器失败: %w", err), "client_id", clientID)
		// 出错时默认放行，避免影响正常业务
		return true
	}

	// 检查是否允许
	allowed := limiter.CheckAllow(ctx)
	if !allowed {
		m.logger.DebugWithCtx(ctx, "消息限流触发",
			"client_id", clientID,
			"user_type", userType,
			"limit", userConfig.MessagesPerMinute,
			"window", m.config.MessageWindow)
	}

	return allowed
}

// CheckSubscriptionLimit 检查订阅限流（基于用户类型）
// 返回true表示允许订阅，false表示拒绝
func (m *Manager) CheckSubscriptionLimit(ctx context.Context, clientID string, userType UserType) bool {
	// 如果限流未启用，直接放行
	if !m.config.Enabled {
		return true
	}

	// 获取用户类型对应的配置
	userConfig := m.getUserRateLimitConfig(userType)

	// 获取或创建订阅限流器
	limiter, err := m.getOrCreateLimiter(
		m.keyManager.CentrifugeSubscriptionLimit(clientID),
		userConfig.SubscriptionsPerMinute,
		m.config.SubscriptionWindow,
	)
	if err != nil {
		m.logger.ErrorWithCtx(ctx, fmt.Errorf("创建订阅限流器失败: %w", err), "client_id", clientID)
		// 出错时默认放行
		return true
	}

	// 检查是否允许
	allowed := limiter.CheckAllow(ctx)
	if !allowed {
		m.logger.DebugWithCtx(ctx, "订阅限流触发",
			"client_id", clientID,
			"user_type", userType,
			"limit", userConfig.SubscriptionsPerMinute,
			"window", m.config.SubscriptionWindow)
	}

	return allowed
}

// CheckBroadcastLimit 检查广播限流
// 返回true表示允许广播，false表示拒绝
func (m *Manager) CheckBroadcastLimit(ctx context.Context, channel string) bool {
	// 如果限流未启用，直接放行
	if !m.config.Enabled {
		return true
	}

	// 获取或创建广播限流器（使用B端用户的广播限制，因为C端用户很少需要广播）
	limiter, err := m.getOrCreateLimiter(
		m.keyManager.CentrifugeBroadcastLimit(channel),
		m.config.BUser.BroadcastsPerMinute,
		m.config.BroadcastWindow,
	)
	if err != nil {
		m.logger.ErrorWithCtx(ctx, fmt.Errorf("创建广播限流器失败: %w", err), "channel", channel)
		// 出错时默认放行
		return true
	}

	// 检查是否允许
	allowed := limiter.CheckAllow(ctx)
	if !allowed {
		m.logger.DebugWithCtx(ctx, "广播限流触发",
			"channel", channel,
			"limit", m.config.BUser.BroadcastsPerMinute,
			"window", m.config.BroadcastWindow)
	}

	return allowed
}

// IncrementConnectionCount 增加连接计数（基于用户ID）
func (m *Manager) IncrementConnectionCount(ctx context.Context, userID string) error {
	// 用户维度计数
	if userID != "" {
		userKey := m.keyManager.CentrifugeConnectionCount("user", userID)
		if err := m.redisClient.Incr(ctx, userKey).Err(); err != nil {
			return fmt.Errorf("增加用户连接计数失败: %w", err)
		}
		// 设置过期时间
		m.redisClient.Expire(ctx, userKey, m.config.RedisTTL)
	}

	return nil
}

// DecrementConnectionCount 减少连接计数（基于用户ID）
func (m *Manager) DecrementConnectionCount(ctx context.Context, userID string) error {
	// 用户维度计数
	if userID != "" {
		userKey := m.keyManager.CentrifugeConnectionCount("user", userID)
		count, err := m.redisClient.Decr(ctx, userKey).Result()
		if err != nil {
			return fmt.Errorf("减少用户连接计数失败: %w", err)
		}
		// 如果计数为0或负数，删除key
		if count <= 0 {
			m.redisClient.Del(ctx, userKey)
		}
	}

	return nil
}

// GetConnectionCount 获取当前连接数
func (m *Manager) GetConnectionCount(ctx context.Context, dimension, id string) (int64, error) {
	key := m.keyManager.CentrifugeConnectionCount(dimension, id)
	count, err := m.redisClient.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return count, err
}

// RecordMetrics 记录限流指标
func (m *Manager) RecordMetrics(ctx context.Context, metricType string, value int64) {
	date := time.Now().Format("2006-01-02")
	key := m.keyManager.CentrifugeRateLimitMetrics(date)
	field := fmt.Sprintf("%s:%d", metricType, time.Now().Unix())

	// 使用Redis Hash存储指标
	m.redisClient.HSet(ctx, key, field, value)
	// 设置24小时过期
	m.redisClient.Expire(ctx, key, 24*time.Hour)
}

// ========== 内部辅助方法 ==========

// checkUserConnectionLimit 检查用户维度的连接限制（基于用户类型）
func (m *Manager) checkUserConnectionLimit(ctx context.Context, userID string, userType UserType) bool {
	// 获取用户类型对应的配置
	userConfig := m.getUserRateLimitConfig(userType)

	// 如果该用户类型的连接数限制为0，表示不限制
	if userConfig.ConnectionsPerUser == 0 {
		m.logger.DebugWithCtx(ctx, "用户类型连接数不限制",
			"user_id", userID,
			"user_type", userType)
		return true
	}

	count, err := m.GetConnectionCount(ctx, "user", userID)
	if err != nil {
		m.logger.ErrorWithCtx(ctx, fmt.Errorf("获取用户连接数失败: %w", err), "user_id", userID)
		// 出错时默认放行
		return true
	}

	allowed := uint(count) < userConfig.ConnectionsPerUser
	m.logger.DebugWithCtx(ctx, "检查用户连接限制",
		"user_id", userID,
		"user_type", userType,
		"current_count", count,
		"limit", userConfig.ConnectionsPerUser,
		"allowed", allowed)

	return allowed
}

// getOrCreateLimiter 获取或创建限流器
func (m *Manager) getOrCreateLimiter(key string, limit uint, window time.Duration) (gredis.Limiter, error) {
	// 先从缓存中查找
	if cached, ok := m.limiters.Load(key); ok {
		return cached.(gredis.Limiter), nil
	}

	// 创建新的限流器
	limiter, err := gredis.NewRedisFixedLimiter(key, limit, window, m.redisClient)
	if err != nil {
		return nil, err
	}

	// 缓存限流器
	m.limiters.Store(key, limiter)

	return limiter, nil
}

// CleanupExpiredLimiters 清理过期的限流器缓存
// 建议定期调用，避免内存泄漏
func (m *Manager) CleanupExpiredLimiters() {
	// 清空所有缓存的限流器
	// 由于限流器本身是无状态的，可以安全地清理
	m.limiters = sync.Map{}
}

// GetConfig 获取当前配置
func (m *Manager) GetConfig() *Config {
	return m.config
}

// UpdateConfig 动态更新配置
func (m *Manager) UpdateConfig(config *Config) error {
	if err := config.Validate(); err != nil {
		return fmt.Errorf("配置验证失败: %w", err)
	}

	m.config = config
	// 清理缓存的限流器，使用新配置重新创建
	m.CleanupExpiredLimiters()

	return nil
}

// IsEnabled 检查限流是否启用
func (m *Manager) IsEnabled() bool {
	return m.config.Enabled
}

// getUserRateLimitConfig 根据用户类型获取对应的限流配置
func (m *Manager) getUserRateLimitConfig(userType UserType) UserRateLimitConfig {
	switch userType {
	case BUser:
		return m.config.BUser
	case CUser:
		return m.config.CUser
	case Unknown:
		// 未知用户类型，返回阻止性配置（0 rate limit）
		m.logger.ErrorWithCtx(context.Background(), nil, "拒绝未知用户类型的访问", "user_type", string(userType))
		return UserRateLimitConfig{
			ConnectionsPerUser:     0, // 0 means no connections allowed
			MessagesPerMinute:      0, // 0 means no messages allowed
			MessageBurst:           0, // 0 burst capacity
			SubscriptionsPerMinute: 0, // 0 means no subscriptions allowed
			MaxSubscriptions:       0, // 0 max subscriptions
			BroadcastsPerMinute:    0, // 0 broadcasts allowed
			BroadcastBurst:         0, // 0 broadcast burst
		}
	default:
		// 这种情况理论上不应该发生，但为了安全起见仍然拒绝访问
		m.logger.ErrorWithCtx(context.Background(), nil, "完全未知的用户类型，拒绝访问", "user_type", string(userType))
		return UserRateLimitConfig{
			ConnectionsPerUser:     0, // 0 means no connections allowed
			MessagesPerMinute:      0, // 0 means no messages allowed
			MessageBurst:           0, // 0 burst capacity
			SubscriptionsPerMinute: 0, // 0 means no subscriptions allowed
			MaxSubscriptions:       0, // 0 max subscriptions
			BroadcastsPerMinute:    0, // 0 broadcasts allowed
			BroadcastBurst:         0, // 0 broadcast burst
		}
	}
}

// ParseUserType 从字符串解析用户类型
func ParseUserType(userTypeStr string) UserType {
	// 直接转换并比较
	userType := UserType(userTypeStr)
	switch userType {
	case BUser:
		return BUser
	case CUser:
		return CUser
	default:
		// 无法识别的用户类型，返回Unknown让调用者处理拒绝逻辑
		return Unknown
	}
}
