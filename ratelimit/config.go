package ratelimit

import (
	"time"
)

// UserRateLimitConfig 用户类型限流配置
type UserRateLimitConfig struct {
	// ConnectionsPerUser 单个用户最大连接数（0表示不限制）
	ConnectionsPerUser uint `yaml:"connections_per_user" json:"connections_per_user"`

	// MessagesPerSecond 每秒最大消息数（单个连接）
	MessagesPerSecond uint `yaml:"messages_per_second" json:"messages_per_second"`

	// MessageBurst 消息突发容量
	MessageBurst uint `yaml:"message_burst" json:"message_burst"`

	// SubscriptionsPerMinute 每分钟最大订阅数（单个连接）
	SubscriptionsPerMinute uint `yaml:"subscriptions_per_minute" json:"subscriptions_per_minute"`

	// MaxSubscriptions 单个连接最大订阅总数
	MaxSubscriptions uint `yaml:"max_subscriptions" json:"max_subscriptions"`

	// BroadcastsPerSecond 每秒最大广播数（单个频道）
	BroadcastsPerSecond uint `yaml:"broadcasts_per_second" json:"broadcasts_per_second"`

	// BroadcastBurst 广播突发容量
	BroadcastBurst uint `yaml:"broadcast_burst" json:"broadcast_burst"`
}

// Config 限流配置
type Config struct {
	// 是否启用限流
	Enabled bool `yaml:"enabled" json:"enabled"`

	// B端用户（客服）限流配置
	BUser UserRateLimitConfig `yaml:"b_user" json:"b_user"`

	// C端用户（客户）限流配置
	CUser UserRateLimitConfig `yaml:"c_user" json:"c_user"`

	// ========== 时间窗口配置 ==========

	// ConnectionWindow 连接限流时间窗口
	ConnectionWindow time.Duration `yaml:"connection_window" json:"connection_window"`

	// MessageWindow 消息限流时间窗口
	MessageWindow time.Duration `yaml:"message_window" json:"message_window"`

	// SubscriptionWindow 订阅限流时间窗口
	SubscriptionWindow time.Duration `yaml:"subscription_window" json:"subscription_window"`

	// BroadcastWindow 广播限流时间窗口
	BroadcastWindow time.Duration `yaml:"broadcast_window" json:"broadcast_window"`

	// ========== 白名单配置 ==========

	// WhitelistUsers 免限流的用户ID列表
	WhitelistUsers []string `yaml:"whitelist_users" json:"whitelist_users"`

	// ========== Redis配置 ==========

	// RedisTTL Redis key过期时间
	RedisTTL time.Duration `yaml:"redis_ttl" json:"redis_ttl"`
}

// DefaultConfig 返回默认的限流配置
func DefaultConfig() *Config {
	return &Config{
		Enabled: false, // 默认关闭，需要显式启用

		// B端用户（客服）配置 - 宽松限制
		BUser: UserRateLimitConfig{
			ConnectionsPerUser:     0,   // 不限制连接数（客服需要多设备同时在线）
			MessagesPerSecond:      500, // 客服需要快速回复大量消息
			MessageBurst:           1000,
			SubscriptionsPerMinute: 100, // 客服需要订阅多个会话频道
			MaxSubscriptions:       500,
			BroadcastsPerSecond:    100, // 客服可能需要群发消息
			BroadcastBurst:         200,
		},

		// C端用户（客户）配置 - 严格限制
		CUser: UserRateLimitConfig{
			ConnectionsPerUser:     3,  // 客户限制3个连接（手机+电脑+平板）
			MessagesPerSecond:      50, // 普通客户发送消息频率较低
			MessageBurst:           100,
			SubscriptionsPerMinute: 20, // 客户订阅频道较少
			MaxSubscriptions:       50,
			BroadcastsPerSecond:    10, // 客户很少需要广播
			BroadcastBurst:         20,
		},

		// 时间窗口配置
		ConnectionWindow:   60 * time.Second,
		MessageWindow:      time.Second,
		SubscriptionWindow: time.Minute,
		BroadcastWindow:    time.Second,

		// 白名单
		WhitelistUsers: []string{},

		// Redis
		RedisTTL: 60 * time.Second,
	}
}

// Validate 验证配置的合法性
func (c *Config) Validate() error {
	// 验证B端用户配置
	c.validateUserConfig(&c.BUser, "B端用户")

	// 验证C端用户配置
	c.validateUserConfig(&c.CUser, "C端用户")

	// 时间窗口配置验证
	if c.ConnectionWindow == 0 {
		c.ConnectionWindow = 60 * time.Second
	}
	if c.MessageWindow == 0 {
		c.MessageWindow = time.Second
	}
	if c.SubscriptionWindow == 0 {
		c.SubscriptionWindow = time.Minute
	}
	if c.BroadcastWindow == 0 {
		c.BroadcastWindow = time.Second
	}

	// Redis TTL验证
	if c.RedisTTL == 0 {
		c.RedisTTL = 60 * time.Second
	}

	return nil
}

// validateUserConfig 验证用户类型配置的合法性
func (c *Config) validateUserConfig(userConfig *UserRateLimitConfig, userType string) {
	// 消息限流验证（0表示不限制，但需要设置合理的默认值）
	if userConfig.MessagesPerSecond == 0 {
		if userType == "B端用户" {
			userConfig.MessagesPerSecond = 500 // B端用户默认较高限制
		} else {
			userConfig.MessagesPerSecond = 50 // C端用户默认较低限制
		}
	}
	if userConfig.MessageBurst == 0 {
		userConfig.MessageBurst = userConfig.MessagesPerSecond * 2
	}

	// 订阅限流验证
	if userConfig.SubscriptionsPerMinute == 0 {
		if userType == "B端用户" {
			userConfig.SubscriptionsPerMinute = 100
		} else {
			userConfig.SubscriptionsPerMinute = 20
		}
	}
	if userConfig.MaxSubscriptions == 0 {
		if userType == "B端用户" {
			userConfig.MaxSubscriptions = 500
		} else {
			userConfig.MaxSubscriptions = 50
		}
	}

	// 广播限流验证
	if userConfig.BroadcastsPerSecond == 0 {
		if userType == "B端用户" {
			userConfig.BroadcastsPerSecond = 100
		} else {
			userConfig.BroadcastsPerSecond = 10
		}
	}
	if userConfig.BroadcastBurst == 0 {
		userConfig.BroadcastBurst = userConfig.BroadcastsPerSecond * 2
	}
}

// IsUserWhitelisted 检查用户是否在白名单中
func (c *Config) IsUserWhitelisted(userID string) bool {
	for _, whitelistedUser := range c.WhitelistUsers {
		if whitelistedUser == userID {
			return true
		}
	}
	return false
}
