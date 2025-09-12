package ratelimit

import (
	"time"
)

// UserRateLimitConfig 用户类型限流配置
type UserRateLimitConfig struct {
	// ConnectionsPerUser 单个用户最大连接数（0表示不限制）
	ConnectionsPerUser uint `yaml:"connections_per_user" json:"connections_per_user"`

	// MessagesPerMinute 每分钟最大消息数（单个连接）
	MessagesPerMinute uint `yaml:"messages_per_minute" json:"messages_per_minute"`

	// MessageBurst 消息突发容量
	MessageBurst uint `yaml:"message_burst" json:"message_burst"`

	// SubscriptionsPerMinute 每分钟最大订阅数（单个连接）
	SubscriptionsPerMinute uint `yaml:"subscriptions_per_minute" json:"subscriptions_per_minute"`

	// MaxSubscriptions 单个连接最大订阅总数
	MaxSubscriptions uint `yaml:"max_subscriptions" json:"max_subscriptions"`

	// BroadcastsPerMinute 每分钟最大广播数（单个频道）
	BroadcastsPerMinute uint `yaml:"broadcasts_per_minute" json:"broadcasts_per_minute"`

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

		// B端用户（客服）配置 - 合理限制
		BUser: UserRateLimitConfig{
			ConnectionsPerUser:     0,   // 不限制连接数（客服需要多设备同时在线）
			MessagesPerMinute:      100, // 客服每分钟100条消息
			MessageBurst:           20,
			SubscriptionsPerMinute: 30, // 客服每分钟30个订阅
			MaxSubscriptions:       100,
			BroadcastsPerMinute:    60, // 客服每分钟60个广播
			BroadcastBurst:         10,
		},

		// C端用户（客户）配置 - 严格限制
		CUser: UserRateLimitConfig{
			ConnectionsPerUser:     3,  // 客户限制3个连接（手机+电脑+平板）
			MessagesPerMinute:      50, // 普通客户每分钟50条消息
			MessageBurst:           10,
			SubscriptionsPerMinute: 20, // 客户订阅频道较少
			MaxSubscriptions:       50,
			BroadcastsPerMinute:    10, // 客户很少需要广播
			BroadcastBurst:         5,
		},

		// 时间窗口配置 - 统一使用分钟级别
		ConnectionWindow:   time.Minute,
		MessageWindow:      time.Minute,
		SubscriptionWindow: time.Minute,
		BroadcastWindow:    time.Minute,

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

	// 时间窗口配置验证 - 统一使用分钟级别
	if c.ConnectionWindow == 0 {
		c.ConnectionWindow = time.Minute
	}
	if c.MessageWindow == 0 {
		c.MessageWindow = time.Minute
	}
	if c.SubscriptionWindow == 0 {
		c.SubscriptionWindow = time.Minute
	}
	if c.BroadcastWindow == 0 {
		c.BroadcastWindow = time.Minute
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
	if userConfig.MessagesPerMinute == 0 {
		if userType == "B端用户" {
			userConfig.MessagesPerMinute = 100 // B端用户默认限制
		} else {
			userConfig.MessagesPerMinute = 50 // C端用户默认限制
		}
	}
	if userConfig.MessageBurst == 0 {
		if userType == "B端用户" {
			userConfig.MessageBurst = 20
		} else {
			userConfig.MessageBurst = 10
		}
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
	if userConfig.BroadcastsPerMinute == 0 {
		if userType == "B端用户" {
			userConfig.BroadcastsPerMinute = 60
		} else {
			userConfig.BroadcastsPerMinute = 10
		}
	}
	if userConfig.BroadcastBurst == 0 {
		if userType == "B端用户" {
			userConfig.BroadcastBurst = 10
		} else {
			userConfig.BroadcastBurst = 5
		}
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
