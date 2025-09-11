package ratelimit

import "context"

// Limiter 限流器接口
// 这个接口已经在 gredis 包中定义，这里为了包的独立性重新定义
type Limiter interface {
	// CheckAllow 检查是否允许通过
	CheckAllow(ctx context.Context) bool
}

// ConnectionInfo 连接信息（专注于基于用户ID和用户类型的限流）
type ConnectionInfo struct {
	ClientID string   // 客户端ID
	UserID   string   // 用户ID
	UserType UserType // 用户类型（B端/C端）
}

// RateLimitResult 限流检查结果
type RateLimitResult struct {
	Allowed bool   // 是否允许
	Reason  string // 拒绝原因（如果被拒绝）
	Limit   uint   // 限制值
	Current uint   // 当前值
}
