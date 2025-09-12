# Centrifuge 限流功能使用指南

## 概述

本模块为 Centrifuge WebSocket 实时通信引擎提供了全面的限流功能，用于防止恶意用户或异常客户端耗尽服务器资源。限流功能遵循 DRY（Don't Repeat Yourself）和 KISS（Keep It Simple, Stupid）原则，复用项目现有的 Redis 限流器，并通过装饰器模式优雅地集成到 Centrifuge 中。

## 功能特性

### 多维度限流
- **连接限流**：限制每个 IP 和每个用户的最大连接数
- **消息限流**：限制每个连接的消息发送速率
- **订阅限流**：限制每个连接的频道订阅速率

### 灵活配置
- 支持启用/禁用整体限流或单项限流
- 可配置限流阈值和时间窗口
- 支持突发流量容忍（burst size）

### 高性能设计
- 基于 Redis 的分布式限流
- 使用固定窗口算法，性能优秀
- 支持集群部署

## 快速开始

### 1. 配置文件

在 `configs/base.yaml` 中添加限流配置：

```yaml
centrifuge:
  rate_limit:
    enabled: true                      # 是否启用限流
    connections_per_ip: 10             # 每个IP最大连接数
    connections_per_user: 5            # 每个用户最大连接数
    messages_per_minute: 1000          # 每分钟最大消息数
    subscriptions_per_minute: 100      # 每分钟最大订阅数
    connection_burst_size: 20          # 连接突发容量
    message_burst_size: 200            # 消息突发容量
    subscription_burst_size: 50        # 订阅突发容量
    cleanup_interval: "5m"             # 清理间隔
    enable_ip_rate_limit: true         # 启用IP限流
    enable_user_rate_limit: true       # 启用用户限流
    enable_message_rate_limit: true    # 启用消息限流
    enable_subscription_limit: true    # 启用订阅限流
```

### 2. 初始化限流

```go
import (
    "github.com/centrifugal/centrifuge"
    "github.com/channelwill/cw2-live-chat-centrifuge/internal/ratelimit"
    "github.com/channelwill/cw2-live-chat-common/pkg/zaplog"
    "github.com/redis/go-redis/v9"
)

func initCentrifugeWithRateLimit() {
    // 1. 创建 Redis 客户端
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // 2. 加载限流配置
    config := &ratelimit.Config{
        Enabled:            true,
        ConnectionsPerIP:   10,
        ConnectionsPerUser: 5,
        MessagesPerMinute:  100,
        // ... 其他配置
    }

    // 3. 创建标准的 Centrifuge Node
    node, _ := centrifuge.New(centrifuge.Config{
        LogLevel: centrifuge.LogLevelInfo,
    })

    // 4. 创建限流管理器
    logger := zaplog.GetGlobalLogger()
    rateLimiter := ratelimit.NewManager(redisClient, config, logger)

    // 5. 创建 NodeWrapper
    wrapper := ratelimit.NewNodeWrapper(node, rateLimiter, logger)

    // 6. 设置连接处理器
    node.OnConnect(func(client *centrifuge.Client) {
        wrapper.SetupRateLimitHandlers(client)
    })

    // 7. 启动 Node
    node.Run()

    // 8. 创建 WebSocket 处理器
    wsHandler := wrapper.WebsocketHandler(centrifuge.WebsocketConfig{
        CheckOrigin: func(r *http.Request) bool { return true },
    })

    // 9. 启动 HTTP 服务
    http.Handle("/connection/websocket", wsHandler)
    http.ListenAndServe(":8080", nil)
}
```

### 3. 与现有系统集成

如果你已经有一个运行中的 Centrifuge 服务，可以这样添加限流：

```go
func addRateLimitToExistingNode(node *centrifuge.Node, redisClient redis.Cmdable) {
    // 创建限流配置
    config := loadRateLimitConfig() // 从配置文件加载
    
    // 创建限流管理器
    logger := zaplog.GetGlobalLogger()
    rateLimiter := ratelimit.NewManager(redisClient, config, logger)
    
    // 包装现有 Node
    wrapper := ratelimit.NewNodeWrapper(node, rateLimiter, logger)
    
    // 修改 OnConnect 处理器
    node.OnConnect(func(client *centrifuge.Client) {
        // 设置限流处理器
        wrapper.SetupRateLimitHandlers(client)
        
        // 你原有的连接处理逻辑...
    })
}
```

## 限流策略详解

### 连接限流

连接限流在两个维度进行控制：

1. **IP 维度**：防止单个 IP 创建过多连接
2. **用户维度**：防止单个用户（通过认证后）创建过多连接

当连接被限流时，会返回相应的错误：
- `ErrorIPConnectionLimitExceeded`：IP 连接数超限
- `ErrorUserConnectionLimitExceeded`：用户连接数超限

### 消息限流

消息限流控制每个客户端的消息发送速率：

- 使用固定窗口算法
- 支持分钟级限流
- 被限流的消息会被丢弃，但不会断开连接

### 订阅限流

订阅限流控制每个客户端的频道订阅速率：

- 防止恶意订阅大量频道
- 被限流时返回 `ErrorSubscriptionRateLimitExceeded`

## 监控和调试

### 获取连接数

```go
// 获取某个 IP 的当前连接数
count, err := rateLimiter.GetConnectionCount(ctx, "ip", "192.168.1.100")

// 获取某个用户的当前连接数
count, err := rateLimiter.GetConnectionCount(ctx, "user", "user_123")
```

### 日志记录

限流模块会记录详细的日志信息：

```go
// 连接被限流
"连接被限流拒绝" ip="192.168.1.100" user_id="user_123"

// 消息被限流
"消息被限流" client_id="client_001"

// 订阅被限流
"订阅被限流" client_id="client_001" channel="chat:room:123"
```

## 性能优化建议

1. **合理设置限流阈值**
   - 根据服务器性能和业务需求调整
   - 建议先从较宽松的限制开始，逐步调优

2. **使用连接池**
   - Redis 连接应使用连接池
   - 避免每次检查都创建新连接

3. **启用部分限流**
   - 如果不需要某些限流功能，可以单独禁用
   - 减少不必要的 Redis 调用

4. **监控 Redis 性能**
   - 限流依赖 Redis，需要确保 Redis 性能良好
   - 考虑使用 Redis 集群或主从复制

## 测试

运行集成测试：

```bash
cd pkg/cw2-live-chat-centrifuge/internal/ratelimit
go test -v -run TestRateLimitIntegration
```

运行性能测试：

```bash
go test -bench=. -benchmem
```

## 故障排除

### 问题：限流不生效

检查点：
1. 确认配置中 `enabled: true`
2. 确认 Redis 连接正常
3. 检查日志中是否有错误信息

### 问题：连接计数不准确

可能原因：
1. 客户端异常断开，计数未正确减少
2. Redis key 过期时间设置不当

解决方案：
- 确保正确处理 OnDisconnect 事件
- 调整 cleanup_interval 配置

### 问题：性能下降

优化建议：
1. 增加 Redis 连接池大小
2. 考虑禁用不需要的限流维度
3. 调整限流时间窗口

## 架构设计

### 核心组件

1. **Manager**：限流管理器，负责所有限流逻辑
2. **NodeWrapper**：Centrifuge Node 的装饰器，注入限流功能
3. **Config**：限流配置管理
4. **ConnectionInfo**：连接信息追踪

### 设计原则

- **DRY 原则**：复用 `gredis.Limiter`，避免重复实现
- **KISS 原则**：使用简单的装饰器模式，不修改 Centrifuge 核心代码
- **单一职责**：每个组件职责明确，易于维护和测试

## 扩展开发

### 添加新的限流维度

1. 在 `Config` 中添加配置项
2. 在 `Manager` 中实现检查方法
3. 在 `NodeWrapper` 中集成到相应的处理器
4. 在 `keys` 包中定义 Redis key

### 自定义限流算法

如果需要更复杂的限流算法（如令牌桶、漏桶），可以：

1. 实现新的 Limiter 接口
2. 在 Manager 中切换使用
3. 保持对外接口不变

## 相关文档

- [Centrifuge 官方文档](https://centrifugal.dev/)
- [Redis 限流最佳实践](https://redis.io/docs/manual/patterns/rate-limiting/)
- [项目整体架构](../../CLAUDE.md)