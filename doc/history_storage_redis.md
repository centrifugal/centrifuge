# Redis History Storage 使用指南

基于 Common 包的 Redis 历史存储实现，解决分布式环境下的历史消息丢失问题。

## 🎯 为什么选择 Redis 而不是 Kafka 存储历史消息？

### Kafka vs Redis 在历史消息场景的差异

| 特性         | Kafka                        | Redis Streams            |
| ------------ | ---------------------------- | ------------------------ |
| **访问模式** | 顺序访问，基于 offset        | 支持高效随机访问         |
| **查询性能** | 需要扫描消息才能找到特定位置 | 直接根据 ID/时间范围查询 |
| **内存占用** | 较低，主要在磁盘             | 较高，主要在内存         |
| **持久化**   | 原生支持，可配置多种策略     | 支持 RDB + AOF           |
| **TTL 支持** | 基于时间或大小的策略删除     | 原生支持 TTL             |
| **分区支持** | 原生分区支持                 | 需要应用层实现           |

### 实时通信场景的特殊需求

```go
// 用户重连时的典型场景
func (client *Client) onReconnect() {
    // 需要快速获取特定 channel 的最近消息
    history, _ := broker.History("chat:room:123", HistoryOptions{
        Filter: HistoryFilter{
            Since: &StreamPosition{Offset: lastOffset, Epoch: epoch},
            Limit: 50,
        },
    })
    
    // 发送给客户端进行消息同步  
    client.sendHistory(history)
}
```

**Redis 的优势**：
- 🚀 **亚毫秒级查询**：根据时间范围直接查询
- 🎯 **精确过滤**：支持 `Since` 位置的精确恢复
- 💾 **自动过期**：TTL 自动清理历史消息
- 🔄 **高并发读取**：支持大量客户端同时查询历史

## 📖 使用示例

### 1. 基本配置和初始化

```go
package main

import (
    "context"
    
    "github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis"
    "github.com/channelwill/cw2-live-chat-centrifuge"
)

func main() {
    // 1. 配置 Redis
    redisConf := gredis.RedisConf{
        Address:     "localhost:6379",
        Password:    "your-password",
        DB:          0,
        PoolSize:    10,
        DialTimeout: 5 * time.Second,
    }
    
    // 2. 创建 Redis 历史存储
    redisStorage := centrifuge.NewRedisHistoryStorageFromConf(redisConf, "livechat")
    
    // 3. 配置 Kafka Broker 使用 Redis 历史存储
    kafkaConfig := centrifuge.KafkaBrokerConfig{
        KafkaConfig: gkafka.KafkaConfig{
            Brokers: []string{"localhost:9092"},
        },
        TopicPrefix:    "livechat",
        HistoryStorage: redisStorage, // 使用 Redis 存储历史消息
    }
    
    // 4. 创建 Centrifuge Node
    node, _ := centrifuge.New(centrifuge.Config{})
    
    // 5. 创建 Kafka Broker
    broker, _ := centrifuge.NewKafkaBroker(node, kafkaConfig)
    
    // 6. 设置 Broker
    node.SetBroker(broker)
}
```

### 2. 高级配置

```go
// 使用更详细的配置
redisClient := gredis.RedisConf{
    Address:         "redis-cluster.example.com:6379",
    Password:        "secure-password",
    DB:              1,
    MaxRetries:      3,
    DialTimeout:     5 * time.Second,
    ReadTimeout:     3 * time.Second,
    WriteTimeout:    3 * time.Second,
    PoolSize:        20,
    PoolTimeout:     4 * time.Second,
}.InitClient()

// 创建存储实例
redisStorage := centrifuge.NewRedisHistoryStorage(centrifuge.RedisHistoryStorageConfig{
    Client: redisClient,
    Prefix: "livechat",         // Redis key 前缀
    Name:   "production-redis", // 用于监控和日志
})

// 健康检查
if err := redisStorage.Health(context.Background()); err != nil {
    log.Fatal("Redis health check failed:", err)
}

// 获取统计信息
stats, _ := redisStorage.Stats(context.Background())
log.Printf("Storage stats: %+v", stats)
```

### 3. 集成到现有项目

```go
// 如果你已经有 Kafka Broker，只需要替换历史存储
func upgradeToRedisHistory(node *centrifuge.Node, existingBroker *centrifuge.KafkaBroker) {
    // 创建 Redis 存储
    redisConf := gredis.RedisConf{
        Address: os.Getenv("REDIS_URL"),
        Password: os.Getenv("REDIS_PASSWORD"),
    }
    
    redisStorage := centrifuge.NewRedisHistoryStorageFromConf(redisConf, "upgrade")
    
    // 重新配置 Broker
    newConfig := centrifuge.KafkaBrokerConfig{
        KafkaConfig:    existingBroker.GetKafkaConfig(), // 保持 Kafka 配置不变
        HistoryStorage: redisStorage,                    // 只升级历史存储
    }
    
    newBroker, _ := centrifuge.NewKafkaBroker(node, newConfig)
    node.SetBroker(newBroker)
}
```

## 🏗️ Redis Key 结构

### 历史消息存储

```
livechat:history:stream:chat:room:123  # Redis Stream 存储消息
livechat:history:meta:chat:room:123    # Hash 存储元数据
```

### 数据结构详解

**Stream 结构**：
```
XRANGE livechat:history:stream:chat:room:123 - +
1) 1) "1609459200000-0"
   2) 1) "data"
      2) "<protobuf-encoded-publication>"
```

**Meta 结构**：
```
HGETALL livechat:history:meta:chat:room:123
1) "epoch"
2) "a1b2c3d4-1609459200"
3) "updated_at"  
4) "1609459200"
```

## 🔧 性能优化建议

### 1. Redis 配置优化

```conf
# redis.conf 建议配置
maxmemory 8gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000

# Stream 相关配置
stream-node-max-bytes 4096
stream-node-max-entries 100
```

### 2. 连接池配置

```go
redisConf := gredis.RedisConf{
    PoolSize:        runtime.NumCPU() * 4, // 根据 CPU 核心数调整
    PoolTimeout:     4 * time.Second,
    ConnMaxLifetime: 30 * time.Minute,
    ConnMaxIdleTime: 5 * time.Minute,
}
```

### 3. 历史消息策略

```go
opts := centrifuge.PublishOptions{
    HistorySize: 1000,           // 每个 channel 保留 1000 条消息
    HistoryTTL:  24 * time.Hour, // 消息保留 24 小时
}
```

## 📊 监控和运维

### 1. 健康检查

```go
func healthCheck(storage *centrifuge.RedisHistoryStorage) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    return storage.Health(ctx)
}
```

### 2. 性能监控

```go
func monitorRedisStorage(storage *centrifuge.RedisHistoryStorage) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        stats, err := storage.Stats(context.Background())
        if err != nil {
            log.Printf("Failed to get Redis stats: %v", err)
            continue
        }
        
        // 发送到监控系统
        sendMetrics(stats)
    }
}
```

## ⚠️ 注意事项

1. **内存使用**：Redis 主要使用内存，确保有足够的内存资源
2. **数据持久化**：配置适当的 RDB + AOF 策略
3. **网络延迟**：Redis 和应用服务器之间的网络延迟会影响性能
4. **备份策略**：制定 Redis 数据备份和恢复策略
5. **监控告警**：监控 Redis 的内存使用、连接数、响应时间等指标

## 🎯 总结

Redis 历史存储为 Centrifuge 提供了：

- ✅ **分布式支持**：解决多节点部署时的历史消息一致性
- ✅ **高性能查询**：亚毫秒级的历史消息检索
- ✅ **自动过期**：TTL 自动管理存储空间
- ✅ **易于运维**：基于成熟的 Redis 生态
