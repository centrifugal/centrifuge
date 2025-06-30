# Kafka Broker 使用指南

本文档提供 Kafka Broker 的完整使用指南，包括安装配置、部署实践、监控运维等内容。

## 📋 目录

- [1. 快速开始](#1-快速开始)
- [2. 配置详解](#2-配置详解)
- [3. 历史存储配置](#3-历史存储配置)
- [4. 部署指南](#4-部署指南)
- [5. 监控和运维](#5-监控和运维)
- [6. 性能调优](#6-性能调优)
- [7. 故障排查](#7-故障排查)
- [8. 最佳实践](#8-最佳实践)

## 1. 快速开始

### 1.1 基本示例

```go
package main

import (
    "log"
    "os"
    "os/signal"
    "syscall"
    
    "github.com/channelwill/cw2-live-chat-common/pkg/broker/gkafka"
    "github.com/channelwill/cw2-live-chat-common/pkg/storage/gredis"
    centrifuge "github.com/channelwill/cw2-live-chat-centrifuge"
)

func main() {
    // 1. 创建 Centrifuge Node
    node, err := centrifuge.New(centrifuge.Config{
        LogLevel:   centrifuge.LogLevelInfo,
        LogHandler: func(entry centrifuge.LogEntry) {
            log.Printf("[%s] %s", entry.Level, entry.Message)
        },
    })
    if err != nil {
        log.Fatal("Failed to create node:", err)
    }

    // 2. 配置 Kafka Broker
    kafkaConfig := centrifuge.KafkaBrokerConfig{
        KafkaConfig: gkafka.KafkaConfig{
            Brokers: []string{"localhost:9092"},
        },
        TopicPrefix:     "livechat",
        ConsumerGroupID: "livechat-node-" + node.ID(),
        Name:           "production-kafka-broker",
    }

    // 3. 创建并设置 Broker
    broker, err := centrifuge.NewKafkaBroker(node, kafkaConfig)
    if err != nil {
        log.Fatal("Failed to create Kafka broker:", err)
    }

    node.SetBroker(broker)

    // 4. 启动服务
    if err := node.Run(); err != nil {
        log.Fatal("Failed to run node:", err)
    }

    // 5. 优雅退出
    waitExitSignal()
    
    log.Println("Shutting down...")
    _ = node.Shutdown()
}

func waitExitSignal() {
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
}
```

### 1.2 Docker Compose 快速启动

```yaml
# docker-compose.yml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

  livechat:
    build: .
    depends_on:
      - kafka
      - redis
    ports:
      - "8000:8000"
    environment:
      KAFKA_BROKERS: "kafka:9092"
      REDIS_URL: "redis:6379"
```

## 2. 配置详解

### 2.1 KafkaBrokerConfig 完整配置

```go
type KafkaBrokerConfig struct {
    // === Kafka 配置（基于 Common 包）===
    KafkaConfig gkafka.KafkaConfig {
        // 基础连接配置
        Brokers              []string      // Kafka broker 地址列表
        Username             string        // SASL 用户名（可选）
        Password             string        // SASL 密码（可选）
        
        // 安全配置
        SecurityProtocol     string        // 安全协议：PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, SSL
        SASLMechanism        string        // SASL 机制：PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
        SASLTypeSCRAMSHA     string        // SCRAM 类型
        CACertPath           string        // CA 证书路径
        
        // 性能配置
        Version              string        // Kafka 版本
        DialTimeout          time.Duration // 连接超时
        ProducerTimeout      time.Duration // 生产者超时
        ProducerRetryMax     int           // 生产者重试次数
        ProducerMaxMessageBytes int        // 单条消息最大字节数
        
        // 消费者配置
        ConsumerAutoCommitInterval time.Duration // 自动提交间隔
        ConsumerOffsetsInitial     int64         // 初始 offset：-1(newest), -2(oldest)
        
        // 优雅关闭
        GracefulWait         time.Duration // 优雅关闭等待时间
    }
    
    // === Centrifuge 特有配置 ===
    TopicPrefix          string           // Topic 前缀，默认 "centrifuge"
    ConsumerGroupID      string           // 消费者组 ID，默认 "centrifuge-node-{nodeID}"
    Name                string           // Broker 名称，用于日志标识
    NumPartitions       int              // Topic 分区数，默认 1
    
    // === 历史存储配置 ===
    HistoryStorage      HistoryStorage   // 历史存储实现，默认 MemoryHistoryStorage
}
```

### 2.2 环境变量配置

```bash
# Kafka 配置
export KAFKA_BROKERS="broker1:9092,broker2:9092,broker3:9092"
export KAFKA_USERNAME="your-username"
export KAFKA_PASSWORD="your-password"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_SASL_MECHANISM="SCRAM-SHA-256"

# Centrifuge 配置
export CENTRIFUGE_TOPIC_PREFIX="production"
export CENTRIFUGE_CONSUMER_GROUP="livechat-prod"

# Redis 配置（历史存储）
export REDIS_URL="redis-cluster.example.com:6379"
export REDIS_PASSWORD="your-redis-password"
export REDIS_DB="0"
```

```go
// 从环境变量加载配置
func loadConfigFromEnv() centrifuge.KafkaBrokerConfig {
    kafkaConfig := gkafka.KafkaConfig{
        Brokers:          strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
        Username:         os.Getenv("KAFKA_USERNAME"),
        Password:         os.Getenv("KAFKA_PASSWORD"),
        SecurityProtocol: os.Getenv("KAFKA_SECURITY_PROTOCOL"),
        SASLMechanism:    os.Getenv("KAFKA_SASL_MECHANISM"),
    }
    
    return centrifuge.KafkaBrokerConfig{
        KafkaConfig:     kafkaConfig,
        TopicPrefix:     getEnvOrDefault("CENTRIFUGE_TOPIC_PREFIX", "centrifuge"),
        ConsumerGroupID: getEnvOrDefault("CENTRIFUGE_CONSUMER_GROUP", ""),
        Name:           getEnvOrDefault("CENTRIFUGE_BROKER_NAME", "kafka-broker"),
    }
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
```

## 3. 历史存储配置

### 3.1 Redis 历史存储（推荐）

```go
// 配置 Redis 历史存储
func setupRedisHistoryStorage() centrifuge.HistoryStorage {
    redisConf := gredis.RedisConf{
        Address:         os.Getenv("REDIS_URL"),
        Password:        os.Getenv("REDIS_PASSWORD"),
        DB:              0,
        PoolSize:        20,
        DialTimeout:     5 * time.Second,
        ReadTimeout:     3 * time.Second,
        WriteTimeout:    3 * time.Second,
        PoolTimeout:     4 * time.Second,
        ConnMaxLifetime: 30 * time.Minute,
        ConnMaxIdleTime: 5 * time.Minute,
    }
    
    return centrifuge.NewRedisHistoryStorageFromConf(redisConf, "livechat")
}

// 集成到 Kafka Broker
kafkaConfig := centrifuge.KafkaBrokerConfig{
    KafkaConfig:    kafkaConfig,
    HistoryStorage: setupRedisHistoryStorage(), // 使用 Redis 存储
}
```

### 3.2 内存历史存储（开发/测试）

```go
// 使用默认内存存储（无需额外配置）
kafkaConfig := centrifuge.KafkaBrokerConfig{
    KafkaConfig: kafkaConfig,
    // HistoryStorage 留空，自动使用 MemoryHistoryStorage
}

// 或者显式创建
kafkaConfig := centrifuge.KafkaBrokerConfig{
    KafkaConfig:    kafkaConfig,
    HistoryStorage: centrifuge.NewMemoryHistoryStorage(),
}
```

### 3.3 自定义历史存储

```go
// 实现自定义存储
type CustomHistoryStorage struct {
    // 你的存储实现
}

func (c *CustomHistoryStorage) AddToHistory(ctx context.Context, channel string, pub *centrifuge.Publication, opts centrifuge.PublishOptions) (centrifuge.StreamPosition, error) {
    // 实现添加历史消息逻辑
    return centrifuge.StreamPosition{}, nil
}

func (c *CustomHistoryStorage) GetHistory(ctx context.Context, channel string, opts centrifuge.HistoryOptions) ([]*centrifuge.Publication, centrifuge.StreamPosition, error) {
    // 实现获取历史消息逻辑
    return nil, centrifuge.StreamPosition{}, nil
}

func (c *CustomHistoryStorage) RemoveHistory(ctx context.Context, channel string) error {
    // 实现删除历史消息逻辑
    return nil
}

// 使用自定义存储
kafkaConfig := centrifuge.KafkaBrokerConfig{
    KafkaConfig:    kafkaConfig,
    HistoryStorage: &CustomHistoryStorage{},
}
```

## 4. 部署指南

### 4.1 Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: livechat-centrifuge
  labels:
    app: livechat-centrifuge
spec:
  replicas: 3  # 3 个节点实例
  selector:
    matchLabels:
      app: livechat-centrifuge
  template:
    metadata:
      labels:
        app: livechat-centrifuge
    spec:
      containers:
      - name: centrifuge
        image: livechat/centrifuge:latest
        ports:
        - containerPort: 8000
        env:
        - name: KAFKA_BROKERS
          value: "kafka-cluster.kafka.svc.cluster.local:9092"
        - name: REDIS_URL
          value: "redis-cluster.redis.svc.cluster.local:6379"
        - name: CENTRIFUGE_TOPIC_PREFIX
          value: "livechat-prod"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
```

### 4.2 健康检查实现

```go
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
    // 检查节点状态
    if !node.IsRunning() {
        http.Error(w, "Node not running", http.StatusServiceUnavailable)
        return
    }
    
    // 检查 Redis 连接（如果使用 Redis 历史存储）
    if redisStorage, ok := broker.GetHistoryStorage().(*centrifuge.RedisHistoryStorage); ok {
        if err := redisStorage.Health(context.Background()); err != nil {
            http.Error(w, "Redis unhealthy", http.StatusServiceUnavailable)
            return
        }
    }
    
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
}
```

## 5. 监控和运维

### 5.1 日志配置

```go
// 结构化日志配置
node, err := centrifuge.New(centrifuge.Config{
    LogLevel: centrifuge.LogLevelInfo,
    LogHandler: func(entry centrifuge.LogEntry) {
        // 使用结构化日志库（如 zap, logrus）
        logger.WithFields(map[string]interface{}{
            "level":     entry.Level.String(),
            "message":   entry.Message,
            "fields":    entry.Fields,
            "timestamp": time.Now(),
            "service":   "centrifuge",
        }).Info("centrifuge log")
    },
})
```

### 5.2 Prometheus 监控指标

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    // 消息发布指标
    publishedMessages = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "centrifuge_published_messages_total",
            Help: "The total number of published messages",
        },
        []string{"channel", "status"},
    )
    
    // 连接数指标
    activeConnections = promauto.NewGauge(
        prometheus.GaugeOpts{
            Name: "centrifuge_active_connections",
            Help: "The number of active connections",
        },
    )
)

// 启动 Prometheus 指标服务
func startMetricsServer() {
    http.Handle("/metrics", promhttp.Handler())
    go func() {
        log.Fatal(http.ListenAndServe(":9090", nil))
    }()
}
```

## 6. 性能调优

### 6.1 Kafka 配置调优

```go
kafkaConfig := gkafka.KafkaConfig{
    Brokers: []string{"localhost:9092"},
    
    // 生产者性能优化
    ProducerTimeout:          5 * time.Second,     // 发送超时
    ProducerRetryMax:         3,                   // 重试次数
    ProducerMaxMessageBytes:  1024 * 1024,         // 1MB 最大消息大小
    
    // 消费者配置
    ConsumerAutoCommitInterval: time.Second,       // 自动提交间隔
    ConsumerOffsetsInitial:     -1,               // 从最新消息开始
    
    // 连接优化
    DialTimeout:              10 * time.Second,    // 连接超时
}
```

### 6.2 Redis 历史存储调优

```go
redisConf := gredis.RedisConf{
    Address:  "localhost:6379",
    Password: "",
    DB:       0,
    
    // 连接池优化
    PoolSize:        runtime.NumCPU() * 4,    // 连接池大小
    PoolTimeout:     4 * time.Second,         // 获取连接超时
    ConnMaxLifetime: 30 * time.Minute,        // 连接最大生命周期
    ConnMaxIdleTime: 5 * time.Minute,         // 连接最大空闲时间
    
    // 读写超时优化
    DialTimeout:  5 * time.Second,            // 连接超时
    ReadTimeout:  3 * time.Second,            // 读取超时
    WriteTimeout: 3 * time.Second,            // 写入超时
}
```

## 7. 故障排查

### 7.1 常见问题诊断

#### Kafka 连接问题

```bash
# 检查 Kafka 连接
kubectl exec -it <pod-name> -- telnet kafka-broker 9092

# 查看 Kafka topic
kafka-topics --bootstrap-server localhost:9092 --list

# 查看消费者组状态
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group livechat-node-xxx
```

#### Redis 连接问题

```bash
# 检查 Redis 连接
redis-cli -h redis-host -p 6379 ping

# 查看 Redis 内存使用
redis-cli info memory

# 查看历史消息存储
redis-cli XLEN livechat:history:stream:chat:room:123
```

### 7.2 关键错误日志

```go
// 常见错误模式
const (
    "kafka broker global consumer failed"     // Kafka 消费者连接失败
    "failed to add to history"                // 历史消息存储失败
    "failed to unmarshal kafka message"       // 消息解析失败
    "failed to handle publication"            // 消息处理失败
)
```

## 8. 最佳实践

### 8.1 生产环境配置

```go
// 生产环境推荐配置
func productionConfig() centrifuge.KafkaBrokerConfig {
    return centrifuge.KafkaBrokerConfig{
        KafkaConfig: gkafka.KafkaConfig{
            Brokers: strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
            
            // 安全配置
            Username:         os.Getenv("KAFKA_USERNAME"),
            Password:         os.Getenv("KAFKA_PASSWORD"),
            SecurityProtocol: "SASL_SSL",
            SASLMechanism:    "SCRAM-SHA-256",
            
            // 性能配置
            ProducerTimeout:          5 * time.Second,
            ProducerRetryMax:         3,
            ProducerMaxMessageBytes:  1024 * 1024,
            
            // 消费者配置
            ConsumerAutoCommitInterval: time.Second,
            ConsumerOffsetsInitial:     -1, // 从最新消息开始
        },
        
        // Centrifuge 配置
        TopicPrefix:     "production",
        ConsumerGroupID: "", // 自动生成
        Name:           "production-kafka-broker",
        NumPartitions:   3,  // 根据负载调整
        
        // 使用 Redis 历史存储
        HistoryStorage: setupRedisHistoryStorage(),
    }
}
```

### 8.2 错误处理最佳实践

```go
// 优雅错误处理
func (b *KafkaBroker) PublishWithRetry(ch string, data []byte, opts PublishOptions) (StreamPosition, bool, error) {
    const maxRetries = 3
    const baseDelay = 100 * time.Millisecond
    
    for attempt := 0; attempt < maxRetries; attempt++ {
        sp, suppressed, err := b.Publish(ch, data, opts)
        if err == nil {
            return sp, suppressed, nil
        }
        
        if attempt < maxRetries-1 {
            // 指数退避
            delay := baseDelay * time.Duration(1<<attempt)
            time.Sleep(delay)
        }
    }
    
    return StreamPosition{}, false, fmt.Errorf("failed to publish after %d attempts", maxRetries)
}
```

### 8.3 部署检查清单

- ✅ **环境配置**：确认所有环境变量正确设置
- ✅ **网络连通性**：验证到 Kafka 和 Redis 的网络连接
- ✅ **资源配额**：确保有足够的 CPU 和内存资源
- ✅ **健康检查**：配置 liveness 和 readiness 探针
- ✅ **监控指标**：启用 Prometheus 指标收集
- ✅ **日志收集**：配置结构化日志输出
- ✅ **安全认证**：启用 Kafka 和 Redis 认证
- ✅ **备份策略**：制定 Redis 数据备份计划

## 总结

本使用指南提供了 Kafka Broker 从开发到生产的完整部署流程。关键要点：

### 🔧 **配置要点**
- 使用环境变量管理配置
- 生产环境启用安全认证
- 合理配置连接池和超时参数

### 🚀 **部署要点**
- 使用 Redis 历史存储提升分布式一致性
- 配置健康检查和优雅关闭
- 合理设置资源限制和监控指标

### 📊 **运维要点**
- 建立完善的监控和告警体系
- 定期检查 Kafka 和 Redis 集群状态
- 制定故障应急预案

按照本指南可以构建稳定、高性能的分布式实时通信系统！ 