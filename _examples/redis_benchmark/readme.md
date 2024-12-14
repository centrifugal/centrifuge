Tool to put Centrifuge specific load on Redis setup.

Once you run it you can estimate the load on Redis setup, Redis resource usage.

Example usage:

```bash
go build
REDIS_ADDRESS="redis://test:test@127.0.0.1:6379" ./redis-benchmark
```

Or with Redis Cluster:

```bash
REDIS_ADDRESS="redis+cluster://test:test@127.0.0.1:7000" ./redis-benchmark
```

Or with sharding over 2 isolated Redis instances:

```bash
REDIS_ADDRESS="redis://test:test@127.0.0.1:6379,redis://test:test@127.0.0.1:6380" ./redis-benchmark
```

With different bench parameters:

```bash
PUBLISH_RATE=200 SUBSCRIBE_RATE=100 UNSUBSCRIBE_RATE=100 REDIS_ADDRESS="redis://test:test@127.0.0.1:6379" ./redis-benchmark
```

Or with history streams enabled and history requests:

```bash
HISTORY_SIZE=100 HISTORY_TTL=60s HISTORY_RATE=10000 REDIS_ADDRESS="redis://test:test@127.0.0.1:6379" ./redis-benchmark
```
