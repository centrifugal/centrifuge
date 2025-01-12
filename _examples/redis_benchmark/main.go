package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/centrifugal/centrifuge"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/ratelimit"
)

type Config struct {
	Port int `envconfig:"PORT" default:"8000"`

	RedisAddress []string `envconfig:"REDIS_ADDRESS" default:"127.0.0.1:6379"`

	HistorySize int           `envconfig:"HISTORY_SIZE" default:"0"`
	HistoryTTL  time.Duration `envconfig:"HISTORY_TTL" default:"60s"`

	NumDifferentChannels int `envconfig:"NUM_DIFFERENT_CHANNELS" default:"1024"`

	PublishRateLimit     int `envconfig:"PUBLISH_RATE" default:"50000"`
	SubscribeRateLimit   int `envconfig:"SUBSCRIBE_RATE" default:"50000"`
	UnsubscribeRateLimit int `envconfig:"UNSUBSCRIBE_RATE" default:"50000"`
	HistoryRateLimit     int `envconfig:"HISTORY_RATE" default:"0"`

	MessageSize int `envconfig:"MESSAGE_SIZE" default:"128"`
}

func handleLog(e centrifuge.LogEntry) {
	log.Printf("[centrifuge] %s: %v", e.Message, e.Fields)
}

func authMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		ctx = centrifuge.SetCredentials(ctx, &centrifuge.Credentials{
			UserID: "42",
			Info:   []byte(`{"name": "Alexander"}`),
		})
		r = r.WithContext(ctx)
		h.ServeHTTP(w, r)
	})
}

func waitExitSignal(n *centrifuge.Node) {
	sigCh := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		_ = n.Shutdown(context.Background())
		done <- true
	}()
	<-done
}

func main() {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("NUM_DIFFERENT_CHANNELS=%d, MESSAGE_SIZE=%d, HISTORY_SIZE=%d, HISTORY_TTL=%s, "+
		"PUBLISH_RATE=%d, SUBSCRIBE_RATE=%d, UNSUBSCRIBE_RATE=%d, HISTORY_RATE=%d", cfg.NumDifferentChannels,
		cfg.MessageSize, cfg.HistorySize, cfg.HistoryTTL, cfg.PublishRateLimit, cfg.SubscribeRateLimit,
		cfg.UnsubscribeRateLimit, cfg.HistoryRateLimit)

	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelError,
		LogHandler: handleLog,
	})

	node.OnConnect(func(client *centrifuge.Client) {
		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			cb(centrifuge.SubscribeReply{}, nil)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			cb(centrifuge.PublishReply{}, nil)
		})
	})

	var redisShardConfigs []centrifuge.RedisShardConfig

	for _, addr := range cfg.RedisAddress {
		if u, err := url.Parse(addr); err != nil {
			log.Printf("connecting to Redis: %s", addr)
		} else {
			log.Printf("connecting to Redis: %s", u.Redacted())
		}
		redisShardConfigs = append(redisShardConfigs, centrifuge.RedisShardConfig{
			Address: addr,
		})
	}

	var redisShards []*centrifuge.RedisShard
	for _, redisConf := range redisShardConfigs {
		redisShard, err := centrifuge.NewRedisShard(node, redisConf)
		if err != nil {
			log.Fatal(err)
		}
		redisShards = append(redisShards, redisShard)
	}

	broker, err := centrifuge.NewRedisBroker(node, centrifuge.RedisBrokerConfig{
		// And configure a couple of shards to use.
		Shards: redisShards,
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetBroker(broker)

	presenceManager, err := centrifuge.NewRedisPresenceManager(node, centrifuge.RedisPresenceManagerConfig{
		Shards: redisShards,
	})
	if err != nil {
		log.Fatal(err)
	}
	node.SetPresenceManager(presenceManager)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

	var publishNum int64
	var subNum int64
	var unsubNum int64
	var historyNum int64

	if cfg.PublishRateLimit > 0 {
		publishRateLimiter := ratelimit.New(cfg.PublishRateLimit, ratelimit.Per(time.Second))
		var publishOptions []centrifuge.PublishOption
		if cfg.HistorySize > 0 {
			publishOptions = append(publishOptions, centrifuge.WithHistory(cfg.HistorySize, cfg.HistoryTTL))
		}

		for i := 0; i < cfg.NumDifferentChannels; i++ {
			i := i
			go func() {
				msg := randString(cfg.MessageSize)
				for {
					publishRateLimiter.Take()
					_, err := node.Publish(
						"channel"+strconv.Itoa(i%cfg.NumDifferentChannels),
						[]byte(`{"d": "`+msg+`"}`),
						publishOptions...,
					)
					if err != nil {
						log.Printf("error publishing to channel: %s", err)
					}
					atomic.AddInt64(&publishNum, 1)
				}
			}()
		}
	}

	if cfg.SubscribeRateLimit > 0 {
		subRateLimiter := ratelimit.New(cfg.SubscribeRateLimit, ratelimit.Per(time.Second))
		for i := 0; i < cfg.NumDifferentChannels; i++ {
			i := i
			go func() {
				for {
					subRateLimiter.Take()
					err := broker.Subscribe("channel" + strconv.Itoa(i%cfg.NumDifferentChannels))
					if err != nil {
						log.Printf("error subscribing to channel: %s", err)
					}
					atomic.AddInt64(&subNum, 1)
				}
			}()
		}
	}

	if cfg.UnsubscribeRateLimit > 0 {
		unsubRateLimiter := ratelimit.New(cfg.UnsubscribeRateLimit, ratelimit.Per(time.Second))
		for i := 0; i < cfg.NumDifferentChannels; i++ {
			i := i
			go func() {
				for {
					unsubRateLimiter.Take()
					err := broker.Unsubscribe("channel" + strconv.Itoa(i%cfg.NumDifferentChannels))
					if err != nil {
						log.Printf("error unsubscribing from channel: %s", err)
					}
					atomic.AddInt64(&unsubNum, 1)
				}
			}()
		}
	}

	if cfg.HistoryRateLimit > 0 {
		historyRateLimiter := ratelimit.New(cfg.HistoryRateLimit, ratelimit.Per(time.Second))
		for i := 0; i < cfg.NumDifferentChannels; i++ {
			i := i
			go func() {
				for {
					historyRateLimiter.Take()
					// No publications loaded here now, only stream position.
					_, err := node.History("channel" + strconv.Itoa(i%cfg.NumDifferentChannels))
					if err != nil {
						log.Printf("error getting history from channel: %s", err)
					}
					atomic.AddInt64(&historyNum, 1)
				}
			}()
		}
	}

	go func() {
		time.Sleep(time.Second)
		prevPublished := int64(0)
		prevSubscribes := int64(0)
		prevUnsubscribes := int64(0)
		prevHistory := int64(0)
		for {
			currentPublished := atomic.LoadInt64(&publishNum)
			currentSubscribes := atomic.LoadInt64(&subNum)
			currentUnsubscribes := atomic.LoadInt64(&unsubNum)
			currentHistory := atomic.LoadInt64(&historyNum)
			log.Printf("Stats per second: published %d, subscribed: %d, unsubscribed: %d, history: %d",
				currentPublished-prevPublished,
				currentSubscribes-prevSubscribes,
				currentUnsubscribes-prevUnsubscribes,
				currentHistory-prevHistory,
			)
			prevPublished = currentPublished
			prevSubscribes = currentSubscribes
			prevUnsubscribes = currentUnsubscribes
			prevHistory = currentHistory
			time.Sleep(time.Second)
		}
	}()

	http.Handle("/connection/websocket", authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{})))
	http.Handle("/", http.FileServer(http.Dir("./")))

	go func() {
		if err := http.ListenAndServe(":"+strconv.Itoa(cfg.Port), nil); err != nil {
			log.Fatal(err)
		}
	}()

	waitExitSignal(node)
	log.Println("bye!")
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[random.Intn(len(letterRunes))]
	}
	return string(b)
}
