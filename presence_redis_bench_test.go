//go:build integration

package centrifuge

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

// BenchmarkRedisAddPresenceBatch benchmarks batch presence updates using ExecMulti
// pipeline vs individual AddPresence calls. This measures the improvement from
// commits eb91ac4 and b168872.
//
// Each sub-benchmark uses the naming convention:
//   - "Individual" – calls AddPresence once per channel (old path).
//   - "Batch"      – calls AddPresenceBatch for all channels at once (new path).
//
// The "numCh" suffix indicates the number of channels per operation.

func BenchmarkRedisAddPresenceBatch_vs_Individual(b *testing.B) {
	channelCounts := []int{1, 5, 10, 50, 100}

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		for _, numCh := range channelCounts {
			// --- Individual AddPresence (baseline) ---
			b.Run(fmt.Sprintf("%s/Individual/numCh_%d", tt.Name, numCh), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				channels := make([]string, numCh)
				for i := range channels {
					channels[i] = "bench_ch_" + strconv.Itoa(i)
				}

				info := &ClientInfo{
					ClientID: "uid-bench",
					UserID:   "user-bench",
				}

				b.SetParallelism(getBenchParallelism())
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for _, ch := range channels {
							if err := pm.AddPresence(ch, "uid-bench", info); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			})

			// --- Batch AddPresenceBatch (new path) ---
			b.Run(fmt.Sprintf("%s/Batch/numCh_%d", tt.Name, numCh), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				items := make([]PresenceBatchItem, numCh)
				for i := range items {
					items[i] = PresenceBatchItem{
						Channel:  "bench_ch_" + strconv.Itoa(i),
						ClientID: "uid-bench",
						Info: &ClientInfo{
							ClientID: "uid-bench",
							UserID:   "user-bench",
						},
					}
				}

				b.SetParallelism(getBenchParallelism())
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := pm.AddPresenceBatch(items); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// BenchmarkRedisAddPresenceBatch_Scalability benchmarks how batch performance
// scales with increasing channel counts. Useful for profiling pipeline overhead.
func BenchmarkRedisAddPresenceBatch_Scalability(b *testing.B) {
	channelCounts := []int{10, 50, 100, 200, 500}

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		for _, numCh := range channelCounts {
			b.Run(fmt.Sprintf("%s/numCh_%d", tt.Name, numCh), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				items := make([]PresenceBatchItem, numCh)
				for i := range items {
					items[i] = PresenceBatchItem{
						Channel:  "scale_ch_" + strconv.Itoa(i),
						ClientID: "uid-scale",
						Info: &ClientInfo{
							ClientID: "uid-scale",
							UserID:   "user-scale",
						},
					}
				}

				b.SetParallelism(getBenchParallelism())
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := pm.AddPresenceBatch(items); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// makePresenceBatchItems creates N PresenceBatchItem for benchmarking.
func makePresenceBatchItems(n int, prefix string) []PresenceBatchItem {
	items := make([]PresenceBatchItem, n)
	for i := range items {
		items[i] = PresenceBatchItem{
			Channel:  prefix + strconv.Itoa(i),
			ClientID: "uid-large",
			Info: &ClientInfo{
				ClientID: "uid-large",
				UserID:   "user-large",
			},
		}
	}
	return items
}

// splitBatch divides items into chunks of batchSize, simulating
// the ClientPresenceUpdateBatchSize splitting done in client.updatePresence.
func splitBatch(items []PresenceBatchItem, batchSize int) [][]PresenceBatchItem {
	var batches [][]PresenceBatchItem
	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

// BenchmarkRedisLargeScale_Individual benchmarks the old per-channel AddPresence
// path at large channel counts (1000 ~ 10000).
func BenchmarkRedisLargeScale_Individual(b *testing.B) {
	channelCounts := []int{500, 1000, 2000, 5000, 10000}

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		if tt.Port != 6379 {
			continue
		}
		for _, numCh := range channelCounts {
			b.Run(fmt.Sprintf("%s/numCh_%d", tt.Name, numCh), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				channels := make([]string, numCh)
				for i := range channels {
					channels[i] = "large_ch_" + strconv.Itoa(i)
				}
				info := &ClientInfo{
					ClientID: "uid-large",
					UserID:   "user-large",
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, ch := range channels {
						if err := pm.AddPresence(ch, "uid-large", info); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// BenchmarkRedisLargeScale_BatchNoBatchSplit benchmarks AddPresenceBatch with
// all channels in a single ExecMulti call (no splitting), at large channel counts.
func BenchmarkRedisLargeScale_BatchNoBatchSplit(b *testing.B) {
	channelCounts := []int{500, 1000, 2000, 5000, 10000}

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		if tt.Port != 6379 {
			continue
		}
		for _, numCh := range channelCounts {
			b.Run(fmt.Sprintf("%s/numCh_%d", tt.Name, numCh), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				items := makePresenceBatchItems(numCh, "large_ch_")

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := pm.AddPresenceBatch(items); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkRedisLargeScale_BatchWithSplit benchmarks AddPresenceBatch with
// batch splitting at various batchSize values, simulating how client.updatePresence
// splits channels into fixed-size chunks before calling AddPresenceBatch.
//
// This reveals the optimal batchSize for a given total channel count.
func BenchmarkRedisLargeScale_BatchWithSplit(b *testing.B) {
	type testCase struct {
		totalChannels int
		batchSize     int
	}

	cases := []testCase{
		// 1000 channels with various batch sizes
		{1000, 10},
		{1000, 50},
		{1000, 100},
		{1000, 200},
		{1000, 500},
		{1000, 1000},

		// 5000 channels with various batch sizes
		{5000, 50},
		{5000, 100},
		{5000, 200},
		{5000, 500},
		{5000, 1000},
		{5000, 2500},
		{5000, 5000},

		// 10000 channels with various batch sizes
		{10000, 100},
		{10000, 500},
		{10000, 1000},
		{10000, 2500},
		{10000, 5000},
		{10000, 10000},
	}

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		if tt.Port != 6379 {
			continue
		}
		for _, tc := range cases {
			name := fmt.Sprintf("%s/total_%d/batch_%d", tt.Name, tc.totalChannels, tc.batchSize)
			b.Run(name, func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				allItems := makePresenceBatchItems(tc.totalChannels, "split_ch_")
				batches := splitBatch(allItems, tc.batchSize)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, batch := range batches {
						if err := pm.AddPresenceBatch(batch); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// BenchmarkRedisLargeScale_Parallel benchmarks 10,000 channels under b.RunParallel,
// comparing Individual (N sequential AddPresence per goroutine iteration) against
// Batch with various batchSize splits. This simulates real-world concurrent clients
// each updating presence for many channels simultaneously.
func BenchmarkRedisLargeScale_Parallel(b *testing.B) {
	const totalChannels = 10000

	for _, tt := range excludeClusterPresenceTests(redisPresenceTests) {
		if tt.Port != 6379 {
			continue
		}

		// --- Individual: each parallel goroutine calls AddPresence 10,000 times ---
		b.Run(fmt.Sprintf("%s/Individual/numCh_%d", tt.Name, totalChannels), func(b *testing.B) {
			node := benchNode(b)
			pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
			defer func() { _ = node.Shutdown(context.Background()) }()
			defer stopRedisPresenceManager(pm)

			channels := make([]string, totalChannels)
			for i := range channels {
				channels[i] = "par_ch_" + strconv.Itoa(i)
			}
			info := &ClientInfo{
				ClientID: "uid-par",
				UserID:   "user-par",
			}

			b.SetParallelism(getBenchParallelism())
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for _, ch := range channels {
						if err := pm.AddPresence(ch, "uid-par", info); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		})

		// --- Batch with various batchSize splits ---
		batchSizes := []int{10, 50, 100, 500, 1000, 2500, 5000, 10000}
		for _, bs := range batchSizes {
			bs := bs
			b.Run(fmt.Sprintf("%s/Batch_bs%d/numCh_%d", tt.Name, bs, totalChannels), func(b *testing.B) {
				node := benchNode(b)
				pm := newTestRedisPresenceManager(b, node, tt.UseCluster, false, false, tt.Port)
				defer func() { _ = node.Shutdown(context.Background()) }()
				defer stopRedisPresenceManager(pm)

				allItems := makePresenceBatchItems(totalChannels, "par_ch_")
				batches := splitBatch(allItems, bs)

				b.SetParallelism(getBenchParallelism())
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for _, batch := range batches {
							if err := pm.AddPresenceBatch(batch); err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			})
		}
	}
}

// BenchmarkMemoryAddPresenceBatch_vs_Individual benchmarks batch vs individual
// presence updates using the in-memory presence manager. This provides a
// baseline to isolate Redis network overhead from the batching logic itself.
func BenchmarkMemoryAddPresenceBatch_vs_Individual(b *testing.B) {
	channelCounts := []int{1, 10, 50, 100}

	for _, numCh := range channelCounts {
		// --- Individual ---
		b.Run(fmt.Sprintf("Individual/numCh_%d", numCh), func(b *testing.B) {
			m := testMemoryPresenceManager(b)
			defer func() { _ = m.node.Shutdown(context.Background()) }()

			channels := make([]string, numCh)
			for i := range channels {
				channels[i] = "bench_ch_" + strconv.Itoa(i)
			}

			info := &ClientInfo{
				ClientID: "uid-bench",
				UserID:   "user-bench",
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, ch := range channels {
					if err := m.AddPresence(ch, "uid-bench", info); err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		// --- Batch ---
		b.Run(fmt.Sprintf("Batch/numCh_%d", numCh), func(b *testing.B) {
			m := testMemoryPresenceManager(b)
			defer func() { _ = m.node.Shutdown(context.Background()) }()

			items := make([]PresenceBatchItem, numCh)
			for i := range items {
				items[i] = PresenceBatchItem{
					Channel:  "bench_ch_" + strconv.Itoa(i),
					ClientID: "uid-bench",
					Info: &ClientInfo{
						ClientID: "uid-bench",
						UserID:   "user-bench",
					},
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := m.AddPresenceBatch(items); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
