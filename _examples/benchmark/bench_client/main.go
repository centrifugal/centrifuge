// This code is an adapted version of Nats benchmarking suite from
// https://github.com/nats-io/go-nats/blob/master/examples/nats-bench.go
// for Centrifuge. Use together with benchmark program from Centrifuge repo:
// https://github.com/centrifugal/centrifuge/blob/master/examples/benchmark/main.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge-go"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"go.uber.org/ratelimit"
)

// Some sane defaults.
const (
	DefaultNumMsg      = 100000
	DefaultNumPubs     = 1
	DefaultNumSubs     = 0
	DefaultMessageSize = 128
	DefaultDeadline    = 300
)

var messageReceivedCounter int32 = 0
var messagePublishedCounter int32 = 0

func usage() {
	log.Printf("Usage: bench_client [options] <channel>\n\n")
	log.Printf("Modes (-m):\n")
	log.Printf("  pubsub (default) - Publish/subscribe benchmark\n")
	log.Printf("  idle             - Keep connections idle to test connection overhead\n")
	log.Printf("  connect-rate     - Measure connection rate performance\n")
	log.Printf("  rpc              - Measure RPC throughput\n\n")
	log.Printf("Common options:\n")
	log.Printf("  -s uri           Server URI (default: ws://localhost:8000/connection/websocket)\n")
	log.Printf("  -ns NUM          Number of subscribers/connections (default: %d)\n", DefaultNumSubs)
	log.Printf("  -p               Use protobuf format (default: JSON)\n\n")
	log.Printf("PubSub mode options:\n")
	log.Printf("  -np NUM          Number of publishers (default: %d)\n", DefaultNumPubs)
	log.Printf("  -n NUM           Number of messages to publish (default: %d)\n", DefaultNumMsg)
	log.Printf("  -ms SIZE         Message size in bytes (default: %d)\n", DefaultMessageSize)
	log.Printf("  -pl RATE         Publish rate limit per publisher (msgs/sec, 0=unlimited)\n")
	log.Printf("  -d SECONDS       Deadline for test completion (default: %d)\n\n", DefaultDeadline)
	log.Printf("Idle mode options:\n")
	log.Printf("  -w SECONDS       Wait time in seconds (default: 60)\n\n")
	log.Printf("Connect-rate mode options:\n")
	log.Printf("  -cr RATE         Target connection rate per second (default: 100)\n\n")
	log.Printf("RPC mode options:\n")
	log.Printf("  -rr RATE         RPC calls per second per connection (default: 100)\n")
	log.Printf("  -n NUM           Total number of RPC calls (0=duration-based, default: 0)\n")
	log.Printf("  -d SECONDS       Test duration in seconds (used when -n is 0, default: %d)\n\n", DefaultDeadline)
	log.Fatalf("Example: bench_client -m idle -ns 1000 -w 120 test\n")
}

var url = flag.String("s", "ws://localhost:8000/connection/websocket", "Connection URI")
var useProtobuf = flag.Bool("p", false, "Use protobuf format (by default JSON is used)")
var numPubs = flag.Int("np", DefaultNumPubs, "Number of concurrent publishers")
var numSubs = flag.Int("ns", DefaultNumSubs, "Number of concurrent subscribers")
var numMsg = flag.Int("n", DefaultNumMsg, "Number of messages to publish")
var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message")
var deadline = flag.Int("d", DefaultDeadline, "Deadline for the test to finish")
var pubRateLimit = flag.Int("pl", 0, "Rate limit for each publisher in messages per second")
var mode = flag.String("m", "pubsub", "Benchmark mode: pubsub (default), idle, connect-rate, rpc")
var connectRate = flag.Int("cr", 100, "Target connection rate per second (for connect-rate mode)")
var waitTime = flag.Int("w", 60, "Wait time in seconds (for idle mode)")
var rpcRate = flag.Int("rr", 100, "RPC calls per second per connection (for rpc mode)")

var benchmark *Benchmark

func main() {
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()

	switch *mode {
	case "pubsub":
		runPubSubMode(args)
	case "idle":
		runIdleMode(args)
	case "connect-rate":
		runConnectRateMode(args)
	case "rpc":
		runRPCMode(args)
	default:
		log.Fatalf("Unknown mode: %s. Available modes: pubsub, idle, connect-rate, rpc", *mode)
	}
}

func runPubSubMode(args []string) {
	if len(args) != 1 {
		usage()
	}

	if *numMsg <= 0 {
		log.Fatal("Number of messages should be greater than zero.")
	}
	benchmark = NewBenchmark("Centrifuge", *numSubs, *numPubs)

	model := NewUIModel("pubsub", 0, globalErrorCollector) // No fixed duration for pubsub
	model.stats["publishers"] = int64(*numPubs)
	model.stats["subscribers"] = int64(*numSubs)
	model.stats["published"] = &messagePublishedCounter
	model.stats["received"] = &messageReceivedCounter
	model.stats["target"] = int64(*numMsg * *numSubs) // Total expected messages

	var startWg sync.WaitGroup
	var doneWg sync.WaitGroup

	doneWg.Add(*numPubs + *numSubs)

	// Run Subscribers first
	startWg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		time.Sleep(500 * time.Microsecond)
		go runSubscriber(&startWg, &doneWg, *numMsg, *msgSize)
	}
	startWg.Wait()

	// Now Publishers
	startWg.Add(*numPubs)
	pubCounts := MsgPerClient(*numMsg, *numPubs)
	for i := 0; i < *numPubs; i++ {
		time.Sleep(1 * time.Millisecond)
		go runPublisher(&startWg, &doneWg, pubCounts[i], *msgSize)
	}

	startWg.Wait()

	p := tea.NewProgram(model)
	go func() {
		waitForTestCompletion(&doneWg, deadline)
		benchmark.Close()
		time.Sleep(100 * time.Millisecond)
		p.Quit()
	}()

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}

	globalErrorCollector.display()
	fmt.Print(benchmark.Report())
}

type errorCollector struct {
	errors []string
	mu     sync.Mutex
}

func (ec *errorCollector) add(err string) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	ec.errors = append(ec.errors, err)
}

func (ec *errorCollector) hasErrors() bool {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return len(ec.errors) > 0
}

func (ec *errorCollector) display() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if len(ec.errors) > 0 {
		fmt.Printf("\n%s\n", strings.Repeat("!", 70))
		fmt.Printf("  ERRORS DURING BENCHMARK\n")
		fmt.Printf("%s\n", strings.Repeat("!", 70))
		for i, err := range ec.errors {
			if i >= 10 {
				fmt.Printf("... and %d more errors\n", len(ec.errors)-10)
				break
			}
			fmt.Printf("  %s\n", err)
		}
		fmt.Printf("%s\n\n", strings.Repeat("!", 70))
	}
}

var globalErrorCollector = &errorCollector{}

func newConnection(connType string) *centrifuge.Client {
	var c *centrifuge.Client
	config := centrifuge.Config{}
	if *useProtobuf {
		c = centrifuge.NewProtobufClient(*url, config)
	} else {
		c = centrifuge.NewJsonClient(*url, config)
	}
	c.OnError(func(e centrifuge.ErrorEvent) {
		errMsg := fmt.Sprintf("error (%s): %v", connType, e.Error)
		globalErrorCollector.add(errMsg)
	})
	c.OnConnecting(func(e centrifuge.ConnectingEvent) {
		if e.Code != 0 {
			errMsg := fmt.Sprintf("connecting (%s): %d, %s", connType, e.Code, e.Reason)
			globalErrorCollector.add(errMsg)
		}
	})
	c.OnDisconnected(func(e centrifuge.DisconnectedEvent) {
		if e.Code != 0 {
			errMsg := fmt.Sprintf("disconnect (%s): %d, %s", connType, e.Code, e.Reason)
			globalErrorCollector.add(errMsg)
		}
	})
	return c
}

func runPublisher(startWg, doneWg *sync.WaitGroup, numMsg int, msgSize int) {
	c := newConnection("publisher")
	defer c.Close()

	args := flag.Args()
	subj := args[0]
	var msg []byte
	if msgSize > 0 {
		msg = make([]byte, msgSize)
	}

	payload, _ := json.Marshal(msg)

	start := time.Now()

	err := c.Connect()
	if err != nil {
		errMsg := fmt.Sprintf("Publisher can't connect: %v", err)
		globalErrorCollector.add(errMsg)
		startWg.Done()
		doneWg.Done()
		return
	}

	startWg.Done()

	rl := ratelimit.NewUnlimited()
	if *pubRateLimit > 0 {
		rl = ratelimit.New(*pubRateLimit, ratelimit.Per(time.Second))
	}

	for i := 0; i < numMsg; i++ {
		rl.Take()
		_, err := c.Publish(context.Background(), subj, payload)
		if err != nil {
			errMsg := fmt.Sprintf("Publish error: %v", err)
			globalErrorCollector.add(errMsg)
			break
		} else {
			atomic.AddInt32(&messagePublishedCounter, 1)
		}
	}

	benchmark.AddPubSample(NewSample(numMsg, msgSize, start, time.Now()))
	doneWg.Done()
}

type subEventHandler struct {
	numMsg    int
	msgSize   int
	received  int
	connected bool
	doneWg    *sync.WaitGroup
	startWg   *sync.WaitGroup
	client    *centrifuge.Client
	start     time.Time
}

func (h *subEventHandler) OnPublication(_ centrifuge.PublicationEvent) {
	h.received++
	atomic.AddInt32(&messageReceivedCounter, 1)
	if h.received >= h.numMsg {
		benchmark.AddSubSample(NewSample(h.numMsg, h.msgSize, h.start, time.Now()))
		h.doneWg.Done()
		h.client.Close()
	}
}

func (h *subEventHandler) OnSubscribe(_ centrifuge.SubscribedEvent) {
	// call Done() only for the first time and not on reconnects to avoid a panic
	if !h.connected {
		h.startWg.Done()
		h.connected = true
	}
}

func (h *subEventHandler) OnError(e centrifuge.SubscriptionErrorEvent) {
	errMsg := fmt.Sprintf("subscribe error: %v", e.Error)
	globalErrorCollector.add(errMsg)
	h.doneWg.Done()
	h.client.Close()
}

func runSubscriber(startWg, doneWg *sync.WaitGroup, numMsg int, msgSize int) {
	c := newConnection("subscriber")

	args := flag.Args()
	subj := args[0]

	sub, err := c.NewSubscription(subj)
	if err != nil {
		errMsg := fmt.Sprintf("Subscriber can't create subscription: %v", err)
		globalErrorCollector.add(errMsg)
		startWg.Done()
		doneWg.Done()
		return
	}

	subEvents := &subEventHandler{
		numMsg:    numMsg,
		msgSize:   msgSize,
		connected: false,
		doneWg:    doneWg,
		startWg:   startWg,
		client:    c,
		start:     time.Now(),
	}

	sub.OnPublication(subEvents.OnPublication)
	sub.OnSubscribed(subEvents.OnSubscribe)
	sub.OnError(subEvents.OnError)

	err = c.Connect()
	if err != nil {
		err = c.Connect()
		if err != nil {
			errMsg := fmt.Sprintf("Subscriber can't connect: %v", err)
			globalErrorCollector.add(errMsg)
			startWg.Done()
			doneWg.Done()
			c.Close()
			return
		}
	}

	_ = sub.Subscribe()
}

func runIdleMode(_ []string) {
	var connectedCount int64
	var pingCount int64
	var pongCount int64

	model := NewUIModel("idle", time.Duration(*waitTime)*time.Second, globalErrorCollector)
	model.stats["connected"] = &connectedCount
	model.stats["pings"] = &pingCount
	model.stats["pongs"] = &pongCount

	var connectWg sync.WaitGroup
	clients := make([]*centrifuge.Client, *numSubs)

	connectWg.Add(*numSubs)

	for i := 0; i < *numSubs; i++ {
		go func(idx int) {
			c := newConnection("idle")

			err := c.Connect()
			if err != nil {
				connectWg.Done()
				return
			}

			atomic.AddInt64(&connectedCount, 1)
			clients[idx] = c
			connectWg.Done()
		}(i)
		time.Sleep(time.Millisecond)
	}

	connectWg.Wait()

	p := tea.NewProgram(model)
	go func() {
		time.Sleep(time.Duration(*waitTime) * time.Second)
		p.Quit()
	}()

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}

	for _, c := range clients {
		if c != nil {
			c.Close()
		}
	}

	globalErrorCollector.display()
}

func runConnectRateMode(_ []string) {
	var completedCount int64
	var failedCount int64
	latencies := &sync.Map{} // Thread-safe map to store latencies

	model := NewUIModel("connect-rate", 0, globalErrorCollector)
	model.stats["total"] = int64(*numSubs)
	model.stats["completed"] = &completedCount
	model.stats["failed"] = &failedCount

	type ConnectResult struct {
		idx     int
		latency time.Duration
		success bool
		err     error
	}

	results := make(chan ConnectResult, *numSubs)
	rl := ratelimit.New(*connectRate)
	start := time.Now()

	// Start the UI
	p := tea.NewProgram(model)

	// Goroutine to collect results as they come in
	go func() {
		for i := 0; i < *numSubs; i++ {
			result := <-results
			if result.success {
				count := atomic.AddInt64(&completedCount, 1)
				latencies.Store(count, result.latency)
			} else {
				atomic.AddInt64(&failedCount, 1)
				globalErrorCollector.add(fmt.Sprintf("Connection %d failed: %v", result.idx, result.err))
			}
		}
		time.Sleep(100 * time.Millisecond)
		p.Quit()
	}()

	// Goroutine to spawn connections at target rate
	go func() {
		for i := 0; i < *numSubs; i++ {
			rl.Take()
			go func(idx int) {
				connStart := time.Now()
				c := newConnection("connect-rate")

				connected := make(chan bool, 1)
				c.OnConnected(func(centrifuge.ConnectedEvent) {
					connected <- true
				})

				err := c.Connect()
				if err != nil {
					results <- ConnectResult{idx: idx, success: false, err: err}
					return
				}

				select {
				case <-connected:
					latency := time.Since(connStart)
					results <- ConnectResult{idx: idx, latency: latency, success: true}
				case <-time.After(10 * time.Second):
					results <- ConnectResult{idx: idx, success: false, err: fmt.Errorf("timeout waiting for connected event")}
				}
			}(i)
		}
	}()

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}

	// Collect latencies into a slice for stats
	var latencySlice []time.Duration
	latencies.Range(func(key, value interface{}) bool {
		if lat, ok := value.(time.Duration); ok {
			latencySlice = append(latencySlice, lat)
		}
		return true
	})

	totalDuration := time.Since(start)
	finalCompleted := atomic.LoadInt64(&completedCount)
	finalFailed := atomic.LoadInt64(&failedCount)
	actualRate := float64(finalCompleted) / totalDuration.Seconds()

	globalErrorCollector.display()
	displayConnectRateStats(int(finalCompleted), int(finalFailed), totalDuration, actualRate, latencySlice)
}

func displayConnectRateStats(successCount, failedCount int, totalDuration time.Duration, actualRate float64, latencies []time.Duration) {
	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("  CONNECTION RATE BENCHMARK RESULTS\n")
	fmt.Printf("%s\n\n", strings.Repeat("=", 70))

	fmt.Printf("Total connections: %s\n", commaFormat(int64(successCount)))
	fmt.Printf("Failed: %s\n", commaFormat(int64(failedCount)))
	fmt.Printf("Duration: %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Actual rate: %.2f conn/sec\n", actualRate)

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		var sum time.Duration
		for _, l := range latencies {
			sum += l
		}
		avg := sum / time.Duration(len(latencies))

		p50 := latencies[len(latencies)*50/100]
		p90 := latencies[len(latencies)*90/100]
		p99 := latencies[len(latencies)*99/100]
		minLat := latencies[0]
		maxLat := latencies[len(latencies)-1]

		fmt.Printf("\nConnect Latency Distribution:\n")
		fmt.Printf("  Min:  %v\n", minLat)
		fmt.Printf("  Avg:  %v\n", avg)
		fmt.Printf("  p50:  %v\n", p50)
		fmt.Printf("  p90:  %v\n", p90)
		fmt.Printf("  p99:  %v\n", p99)
		fmt.Printf("  Max:  %v\n", maxLat)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
}

func runRPCMode(_ []string) {
	var connectedCount int64
	var rpcSentCount int64
	var rpcSuccessCount int64
	var rpcErrorCount int64
	latencies := &sync.Map{} // Thread-safe map to store latencies
	// Determine if we're using count-based or duration-based mode
	countBased := *numMsg > 0
	var duration time.Duration
	if !countBased {
		duration = time.Duration(*deadline) * time.Second
	}

	model := NewUIModel("rpc", duration, globalErrorCollector)
	model.stats["connected"] = connectedCount
	model.stats["sent"] = rpcSentCount
	model.stats["success"] = rpcSuccessCount
	model.stats["errors"] = rpcErrorCount
	if countBased {
		model.stats["target"] = int64(*numMsg)
	}

	var connectWg sync.WaitGroup
	clients := make([]*centrifuge.Client, *numSubs)
	stopChan := make(chan struct{})
	startTime := time.Now()

	connectWg.Add(*numSubs)

	for i := 0; i < *numSubs; i++ {
		go func(idx int) {
			c := newConnection("rpc")
			err := c.Connect()
			if err != nil {
				connectWg.Done()
				return
			}

			atomic.AddInt64(&connectedCount, 1)
			clients[idx] = c
			connectWg.Done()

			var rl ratelimit.Limiter
			if *rpcRate > 0 {
				rl = ratelimit.New(*rpcRate)
			}

			for {
				select {
				case <-stopChan:
					return
				default:
					// Check if we've reached the target count
					if countBased && atomic.LoadInt64(&rpcSuccessCount) >= int64(*numMsg) {
						return
					}

					if rl != nil {
						rl.Take()
					}

					rpcStart := time.Now()
					atomic.AddInt64(&rpcSentCount, 1)

					data, _ := json.Marshal(map[string]interface{}{
						"timestamp": time.Now().UnixNano(),
						"client_id": idx,
					})

					_, err := c.RPC(context.Background(), "benchmark", data)
					latency := time.Since(rpcStart)

					if err != nil {
						atomic.AddInt64(&rpcErrorCount, 1)
					} else {
						atomic.AddInt64(&rpcSuccessCount, 1)
						// Store latency (key is just a counter)
						latencies.Store(atomic.LoadInt64(&rpcSuccessCount), latency)
					}
				}
			}
		}(i)
		time.Sleep(time.Millisecond)
	}

	connectWg.Wait()

	p := tea.NewProgram(model)
	go func() {
		if countBased {
			// Wait until we reach the target
			for atomic.LoadInt64(&rpcSuccessCount) < int64(*numMsg) {
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			// Wait for duration
			time.Sleep(time.Duration(*deadline) * time.Second)
		}
		close(stopChan)
		time.Sleep(100 * time.Millisecond)
		p.Quit()
	}()

	if _, err := p.Run(); err != nil {
		log.Fatal(err)
	}

	for _, c := range clients {
		if c != nil {
			c.Close()
		}
	}

	// Display errors and final stats
	globalErrorCollector.display()
	displayRPCStats(startTime, &rpcSentCount, &rpcSuccessCount, &rpcErrorCount, latencies)
}

func displayRPCStats(startTime time.Time, rpcSentCount, rpcSuccessCount, rpcErrorCount *int64, latencies *sync.Map) {
	totalDuration := time.Since(startTime)
	finalSent := atomic.LoadInt64(rpcSentCount)
	finalSuccess := atomic.LoadInt64(rpcSuccessCount)
	finalErrors := atomic.LoadInt64(rpcErrorCount)

	// Collect latencies into a slice
	var latencySlice []time.Duration
	latencies.Range(func(key, value interface{}) bool {
		if lat, ok := value.(time.Duration); ok {
			latencySlice = append(latencySlice, lat)
		}
		return true
	})

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("  RPC BENCHMARK RESULTS\n")
	fmt.Printf("%s\n\n", strings.Repeat("=", 70))

	fmt.Printf("Duration: %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Total RPCs sent: %s\n", commaFormat(finalSent))
	fmt.Printf("Successful: %s\n", commaFormat(finalSuccess))
	fmt.Printf("Errors: %s\n", commaFormat(finalErrors))

	if totalDuration.Seconds() > 0 {
		fmt.Printf("Average rate: %.2f calls/sec\n", float64(finalSent)/totalDuration.Seconds())
	}

	if len(latencySlice) > 0 {
		sort.Slice(latencySlice, func(i, j int) bool {
			return latencySlice[i] < latencySlice[j]
		})

		var sum time.Duration
		for _, l := range latencySlice {
			sum += l
		}
		avg := sum / time.Duration(len(latencySlice))

		p50 := latencySlice[len(latencySlice)*50/100]
		p90 := latencySlice[len(latencySlice)*90/100]
		p99 := latencySlice[len(latencySlice)*99/100]
		minLat := latencySlice[0]
		maxLat := latencySlice[len(latencySlice)-1]

		fmt.Printf("\nRPC Latency Distribution:\n")
		fmt.Printf("  Min:  %v\n", minLat)
		fmt.Printf("  Avg:  %v\n", avg)
		fmt.Printf("  p50:  %v\n", p50)
		fmt.Printf("  p90:  %v\n", p90)
		fmt.Printf("  p99:  %v\n", p99)
		fmt.Printf("  Max:  %v\n", maxLat)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
}

// A Sample for a particular client
type Sample struct {
	JobMsgCnt int
	MsgCnt    uint64
	MsgBytes  uint64
	IOBytes   uint64
	Start     time.Time
	End       time.Time
}

// SampleGroup for a number of samples, the group is a Sample itself agregating the values the Samples
type SampleGroup struct {
	Sample
	Samples []*Sample
}

// Benchmark to hold the various Samples organized by publishers and subscribers
type Benchmark struct {
	Sample
	Name       string
	RunID      string
	Pubs       *SampleGroup
	Subs       *SampleGroup
	subChannel chan *Sample
	pubChannel chan *Sample
}

// NewBenchmark initializes a Benchmark. After creating a bench call AddSubSample/AddPubSample.
// When done collecting samples, call EndBenchmark
func NewBenchmark(name string, subCnt, pubCnt int) *Benchmark {
	bm := Benchmark{Name: name, RunID: strconv.Itoa(int(time.Now().Unix()))}
	bm.Subs = NewSampleGroup()
	bm.Pubs = NewSampleGroup()
	bm.subChannel = make(chan *Sample, subCnt)
	bm.pubChannel = make(chan *Sample, pubCnt)
	return &bm
}

// Close organizes collected Samples and calculates aggregates. After Close(), no more samples can be added.
func (bm *Benchmark) Close() {
	close(bm.subChannel)
	close(bm.pubChannel)

	for s := range bm.subChannel {
		bm.Subs.AddSample(s)
	}
	for s := range bm.pubChannel {
		bm.Pubs.AddSample(s)
	}

	if bm.Subs.HasSamples() {
		bm.Start = bm.Subs.Start
		bm.End = bm.Subs.End
	} else {
		bm.Start = bm.Pubs.Start
		bm.End = bm.Pubs.End
	}

	if bm.Subs.HasSamples() && bm.Pubs.HasSamples() {
		if bm.Start.After(bm.Subs.Start) {
			bm.Start = bm.Subs.Start
		}
		if bm.Start.After(bm.Pubs.Start) {
			bm.Start = bm.Pubs.Start
		}

		if bm.End.Before(bm.Subs.End) {
			bm.End = bm.Subs.End
		}
		if bm.End.Before(bm.Pubs.End) {
			bm.End = bm.Pubs.End
		}
	}

	bm.MsgBytes = bm.Pubs.MsgBytes + bm.Subs.MsgBytes
	bm.IOBytes = bm.Pubs.IOBytes + bm.Subs.IOBytes
	bm.MsgCnt = bm.Pubs.MsgCnt + bm.Subs.MsgCnt
	bm.JobMsgCnt = bm.Pubs.JobMsgCnt + bm.Subs.JobMsgCnt
}

// AddSubSample to the benchmark
func (bm *Benchmark) AddSubSample(s *Sample) {
	bm.subChannel <- s
}

// AddPubSample to the benchmark
func (bm *Benchmark) AddPubSample(s *Sample) {
	bm.pubChannel <- s
}

// NewSample creates a new Sample initialized to the provided values.
func NewSample(jobCount int, msgSize int, start, end time.Time) *Sample {
	s := Sample{JobMsgCnt: jobCount, Start: start, End: end}
	s.MsgCnt = uint64(jobCount)
	s.MsgBytes = uint64(msgSize * jobCount)
	return &s
}

// Throughput of bytes per second.
func (s *Sample) Throughput() float64 {
	return float64(s.MsgBytes) / s.Duration().Seconds()
}

// Rate of messages in the job per second.
func (s *Sample) Rate() int64 {
	return int64(float64(s.JobMsgCnt) / s.Duration().Seconds())
}

func (s *Sample) String() string {
	rate := commaFormat(s.Rate())
	throughput := HumanBytes(s.Throughput(), false)
	return fmt.Sprintf("%s msgs/sec ~ %s/sec", rate, throughput)
}

// Duration that the sample was active.
func (s *Sample) Duration() time.Duration {
	return s.End.Sub(s.Start)
}

// Seconds that the sample or samples were active.
func (s *Sample) Seconds() float64 {
	return s.Duration().Seconds()
}

// NewSampleGroup initializer.
func NewSampleGroup() *SampleGroup {
	s := new(SampleGroup)
	s.Samples = make([]*Sample, 0)
	return s
}

// Statistics information of the sample group (min, average, max and standard deviation).
func (sg *SampleGroup) Statistics() string {
	return fmt.Sprintf("min %s | avg %s | max %s | stddev %s msgs", commaFormat(sg.MinRate()), commaFormat(sg.AvgRate()), commaFormat(sg.MaxRate()), commaFormat(int64(sg.StdDev())))
}

// MinRate returns the smallest message rate in the SampleGroup.
func (sg *SampleGroup) MinRate() int64 {
	m := int64(0)
	for i, s := range sg.Samples {
		if i == 0 {
			m = s.Rate()
		}
		m = min(m, s.Rate())
	}
	return m
}

// MaxRate returns the largest message rate in the SampleGroup.
func (sg *SampleGroup) MaxRate() int64 {
	m := int64(0)
	for i, s := range sg.Samples {
		if i == 0 {
			m = s.Rate()
		}
		m = max(m, s.Rate())
	}
	return m
}

// AvgRate returns the average of all the message rates in the SampleGroup.
func (sg *SampleGroup) AvgRate() int64 {
	sum := uint64(0)
	for _, s := range sg.Samples {
		sum += uint64(s.Rate())
	}
	return int64(sum / uint64(len(sg.Samples)))
}

// StdDev returns the standard deviation the message rates in the SampleGroup.
func (sg *SampleGroup) StdDev() float64 {
	avg := float64(sg.AvgRate())
	sum := float64(0)
	for _, c := range sg.Samples {
		sum += math.Pow(float64(c.Rate())-avg, 2)
	}
	variance := sum / float64(len(sg.Samples))
	return math.Sqrt(variance)
}

// AddSample adds a Sample to the SampleGroup. After adding a Sample it shouldn't be modified.
func (sg *SampleGroup) AddSample(e *Sample) {
	sg.Samples = append(sg.Samples, e)

	if len(sg.Samples) == 1 {
		sg.Start = e.Start
		sg.End = e.End
	}
	sg.IOBytes += e.IOBytes
	sg.JobMsgCnt += e.JobMsgCnt
	sg.MsgCnt += e.MsgCnt
	sg.MsgBytes += e.MsgBytes

	if e.Start.Before(sg.Start) {
		sg.Start = e.Start
	}

	if e.End.After(sg.End) {
		sg.End = e.End
	}
}

// HasSamples returns true if the group has samples.
func (sg *SampleGroup) HasSamples() bool {
	return len(sg.Samples) > 0
}

// Report returns a human readable report of the samples taken in the Benchmark.
func (bm *Benchmark) Report() string {
	var buffer bytes.Buffer

	if !bm.Pubs.HasSamples() && !bm.Subs.HasSamples() {
		return "No publisher or subscribers data captured. Nothing to report."
	}

	buffer.WriteString("\n" + strings.Repeat("=", 70) + "\n")
	buffer.WriteString("  BENCHMARK RESULTS\n")
	buffer.WriteString(strings.Repeat("=", 70) + "\n\n")

	totalDuration := bm.End.Sub(bm.Start)
	buffer.WriteString(fmt.Sprintf("Total Duration: %v\n\n", totalDuration.Round(time.Millisecond)))

	if bm.Pubs.HasSamples() {
		buffer.WriteString("PUBLISHERS\n")
		buffer.WriteString(strings.Repeat("-", 70) + "\n")
		buffer.WriteString(fmt.Sprintf("  Count:           %s\n", commaFormat(int64(len(bm.Pubs.Samples)))))
		buffer.WriteString(fmt.Sprintf("  Total Messages:  %s\n", commaFormat(int64(bm.Pubs.JobMsgCnt))))
		if bm.Pubs.MsgCnt > 0 {
			buffer.WriteString(fmt.Sprintf("  Message Size:    %s\n", HumanBytes(float64(bm.Pubs.MsgBytes/bm.Pubs.MsgCnt), false)))
		}
		buffer.WriteString(fmt.Sprintf("  Total Bytes:     %s\n", HumanBytes(float64(bm.Pubs.MsgBytes), false)))
		buffer.WriteString(fmt.Sprintf("  Duration:        %v\n", bm.Pubs.Duration().Round(time.Millisecond)))
		buffer.WriteString(fmt.Sprintf("  Throughput:      %s/sec\n", HumanBytes(bm.Pubs.Throughput(), false)))
		buffer.WriteString(fmt.Sprintf("  Message Rate:    %s msgs/sec\n", commaFormat(bm.Pubs.Rate())))
		if len(bm.Pubs.Samples) > 1 {
			buffer.WriteString(fmt.Sprintf("  Rate Stats:      min %s | avg %s | max %s | stddev %s msgs/sec\n",
				commaFormat(bm.Pubs.MinRate()),
				commaFormat(bm.Pubs.AvgRate()),
				commaFormat(bm.Pubs.MaxRate()),
				commaFormat(int64(bm.Pubs.StdDev()))))
		}
		buffer.WriteString("\n")
	}

	if bm.Subs.HasSamples() {
		buffer.WriteString("SUBSCRIBERS\n")
		buffer.WriteString(strings.Repeat("-", 70) + "\n")
		buffer.WriteString(fmt.Sprintf("  Count:           %s\n", commaFormat(int64(len(bm.Subs.Samples)))))
		buffer.WriteString(fmt.Sprintf("  Total Messages:  %s\n", commaFormat(int64(bm.Subs.JobMsgCnt))))
		if bm.Subs.MsgCnt > 0 {
			buffer.WriteString(fmt.Sprintf("  Message Size:    %s\n", HumanBytes(float64(bm.Subs.MsgBytes/bm.Subs.MsgCnt), false)))
		}
		buffer.WriteString(fmt.Sprintf("  Total Bytes:     %s\n", HumanBytes(float64(bm.Subs.MsgBytes), false)))
		buffer.WriteString(fmt.Sprintf("  Duration:        %v\n", bm.Subs.Duration().Round(time.Millisecond)))
		buffer.WriteString(fmt.Sprintf("  Throughput:      %s/sec\n", HumanBytes(bm.Subs.Throughput(), false)))
		buffer.WriteString(fmt.Sprintf("  Message Rate:    %s msgs/sec\n", commaFormat(bm.Subs.Rate())))
		if len(bm.Subs.Samples) > 1 {
			buffer.WriteString(fmt.Sprintf("  Rate Stats:      min %s | avg %s | max %s | stddev %s msgs/sec\n",
				commaFormat(bm.Subs.MinRate()),
				commaFormat(bm.Subs.AvgRate()),
				commaFormat(bm.Subs.MaxRate()),
				commaFormat(int64(bm.Subs.StdDev()))))
		}
		buffer.WriteString("\n")
	}

	if bm.Pubs.HasSamples() && bm.Subs.HasSamples() {
		buffer.WriteString("OVERALL\n")
		buffer.WriteString(strings.Repeat("-", 70) + "\n")
		buffer.WriteString(fmt.Sprintf("  Total Messages:  %s\n", commaFormat(int64(bm.JobMsgCnt))))
		buffer.WriteString(fmt.Sprintf("  Total Bytes:     %s\n", HumanBytes(float64(bm.MsgBytes), false)))
		buffer.WriteString(fmt.Sprintf("  Throughput:      %s/sec\n", HumanBytes(bm.Throughput(), false)))
		buffer.WriteString(fmt.Sprintf("  Message Rate:    %s msgs/sec\n", commaFormat(bm.Rate())))
		buffer.WriteString("\n")
	}

	buffer.WriteString(strings.Repeat("=", 70) + "\n")

	return buffer.String()
}

func commaFormat(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}
	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}

// HumanBytes formats bytes as a human readable string
func HumanBytes(bytes float64, si bool) string {
	var base = 1024
	pre := []string{"K", "M", "G", "T", "P", "E"}
	var post = "B"
	if si {
		base = 1000
		pre = []string{"k", "M", "G", "T", "P", "E"}
		post = "iB"
	}
	if bytes < float64(base) {
		return fmt.Sprintf("%.2f B", bytes)
	}
	exp := int(math.Log(bytes) / math.Log(float64(base)))
	index := exp - 1
	units := pre[index] + post
	return fmt.Sprintf("%.2f %s", bytes/math.Pow(float64(base), float64(exp)), units)
}

// MsgPerClient divides the number of messages by the number of clients and tries
// to distribute them as evenly as possible
func MsgPerClient(numMsg, numClients int) []int {
	var counts []int
	if numClients == 0 || numMsg == 0 {
		return counts
	}
	counts = make([]int, numClients)
	mc := numMsg / numClients
	for i := 0; i < numClients; i++ {
		counts[i] = mc
	}
	extra := numMsg % numClients
	for i := 0; i < extra; i++ {
		counts[i]++
	}
	return counts
}

// In case of network reconnects, the subscriber clients can lose messages and might never finish waiting for the messages.
// In such a scenario, even a single subscriber client can block the test from finishing. so use the configured deadline to finish waiting and get the accumulated report.
func waitForTestCompletion(wg *sync.WaitGroup, deadlineSeconds *int) bool {
	c := make(chan struct{})
	deadLine := time.NewTimer(time.Duration(*deadlineSeconds) * time.Second)
	go func() {
		wg.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		deadLine.Stop()
		return true // completed normally
	case <-deadLine.C:
		return false // wait timed out
	}
}

var (
	titleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4"))

	headerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7D56F4"))

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#888888"))

	valueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4444"))

	boxStyle = lipgloss.NewStyle().
			Padding(0, 0)
)

type tickMsg time.Time

type UIModel struct {
	mode           string
	startTime      time.Time
	duration       time.Duration
	stats          map[string]interface{}
	quitting       bool
	width          int
	height         int
	errorCollector *errorCollector
	lastTickTime   time.Time
	lastPubCount   int64
	lastSubCount   int64
	currentPubRate float64
	currentSubRate float64
}

func NewUIModel(mode string, duration time.Duration, errorCollector *errorCollector) UIModel {
	now := time.Now()
	return UIModel{
		mode:           mode,
		startTime:      now,
		duration:       duration,
		stats:          make(map[string]interface{}),
		width:          80,
		height:         24,
		errorCollector: errorCollector,
		lastTickTime:   now,
		lastPubCount:   0,
		lastSubCount:   0,
		currentPubRate: 0,
		currentSubRate: 0,
	}
}

func (m UIModel) Init() tea.Cmd {
	return tickCmd()
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m UIModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tickMsg:
		elapsed := time.Since(m.startTime)
		if m.duration > 0 && elapsed >= m.duration {
			m.quitting = true
			return m, tea.Quit
		}

		// Calculate per-second rates for pubsub mode
		if m.mode == "pubsub" {
			now := time.Now()
			timeDelta := now.Sub(m.lastTickTime).Seconds()
			if timeDelta > 0 {
				currentPub := m.getInt64Stat("published")
				currentSub := m.getInt64Stat("received")

				m.currentPubRate = float64(currentPub-m.lastPubCount) / timeDelta
				m.currentSubRate = float64(currentSub-m.lastSubCount) / timeDelta

				m.lastPubCount = currentPub
				m.lastSubCount = currentSub
				m.lastTickTime = now
			}
		}

		return m, tickCmd()

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		}
	}

	return m, nil
}

func (m UIModel) View() string {
	if m.quitting {
		return ""
	}

	var s strings.Builder

	// Title
	title := titleStyle.Render(fmt.Sprintf("Centrifuge Benchmark - %s Mode", strings.ToUpper(m.mode)))
	s.WriteString(title + "\n\n")

	// Time info
	elapsed := time.Since(m.startTime)
	var timeInfo string
	if m.duration > 0 {
		remaining := m.duration - elapsed
		if remaining < 0 {
			remaining = 0
		}
		timeInfo = fmt.Sprintf("%s Elapsed: %s | %s Remaining: %s",
			labelStyle.Render("⏱"),
			valueStyle.Render(elapsed.Round(time.Second).String()),
			labelStyle.Render("⏳"),
			valueStyle.Render(remaining.Round(time.Second).String()))
	} else {
		timeInfo = fmt.Sprintf("%s Elapsed: %s",
			labelStyle.Render("⏱"),
			valueStyle.Render(elapsed.Round(time.Second).String()))
	}
	s.WriteString(timeInfo + "\n\n")

	// Mode-specific stats
	switch m.mode {
	case "idle":
		s.WriteString(m.renderIdleStats())
	case "connect-rate":
		s.WriteString(m.renderConnectRateStats())
	case "rpc":
		s.WriteString(m.renderRPCStats())
	case "pubsub":
		s.WriteString(m.renderPubSubStats())
	}

	// Show error count if there are errors
	if m.errorCollector != nil && m.errorCollector.hasErrors() {
		errorCount := len(m.errorCollector.errors)
		s.WriteString("\n\n")
		s.WriteString(errorStyle.Render(fmt.Sprintf("⚠ %d error(s) occurred", errorCount)))
	}

	s.WriteString("\n\n")
	s.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("#666666")).Render("Press 'q' or Ctrl+C to quit"))

	return boxStyle.Render(s.String())
}

func (m UIModel) renderIdleStats() string {
	connected := m.getInt64Stat("connected")

	elapsed := time.Since(m.startTime)
	uptime := elapsed.Round(time.Second)

	stats := []string{
		headerStyle.Render(" Connection Stats "),
		"",
		fmt.Sprintf("%s %s", labelStyle.Render("Active Connections:"), valueStyle.Render(formatInt(connected))),
		fmt.Sprintf("%s %s", labelStyle.Render("Uptime:"), valueStyle.Render(uptime.String())),
	}

	return strings.Join(stats, "\n")
}

func (m UIModel) renderConnectRateStats() string {
	total := m.getInt64Stat("total")
	completed := m.getInt64Stat("completed")
	failed := m.getInt64Stat("failed")

	var progress string
	if total > 0 {
		pct := float64(completed+failed) / float64(total) * 100
		progress = fmt.Sprintf("%.1f%%", pct)
	} else {
		progress = "0%"
	}

	stats := []string{
		headerStyle.Render(" Connection Progress "),
		"",
		fmt.Sprintf("%s %s", labelStyle.Render("Target:"), valueStyle.Render(formatInt(total))),
		fmt.Sprintf("%s %s", labelStyle.Render("Completed:"), valueStyle.Render(formatInt(completed))),
		fmt.Sprintf("%s %s", labelStyle.Render("Failed:"), errorStyle.Render(formatInt(failed))),
		fmt.Sprintf("%s %s", labelStyle.Render("Progress:"), valueStyle.Render(progress)),
	}

	return strings.Join(stats, "\n")
}

func (m UIModel) renderRPCStats() string {
	connected := m.getInt64Stat("connected")
	sent := m.getInt64Stat("sent")
	success := m.getInt64Stat("success")
	errors := m.getInt64Stat("errors")
	target := m.getInt64Stat("target")

	elapsed := time.Since(m.startTime).Seconds()
	rate := float64(0)
	if elapsed > 0 {
		rate = float64(sent) / elapsed
	}

	stats := []string{
		headerStyle.Render(" Configuration "),
		"",
		fmt.Sprintf("%s %s", labelStyle.Render("Connections:"), valueStyle.Render(formatInt(connected))),
	}

	if target > 0 {
		stats = append(stats, fmt.Sprintf("%s %s", labelStyle.Render("Target RPCs:"), valueStyle.Render(formatInt(target))))
	}

	stats = append(stats, "")
	stats = append(stats, headerStyle.Render(" RPC Calls "))
	stats = append(stats, "")

	if target > 0 {
		stats = append(stats, fmt.Sprintf("%s %s / %s", labelStyle.Render("Successful:"), valueStyle.Render(formatInt(success)), lipgloss.NewStyle().Foreground(lipgloss.Color("#666")).Render(formatInt(target))))
	} else {
		stats = append(stats, fmt.Sprintf("%s %s", labelStyle.Render("Successful:"), valueStyle.Render(formatInt(success))))
	}

	stats = append(stats,
		fmt.Sprintf("%s %s", labelStyle.Render("Total Sent:"), valueStyle.Render(formatInt(sent))),
		fmt.Sprintf("%s %s", labelStyle.Render("Errors:"), errorStyle.Render(formatInt(errors))),
		fmt.Sprintf("%s %s calls/sec", labelStyle.Render("Rate:"), valueStyle.Render(formatInt(int64(rate)))),
	)

	// Progress bar if we have a target
	if target > 0 {
		progressPct := float64(success) / float64(target) * 100
		if progressPct > 100 {
			progressPct = 100
		}
		barWidth := m.width - 20 // Leave padding for box borders and margins
		if barWidth < 20 {
			barWidth = 20
		}
		progressBar := renderProgressBar(int(progressPct), barWidth)

		stats = append(stats, "")
		stats = append(stats, progressBar)
		stats = append(stats, fmt.Sprintf("%s %.1f%%", labelStyle.Render("Progress:"), progressPct))
	}

	return strings.Join(stats, "\n")
}

func (m UIModel) renderPubSubStats() string {
	publishers := m.getInt64Stat("publishers")
	subscribers := m.getInt64Stat("subscribers")
	published := m.getInt64Stat("published")
	received := m.getInt64Stat("received")
	target := m.getInt64Stat("target")

	// Use the calculated per-second rates
	pubRate := m.currentPubRate
	subRate := m.currentSubRate

	// Progress bar for received messages
	var progressBar string
	var progressPct float64
	if target > 0 {
		progressPct = float64(received) / float64(target) * 100
		if progressPct > 100 {
			progressPct = 100
		}
		barWidth := m.width - 20 // Leave padding for box borders and margins
		if barWidth < 20 {
			barWidth = 20
		}
		progressBar = renderProgressBar(int(progressPct), barWidth)
	}

	stats := []string{
		headerStyle.Render(" Configuration "),
		"",
		fmt.Sprintf("%s %s", labelStyle.Render("Publishers:"), valueStyle.Render(formatInt(publishers))),
		fmt.Sprintf("%s %s", labelStyle.Render("Subscribers:"), valueStyle.Render(formatInt(subscribers))),
		"",
		headerStyle.Render(" Publishing "),
		"",
		fmt.Sprintf("%s %s", labelStyle.Render("Published:"), valueStyle.Render(formatInt(published))),
		fmt.Sprintf("%s %s msg/sec", labelStyle.Render("Pub Rate:"), valueStyle.Render(formatInt(int64(pubRate)))),
		"",
		headerStyle.Render(" Receiving "),
		"",
		fmt.Sprintf("%s %s / %s", labelStyle.Render("Received:"), valueStyle.Render(formatInt(received)), lipgloss.NewStyle().Foreground(lipgloss.Color("#666")).Render(formatInt(target))),
		fmt.Sprintf("%s %s msg/sec", labelStyle.Render("Sub Rate:"), valueStyle.Render(formatInt(int64(subRate)))),
	}

	if target > 0 {
		stats = append(stats, "")
		stats = append(stats, progressBar)
		stats = append(stats, fmt.Sprintf("%s %.1f%%", labelStyle.Render("Progress:"), progressPct))
	}

	return strings.Join(stats, "\n")
}

func renderProgressBar(percent int, width int) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	filled := percent * width / 100
	empty := width - filled

	barStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#04B575"))
	emptyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("#333333"))

	bar := barStyle.Render(strings.Repeat("█", filled)) +
		emptyStyle.Render(strings.Repeat("░", empty))

	return bar
}

func (m UIModel) getInt64Stat(key string) int64 {
	if val, ok := m.stats[key]; ok {
		if ptr, ok := val.(*int64); ok {
			return atomic.LoadInt64(ptr)
		}
		if num, ok := val.(int64); ok {
			return num
		}
		if ptr, ok := val.(*int32); ok {
			return int64(atomic.LoadInt32(ptr))
		}
	}
	return 0
}

func formatInt(n int64) string {
	return commaFormat(n)
}
