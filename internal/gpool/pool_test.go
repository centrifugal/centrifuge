package gpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorker_New(t *testing.T) {
	jobQueue := make(chan Job)
	worker := newWorker(jobQueue)
	worker.start()
	require.NotNil(t, worker)

	called := false
	done := make(chan bool)

	job := func() {
		called = true
		done <- true
	}

	worker.jobQueue <- job
	<-done
	require.Equal(t, true, called)
}

func TestPool_New(t *testing.T) {
	pool := NewPool(1000, 10000)
	defer func() { _ = pool.Close(context.Background()) }()

	numJobs := 10000
	var wg sync.WaitGroup
	wg.Add(numJobs)
	var counter uint64

	for i := 0; i < numJobs; i++ {
		arg := uint64(1)

		job := func() {
			defer wg.Done()
			atomic.AddUint64(&counter, arg)
			require.Equal(t, uint64(1), arg)
		}

		pool.JobQueue <- job
	}

	wg.Wait()

	require.Equal(t, uint64(numJobs), atomic.LoadUint64(&counter))
}

func TestPool_Close(t *testing.T) {
	pool := NewPool(100, 10)

	numJobs := 1000

	for i := 0; i < numJobs; i++ {
		job := func() {}
		pool.JobQueue <- job
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = pool.Close(ctx)
}

func TestPool_CloseContext(t *testing.T) {
	pool := NewPool(1, 0)

	pool.JobQueue <- func() {
		time.Sleep(5 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := pool.Close(ctx)
	require.Equal(t, context.DeadlineExceeded, err)
}

func BenchmarkPool_RawPerformance(b *testing.B) {
	pool := NewPool(1, 0)
	defer func() { _ = pool.Close(context.Background()) }()

	ch := make(chan struct{}, 1)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		pool.JobQueue <- func() {
			ch <- struct{}{}
		}
		<-ch
	}
}

func BenchmarkPool_Sequential(b *testing.B) {
	pool := NewPool(16, 0)
	defer func() { _ = pool.Close(context.Background()) }()

	for n := 0; n < b.N; n++ {
		var wg sync.WaitGroup
		wg.Add(1)
		pool.JobQueue <- func() {
			time.Sleep(100 * time.Millisecond)
			wg.Done()
		}
		wg.Wait()
	}
}

func BenchmarkPool_Parallel(b *testing.B) {
	pool := NewPool(4096, 0)
	defer func() { _ = pool.Close(context.Background()) }()

	b.SetParallelism(4096)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var wg sync.WaitGroup
			wg.Add(1)
			pool.JobQueue <- func() {
				time.Sleep(100 * time.Millisecond)
				wg.Done()
			}
			wg.Wait()
		}
	})
}
