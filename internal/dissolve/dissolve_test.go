package dissolve

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDissolver(t *testing.T) {
	d := New(4)
	_ = d.Run()
	defer func() { _ = d.Close() }()
	ch := make(chan struct{})
	numJobs := 1024
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(numJobs)
	go func() {
		for i := 0; i < numJobs; i++ {
			err := d.Submit(func() error {
				defer wg.Done()
				return nil
			})
			if err != nil {
				errCh <- err
			}
		}
	}()
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	select {
	case err := <-errCh:
		t.Fatalf("Submit returned error: %v", err)
	default:
	}
}

func TestDissolverErrorHandling(t *testing.T) {
	d := New(4)
	_ = d.Run()
	defer func() { _ = d.Close() }()
	var numFails int
	ch := make(chan struct{}, 1)
	err := d.Submit(func() error {
		if numFails < 10 {
			// Fail Job several times.
			numFails++
			return errors.New("artificial error")
		}
		ch <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("Submit returned error: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestDissolverClose(t *testing.T) {
	d := New(4)
	_ = d.Run()
	_ = d.Close()
	ch := make(chan struct{}, 1)
	err := d.Submit(func() error {
		ch <- struct{}{}
		return nil
	})
	if err == nil {
		t.Fatal("Submit should return error")
	}
}

func BenchmarkSubmitAndProcess(b *testing.B) {
	d := New(1)
	_ = d.Run()
	defer func() { _ = d.Close() }()
	ch := make(chan struct{}, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = d.Submit(func() error {
			ch <- struct{}{}
			return nil
		})
		<-ch
	}
	b.ReportAllocs()
}

func BenchmarkSubmitAndProcessParallel(b *testing.B) {
	d := New(128)
	_ = d.Run()
	defer func() { _ = d.Close() }()
	ch := make(chan struct{}, 1)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = d.Submit(func() error {
				ch <- struct{}{}
				return nil
			})
			<-ch
		}
	})
	b.ReportAllocs()
}
