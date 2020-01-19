package dissolve

import (
	"errors"
	"testing"
	"time"
)

func TestDissolver(t *testing.T) {
	d := New(4)
	d.Run()
	defer d.Close()
	ch := make(chan struct{}, 1)
	err := d.Submit(func() error {
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

func TestDissolverErrorHandling(t *testing.T) {
	d := New(4)
	d.Run()
	defer d.Close()
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
	d.Run()
	d.Close()
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
	d.Run()
	defer d.Close()
	ch := make(chan struct{}, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Submit(func() error {
			ch <- struct{}{}
			return nil
		})
		<-ch
	}
	b.ReportAllocs()
}

func BenchmarkSubmitAndProcessParallel(b *testing.B) {
	d := New(128)
	d.Run()
	defer d.Close()
	ch := make(chan struct{}, 1)
	b.SetParallelism(128)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Submit(func() error {
				ch <- struct{}{}
				return nil
			})
			<-ch
		}
	})
	b.ReportAllocs()
}
