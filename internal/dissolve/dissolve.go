package dissolve

import (
	"errors"
	"runtime"
)

// Dissolver ...
type Dissolver struct {
	queue     Queue
	semaphore chan struct{}
	nWorkers  int
}

// New ...
func New(maxConcurrency int, nWorkers int) *Dissolver {
	return &Dissolver{
		queue:     newQueue(),
		nWorkers:  nWorkers,
		semaphore: make(chan struct{}, maxConcurrency),
	}
}

// Run ...
func (d *Dissolver) Run() error {
	for i := 0; i < d.nWorkers; i++ {
		go d.runWorker()
	}
	return nil
}

// Close ...
func (d *Dissolver) Close() error {
	d.queue.Close()
	return nil
}

// Submit ...
func (d *Dissolver) Submit(job Job) error {
	if !d.queue.Add(job) {
		return errors.New("can not submit job to closed dissolver")
	}
	return nil
}

func (d *Dissolver) runWorker() {
	for {
		d.semaphore <- struct{}{}
		job, ok := d.queue.Wait()
		if !ok {
			<-d.semaphore
			break
		}
		err := job()
		if err != nil {
			// Put to the end of queue.
			runtime.Gosched()
			d.queue.Add(job)
		}
		<-d.semaphore
	}
}
