package gpool

import "context"

// Job represents function to be executed in worker.
type Job func()

type worker struct {
	jobs chan Job
	stop chan struct{}
	done chan struct{}
}

func newWorker(jobs chan Job) *worker {
	return &worker{
		jobs: jobs,
		stop: make(chan struct{}, 1),
		done: make(chan struct{}, 1),
	}
}

func (w *worker) start() {
	go func() {
		for {
			select {
			case job := <-w.jobs:
				job()
			case <-w.stop:
				w.done <- struct{}{}
				return
			}
		}
	}()
}

// Pool of worker goroutines.
type Pool struct {
	workers []*worker
	Jobs    chan Job
}

// NewPool will make a pool of worker goroutines.
// Returned object contains Jobs to send a job for execution.
func NewPool(numWorkers int) *Pool {
	jobs := make(chan Job, 0)
	workers := make([]*worker, 0, numWorkers)

	for i := 0; i < numWorkers; i++ {
		worker := newWorker(jobs)
		worker.start()
		workers = append(workers, worker)
	}

	return &Pool{
		Jobs:    jobs,
		workers: workers,
	}
}

// Close will release resources used by a pool.
func (p *Pool) Close(ctx context.Context) error {
	for i := 0; i < len(p.workers); i++ {
		worker := p.workers[i]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case worker.stop <- struct{}{}:
		}
	}

	for i := 0; i < len(p.workers); i++ {
		worker := p.workers[i]
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-worker.done:
		}
	}

	return nil
}
