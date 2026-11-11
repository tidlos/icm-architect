// Package pool is a fixed-size worker pool with graceful shutdown.
package workerpool

import (
	"context"
	"errors"
	"sync"
)

// Job is a unit of work. It should return promptly when ctx is cancelled.
type Job func(ctx context.Context) error

var ErrClosed = errors.New("pool: closed")

type Pool struct {
	jobs   chan Job
	wg     sync.WaitGroup
	mu     sync.RWMutex // guards closed and sends on jobs
	closed bool
	cancel context.CancelFunc
	errs   chan error
}

// New starts n workers with a queue of size queue.
func New(parent context.Context, n, queue int) *Pool {
	ctx, cancel := context.WithCancel(parent)
	p := &Pool{
		jobs:   make(chan Job, queue),
		cancel: cancel,
		errs:   make(chan error, queue+n),
	}
	p.wg.Add(n)
	for i := 0; i < n; i++ {
		go p.worker(ctx)
	}
	return p
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for job := range p.jobs { // drains the queue after close
		if err := job(ctx); err != nil {
			select {
			case p.errs <- err:
			default: // drop if nobody is reading
			}
		}
	}
}

// Submit queues a job. It blocks while the queue is full, and fails
// if the pool is closed or ctx is done.
func (p *Pool) Submit(ctx context.Context, j Job) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case p.jobs <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Errors reports job errors (best effort, non-blocking producer side).
func (p *Pool) Errors() <-chan error { return p.errs }

// Shutdown stops accepting jobs and waits for queued and running jobs to
// finish. If ctx expires first, running jobs are cancelled and Shutdown
// still waits for workers to return, then reports ctx.Err().
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()

	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel() // tell jobs to bail out
		<-done
		return ctx.Err()
	}
}
