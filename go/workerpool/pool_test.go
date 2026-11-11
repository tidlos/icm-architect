package workerpool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainsOnShutdown(t *testing.T) {
	p := New(context.Background(), 4, 16)
	var n atomic.Int32
	for i := 0; i < 50; i++ {
		if err := p.Submit(context.Background(), func(context.Context) error {
			time.Sleep(time.Millisecond)
			n.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 50 {
		t.Fatalf("ran %d, want 50", n.Load())
	}
	if err := p.Submit(context.Background(), nil); err != ErrClosed {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

func TestShutdownTimeoutCancelsJobs(t *testing.T) {
	p := New(context.Background(), 1, 1)
	_ = p.Submit(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
}
