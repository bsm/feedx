package feedx_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestProducer(t *testing.T) {
	numRuns := new(atomic.Int32)

	t.Run("default", func(t *testing.T) {
		p, _ := testProducer(t, nil, numRuns)
		defer func() { _ = p.Close() }()

		if dur := time.Since(p.LastModified()); dur > time.Second {
			t.Errorf("expected to be recent, but was %s ago", dur)
		}

		if err := p.Close(); err != nil {
			t.Fatal("unexpected error", err)
		}
	})

	t.Run("custom last-mod-check", func(t *testing.T) {
		opt := &feedx.ProducerOptions{
			Interval:     5 * time.Millisecond,
			LastModCheck: func(_ context.Context) (time.Time, error) { return time.Unix(1515151515, 987654321), nil },
		}

		p, info := testProducer(t, opt, numRuns)
		if exp, got := time.Unix(1515151515, 987000000), p.LastModified(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := "1515151515987", info.Metadata.Get("X-Feedx-Last-Modified"); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		lastAttempt := p.LastAttempt()
		runCount := numRuns.Load()

		for i := 0; i < 20; i++ {
			if numRuns.Load() > runCount {
				break
			}
			time.Sleep(time.Millisecond)
		}

		if dur := p.LastAttempt().Sub(lastAttempt); dur < opt.Interval {
			t.Errorf("expected interval between runs to be %v, but was %v", opt.Interval, dur)
		}

		if err := p.Close(); err != nil {
			t.Fatal("unexpected error", err)
		}
	})
}

func testProducer(t *testing.T, opt *feedx.ProducerOptions, numRuns *atomic.Int32) (*feedx.Producer, *bfs.MetaInfo) {
	obj := bfs.NewInMemObject("path/to/file.json")
	runCount := numRuns.Load()

	p, err := feedx.NewProducerForRemote(t.Context(), obj, opt, func(w *feedx.Writer) error {
		numRuns.Add(1)

		for i := 0; i < 10; i++ {
			if err := w.Encode(seed()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if exp, got := runCount+1, numRuns.Load(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if dur := time.Since(p.LastAttempt()); dur > time.Second {
		t.Errorf("expected to be recent, but was %s ago", dur)
	}
	if exp, got := 10, p.NumWritten(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	info, err := obj.Head(t.Context())
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if exp, got := int64(370), info.Size; exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}

	return p, info
}
