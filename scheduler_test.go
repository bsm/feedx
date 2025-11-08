package feedx_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestSchedulerConsume(t *testing.T) {
	beforeCallbacks := new(atomic.Int32)
	afterCallbacks := new(atomic.Int32)
	numCycles := new(atomic.Int32)
	numErrors := new(atomic.Int32)

	obj := bfs.NewInMemObject("file.json")
	defer obj.Close()

	csm := feedx.NewConsumerForRemote(obj)
	defer csm.Close()

	job := feedx.Every(time.Millisecond).
		BeforeConsume(func() bool {
			beforeCallbacks.Add(1)
			return true
		}).
		AfterConsume(func(cs *feedx.ConsumeStatus, err error) {
			afterCallbacks.Add(1)

			if err != nil {
				numErrors.Add(1)
			}
		}).
		Consume(csm, func(ctx context.Context, r *feedx.Reader) error {
			if numCycles.Add(1)%2 == 0 {
				return fmt.Errorf("failed!")
			}
			return nil
		})

	time.Sleep(5 * time.Millisecond)
	job.Stop()
	time.Sleep(2 * time.Millisecond)

	ranTimes := numCycles.Load()
	if min, got := 4, int(ranTimes); got <= min {
		t.Errorf("expected %d >= %d", got, min)
	}
	if exp, got := ranTimes, beforeCallbacks.Load(); exp != got {
		t.Errorf("expected %d, got %d", exp, got)
	}
	if exp, got := ranTimes, afterCallbacks.Load(); exp != got {
		t.Errorf("expected %d, got %d", exp, got)
	}
	if exp, got := ranTimes/2, numErrors.Load(); exp != got {
		t.Errorf("expected %d, got %d", exp, got)
	}

	// wait a little longer, make sure job was stopped
	time.Sleep(2 * time.Millisecond)
	if exp, got := ranTimes, numCycles.Load(); exp != got {
		t.Errorf("expected %d, got %d", exp, got)
	}
}
