package feedx_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestScheduler(t *testing.T) {
	beforeCallbacks := new(atomic.Int32)
	afterCallbacks := new(atomic.Int32)
	numCycles := new(atomic.Int32)
	numErrors := new(atomic.Int32)

	resetCounters := func() {
		beforeCallbacks.Store(0)
		afterCallbacks.Store(0)
		numCycles.Store(0)
		numErrors.Store(0)
	}

	obj := bfs.NewInMemObject("file.json")
	defer obj.Close()

	t.Run("produce", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		job, err := feedx.Every(time.Millisecond).
			BeforeSync(func(_ int64) bool {
				beforeCallbacks.Add(1)
				return true
			}).
			AfterSync(func(_ *feedx.Status, err error) {
				afterCallbacks.Add(1)

				if err != nil {
					numErrors.Add(1)
				}
			}).
			WithVersionCheck(func(_ context.Context) (int64, error) {
				return 101, nil
			}).
			Produce(pcr, func(w *feedx.Writer) error {
				if numCycles.Add(1)%2 == 0 {
					return fmt.Errorf("failed!")
				}
				return nil
			})
		if err != nil {
			t.Fatal("unexpected error", err)
		}

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
	})

	t.Run("produce may fail", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		exp := fmt.Errorf("failed!")
		_, err := feedx.Every(time.Millisecond).
			Produce(pcr, func(w *feedx.Writer) error {
				return exp
			})
		if !errors.Is(err, exp) {
			t.Errorf("expected %v, got %v", exp, err)
		}
	})

	t.Run("consume", func(t *testing.T) {
		resetCounters()

		csm := feedx.NewConsumerForRemote(obj)
		defer csm.Close()

		job, err := feedx.Every(time.Millisecond).
			BeforeSync(func(_ int64) bool {
				beforeCallbacks.Add(1)
				return true
			}).
			AfterSync(func(_ *feedx.Status, err error) {
				afterCallbacks.Add(1)

				if err != nil {
					numErrors.Add(1)
				}
			}).
			Consume(csm, func(r *feedx.Reader) error {
				if numCycles.Add(1)%2 == 0 {
					return fmt.Errorf("failed!")
				}
				return nil
			})
		if err != nil {
			t.Fatal("unexpected error", err)
		}

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
	})

	t.Run("consume may fail", func(t *testing.T) {
		resetCounters()

		csm := feedx.NewConsumerForRemote(obj)
		defer csm.Close()

		exp := fmt.Errorf("failed!")
		_, err := feedx.Every(time.Millisecond).
			Consume(csm, func(r *feedx.Reader) error {
				return exp
			})
		if !errors.Is(err, exp) {
			t.Errorf("expected %v, got %v", exp, err)
		}
	})
}
