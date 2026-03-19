package feedx_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestJob(t *testing.T) {
	beforeCallbacks := new(atomic.Int32)
	numCycles := new(atomic.Int32)

	resetCounters := func() {
		beforeCallbacks.Store(0)
		numCycles.Store(0)
	}

	obj := bfs.NewInMemObject("file.json")
	defer obj.Close()

	t.Run("produce", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		status, err := feedx.NewJob(nil).
			BeforeSync(func(_ int64) bool {
				beforeCallbacks.Add(1)
				return true
			}).
			WithVersionCheck(func(_ context.Context) (int64, error) {
				return 101, nil
			}).
			ProduceWith(context.Background(), pcr, func(w *feedx.Writer) error {
				numCycles.Add(1)
				return nil
			})
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if status == nil {
			t.Fatal("expected status, got nil")
		}
		if exp, got := int32(1), numCycles.Load(); exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
		if exp, got := int32(1), beforeCallbacks.Load(); exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
		if exp, got := int64(101), status.LocalVersion; exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
	})

	t.Run("produce skipped by before hook", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		status, err := feedx.NewJob(nil).
			BeforeSync(func(_ int64) bool {
				beforeCallbacks.Add(1)
				return false
			}).
			ProduceWith(context.Background(), pcr, func(w *feedx.Writer) error {
				numCycles.Add(1)
				return nil
			})
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if status == nil {
			t.Fatal("expected status, got nil")
		}
		if !status.Skipped {
			t.Error("expected status to be skipped")
		}
		if exp, got := int32(0), numCycles.Load(); exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
		if exp, got := int32(1), beforeCallbacks.Load(); exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
	})

	t.Run("produce may fail", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		exp := fmt.Errorf("failed!")
		_, err := feedx.NewJob(nil).
			ProduceWith(context.Background(), pcr, func(w *feedx.Writer) error {
				return exp
			})
		if !errors.Is(err, exp) {
			t.Errorf("expected %v, got %v", exp, err)
		}
	})

	t.Run("produce version check may fail", func(t *testing.T) {
		resetCounters()

		pcr := feedx.NewProducerForRemote(obj)
		defer pcr.Close()

		exp := fmt.Errorf("version check failed!")
		_, err := feedx.NewJob(nil).
			WithVersionCheck(func(_ context.Context) (int64, error) {
				return 0, exp
			}).
			ProduceWith(context.Background(), pcr, func(w *feedx.Writer) error {
				numCycles.Add(1)
				return nil
			})
		if !errors.Is(err, exp) {
			t.Errorf("expected %v, got %v", exp, err)
		}
		if exp, got := int32(0), numCycles.Load(); exp != got {
			t.Errorf("expected %d, got %d", exp, got)
		}
	})
}
