package feedx_test

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestIncrementalProducer(t *testing.T) {
	numRuns := new(atomic.Int32)
	bucket := bfs.NewInMem()
	version := int64(33)

	prodFunc := func(_ int64) feedx.ProduceFunc {
		return func(w *feedx.Writer) error {
			numRuns.Add(1)

			for i := 0; i < 10; i++ {
				if err := w.Encode(seed()); err != nil {
					return err
				}
			}
			return nil
		}
	}
	versionCheck := func(_ context.Context) (int64, error) { return version, nil }

	t.Run("produces", func(t *testing.T) {
		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, versionCheck, prodFunc)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = p.Close() }()

		if exp, got := int32(1), numRuns.Load(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if dur := time.Since(p.LastAttempt()); dur > time.Second {
			t.Errorf("expected to be recent, but was %s ago", dur)
		}
		if exp, got := version, p.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 10, p.NumWritten(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		mft, err := feedx.LoadManifest(t.Context(), bfs.NewObjectFromBucket(bucket, "manifest.json"))
		if err != nil {
			t.Fatal("unexpected error", err)
		} else if exp := (&feedx.Manifest{
			Version: 33,
			Files:   []string{"data-0-33.pbz"},
		}); !reflect.DeepEqual(exp, mft) {
			t.Errorf("expected %#v, got %#v", exp, mft)
		}

		info, err := bucket.Head(t.Context(), "data-0-33.pbz")
		if err != nil {
			t.Fatal("unexpected error", err)
		} else if max, got := int64(45), info.Size; got > max {
			t.Errorf("expected %v to be < %v", got, max)
		}

		if exp, got := "33", info.Metadata.Get("X-Feedx-Version"); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("skip if not changed", func(t *testing.T) {
		runCount := numRuns.Load()

		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, versionCheck, prodFunc)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = p.Close() }()

		if exp, got := runCount, numRuns.Load(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("run if changed", func(t *testing.T) {
		runCount := numRuns.Load()

		newVersion := func(ctx context.Context) (int64, error) { return 55, nil }
		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, newVersion, prodFunc)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = p.Close() }()

		if exp, got := runCount+1, numRuns.Load(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}
