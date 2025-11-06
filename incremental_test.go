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
	modTime := time.Unix(1515151515, 123456789)

	prodFunc := func(_ time.Time) feedx.ProduceFunc {
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
	lastModFunc := func(_ context.Context) (time.Time, error) { return modTime, nil }

	t.Run("produces", func(t *testing.T) {
		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, lastModFunc, prodFunc)
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
		if exp, got := time.Unix(1515151515, 123000000), p.LastModified(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 10, p.NumWritten(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		mft, err := feedx.LoadManifest(t.Context(), bfs.NewObjectFromBucket(bucket, "manifest.json"))
		if err != nil {
			t.Fatal("unexpected error", err)
		} else if exp := (&feedx.Manifest{
			LastModified: feedx.TimestampFromTime(modTime),
			Files:        []string{"data-0-20180105-112515123.pbz"},
		}); !reflect.DeepEqual(exp, mft) {
			t.Errorf("expected %#v, got %#v", exp, mft)
		}

		info, err := bucket.Head(t.Context(), "data-0-20180105-112515123.pbz")
		if err != nil {
			t.Fatal("unexpected error", err)
		} else if max, got := int64(45), info.Size; got > max {
			t.Errorf("expected %v to be < %v", got, max)
		}

		if exp, got := "1515151515123", info.Metadata.Get("X-Feedx-Last-Modified"); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("skip if not changed", func(t *testing.T) {
		runCount := numRuns.Load()

		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, lastModFunc, prodFunc)
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

		newLastMod := func(ctx context.Context) (time.Time, error) { return modTime.Add(time.Hour), nil }
		p, err := feedx.NewIncrementalProducerForBucket(t.Context(), bucket, nil, newLastMod, prodFunc)
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		defer func() { _ = p.Close() }()

		if exp, got := runCount+1, numRuns.Load(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}
