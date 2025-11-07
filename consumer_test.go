package feedx_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
)

func TestConsumer(t *testing.T) {
	t.Run("consumes", func(t *testing.T) {
		csm := fixConsumer(t, 33)
		defer csm.Close()

		if exp, got := int64(0), csm.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// first attempt
		msgs := testConsume(t, csm, &feedx.ConsumeStatus{
			PreviousVersion: 0,
			Version:         33,
			Skipped:         false,
			NumRead:         2,
		})
		if exp, got := int64(33), csm.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 2, len(msgs); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// second attempt
		_ = testConsume(t, csm, &feedx.ConsumeStatus{
			PreviousVersion: 33,
			Version:         33,
			Skipped:         true,
			NumRead:         0,
		})
	})

	t.Run("always if no version", func(t *testing.T) {
		csm := fixConsumer(t, 0)
		defer csm.Close()

		testConsume(t, csm, &feedx.ConsumeStatus{NumRead: 2})
		testConsume(t, csm, &feedx.ConsumeStatus{NumRead: 2})
	})

	t.Run("incremental", func(t *testing.T) {
		csm := fixIncrementalConsumer(t, 33)
		defer csm.Close()

		// first attempt
		msgs := testConsume(t, csm, &feedx.ConsumeStatus{
			PreviousVersion: 0,
			Version:         33,
			NumRead:         4,
		})
		if exp, got := int64(33), csm.Version(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		if exp, got := 4, len(msgs); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		// second attempt
		_ = testConsume(t, csm, &feedx.ConsumeStatus{
			PreviousVersion: 33,
			Version:         33,
			Skipped:         true,
		})
	})

}

func fixConsumer(t *testing.T, version int64) feedx.Consumer {
	t.Helper()

	obj := bfs.NewInMemObject("path/to/file.json")
	if err := writeN(obj, 2, version); err != nil {
		t.Fatal("unexpected error", err)
	}
	csm := feedx.NewConsumerForRemote(obj)
	t.Cleanup(func() { _ = csm.Close() })

	return csm
}

func fixIncrementalConsumer(t *testing.T, version int64) feedx.Consumer {
	t.Helper()

	bucket := bfs.NewInMem()
	obj1 := bfs.NewObjectFromBucket(bucket, "data-0-0.json")
	if err := writeN(obj1, 2, 0); err != nil {
		t.Fatal("unexpected error", err)
	}
	defer obj1.Close()

	obj2 := bfs.NewObjectFromBucket(bucket, "data-0-1.json")
	if err := writeN(obj2, 2, 0); err != nil {
		t.Fatal("unexpected error", err)
	}
	defer obj2.Close()

	objm := bfs.NewObjectFromBucket(bucket, "manifest.json")
	defer objm.Close()

	manifest := &feedx.Manifest{
		Version: version,
		Files:   []string{obj1.Name(), obj2.Name()},
	}
	writer := feedx.NewWriter(t.Context(), objm, &feedx.WriterOptions{Version: version})
	defer writer.Discard()

	if err := writer.Encode(manifest); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := writer.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}

	csm := feedx.NewIncrementalConsumerForBucket(bucket)
	t.Cleanup(func() { _ = csm.Close() })

	return csm
}

func testConsume(t *testing.T, csm feedx.Consumer, exp *feedx.ConsumeStatus) (msgs []*testdata.MockMessage) {
	t.Helper()

	status, err := csm.Consume(t.Context(), nil, func(ctx context.Context, r *feedx.Reader) (err error) {
		msgs, err = readMessages(r)
		return err
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	if !reflect.DeepEqual(exp, status) {
		t.Errorf("expected %#v, got %#v", exp, status)
	}
	return
}
