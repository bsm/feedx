package feedx_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestConsumer(t *testing.T) {
	modTime := time.Unix(1515151515, 123456789)

	t.Run("consumes", func(t *testing.T) {
		csm := fixConsumer(t, modTime)
		if exp, got := csm.LastAttempt(), csm.LastSuccess(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		testConsumer(t, csm, time.Now().Add(-time.Second), modTime, 2)
	})

	t.Run("not if not changed", func(t *testing.T) {
		csm := fixConsumer(t, modTime)
		prevAttempt := csm.LastAttempt()
		time.Sleep(time.Millisecond)

		if err := csm.(interface{ TestSync() error }).TestSync(); err != nil {
			t.Fatal("unexpected error", err)
		}
		if exp, got := prevAttempt, csm.LastSuccess(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		testConsumer(t, csm, prevAttempt, modTime, 2)
	})

	t.Run("always if no mtime", func(t *testing.T) {
		csm := fixConsumer(t, time.Time{})
		prevSuccess := csm.LastSuccess()
		time.Sleep(time.Millisecond)

		if err := csm.(interface{ TestSync() error }).TestSync(); err != nil {
			t.Fatal("unexpected error", err)
		}
		if prev, cur := prevSuccess, csm.LastSuccess(); !prev.Before(cur) {
			t.Errorf("expected %v to be > %v", cur, prev)
		}
		if exp, got := time.Unix(0, 0), csm.LastModified(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}

func TestIncrementalConsumer(t *testing.T) {
	modTime := time.Unix(1515151515, 123456789)

	t.Run("consumes", func(t *testing.T) {
		csm := fixIncrementalConsumer(t, modTime)
		if exp, got := csm.LastAttempt(), csm.LastSuccess(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		testConsumer(t, csm, time.Now().Add(-time.Second), modTime, 4)
	})

	t.Run("not if not changed", func(t *testing.T) {
		csm := fixIncrementalConsumer(t, modTime)
		prevAttempt := csm.LastAttempt()
		time.Sleep(time.Millisecond)

		if err := csm.(interface{ TestSync() error }).TestSync(); err != nil {
			t.Fatal("unexpected error", err)
		}
		if exp, got := prevAttempt, csm.LastSuccess(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
		testConsumer(t, csm, prevAttempt, modTime, 4)
	})

	t.Run("always if no mtime", func(t *testing.T) {
		csm := fixIncrementalConsumer(t, time.Time{})
		prevSuccess := csm.LastSuccess()
		time.Sleep(time.Millisecond)

		if err := csm.(interface{ TestSync() error }).TestSync(); err != nil {
			t.Fatal("unexpected error", err)
		}
		if prev, cur := prevSuccess, csm.LastSuccess(); !prev.Before(cur) {
			t.Errorf("expected %v to be > %v", cur, prev)
		}
		if exp, got := time.Unix(0, 0), csm.LastModified(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}

func fixConsumer(t *testing.T, modTime time.Time) feedx.Consumer {
	t.Helper()

	obj := bfs.NewInMemObject("path/to/file.json")
	if err := writeN(obj, 2, modTime); err != nil {
		t.Fatal("unexpected error", err)
	}
	csm, err := feedx.NewConsumerForRemote(t.Context(), obj, nil, func(r *feedx.Reader) (interface{}, error) {
		return readMessages(r)
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	t.Cleanup(func() { _ = csm.Close() })

	return csm
}

func fixIncrementalConsumer(t *testing.T, modTime time.Time) feedx.Consumer {
	t.Helper()

	bucket := bfs.NewInMem()
	dataFile := bfs.NewObjectFromBucket(bucket, "data-0-20230501-120023123.jsonz")
	if err := writeN(dataFile, 2, modTime); err != nil {
		t.Fatal("unexpected error", err)
	}

	manifest := &feedx.Manifest{
		LastModified: feedx.TimestampFromTime(modTime),
		Files:        []string{dataFile.Name(), dataFile.Name()},
	}
	writer := feedx.NewWriter(t.Context(), bfs.NewObjectFromBucket(bucket, "manifest.json"), &feedx.WriterOptions{LastMod: modTime})
	defer writer.Discard()

	if err := writer.Encode(manifest); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := writer.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}

	csm, err := feedx.NewIncrementalConsumerForBucket(t.Context(), bucket, nil, func(r *feedx.Reader) (interface{}, error) {
		return readMessages(r)
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	t.Cleanup(func() { _ = csm.Close() })

	return csm
}

func testConsumer(t *testing.T, csm feedx.Consumer, minLastAttempt, modTime time.Time, numRead int) {
	if min, got := minLastAttempt, csm.LastAttempt(); !min.Before(got) {
		t.Errorf("expected %v to be not before %v", got, min)
	}
	if exp, got := modTime.Truncate(time.Millisecond), csm.LastModified(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := numRead, csm.NumRead(); exp != got {
		t.Errorf("expected %v, got %v", exp, got)
	}
	if exp, got := seedN(numRead), csm.Data(); !reflect.DeepEqual(exp, got) {
		t.Errorf("expected %#v, got %#v", exp, got)
	}
}
