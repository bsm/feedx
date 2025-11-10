package feedx_test

import (
	"reflect"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestIncrementalProducer(t *testing.T) {
	bucket := bfs.NewInMem()
	defer bucket.Close()

	pcr := feedx.NewIncrementalProducerForBucket(bucket)
	defer pcr.Close()

	// first produce
	testIncProduce(t, pcr, 101, &feedx.Status{LocalVersion: 101, NumItems: 10})

	// second produce
	testIncProduce(t, pcr, 101, &feedx.Status{Skipped: true, LocalVersion: 101, RemoteVersion: 101})

	// increment version
	testIncProduce(t, pcr, 134, &feedx.Status{LocalVersion: 134, RemoteVersion: 101, NumItems: 3})

	obj := bfs.NewObjectFromBucket(bucket, "manifest.json")
	defer obj.Close()

	mft, err := feedx.LoadManifest(t.Context(), obj)
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if exp := (&feedx.Manifest{
		Version: 134,
		Files:   []string{"data-0-101.json", "data-0-134.json"},
	}); !reflect.DeepEqual(exp, mft) {
		t.Errorf("expected %#v, got %#v", exp, mft)
	}
}

func testIncProduce(t *testing.T, pcr *feedx.IncrementalProducer, version int64, exp *feedx.Status) {
	t.Helper()

	status, err := pcr.Produce(t.Context(), version, nil, func(sinceVersion int64) feedx.ProduceFunc {
		return func(w *feedx.Writer) error {
			n := (version - sinceVersion) / 10
			for i := int64(0); i < n; i++ {
				if err := w.Encode(seed()); err != nil {
					return err
				}
			}
			return nil
		}
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	if !reflect.DeepEqual(exp, status) {
		t.Errorf("expected %#v, got %#v", exp, status)
	}
}
