package feedx_test

import (
	"reflect"
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestProducer(t *testing.T) {
	obj := bfs.NewInMemObject("path/to/file.json")
	defer obj.Close()

	pcr := feedx.NewProducerForRemote(obj)
	defer pcr.Close()

	// first attempt
	testProduce(t, pcr, 101, &feedx.Status{
		LocalVersion: 101,
		NumItems:     10,
	})

	// second attempt
	testProduce(t, pcr, 101, &feedx.Status{
		LocalVersion:  101,
		RemoteVersion: 101,
		Skipped:       true,
	})

	// updated version
	testProduce(t, pcr, 134, &feedx.Status{
		LocalVersion:  134,
		RemoteVersion: 101,
		NumItems:      13,
	})

	meta, err := obj.Head(t.Context())
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if exp := (bfs.Metadata{"X-Feedx-Version": "134"}); !reflect.DeepEqual(exp, meta.Metadata) {
		t.Errorf("expected %#v, got %#v", exp, meta)
	}
}

func testProduce(t *testing.T, pcr *feedx.Producer, version int64, exp *feedx.Status) {
	t.Helper()

	status, err := pcr.Produce(t.Context(), version, nil, func(w *feedx.Writer) error {
		for i := int64(0); i < version/10; i++ {
			if err := w.Encode(seed()); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	if !reflect.DeepEqual(exp, status) {
		t.Errorf("expected %#v, got %#v", exp, status)
	}
}
