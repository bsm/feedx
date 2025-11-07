package feedx_test

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func TestWriter(t *testing.T) {
	t.Run("writes plain", func(t *testing.T) {
		obj := bfs.NewInMemObject("path/to/file.json")
		info := testWriter(t, obj, &feedx.WriterOptions{
			LastMod: time.Unix(1515151515, 123456789),
		})

		if exp, got := int64(10000), info.Size; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		meta := bfs.Metadata{"X-Feedx-Last-Modified": "1515151515123"}
		if exp, got := meta, info.Metadata; !reflect.DeepEqual(exp, got) {
			t.Errorf("expected %#v, got %#v", exp, got)
		}
	})

	t.Run("writes compressed", func(t *testing.T) {
		obj := bfs.NewInMemObject("path/to/file.jsonz")
		info := testWriter(t, obj, &feedx.WriterOptions{
			LastMod: time.Unix(1515151515, 123456789),
		})

		if max, got := int64(100), info.Size; got > max {
			t.Errorf("expected %v to be < %v", got, max)
		}

		meta := bfs.Metadata{"X-Feedx-Last-Modified": "1515151515123"}
		if exp, got := meta, info.Metadata; !reflect.DeepEqual(exp, got) {
			t.Errorf("expected %#v, got %#v", exp, got)
		}
	})

	t.Run("encodes", func(t *testing.T) {
		obj := bfs.NewInMemObject("path/to/file.json")
		if err := writeN(obj, 10, time.Unix(1515151515, 123456789)); err != nil {
			t.Fatal("unexpected error", err)
		}

		info, err := obj.Head(t.Context())
		if err != nil {
			t.Fatal("unexpected error", err)
		}
		if exp, got := int64(370), info.Size; exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}

		meta := bfs.Metadata{"X-Feedx-Last-Modified": "1515151515123"}
		if exp, got := meta, info.Metadata; !reflect.DeepEqual(exp, got) {
			t.Errorf("expected %#v, got %#v", exp, got)
		}
	})
}

func testWriter(t *testing.T, obj *bfs.Object, opts *feedx.WriterOptions) *bfs.MetaInfo {
	t.Helper()

	w := feedx.NewWriter(t.Context(), obj, opts)
	t.Cleanup(func() { _ = w.Discard() })

	if _, err := w.Write(bytes.Repeat([]byte{'x'}, 10000)); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}

	info, err := obj.Head(t.Context())
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return info
}
