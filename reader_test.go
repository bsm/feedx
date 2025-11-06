package feedx_test

import (
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
)

func TestReader(t *testing.T) {
	t.Run("reads", func(t *testing.T) {
		r := fixReader(t)

		if data, err := io.ReadAll(r); err != nil {
			t.Fatal("unexpected error", err)
		} else if exp, got := 111, len(data); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		} else if exp, got := int64(0), r.NumRead(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("decodes", func(t *testing.T) {
		r := fixReader(t)
		msgs := drainReader(t, r)
		if exp := seedN(3); !reflect.DeepEqual(exp, msgs) {
			t.Errorf("expected %#v, got %#v", exp, msgs)
		}
		if exp, got := int64(3), r.NumRead(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}

func fixReader(t *testing.T) *feedx.Reader {
	t.Helper()

	obj := bfs.NewInMemObject("path/to/file.jsonz")
	if err := writeN(obj, 3, time.Time{}); err != nil {
		t.Fatal("unexpected error", err)
	}

	r, err := feedx.NewReader(t.Context(), obj, nil)
	if err != nil {
		t.Fatal("unexpected error", err)
	}

	t.Cleanup(func() {
		_ = r.Close()
	})

	return r
}

func TestMultiReader(t *testing.T) {
	t.Run("reads", func(t *testing.T) {
		r := fixMultiReader(t)

		if data, err := io.ReadAll(r); err != nil {
			t.Fatal("unexpected error", err)
		} else if exp, got := 222, len(data); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		} else if exp, got := int64(0), r.NumRead(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})

	t.Run("decodes", func(t *testing.T) {
		r := fixMultiReader(t)
		msgs := drainReader(t, r)
		if exp := seedN(6); !reflect.DeepEqual(exp, msgs) {
			t.Errorf("expected %#v, got %#v", exp, msgs)
		}
		if exp, got := int64(6), r.NumRead(); exp != got {
			t.Errorf("expected %v, got %v", exp, got)
		}
	})
}

func fixMultiReader(t *testing.T) *feedx.Reader {
	t.Helper()

	obj := bfs.NewInMemObject("path/to/file.jsonz")
	if err := writeN(obj, 3, time.Time{}); err != nil {
		t.Fatal("unexpected error", err)
	}

	r := feedx.MultiReader(t.Context(), []*bfs.Object{obj, obj}, nil)
	t.Cleanup(func() {
		_ = r.Close()
	})

	return r
}

func drainReader(t *testing.T, r interface{ Decode(any) error }) []*testdata.MockMessage {
	t.Helper()

	msgs, err := readMessages(r)
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	return msgs
}
