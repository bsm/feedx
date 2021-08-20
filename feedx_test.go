package feedx_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

var memStore *bfs.InMem

func init() {
	memStore = bfs.NewInMem()
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		return memStore, nil
	})
}

// ------------------------------------------------------------------------

func seed() *testdata.MockMessage {
	return &testdata.MockMessage{
		Name:   "Joe",
		Enum:   testdata.MockEnum_FIRST,
		Height: 180,
	}
}

var mockTime = time.Unix(1515151515, 123456789)

// ------------------------------------------------------------------------

func writeMulti(obj *bfs.Object, numEntries int, lastMod time.Time) error {
	w := feedx.NewWriter(context.Background(), obj, &feedx.WriterOptions{LastMod: lastMod})
	defer w.Discard()

	for i := 0; i < numEntries; i++ {
		if err := w.Encode(seed()); err != nil {
			return err
		}
	}
	return w.Commit()
}

// ------------------------------------------------------------------------

type mockStruct struct {
	ID         int      `parquet:"id"`
	Bool       bool     `parquet:"bool_col"`
	TinyInt    int8     `parquet:"tinyint_col"`
	SmallUint  uint16   `parquet:"smallint_col"`
	StdInt     int      `parquet:"int_col"`
	BigInt     int64    `parquet:"bigint_col"`
	Float      *float32 `parquet:"float_col"`
	Double     float64  `parquet:"double_col"`
	DateString string   `parquet:"date_string_col"`
	ByteString []byte   `parquet:"string_col"`
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
