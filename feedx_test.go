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

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
