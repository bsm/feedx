package feedx_test

import (
	"context"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
	. "github.com/bsm/ginkgo/v2"
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

func decode(r *feedx.Reader) []*testdata.MockMessage {
	var msgs []*testdata.MockMessage
	for {
		var msg testdata.MockMessage
		err := r.Decode(&msg)
		if err == io.EOF {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		msgs = append(msgs, &msg)
	}
	return msgs
}

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
