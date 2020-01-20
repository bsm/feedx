package feedx_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	tbp "github.com/golang/protobuf/proto/proto3_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ------------------------------------------------------------------------

var memStore *bfs.InMem

func init() {
	memStore = bfs.NewInMem()
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		return memStore, nil
	})
}

var fixture = tbp.Message{
	Name:       "Joe",
	Hilarity:   tbp.Message_BILL_BAILEY,
	HeightInCm: 180,
}

func writeMulti(obj *bfs.Object, numEntries int) error {
	w := feedx.NewWriter(context.Background(), obj, &feedx.WriterOptions{
		LastMod: time.Unix(1515151515, 123456789),
	})
	defer w.Discard()

	for i := 0; i < numEntries; i++ {
		fix := fixture
		if err := w.Encode(&fix); err != nil {
			return err
		}
	}
	return w.Commit()
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
