package feedx_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsm/bfs"
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

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
