package feedx_test

import (
	"context"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Writer", func() {
	var plain, compressed *bfs.Object
	var ctx = context.Background()

	BeforeEach(func() {
		plain = bfs.NewInMemObject("path/to/file.json")
		compressed = bfs.NewInMemObject("path/to/file.jsonz")
	})

	It("should encode", func() {
		Expect(writeMulti(plain, 10)).To(Succeed())
		Expect(writeMulti(compressed, 10)).To(Succeed())

		info, err := plain.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 470, 10))
		Expect(info.Metadata).To(Equal(map[string]string{"x-feedx-last-modified": "1515151515123"}))

		info, err = compressed.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 76, 10))
		Expect(info.Metadata).To(Equal(map[string]string{"x-feedx-last-modified": "1515151515123"}))
	})
})
