package feedx_test

import (
	"bytes"
	"context"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
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

	It("should write plain", func() {
		w, err := feedx.NewWriter(context.Background(), plain, &feedx.WriterOptions{
			LastMod: time.Unix(1515151515, 123456789),
		})
		Expect(err).NotTo(HaveOccurred())
		defer w.Discard()

		Expect(w.Write(bytes.Repeat([]byte{'x'}, 10000))).To(Equal(10000))
		Expect(w.Commit()).To(Succeed())

		info, err := plain.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(Equal(int64(10000)))
		Expect(info.Metadata).To(Equal(map[string]string{"x-feedx-last-modified": "1515151515123"}))
	})

	It("should write compressed", func() {
		w, err := feedx.NewWriter(context.Background(), compressed, &feedx.WriterOptions{
			LastMod: time.Unix(1515151515, 123456789),
		})
		Expect(err).NotTo(HaveOccurred())
		defer w.Discard()

		Expect(w.Write(bytes.Repeat([]byte{'x'}, 10000))).To(Equal(10000))
		Expect(w.Commit()).To(Succeed())

		info, err := compressed.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 50, 20))
		Expect(info.Metadata).To(Equal(map[string]string{"x-feedx-last-modified": "1515151515123"}))
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
