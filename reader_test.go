package feedx_test

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Reader", func() {
	var subject *feedx.Reader
	var obj *bfs.Object
	var ctx = context.Background()

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.json")
		Expect(writeMulti(obj, 3, time.Time{})).To(Succeed())

		var err error
		subject, err = feedx.NewReader(ctx, obj, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("reads", func() {
		data, err := ioutil.ReadAll(subject)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(data)).To(BeNumerically("~", 110, 20))
		Expect(subject.NumRead()).To(Equal(0))
	})

	It("decodes", func() {
		msgs := decode(subject)
		Expect(msgs).To(ConsistOf(seed(), seed(), seed()))
		Expect(subject.NumRead()).To(Equal(3))
	})
})

var _ = Describe("ReaderIter", func() {
	var subject *feedx.ReaderIter
	var obj *bfs.Object
	var ctx = context.Background()

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.json")
		Expect(writeMulti(obj, 3, time.Time{})).To(Succeed())
		subject = feedx.NewReaderIter(ctx, []*bfs.Object{obj, obj}, nil)
	})

	It("iterates", func() {
		// 1st iteration
		reader, ok := subject.Next()
		Expect(ok).To(BeTrue())
		_ = decode(reader)
		Expect(reader.NumRead()).To(Equal(3))
		Expect(subject.NumRead()).To(Equal(0))

		// 2nd iteration
		reader, ok = subject.Next()
		Expect(ok).To(BeTrue())
		_ = decode(reader)
		Expect(reader.NumRead()).To(Equal(3))
		Expect(subject.NumRead()).To(Equal(3))

		// no more iterations
		reader, ok = subject.Next()
		Expect(ok).To(BeFalse())
		Expect(reader).To(BeNil())
		Expect(subject.NumRead()).To(Equal(6))

		Expect(subject.Err()).NotTo(HaveOccurred())
	})
})
