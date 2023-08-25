package feedx_test

import (
	"context"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Reader", func() {
	var subject *feedx.Reader
	var obj *bfs.Object
	var ctx = context.Background()

	Describe("NewReader", func() {
		BeforeEach(func() {
			obj = bfs.NewInMemObject("path/to/file.json.gz")
			Expect(writeMulti(obj, 3, time.Time{})).To(Succeed())

			var err error
			subject, err = feedx.NewReader(ctx, obj, nil)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(subject.Close()).To(Succeed())
		})

		It("reads", func() {
			data, err := io.ReadAll(subject)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(data)).To(Equal(111))
			Expect(subject.NumRead()).To(Equal(int64(0)))
		})

		It("decodes", func() {
			var msgs []*testdata.MockMessage
			for {
				var msg testdata.MockMessage
				err := subject.Decode(&msg)
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				msgs = append(msgs, &msg)
			}

			Expect(msgs).To(ConsistOf(seed(), seed(), seed()))
			Expect(subject.NumRead()).To(Equal(int64(3)))
		})
	})

	Describe("MultiReader", func() {
		BeforeEach(func() {
			obj = bfs.NewInMemObject("path/to/file.json.gz")
			Expect(writeMulti(obj, 3, time.Time{})).To(Succeed())

			subject = feedx.MultiReader(ctx, []*bfs.Object{obj, obj}, nil)
		})

		AfterEach(func() {
			Expect(subject.Close()).To(Succeed())
		})

		It("reads", func() {
			data, err := io.ReadAll(subject)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(data)).To(Equal(222))
			Expect(subject.NumRead()).To(Equal(int64(0)))
		})

		It("decodes", func() {
			var msgs []*testdata.MockMessage
			for {
				var msg testdata.MockMessage
				err := subject.Decode(&msg)
				if err == io.EOF {
					break
				}
				Expect(err).NotTo(HaveOccurred())
				msgs = append(msgs, &msg)
			}

			Expect(msgs).To(ConsistOf(seed(), seed(), seed(), seed(), seed(), seed()))
			Expect(subject.NumRead()).To(Equal(int64(6)))
		})
	})

})
