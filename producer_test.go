package feedx_test

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Producer", func() {
	var subject *feedx.Producer
	var obj *bfs.Object
	var ctx = context.Background()

	setup := func(o *feedx.ProducerOptions) {
		var err error
		subject, err = feedx.NewProducerForRemote(ctx, obj, o, func(w *feedx.Writer) error {
			for i := 0; i < 10; i++ {
				fix := fixture
				if err := w.Encode(&fix); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.jsonz")
	})

	AfterEach(func() {
		if subject != nil {
			Expect(subject.Close()).To(Succeed())
		}
	})

	It("should produce", func() {
		setup(nil)

		Expect(subject.LastPush()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())

		info, err := obj.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 75, 10))
	})

	It("should produce with custom last-mod check", func() {
		setup(&feedx.ProducerOptions{
			LastModCheck: func(_ context.Context) (time.Time, error) { return time.Unix(1515151515, 0), nil },
		})

		Expect(subject.LastPush()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(Equal(time.Unix(1515151515, 0)))
		Expect(subject.NumWritten()).To(Equal(10))

		info, err := obj.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 75, 10))
		Expect(info.Metadata).To(HaveKeyWithValue("X-Feedx-Last-Modified", "1515151515000"))
	})
})
