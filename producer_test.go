package feedx_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Producer", func() {
	var subject *feedx.Producer
	var obj *bfs.Object
	var numRuns uint32
	var ctx = context.Background()

	setup := func(o *feedx.ProducerOptions) {
		var err error
		subject, err = feedx.NewProducerForRemote(ctx, obj, o, func(w *feedx.Writer) error {
			atomic.AddUint32(&numRuns, 1)

			for i := 0; i < 10; i++ {
				if err := w.Encode(seed()); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		atomic.StoreUint32(&numRuns, 0)
		obj = bfs.NewInMemObject("path/to/file.jsonz")
	})

	AfterEach(func() {
		if subject != nil {
			Expect(subject.Close()).To(Succeed())
		}
	})

	It("produces", func() {
		setup(nil)

		Expect(subject.LastPush()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())

		info, err := obj.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 75, 10))
	})

	It("produces with custom last-mod check", func() {
		setup(&feedx.ProducerOptions{
			Interval:     50 * time.Millisecond,
			LastModCheck: func(_ context.Context) (time.Time, error) { return time.Unix(1515151515, 987654321), nil },
		})

		firstPush := subject.LastPush()
		Expect(firstPush).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(Equal(time.Unix(1515151515, 987000000)))
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(atomic.LoadUint32(&numRuns)).To(Equal(uint32(1)))

		info, err := obj.Head(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 75, 10))
		Expect(info.Metadata).To(HaveKeyWithValue("X-Feedx-Last-Modified", "1515151515987"))

		Eventually(func() bool { return subject.LastPush().After(firstPush) }).Should(BeTrue())
		Expect(atomic.LoadUint32(&numRuns)).To(Equal(uint32(1)))
	})
})
