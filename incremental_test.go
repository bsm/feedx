package feedx_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("IncrementalProducer", func() {
	var subject *feedx.IncrementalProducer
	var bucket bfs.Bucket
	var numRuns uint32
	var ctx = context.Background()
	var lastMod = mockTime

	setup := func(modTime time.Time, o *feedx.IncrementalProducerOptions) {
		var err error

		lastModFunc := func(_ context.Context) (time.Time, error) {
			return modTime, nil
		}
		subject, err = feedx.NewIncrementalProducerForBucket(ctx, bucket, o, lastModFunc, func(_ time.Time) feedx.ProduceFunc {
			return func(w *feedx.Writer) error {
				atomic.AddUint32(&numRuns, 1)

				for i := 0; i < 10; i++ {
					if err := w.Encode(seed()); err != nil {
						return err
					}
				}
				return nil
			}
		})
		Expect(err).NotTo(HaveOccurred())
	}

	BeforeEach(func() {
		atomic.StoreUint32(&numRuns, 0)
		bucket = bfs.NewInMem()
	})

	AfterEach(func() {
		if subject != nil {
			Expect(subject.Close()).To(Succeed())
		}
	})

	It("produces", func() {
		setup(lastMod, nil)

		Expect(subject.LastPush()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", lastMod, time.Second))
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())

		Expect(feedx.LoadManifest(ctx, bfs.NewObjectFromBucket(bucket, "manifest.json"))).To(Equal(&feedx.Manifest{
			LastModified: feedx.TimestampFromTime(lastMod),
			Files:        []string{"data-0-20180105-112515123.pbz"},
		}))

		info, err := bucket.Head(ctx, "data-0-20180105-112515123.pbz")
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 35, 10))

		metaLastMod, err := strconv.ParseInt(info.Metadata.Get("X-Feedx-Last-Modified"), 10, 64)
		Expect(err).NotTo(HaveOccurred())
		Expect(time.UnixMilli(metaLastMod)).To(BeTemporally("~", lastMod, time.Second))
	})

	It("only produces if data changed", func() {
		// run initial producer cycle
		setup(lastMod, nil)
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())

		// run producer cycle with unchanged last mod date
		setup(lastMod, nil)
		Expect(subject.NumWritten()).To(Equal(0))
		Expect(subject.Close()).To(Succeed())

		// run producer cycle after bumping last mod date
		setup(lastMod.Add(time.Hour), nil)
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())
	})
})
