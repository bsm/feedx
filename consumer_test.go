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

var _ = Describe("Consumer", func() {
	var subject feedx.Consumer
	var obj *bfs.Object
	var ctx = context.Background()

	consume := func(r *feedx.Reader) (interface{}, error) {
		var msgs []*testdata.MockMessage
		for {
			var msg testdata.MockMessage
			if err := r.Decode(&msg); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			msgs = append(msgs, &msg)
		}
		return msgs, nil
	}

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.jsonz")
		Expect(writeMulti(obj, 2, mockTime)).To(Succeed())

		var err error
		subject, err = feedx.NewConsumerForRemote(ctx, obj, nil, consume)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("syncs/retrieves feeds from remote", func() {
		Expect(subject.LastSync()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastConsumed()).To(BeTemporally("==", subject.LastSync()))
		Expect(subject.LastModified()).To(BeTemporally("==", mockTime.Truncate(time.Millisecond)))
		Expect(subject.NumRead()).To(Equal(2))
		Expect(subject.Data()).To(ConsistOf(seed(), seed()))
		Expect(subject.Close()).To(Succeed())
	})

	It("consumes feeds only if necessary", func() {
		prevSync := subject.LastSync()
		time.Sleep(2 * time.Millisecond)

		testable := subject.(interface{ TestSync() error })
		Expect(testable.TestSync()).To(Succeed())
		Expect(subject.LastSync()).To(BeTemporally(">", prevSync))
		Expect(subject.LastConsumed()).To(BeTemporally("==", prevSync)) // skipped on last sync
		Expect(subject.LastModified()).To(BeTemporally("==", mockTime.Truncate(time.Millisecond)))
		Expect(subject.NumRead()).To(Equal(2))
	})

	It("always consumes if LastModified not set", func() {
		noModTime := bfs.NewInMemObject("path/to/file.json")
		Expect(writeMulti(noModTime, 2, time.Time{})).To(Succeed())

		csmr, err := feedx.NewConsumerForRemote(ctx, noModTime, nil, consume)
		Expect(err).NotTo(HaveOccurred())

		prevSync := csmr.LastSync()
		time.Sleep(2 * time.Millisecond)

		testable := csmr.(interface{ TestSync() error })
		Expect(testable.TestSync()).To(Succeed())
		Expect(csmr.LastSync()).To(BeTemporally(">", prevSync))
		Expect(csmr.LastConsumed()).To(BeTemporally("==", csmr.LastSync())) // consumed on last sync
		Expect(csmr.LastModified()).To(BeTemporally("==", time.Unix(0, 0)))
	})
})
