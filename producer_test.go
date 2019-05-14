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

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.jsonz")

		var err error
		subject, err = feedx.NewProducerForRemote(ctx, obj, nil, func(enc feedx.FormatPureEncoder) error {
			for i := 0; i < 10; i++ {
				fix := fixture
				if err := enc.Encode(&fix); err != nil {
					return err
				}
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should produce", func() {
		Expect(subject.LastPush()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(Equal(subject.LastPush()))
		Expect(subject.NumWritten()).To(Equal(10))
		Expect(subject.Close()).To(Succeed())
	})
})
