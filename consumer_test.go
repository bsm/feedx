package feedx_test

import (
	"context"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var subject feedx.Consumer
	var obj *bfs.Object
	var ctx = context.Background()

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.jsonz")
		Expect(writeMulti(obj, 2)).To(Succeed())

		var err error
		subject, err = feedx.NewConsumerForRemote(ctx, obj, nil, func(r *feedx.Reader) (interface{}, error) {
			var msgs []MockMessage
			for {
				var msg MockMessage
				if err := r.Decode(&msg); err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, msg)
			}
			return msgs, nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("should sync and retrieve feeds from remote", func() {
		Expect(subject.LastSync()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", time.Unix(1515151515, 0), time.Second))
		Expect(subject.NumRead()).To(Equal(2))
		Expect(subject.Data()).To(Equal([]MockMessage{fixture, fixture}))
		Expect(subject.Close()).To(Succeed())
	})
})
