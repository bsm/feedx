package feedx_test

import (
	"context"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	tbp "github.com/golang/protobuf/proto/proto3_proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var obj *bfs.Object
	var ctx = context.Background()

	pfn := func(dec feedx.FormatDecoder) (interface{}, int64, error) {
		var msgs []tbp.Message
		for {
			var msg tbp.Message
			if err := dec.Decode(&msg); err == io.EOF {
				break
			} else if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, msg)
		}
		return msgs, int64(len(msgs)), nil
	}

	BeforeEach(func() {
		obj = bfs.NewInMemObject("path/to/file.jsonz")
		Expect(writeMulti(obj, 2)).To(Succeed())
	})

	It("should sync and retrieve feeds from remote", func() {
		subject, err := feedx.NewConsumerForRemote(ctx, obj, nil, pfn)
		Expect(err).NotTo(HaveOccurred())
		defer subject.Close()

		Expect(subject.LastCheck()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", time.Unix(1515151515, 0), time.Second))
		Expect(subject.Size()).To(Equal(int64(2)))
		Expect(subject.Data()).To(Equal([]tbp.Message{fixture, fixture}))
		Expect(subject.Close()).To(Succeed())
	})
})
