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
	ctx := context.Background()
	msg := &tbp.Message{
		Name:         "Joe",
		TrueScotsman: true,
		Hilarity:     tbp.Message_BILL_BAILEY,
	}
	pfn := func(dec feedx.FormatDecoder) (interface{}, int64, error) {
		var msgs []*tbp.Message
		for {
			msg := new(tbp.Message)
			if err := dec.Decode(msg); err == io.EOF {
				break
			} else if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, msg)
		}
		return msgs, int64(len(msgs)), nil
	}

	BeforeEach(func() {
		memStore = bfs.NewInMem()
		w, err := memStore.Create(ctx, "path/to/file.jsonz", &bfs.WriteOptions{
			Metadata: map[string]string{"x-feedx-pusher-last-modified": "1544477788899"},
		})
		Expect(err).NotTo(HaveOccurred())
		defer w.Close()

		c, err := feedx.GZipCompression.NewWriter(w)
		Expect(err).NotTo(HaveOccurred())
		defer c.Close()

		f, err := feedx.JSONFormat.NewEncoder(c)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		Expect(f.Encode(msg)).To(Succeed())
		Expect(f.Encode(msg)).To(Succeed())
		Expect(f.Close()).To(Succeed())
		Expect(c.Close()).To(Succeed())
		Expect(w.Close()).To(Succeed())
	})

	It("should sync and retrieve feeds from remote", func() {
		subject, err := feedx.OpenConsumer(ctx, "mem:///path/to/file.jsonz", nil, pfn)
		Expect(err).NotTo(HaveOccurred())
		defer subject.Close()

		Expect(subject.LastCheck()).To(BeTemporally("~", time.Now(), time.Second))
		Expect(subject.LastModified()).To(BeTemporally("~", time.Unix(1544477788, 0), time.Second))
		Expect(subject.Size()).To(Equal(int64(2)))
		Expect(subject.Data()).To(Equal([]*tbp.Message{msg, msg}))
		Expect(subject.Close()).To(Succeed())
	})
})
