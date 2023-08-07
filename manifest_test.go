package feedx_test

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Manifest", func() {
	var subject *feedx.Manifest
	var bucket bfs.Bucket
	var obj *bfs.Object
	var ctx = context.Background()

	BeforeEach(func() {
		var err error
		bucket = bfs.NewInMem()
		obj = bfs.NewObjectFromBucket(bucket, "manifest.json")
		subject, err = feedx.LoadManifest(ctx, obj)
		Expect(err).NotTo(HaveOccurred())
		Expect(subject).To(Equal(new(feedx.Manifest)))
	})

	It("writes data files", func() {
		lastMod := time.Date(2023, 4, 5, 15, 23, 44, 123444444, time.UTC)

		Expect(subject.WriteDataFile(ctx, bucket, &feedx.WriterOptions{LastMod: lastMod, Format: feedx.JSONFormat, Compression: feedx.GZipCompression}, func(w *feedx.Writer) error {
			for i := 0; i < 10; i++ {
				if err := w.Encode(seed()); err != nil {
					return err
				}
			}
			return nil
		})).To(Equal(10))

		Expect(subject.LastModified).To(Equal(feedx.TimestampFromTime(lastMod)))
		Expect(subject.Files).To(Equal([]string{"data-0-20230405-152344123.jsonz"}))

		info, err := bucket.Head(ctx, "data-0-20230405-152344123.jsonz")
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Size).To(BeNumerically("~", 65, 10))
	})

	It("commits manifest to remote", func() {
		lastMod := time.Date(2023, 4, 5, 15, 23, 44, 123444444, time.UTC)

		subject.LastModified = feedx.TimestampFromTime(lastMod)
		subject.Files = []string{"data-0-20230405-152344123.jsonz"}
		Expect(subject.Commit(ctx, obj, &feedx.WriterOptions{LastMod: lastMod})).NotTo(HaveOccurred())

		// reload updated manifest from bucket
		Expect(feedx.LoadManifest(ctx, obj)).To(Equal(&feedx.Manifest{
			LastModified: feedx.TimestampFromTime(lastMod),
			Files:        []string{"data-0-20230405-152344123.jsonz"},
		}))
	})
})
