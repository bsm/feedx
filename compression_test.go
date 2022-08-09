package feedx_test

import (
	"bytes"

	"github.com/bsm/feedx"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Compression", func() {
	var data = bytes.Repeat([]byte("wxyz"), 1024)

	runSharedTest := func(subject feedx.Compression) {
		buf := new(bytes.Buffer)

		w, err := subject.NewWriter(buf)
		Expect(err).NotTo(HaveOccurred())
		defer w.Close()

		Expect(w.Write(data)).To(Equal(4096))
		Expect(w.Write(data)).To(Equal(4096))
		Expect(w.Close()).To(Succeed())

		r, err := subject.NewReader(buf)
		Expect(err).NotTo(HaveOccurred())
		defer r.Close()

		p := make([]byte, 20)
		Expect(r.Read(p)).To(Equal(20))
		Expect(string(p)).To(Equal("wxyzwxyzwxyzwxyzwxyz"))
		Expect(r.Close()).To(Succeed())
	}

	It("detects the format", func() {
		Expect(feedx.DetectCompression("/path/to/file.json")).To(Equal(feedx.NoCompression))
		Expect(feedx.DetectCompression("/path/to/file.json.gz")).To(Equal(feedx.GZipCompression))
		Expect(feedx.DetectCompression("/path/to/file.jsonz")).To(Equal(feedx.GZipCompression))

		Expect(feedx.DetectCompression("/path/to/file.pb")).To(Equal(feedx.NoCompression))
		Expect(feedx.DetectCompression("/path/to/file.pb.gz")).To(Equal(feedx.GZipCompression))
		Expect(feedx.DetectCompression("/path/to/file.pbz")).To(Equal(feedx.GZipCompression))

		Expect(feedx.DetectCompression("/path/to/file.flate")).To(Equal(feedx.FlateCompression))
		Expect(feedx.DetectCompression("/path/to/file.whatever.flate")).To(Equal(feedx.FlateCompression))

		Expect(feedx.DetectCompression("")).To(Equal(feedx.NoCompression))
		Expect(feedx.DetectCompression("/path/to/file")).To(Equal(feedx.NoCompression))
		Expect(feedx.DetectCompression("/path/to/file.txt")).To(Equal(feedx.NoCompression))
	})

	Describe("NoCompression", func() {
		var subject = feedx.NoCompression
		var _ feedx.Compression = subject

		It("writes/reads", func() {
			runSharedTest(subject)
		})
	})

	Describe("GZipCompression", func() {
		var subject = feedx.GZipCompression
		var _ feedx.Compression = subject

		It("writes/reads", func() {
			runSharedTest(subject)
		})
	})

	Describe("FlateCompression", func() {
		var subject = feedx.FlateCompression
		var _ feedx.Compression = subject

		It("writes/reads", func() {
			runSharedTest(subject)
		})
	})
})
