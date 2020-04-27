package feedx_test

import (
	"bytes"
	"io"

	"github.com/bsm/feedx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Format", func() {
	runSharedTest := func(subject feedx.Format) {
		buf := new(bytes.Buffer)

		enc, err := subject.NewEncoder(buf)
		Expect(err).NotTo(HaveOccurred())
		defer enc.Close()

		fix := fixture
		Expect(enc.Encode(&fix)).To(Succeed())
		Expect(enc.Encode(&fix)).To(Succeed())
		Expect(enc.Close()).To(Succeed())

		dec, err := subject.NewDecoder(buf)
		Expect(err).NotTo(HaveOccurred())
		defer dec.Close()

		v1 := new(MockMessage)
		Expect(dec.Decode(v1)).To(Succeed())
		Expect(v1.Name).To(Equal("Joe"))

		v2 := new(MockMessage)
		Expect(dec.Decode(v2)).To(Succeed())
		Expect(v2.Name).To(Equal("Joe"))

		v3 := new(MockMessage)
		Expect(dec.Decode(v3)).To(MatchError(io.EOF))

		Expect(dec.Close()).To(Succeed())
	}

	It("should detect the format", func() {
		Expect(feedx.DetectFormat("/path/to/file.json")).To(Equal(feedx.JSONFormat))
		Expect(feedx.DetectFormat("/path/to/file.json.gz")).To(Equal(feedx.JSONFormat))
		Expect(feedx.DetectFormat("/path/to/file.jsonz")).To(Equal(feedx.JSONFormat))

		Expect(feedx.DetectFormat("/path/to/file.pb")).To(Equal(feedx.ProtobufFormat))
		Expect(feedx.DetectFormat("/path/to/file.pb.gz")).To(Equal(feedx.ProtobufFormat))
		Expect(feedx.DetectFormat("/path/to/file.pbz")).To(Equal(feedx.ProtobufFormat))

		Expect(feedx.DetectFormat("")).To(BeNil())
		Expect(feedx.DetectFormat("/path/to/file")).To(BeNil())
		Expect(feedx.DetectFormat("/path/to/file.txt")).To(BeNil())
	})

	Describe("JSONFormat", func() {
		var subject = feedx.JSONFormat
		var _ feedx.Format = subject

		It("should encode/decode", func() {
			runSharedTest(subject)
		})
	})

	Describe("ProtobufFormat", func() {
		var subject = feedx.ProtobufFormat
		var _ feedx.Format = subject

		It("should encode/decode", func() {
			runSharedTest(subject)
		})
	})
})
