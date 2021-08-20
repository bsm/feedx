package feedx_test

import (
	"bytes"
	"io"
	"os"

	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

var _ = Describe("Format", func() {
	runSharedTest := func(subject feedx.Format) {
		buf := new(bytes.Buffer)

		enc, err := subject.NewEncoder(buf)
		Expect(err).NotTo(HaveOccurred())
		defer enc.Close()

		Expect(enc.Encode(seed())).To(Succeed())
		Expect(enc.Encode(seed())).To(Succeed())
		Expect(enc.Close()).To(Succeed())

		dec, err := subject.NewDecoder(buf)
		Expect(err).NotTo(HaveOccurred())
		defer dec.Close()

		v1 := new(testdata.MockMessage)
		Expect(dec.Decode(v1)).To(Succeed())
		Expect(v1.Name).To(Equal("Joe"))

		v2 := new(testdata.MockMessage)
		Expect(dec.Decode(v2)).To(Succeed())
		Expect(v2.Name).To(Equal("Joe"))

		v3 := new(testdata.MockMessage)
		Expect(dec.Decode(v3)).To(MatchError(io.EOF))

		Expect(dec.Close()).To(Succeed())
	}

	It("detects the format", func() {
		Expect(feedx.DetectFormat("/path/to/file.json")).To(Equal(feedx.JSONFormat))
		Expect(feedx.DetectFormat("/path/to/file.json.gz")).To(Equal(feedx.JSONFormat))
		Expect(feedx.DetectFormat("/path/to/file.json.flate")).To(Equal(feedx.JSONFormat))
		Expect(feedx.DetectFormat("/path/to/file.jsonz")).To(Equal(feedx.JSONFormat))

		Expect(feedx.DetectFormat("/path/to/file.pb")).To(Equal(feedx.ProtobufFormat))
		Expect(feedx.DetectFormat("/path/to/file.pb.gz")).To(Equal(feedx.ProtobufFormat))
		Expect(feedx.DetectFormat("/path/to/file.pb.flate")).To(Equal(feedx.ProtobufFormat))
		Expect(feedx.DetectFormat("/path/to/file.pbz")).To(Equal(feedx.ProtobufFormat))

		Expect(feedx.DetectFormat("")).To(BeNil())
		Expect(feedx.DetectFormat("/path/to/file")).To(BeNil())
		Expect(feedx.DetectFormat("/path/to/file.txt")).To(BeNil())
	})

	Describe("JSONFormat", func() {
		var subject = feedx.JSONFormat
		var _ feedx.Format = subject

		It("encodes/decodes", func() {
			runSharedTest(subject)
		})
	})

	Describe("ProtobufFormat", func() {
		var subject = feedx.ProtobufFormat
		var _ feedx.Format = subject

		It("encodes/decodes", func() {
			runSharedTest(subject)
		})
	})

	// TODO! i'm only doing the decoder here.
	// TODO! I will drop the use of the file when i have an encoder and will encode and decode in one test, as above.
	Describe("ParquetFormat", func() {
		var subject = feedx.ParquetFormat
		var _ feedx.Format = subject
		var fixture *os.File

		f32ptr := func(f float32) *float32 { return &f }

		BeforeEach(func() {
			var err error
			fixture, err = os.Open("ext/parquet/testdata/alltypes_plain.parquet")
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(fixture.Close()).To(Succeed())
		})

		It("decodes", func() {
			enc, err := subject.NewDecoder(fixture)
			Expect(err).NotTo(HaveOccurred())
			defer enc.Close()

			v1 := new(mockStruct)
			Expect(enc.Decode(v1)).To(Succeed())
			Expect(v1).To(Equal(&mockStruct{
				ID:         4,
				Bool:       true,
				Float:      f32ptr(0),
				DateString: "03/01/09",
				ByteString: []byte("0"),
			}))

			v2 := new(mockStruct)
			Expect(enc.Decode(v2)).To(Succeed())
			Expect(v2).To(Equal(&mockStruct{
				ID:      5,
				TinyInt: 1, SmallUint: 1, StdInt: 1, BigInt: 10,
				Float: f32ptr(1.1), Double: 10.1,
				DateString: "03/01/09", ByteString: []byte("1"),
			}))

			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v3
			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v4
			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v5
			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v6
			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v7
			Expect(enc.Decode(new(mockStruct))).To(Succeed()) // v8

			v9 := new(mockStruct)
			Expect(enc.Decode(v9)).To(MatchError(io.EOF))
		})
	})

})
