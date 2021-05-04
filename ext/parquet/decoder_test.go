package parquet_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/bsm/feedx"
	"github.com/bsm/feedx/ext/parquet"
	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

var _ = Describe("Decoder", func() {
	var subject feedx.FormatDecoder
	var fixture *os.File

	f32ptr := func(f float32) *float32 { return &f }

	BeforeEach(func() {
		var err error
		fixture, err = os.Open("testdata/alltypes_plain.parquet")
		Expect(err).NotTo(HaveOccurred())

		format := &parquet.Format{BatchSize: 3}
		subject, err = format.NewDecoder(fixture)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
		Expect(fixture.Close()).To(Succeed())
	})

	It("should decode", func() {
		v1 := new(mockStruct)
		Expect(subject.Decode(v1)).To(Succeed())
		Expect(v1).To(Equal(&mockStruct{
			ID:         4,
			Bool:       true,
			Float:      f32ptr(0),
			DateString: "03/01/09", ByteString: []byte("0"),
			Timestamp: time.Unix(1235865600, 0),
		}))

		v2 := new(mockStruct)
		Expect(subject.Decode(v2)).To(Succeed())
		Expect(v2).To(Equal(&mockStruct{
			ID:      5,
			TinyInt: 1, SmallUint: 1, StdInt: 1, BigInt: 10,
			Float: f32ptr(1.1), Double: 10.1,
			DateString: "03/01/09", ByteString: []byte("1"),
			Timestamp: time.Unix(1235865660, 0),
		}))

		Expect(subject.Decode(new(mockStruct))).To(Succeed()) // v3
		Expect(subject.Decode(new(mockStruct))).To(Succeed()) // v4
		Expect(subject.Decode(new(mockStruct))).To(Succeed()) // v5

		v6 := new(mockStruct)
		Expect(subject.Decode(v6)).To(Succeed())
		Expect(v6).To(Equal(&mockStruct{
			ID:      3,
			Bool:    false,
			TinyInt: 1, SmallUint: 1, StdInt: 1, BigInt: 10,
			Float: f32ptr(1.1), Double: 10.1,
			DateString: "02/01/09", ByteString: []byte("1"),
			Timestamp: time.Unix(1233446460, 0),
		}))

		Expect(subject.Decode(new(mockStruct))).To(Succeed()) // v7
		Expect(subject.Decode(new(mockStruct))).To(Succeed()) // v8

		v9 := new(mockStruct)
		Expect(subject.Decode(v9)).To(MatchError(io.EOF))
	})

	It("should open from non-file readers", func() {
		bin, err := ioutil.ReadFile("testdata/alltypes_plain.parquet")
		Expect(err).NotTo(HaveOccurred())

		dec, err := new(parquet.Format).NewDecoder(bytes.NewReader(bin))
		Expect(err).NotTo(HaveOccurred())
		Expect(dec.Close()).To(Succeed())
	})
})
