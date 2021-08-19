package parquet_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/bsm/feedx/ext/parquet"
	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

type mockStruct struct {
	ID         int       `parquet:"id"`
	Bool       bool      `parquet:"bool_col"`
	TinyInt    int8      `parquet:"tinyint_col"`
	SmallUint  uint16    `parquet:"smallint_col"`
	StdInt     int       `parquet:"int_col"`
	BigInt     int64     `parquet:"bigint_col"`
	Float      *float32  `parquet:"float_col"`
	Double     float64   `parquet:"double_col"`
	DateString string    `parquet:"date_string_col"`
	ByteString []byte    `parquet:"string_col"`
	Timestamp  time.Time `parquet:"timestamp_col"`
}

var _ = Describe("Format", func() {

	Describe("ParquetFormat", func() {
		It("should encode/decode", func() {
			buf := new(bytes.Buffer)
			format := &parquet.Format{BatchSize: 3}
			enc, err := format.NewEncoder(buf)
			Expect(err).NotTo(HaveOccurred())
			defer enc.Close()

			fix := map[string]interface{}{
				"id":         int64(1),
				"city":       []byte("Berlin"),
				"population": int64(3520031),
			}
			Expect(enc.Encode(fix)).To(Succeed())
			Expect(enc.Encode(fix)).To(Succeed())
			Expect(enc.Close()).To(Succeed())
		})
	})

})

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx/ext/parquet")
}
