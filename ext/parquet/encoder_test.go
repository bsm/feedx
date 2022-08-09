package parquet_test

import (
	"bytes"
	"time"

	"github.com/bsm/feedx"
	"github.com/bsm/feedx/ext/parquet"
	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	goparquet "github.com/fraugster/parquet-go"
	parquetopt "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

var _ = Describe("Encoder", func() {
	var subject feedx.FormatEncoder

	f32ptr := func(f float32) *float32 { return &f }

	BeforeEach(func() {
		var err error
		buf := new(bytes.Buffer)

		schemaDef, err := parquetschema.ParseSchemaDefinition(`message stat {
			required int64 bigint_col;
			optional boolean bool_col;
			optional int32 tinyint_col;
			optional int32 smallint_col;
			optional int32 int_col;
			optional int64 bigint_col;
			optional float float_col;
			optional double double_col;
			optional binary date_string_col (STRING);
			optional binary string_col;
			optional int64 timestamp_col (TIMESTAMP(NANOS, true));
			}`)
		Expect(err).NotTo(HaveOccurred())

		format := &parquet.Format{}
		subject, err = format.NewEncoder(buf,
			goparquet.WithSchemaDefinition(schemaDef),
			goparquet.WithCompressionCodec(parquetopt.CompressionCodec_SNAPPY))
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(subject.Close()).To(Succeed())
	})

	It("encodes", func() {
		v1 := &mockStruct{
			ID:         1,
			Bool:       true,
			TinyInt:    int8(5),
			SmallUint:  uint16(12),
			StdInt:     5,
			BigInt:     int64(99),
			Float:      f32ptr(5.5),
			Double:     float64(5.5),
			DateString: "2021-08-11",
			ByteString: []byte("string"),
			Timestamp:  time.Now(),
		}
		Expect(subject.Encode(v1)).To(Succeed())

		v2 := &mockStruct{ID: 1}
		Expect(subject.Encode(v2)).To(Succeed())
	})
})
