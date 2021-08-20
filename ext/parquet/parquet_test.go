package parquet_test

import (
	"testing"

	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
)

type mockStruct struct {
	ID         int      `parquet:"id"`
	Bool       bool     `parquet:"bool_col"`
	TinyInt    int8     `parquet:"tinyint_col"`
	SmallUint  uint16   `parquet:"smallint_col"`
	StdInt     int      `parquet:"int_col"`
	BigInt     int64    `parquet:"bigint_col"`
	Float      *float32 `parquet:"float_col"`
	Double     float64  `parquet:"double_col"`
	DateString string   `parquet:"date_string_col"`
	ByteString []byte   `parquet:"string_col"`
	// Timestamp  time.Time `parquet:"timestamp_col"`
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx/ext/parquet")
}
