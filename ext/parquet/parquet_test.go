package parquet_test

import (
	"testing"
	"time"

	. "github.com/bsm/ginkgo"
	. "github.com/bsm/gomega"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor/interfaces"
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

func (m *mockStruct) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	id, err := obj.GetField("id").Int32()
	if err != nil {
		return err
	}
	m.ID = int(id)

	b, err := obj.GetField("bool_col").Bool()
	if err != nil {
		return err
	}
	m.Bool = b

	tint, err := obj.GetField("tinyint_col").Int32()
	if err != nil {
		return err
	}
	m.TinyInt = int8(tint)

	suint, err := obj.GetField("smallint_col").Int32()
	if err != nil {
		return err
	}
	m.SmallUint = uint16(suint)

	sint, err := obj.GetField("int_col").Int32()
	if err != nil {
		return err
	}
	m.StdInt = int(sint)

	bint, err := obj.GetField("bigint_col").Int64()
	if err != nil {
		return err
	}
	m.BigInt = bint

	f32, err := obj.GetField("float_col").Float32()
	if err != nil {
		return err
	}
	m.Float = &f32

	f64, err := obj.GetField("double_col").Float64()
	if err != nil {
		return err
	}
	m.Double = f64

	dstring, err := obj.GetField("date_string_col").ByteArray()
	if err != nil {
		return err
	}
	m.DateString = string(dstring)

	bstring, err := obj.GetField("string_col").ByteArray()
	if err != nil {
		return err
	}
	m.ByteString = bstring

	ts, err := obj.GetField("timestamp_col").Int96()
	if err != nil {
		return err
	}
	m.Timestamp = goparquet.Int96ToTime(ts)

	return nil
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx/ext/parquet")
}
