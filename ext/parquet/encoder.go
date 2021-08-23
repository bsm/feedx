package parquet

import (
	"fmt"
	"io"

	goparquet "github.com/fraugster/parquet-go"
	fparquet "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

type encoder struct {
	fw *goparquet.FileWriter
}

func newEncoder(w io.Writer, opts *EncoderOpts) (*encoder, error) {
	// determine compression
	comp, err := fparquet.CompressionCodecFromString(opts.Compression)
	if err != nil {
		return nil, err
	}

	// parse the schema def
	schemaDef, err := parquetschema.ParseSchemaDefinition(opts.SchemaDef)
	if err != nil {
		return nil, err
	}

	// create the writer
	fw := goparquet.NewFileWriter(w,
		goparquet.WithCompressionCodec(comp),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	return &encoder{fw: fw}, nil
}

func (w encoder) Encode(v interface{}) error {
	val, ok := v.(map[string]interface{})
	if !ok {
		return fmt.Errorf("feedx: value %v is not a map[string]interface{}", v)
	}
	return w.fw.AddData(val)
}

func (w encoder) Close() error {
	return w.fw.Close()
}
