package parquet

import (
	"io"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	fparquet "github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

type encoder struct {
	fw *floor.Writer
}

func newEncoder(w io.Writer, opts *EncoderOpts) (*encoder, error) {
	// determine compression
	cp, err := fparquet.CompressionCodecFromString(opts.Compression)
	if err != nil {
		return nil, err
	}

	// parse the schema def
	sd, err := parquetschema.ParseSchemaDefinition(opts.SchemaDef)
	if err != nil {
		return nil, err
	}

	// create the writer
	pw := goparquet.NewFileWriter(w,
		goparquet.WithCompressionCodec(cp),
		goparquet.WithSchemaDefinition(sd),
	)

	return &encoder{
		// wrap the parquet writer with a floor writer
		fw: floor.NewWriter(pw),
	}, nil
}

// implements feedx.FormatEncoder
func (w encoder) Encode(v interface{}) error {
	return w.fw.Write(v)
}

// implements feedx.FormatEncoder
func (w encoder) Close() error {
	return w.fw.Close()
}
