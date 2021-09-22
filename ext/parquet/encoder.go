package parquet

import (
	"io"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
)

type encoder struct {
	*floor.Writer
}

func newEncoder(w io.Writer, opts []goparquet.FileWriterOption) (*encoder, error) {
	// create the writer
	pw := goparquet.NewFileWriter(w, opts...)

	return &encoder{
		// wrap the parquet writer with a floor writer
		Writer: floor.NewWriter(pw),
	}, nil
}

// implements feedx.FormatEncoder
func (w *encoder) Encode(v interface{}) error {
	return w.Writer.Write(v)
}
