package parquet

import (
	"io"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"go.uber.org/multierr"
)

type decoder struct {
	pfr *goparquet.FileReader
	ffr *floor.Reader
	tmp *tempFile
}

func newDecoder(rs io.ReadSeeker) (*decoder, error) {
	pfr, err := goparquet.NewFileReader(rs)
	if err != nil {
		return nil, err
	}

	ffr := floor.NewReader(pfr)

	return &decoder{
			pfr: pfr,
			ffr: ffr,
		},
		nil
}

func (w *decoder) Decode(v interface{}) error {
	// read the next value and scan
	if w.ffr.Next() {
		return w.ffr.Scan(v)
	}

	// check for errors
	if err := w.ffr.Err(); err != nil {
		return err
	}

	// end of file
	return io.EOF
}

func (w *decoder) Close() (err error) {
	// close the tmp file if present
	if w.tmp != nil {
		err = multierr.Append(err, w.tmp.Close())
	}

	// close the reader
	err = multierr.Append(err, w.ffr.Close())
	return
}
