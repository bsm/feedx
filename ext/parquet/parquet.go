package parquet

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/bsm/feedx"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// Format is a parquet format.
type Format struct {
	TempDir   string
	Columns   []string // column names to include
	BatchSize int      // batch size, default: 1,000
}

// NewDecoder implements Format.
func (f *Format) NewDecoder(r io.Reader) (feedx.FormatDecoder, error) {
	if rs, ok := r.(io.ReadSeeker); ok {
		return newDecoder(rs, f.Columns, f.BatchSize)
	}

	tmp, err := copyToTempFile(f.TempDir, r)
	if err != nil {
		return nil, err
	}

	dec, err := newDecoder(tmp, f.Columns, f.BatchSize)
	if err != nil {
		_ = tmp.Close()
		return nil, err
	}
	dec.closers = append(dec.closers, tmp)
	return dec, nil
}

// NewEncoder implements Format.
func (f *Format) NewEncoder(w io.Writer) (feedx.FormatEncoder, error) {
	// TODO! schema will need to be configured
	// TODO! we could probably use the format Columns
	schemaDef, err := parquetschema.ParseSchemaDefinition(
		`message test {
                       required int64 id;
                       required binary city (STRING);
                       optional int64 population;
               }`)
	if err != nil {
		return nil, err
	}

	fw := goparquet.NewFileWriter(w,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	return parquetEncoderWrapper{fw: fw}, nil
}

type parquetEncoderWrapper struct{ fw *goparquet.FileWriter }

func (w parquetEncoderWrapper) Encode(v interface{}) error {
	val, ok := v.(map[string]interface{})
	if !ok {
		return fmt.Errorf("feedx: value %v is not a map[string]interface{}", v)
	}
	return w.fw.AddData(val)
}

func (w parquetEncoderWrapper) Close() error {
	return w.fw.Close()
}

// --------------------------------------------------------------------

type tempFile struct{ *os.File }

func copyToTempFile(dir string, r io.Reader) (*tempFile, error) {
	w, err := ioutil.TempFile(dir, "feedx-ext-parquet")
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		_ = os.Remove(w.Name())
		return nil, err
	}
	if err := w.Close(); err != nil {
		_ = os.Remove(w.Name())
		return nil, err
	}

	f, err := os.Open(w.Name())
	if err != nil {
		_ = os.Remove(w.Name())
		return nil, err
	}

	return &tempFile{File: f}, nil
}

func (f tempFile) Close() error {
	err := f.File.Close()
	if e := os.Remove(f.Name()); e != nil {
		err = e
	}
	return err
}
