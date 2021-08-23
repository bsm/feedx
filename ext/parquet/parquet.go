package parquet

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/bsm/feedx"
)

type EncoderOpts struct {
	SchemaDef   string
	Compression string // compession string from github.com/fraugster/parquet-go
}

func (o *EncoderOpts) norm() error {
	if o.SchemaDef == "" {
		return fmt.Errorf("SchemaDef is missing")
	}

	if o.Compression == "" {
		o.Compression = "SNAPPY"
	}

	return nil
}

// --------------------------------------------------------------------

// Format is a parquet format.
type Format struct {
	TempDir     string
	EncoderOpts *EncoderOpts
}

// NewDecoder implements Format.
func (f *Format) NewDecoder(r io.Reader) (feedx.FormatDecoder, error) {
	if rs, ok := r.(io.ReadSeeker); ok {
		return newDecoder(rs)
	}

	tmp, err := copyToTempFile(f.TempDir, r)
	if err != nil {
		return nil, err
	}

	dec, err := newDecoder(tmp)
	if err != nil {
		_ = tmp.Close()
		return nil, err
	}
	dec.tmp = tmp
	return dec, nil
}

// NewEncoder implements Format.
func (f *Format) NewEncoder(w io.Writer) (feedx.FormatEncoder, error) {
	// check options
	if err := f.EncoderOpts.norm(); err != nil {
		return nil, err
	}

	return newEncoder(w, f.EncoderOpts)
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

func (f *tempFile) Close() error {
	err := f.File.Close()
	if e := os.Remove(f.Name()); e != nil {
		err = e
	}
	return err
}
