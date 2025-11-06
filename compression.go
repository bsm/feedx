package feedx

import (
	"compress/flate"
	"compress/gzip"
	"io"
	"path"

	"github.com/klauspost/compress/zstd"
)

// Compression represents the data compression.
type Compression interface {
	// NewReader wraps a reader.
	NewReader(io.Reader) (io.ReadCloser, error)
	// NewWriter wraps a writer.
	NewWriter(io.Writer) (io.WriteCloser, error)
}

// DetectCompression detects the compression type from a URL path or file name.
func DetectCompression(name string) Compression {
	if name != "" {
		ext := path.Ext(path.Base(name))
		if ext != "" && ext[0] == '.' && ext[len(ext)-1] == 'z' {
			return GZipCompression
		} else if ext == ".flate" {
			return FlateCompression
		} else if ext == ".zst" {
			return ZstdCompression
		}
	}
	return NoCompression
}

// --------------------------------------------------------------------

// NoCompression is just a pass-through without compression.
var NoCompression = noCompression{}

type noCompression struct{}

func (noCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return noCompressionWrapper{Reader: r}, nil
}

func (noCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return noCompressionWrapper{Writer: w}, nil
}

type noCompressionWrapper struct {
	io.Reader
	io.Writer
}

func (noCompressionWrapper) Close() error { return nil }

// --------------------------------------------------------------------

// GZipCompression supports gzip compression format.
var GZipCompression = gzipCompression{}

type gzipCompression struct{}

func (gzipCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func (gzipCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

// --------------------------------------------------------------------

// FlateCompression supports flate compression format.
var FlateCompression = flateCompression{}

type flateCompression struct{}

func (flateCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	return flate.NewReader(r), nil
}

func (flateCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return flate.NewWriter(w, flate.BestSpeed)
}

// --------------------------------------------------------------------

// ZstdCompression supports zstd compression format.
var ZstdCompression = zstdCompression{}

type zstdCompression struct{}

func (zstdCompression) NewReader(r io.Reader) (io.ReadCloser, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return zstdDecoder{Decoder: zr}, nil
}

func (zstdCompression) NewWriter(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w)
}

type zstdDecoder struct {
	*zstd.Decoder
}

func (zstdDecoder) Close() error { return nil }
