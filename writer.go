package feedx

import (
	"bufio"
	"context"
	"io"
	"strconv"

	"github.com/bsm/bfs"
)

// WriterOptions configure the producer instance.
type WriterOptions struct {
	// Format specifies the format
	// Default: auto-detected from URL path.
	Format Format

	// Compression specifies the compression type.
	// Default: auto-detected from URL path.
	Compression Compression

	// Provides an optional version which is stored with the remote metadata.
	// Default: 0
	Version int64
}

func (o *WriterOptions) norm(name string) {
	if o.Format == nil {
		o.Format = DetectFormat(name)
	}
	if o.Compression == nil {
		o.Compression = DetectCompression(name)
	}
}

// Writer encodes feeds to remote locations.
type Writer struct {
	ctx    context.Context
	remote *bfs.Object
	opt    WriterOptions
	num    int

	bw bfs.Writer
	cw io.WriteCloser // compression writer
	ww *bufio.Writer
	fe FormatEncoder
}

// NewWriter inits a new feed writer.
func NewWriter(ctx context.Context, remote *bfs.Object, opt *WriterOptions) *Writer {
	var o WriterOptions
	if opt != nil {
		o = *opt
	}
	o.norm(remote.Name())

	return &Writer{
		ctx:    ctx,
		remote: remote,
		opt:    o,
	}
}

// Write write raw bytes to the feed.
func (w *Writer) Write(p []byte) (int, error) {
	if err := w.ensureCreated(); err != nil {
		return 0, err
	}
	return w.ww.Write(p)
}

// WriteString write a raw string to the feed.
func (w *Writer) WriteString(s string) (int, error) {
	if err := w.ensureCreated(); err != nil {
		return 0, err
	}
	return w.ww.WriteString(s)
}

// Encode appends a value to the feed.
func (w *Writer) Encode(v interface{}) error {
	if err := w.ensureCreated(); err != nil {
		return err
	}

	if w.fe == nil {
		fe, err := w.opt.Format.NewEncoder(w.ww)
		if err != nil {
			return err
		}
		w.fe = fe
	}

	if err := w.fe.Encode(v); err != nil {
		return err
	}

	w.num++
	return nil
}

// NumWritten returns the number of written values.
func (w *Writer) NumWritten() int {
	return w.num
}

// Discard closes the writer and discards the contents.
func (w *Writer) Discard() error {
	err := w.close()
	if w.bw != nil {
		if e := w.bw.Discard(); e != nil {
			err = e
		}
	}
	return err
}

// Commit closes the writer and persists the contents.
func (w *Writer) Commit() error {
	err := w.close()
	if w.bw != nil {
		if e := w.bw.Commit(); e != nil {
			err = e
		}
	}
	return err
}

func (w *Writer) close() (err error) {
	if w.fe != nil {
		if e := w.fe.Close(); e != nil {
			err = e
		}
	}
	if w.ww != nil {
		if e := w.ww.Flush(); e != nil {
			err = e
		}
	}
	if w.cw != nil {
		if e := w.cw.Close(); e != nil {
			err = e
		}
	}
	return err
}

func (w *Writer) ensureCreated() error {
	if w.bw == nil {
		bw, err := w.remote.Create(w.ctx, &bfs.WriteOptions{
			Metadata: bfs.Metadata{metaVersion: strconv.FormatInt(w.opt.Version, 10)},
		})
		if err != nil {
			return err
		}
		w.bw = bw
	}

	if w.cw == nil {
		cw, err := w.opt.Compression.NewWriter(w.bw)
		if err != nil {
			return err
		}
		w.cw = cw
	}

	if w.ww == nil {
		w.ww = bufio.NewWriter(w.cw)
	}

	return nil
}
