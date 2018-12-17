package feedx

import (
	"context"
	"fmt"
	"io"
	"time"

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

	// Provides an optional last modified timestamp which is stored with the remote metadata.
	// Default: time.Now().
	LastMod time.Time
}

func (o *WriterOptions) norm(name string) error {
	if o.Format == nil {
		o.Format = DetectFormat(name)

		if o.Format == nil {
			return fmt.Errorf("feedx: unable to detect format from %q", name)
		}
	}

	if o.Compression == nil {
		o.Compression = DetectCompression(name)
	}

	if o.LastMod.IsZero() {
		o.LastMod = time.Now()
	}

	return nil
}

// Writer encodes feeds to remote locations.
type Writer struct {
	ctx    context.Context
	remote *bfs.Object
	opt    WriterOptions
	num    int

	bw io.WriteCloser // bfs writer
	cw io.WriteCloser // compression writer
	fe FormatEncoder
}

// NewWriter inits a new feed writer.
func NewWriter(ctx context.Context, remote *bfs.Object, opt *WriterOptions) (*Writer, error) {
	var o WriterOptions
	if opt != nil {
		o = *opt
	}
	o.norm(remote.Name())

	return &Writer{
		ctx:    ctx,
		remote: remote,
		opt:    o,
	}, nil
}

// Encode implements Producer.
func (w *Writer) Encode(v interface{}) error {
	if w.bw == nil {
		ts := timestampFromTime(w.opt.LastMod)
		bw, err := w.remote.Create(w.ctx, &bfs.WriteOptions{
			Metadata: map[string]string{metaLastModified: ts.String()},
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

	if w.fe == nil {
		fe, err := w.opt.Format.NewEncoder(w.cw)
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

// Close closes the writer.
func (w *Writer) Close() error {
	var err error
	if w.fe != nil {
		if e := w.fe.Close(); e != nil {
			err = e
		}
	}
	if w.cw != nil {
		if e := w.cw.Close(); e != nil {
			err = e
		}
	}
	if w.bw != nil {
		if e := w.bw.Close(); e != nil {
			err = e
		}
	}
	return err
}
