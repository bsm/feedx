package feedx

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bsm/bfs"
)

// ReaderOptions configure the reader instance.
type ReaderOptions struct {
	// Format specifies the format
	// Default: auto-detected from URL path.
	Format Format

	// Compression specifies the compression type.
	// Default: auto-detected from URL path.
	Compression Compression
}

func (o *ReaderOptions) norm(name string) error {
	if o.Format == nil {
		o.Format = DetectFormat(name)

		if o.Format == nil {
			return fmt.Errorf("feedx: unable to detect format from %q", name)
		}
	}
	if o.Compression == nil {
		o.Compression = DetectCompression(name)
	}
	return nil
}

// Reader reads data from a remote feed.
type Reader struct {
	remote *bfs.Object
	opt    ReaderOptions
	ctx    context.Context
	num    int

	br io.ReadCloser // bfs reader
	cr io.ReadCloser // compression reader
	fd FormatDecoder
}

// NewReader inits a new reader.
func NewReader(ctx context.Context, remote *bfs.Object, opt *ReaderOptions) (*Reader, error) {
	var o ReaderOptions
	if opt != nil {
		o = *opt
	}
	if err := o.norm(remote.Name()); err != nil {
		return nil, err
	}

	return &Reader{
		remote: remote,
		opt:    o,
		ctx:    ctx,
	}, nil
}

// Decode decodes the next value from the feed.
func (r *Reader) Decode(v interface{}) error {
	if r.br == nil {
		br, err := r.remote.Open(r.ctx)
		if err != nil {
			return err
		}
		r.br = br
	}

	if r.cr == nil {
		cr, err := r.opt.Compression.NewReader(r.br)
		if err != nil {
			return err
		}
		r.cr = cr
	}

	if r.fd == nil {
		fd, err := r.opt.Format.NewDecoder(r.cr)
		if err != nil {
			return err
		}
		r.fd = fd
	}

	if err := r.fd.Decode(v); err != nil {
		return err
	}

	r.num++
	return nil
}

// NumRead returns the number of read values.
func (r *Reader) NumRead() int {
	return r.num
}

// LastModified returns the last modified time of the remote feed.
func (r *Reader) LastModified() (time.Time, error) {
	lastMod, err := remoteLastModified(r.ctx, r.remote)
	return lastMod.Time(), err
}

// Close closes the reader.
func (r *Reader) Close() error {
	var err error
	if r.fd != nil {
		if e := r.fd.Close(); e != nil {
			err = e
		}
	}
	if r.cr != nil {
		if e := r.cr.Close(); e != nil {
			err = e
		}
	}
	if r.br != nil {
		if e := r.br.Close(); e != nil {
			err = e
		}
	}
	return err
}
