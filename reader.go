package feedx

import (
	"context"
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

func (o *ReaderOptions) norm(name string) {
	if o.Format == nil {
		o.Format = DetectFormat(name)
	}
	if o.Compression == nil {
		o.Compression = DetectCompression(name)
	}
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
	o.norm(remote.Name())

	return &Reader{
		remote: remote,
		opt:    o,
		ctx:    ctx,
	}, nil
}

// Read reads raw bytes from the feed.
func (r *Reader) Read(p []byte) (int, error) {
	if err := r.ensureOpen(); err != nil {
		return 0, err
	}

	return r.cr.Read(p)
}

// Decode decodes the next formatted value from the feed.
func (r *Reader) Decode(v interface{}) error {
	if err := r.ensureOpen(); err != nil {
		return err
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

func (r *Reader) ensureOpen() error {
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

	return nil
}

// -----------------------------------------------------------------------------------------------

// ReaderIter is a reader iterator.
type ReaderIter struct {
	remotes []*bfs.Object
	opt     *ReaderOptions
	ctx     context.Context
	reader  *Reader

	i   int
	err error
	num int
}

// NewReaderIter inits a ReaderIter.
func NewReaderIter(ctx context.Context, remotes []*bfs.Object, opt *ReaderOptions) *ReaderIter {
	return &ReaderIter{
		remotes: remotes,
		ctx:     ctx,
		opt:     opt,
		i:       -1,
	}
}

// Next returns the next Reader in the iteration if there is one.
// Once Next returns ok==false, the iteration is over,
// and all subsequent calls will return ok==false.
func (r *ReaderIter) Next() (*Reader, bool) {
	if r.reader != nil {
		r.num += r.reader.NumRead()
	}

	r.i++
	ok := r.err == nil && r.i >= 0 && r.i < len(r.remotes)
	if ok {
		r.reader, r.err = NewReader(r.ctx, r.remotes[r.i], r.opt)
		if r.err != nil {
			return nil, false
		}
	} else {
		r.reader = nil
	}
	return r.reader, ok
}

// Err returns any errors from prev iteration.
func (r *ReaderIter) Err() error {
	return r.err
}

// NumRead returns total number read values from prev iterations.
func (r *ReaderIter) NumRead() int {
	return r.num
}
