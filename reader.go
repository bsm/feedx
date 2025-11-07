package feedx

import (
	"context"
	"errors"
	"io"

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
	ctx context.Context
	opt *ReaderOptions

	remotes    []*bfs.Object
	ownRemotes bool

	cur *streamReader
	pos int

	num int64
}

// NewReader inits a new reader.
func NewReader(ctx context.Context, remote *bfs.Object, opt *ReaderOptions) (*Reader, error) {
	return MultiReader(ctx, []*bfs.Object{remote}, opt), nil
}

// MultiReader inits a new reader for multiple remotes.  Remotes are read sequentially as if concatenated.
// Once all remotes are fully read, Read will return EOF.
func MultiReader(ctx context.Context, remotes []*bfs.Object, opt *ReaderOptions) *Reader {
	return &Reader{
		remotes: remotes,
		opt:     opt,
		ctx:     ctx,
	}
}

// Read reads raw bytes from the feed.
// At end of feed, Read returns 0, io.EOF.
func (r *Reader) Read(p []byte) (int, error) {
	if !r.ensureCurrent() {
		return 0, io.EOF
	}

	n, err := r.cur.Read(p)
	if errors.Is(err, io.EOF) {
		if more, err := r.nextRemote(); err != nil {
			return n, err
		} else if more {
			return n, nil // dont return EOF until all remotes read
		}
	}
	return n, err
}

// Decode decodes the next formatted value from the feed.
// At end of feed, Read returns io.EOF.
func (r *Reader) Decode(v interface{}) error {
	if !r.ensureCurrent() {
		return io.EOF
	}

	err := r.cur.Decode(v)
	if errors.Is(err, io.EOF) {
		if more, err := r.nextRemote(); err != nil {
			return err
		} else if more {
			return r.Decode(v) // start decoding from next remote
		}
	} else if err == nil {
		r.num++
	}
	return err
}

// NumRead returns the number of read values.
func (r *Reader) NumRead() int64 {
	return r.num
}

// Version returns the version of the remote feed.
func (r *Reader) Version() (int64, error) {
	var max int64
	for _, remote := range r.remotes {
		v, err := fetchRemoteVersion(r.ctx, remote)
		if err != nil {
			return 0, err
		} else if v > max {
			max = v
		}
	}

	return max, nil
}

// Close closes the reader.
func (r *Reader) Close() (err error) {
	if r.cur != nil {
		err = r.cur.Close()
	}
	if r.ownRemotes {
		for _, remote := range r.remotes {
			if e := remote.Close(); e != nil {
				err = e
			}
		}
	}
	return
}

func (r *Reader) ensureCurrent() bool {
	if r.pos >= len(r.remotes) {
		return false
	}

	if r.cur == nil {
		remote := r.remotes[r.pos]

		var o ReaderOptions
		if r.opt != nil {
			o = *r.opt
		}
		o.norm(remote.Name())

		r.cur = &streamReader{
			remote: remote,
			opt:    o,
			ctx:    r.ctx,
		}
	}
	return true
}

func (r *Reader) nextRemote() (bool, error) {
	if err := r.cur.Close(); err != nil {
		return false, err
	}
	// unset current, increment cursor
	r.cur = nil
	r.pos++
	return r.pos < len(r.remotes), nil
}

type streamReader struct {
	remote *bfs.Object
	opt    ReaderOptions
	ctx    context.Context

	br io.ReadCloser // bfs reader
	cr io.ReadCloser // compression reader
	fd FormatDecoder
}

// Read reads raw bytes from the feed.
func (r *streamReader) Read(p []byte) (int, error) {
	if err := r.ensureOpen(); err != nil {
		return 0, err
	}
	return r.cr.Read(p)
}

// Decode decodes the next formatted value from the feed.
func (r *streamReader) Decode(v interface{}) error {
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

	return r.fd.Decode(v)
}

// Close closes the reader.
func (r *streamReader) Close() error {
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

func (r *streamReader) ensureOpen() error {
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
