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
	ctx context.Context
	opt *ReaderOptions

	remotes  []*bfs.Object
	cur      *streamReader
	pos, num int
}

// NewReader inits a new reader.
func NewReader(ctx context.Context, remote *bfs.Object, opt *ReaderOptions) (*Reader, error) {
	return &Reader{
		remotes: []*bfs.Object{remote},
		opt:     opt,
		ctx:     ctx,
	}, nil
}

// MultiReader inits a new reader for multiple remotes.  Remotes are read sequentially as if concatenated.
// Once all remotes are fully read, Read will return EOF.
func MultiReader(ctx context.Context, remotes []*bfs.Object, opt *ReaderOptions) (*Reader, error) {
	return &Reader{
		remotes: remotes,
		opt:     opt,
		ctx:     ctx,
	}, nil
}

// Read reads raw bytes from the feed.
func (r *Reader) Read(p []byte) (int, error) {
	if len(r.remotes) == 0 || r.pos >= len(r.remotes) {
		return 0, io.EOF
	}

	r.ensureCurrent()
	n, err := r.cur.Read(p)
	if err == io.EOF {
		r.cur = nil // remove current reader

		// increment position and check if any more remotes
		if r.pos++; r.pos < len(r.remotes) {
			return r.Read(p) // start reading from next remote
		}
	}

	return n, err
}

// Decode decodes the next formatted value from the feed.
func (r *Reader) Decode(v interface{}) error {
	if len(r.remotes) == 0 || r.pos >= len(r.remotes) {
		return io.EOF
	}

	r.ensureCurrent()
	err := r.cur.Decode(v)
	if err == nil {
		r.num++
	} else if err == io.EOF {
		r.cur = nil // remove current reader

		// increment position and check if any more remotes
		if r.pos++; r.pos < len(r.remotes) {
			return r.Decode(v) // start decoding from next remote
		}
	}

	return err
}

// NumRead returns the number of read values.
func (r *Reader) NumRead() int {
	return r.num
}

// LastModified returns the last modified time of the remote feed.
func (r *Reader) LastModified() (time.Time, error) {
	var lastMod timestamp
	for _, remote := range r.remotes {
		t, err := remoteLastModified(r.ctx, remote)
		if err != nil {
			return time.Time{}, err
		}
		if t > lastMod {
			lastMod = t
		}
	}

	return lastMod.Time(), nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.cur != nil {
		return r.cur.Close()
	}
	return nil
}

func (r *Reader) ensureCurrent() {
	if len(r.remotes) > 0 && r.cur == nil {
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
}

type streamReader struct {
	remote *bfs.Object
	opt    ReaderOptions
	ctx    context.Context
	num    int

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

	if err := r.fd.Decode(v); err != nil {
		return err
	}

	r.num++
	return nil
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
