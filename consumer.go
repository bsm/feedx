package feedx

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// ConsumerOptions configure the Puller instance.
type ConsumerOptions struct {
	// The interval used by Puller to check the remote changes.
	// Default: 1m
	Interval time.Duration

	// Format specifies the format
	// Default: auto-detected from URL path.
	Format Format

	// Compression specifies the compression type.
	// Default: auto-detected from URL path.
	Compression Compression

	// AfterSync callbacks are triggered after each sync, receiving
	// the updated status and error (if occurred).
	AfterSync func(updated bool, err error)
}

func (o *ConsumerOptions) norm(name string) error {
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
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

// ParseFunc is a data parse function.
type ParseFunc func(FormatDecoder) (data interface{}, size int64, err error)

// Consumer manages data retrieval from a remote feed.
// It queries the feed in regular intervals, continuously retrieving new updates.
type Consumer interface {
	// Data returns the data as returned by ParseFunc on last sync.
	Data() interface{}
	// LastCheck returns time of last sync attempt.
	LastCheck() time.Time
	// LastModified returns time at which the remote feed was last modified.
	LastModified() time.Time
	// Size returns the size as returned by ParseFunc on last sync.
	Size() int64
	// Close stops the underlying sync process.
	Close() error
}

// NewConsumer starts a new feed consumer.
func NewConsumer(ctx context.Context, srcURL string, opt *ConsumerOptions, parse ParseFunc) (Consumer, error) {
	src, err := bfs.NewObject(ctx, srcURL)
	if err != nil {
		return nil, err
	}

	var o ConsumerOptions
	if opt != nil {
		o = *opt
	}
	if err := o.norm(src.Name()); err != nil {
		_ = src.Close()
		return nil, err
	}

	ctx, stop := context.WithCancel(ctx)
	f := &consumer{
		src:   src,
		opt:   o,
		ctx:   ctx,
		stop:  stop,
		parse: parse,
	}

	// run initial sync
	if _, err := f.sync(true); err != nil {
		_ = f.Close()
		return nil, err
	}

	// start continuous loop
	go f.loop()

	return f, nil
}

type consumer struct {
	src  *bfs.Object
	opt  ConsumerOptions
	ctx  context.Context
	stop context.CancelFunc

	parse ParseFunc

	size, lastModMs int64
	data, lastCheck atomic.Value
}

// Data implements Feed interface.
func (f *consumer) Data() interface{} {
	return f.data.Load()
}

// Size implements Feed interface.
func (f *consumer) Size() int64 {
	return atomic.LoadInt64(&f.size)
}

// LastCheck implements Feed interface.
func (f *consumer) LastCheck() time.Time {
	return f.lastCheck.Load().(time.Time)
}

// LastModified implements Feed interface.
func (f *consumer) LastModified() time.Time {
	msec := atomic.LoadInt64(&f.lastModMs)
	return time.Unix(msec/1000, msec%1000*1e6)
}

// Close implements Feed interface.
func (f *consumer) Close() error {
	f.stop()
	return f.src.Close()
}

func (f *consumer) sync(force bool) (bool, error) {
	f.lastCheck.Store(time.Now())

	info, err := f.src.Head(f.ctx)
	if err != nil {
		return false, err
	}

	// calculate last modified time
	msec, _ := strconv.ParseInt(info.Metadata[lastModifiedMetaKey], 10, 64)

	// skip update if not forced or modified
	if msec == atomic.LoadInt64(&f.lastModMs) && !force {
		return false, nil
	}

	// open remote for reading
	r, err := f.src.Open(f.ctx)
	if err != nil {
		return false, err
	}
	defer r.Close()

	// wrap in compressed reader
	c, err := f.opt.Compression.NewReader(r)
	if err != nil {
		return false, err
	}
	defer c.Close()

	// open decoder
	d, err := f.opt.Format.NewDecoder(c)
	if err != nil {
		return false, err
	}
	defer f.Close()

	// parse feed
	data, size, err := f.parse(d)
	if err != nil {
		return false, err
	}

	// update stores
	f.data.Store(data)
	atomic.StoreInt64(&f.size, size)
	atomic.StoreInt64(&f.lastModMs, msec)
	return true, nil
}

func (f *consumer) loop() {
	ticker := time.NewTicker(f.opt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			updated, err := f.sync(false)
			f.opt.AfterSync(updated, err)
		}
	}
}
