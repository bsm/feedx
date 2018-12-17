package feedx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// ConsumerOptions configure the consumer instance.
type ConsumerOptions struct {
	ReaderOptions

	// The interval used by consumer to check the remote changes.
	// Default: 1m
	Interval time.Duration

	// AfterSync callbacks are triggered after each sync, receiving
	// the updated status and error (if occurred).
	AfterSync func(updated bool, err error)
}

func (o *ConsumerOptions) norm(name string) error {
	o.ReaderOptions.norm(name)
	if o.Interval <= 0 {
		o.Interval = time.Minute
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
func NewConsumer(ctx context.Context, remoteURL string, opt *ConsumerOptions, parse ParseFunc) (Consumer, error) {
	remote, err := bfs.NewObject(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	csm, err := NewConsumerForRemote(ctx, remote, opt, parse)
	if err != nil {
		_ = remote.Close()
		return nil, err
	}
	csm.(*consumer).ownRemote = true
	return csm, nil
}

// NewConsumerForRemote starts a new feed consumer with a remote.
func NewConsumerForRemote(ctx context.Context, remote *bfs.Object, opt *ConsumerOptions, parse ParseFunc) (Consumer, error) {
	var o ConsumerOptions
	if opt != nil {
		o = *opt
	}
	if err := o.norm(remote.Name()); err != nil {
		return nil, err
	}

	ctx, stop := context.WithCancel(ctx)
	f := &consumer{
		remote: remote,
		opt:    o,
		ctx:    ctx,
		stop:   stop,
		parse:  parse,
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
	remote    *bfs.Object
	ownRemote bool

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
	if f.ownRemote {
		return f.remote.Close()
	}
	return nil
}

func (f *consumer) sync(force bool) (bool, error) {
	f.lastCheck.Store(time.Now())

	// retrieve original last modified time
	msse, err := lastModifiedFromObj(f.ctx, f.remote)
	if err != nil {
		return false, err
	}

	// skip update if not forced or modified
	if int64(msse) == atomic.LoadInt64(&f.lastModMs) && !force {
		return false, nil
	}

	// open remote reader
	reader, err := NewReader(f.ctx, f.remote, &f.opt.ReaderOptions)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	// parse feed
	data, size, err := f.parse(reader)
	if err != nil {
		return false, err
	}

	// update stores
	f.data.Store(data)
	atomic.StoreInt64(&f.size, size)
	atomic.StoreInt64(&f.lastModMs, int64(msse))
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
			if f.opt.AfterSync != nil {
				f.opt.AfterSync(updated, err)
			}
		}
	}
}
