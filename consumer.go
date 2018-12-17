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

// ConsumeFunc is a parsing callback which is run by the consumer every sync interval.
type ConsumeFunc func(FormatDecoder) (data interface{}, err error)

// Consumer manages data retrieval from a remote feed.
// It queries the feed in regular intervals, continuously retrieving new updates.
type Consumer interface {
	// Data returns the data as returned by ConsumeFunc on last sync.
	Data() interface{}
	// LastSync returns time of last sync attempt.
	LastSync() time.Time
	// LastModified returns time at which the remote feed was last modified.
	LastModified() time.Time
	// NumRead returns the number of values consumed during the last sync.
	NumRead() int
	// Close stops the underlying sync process.
	Close() error
}

// NewConsumer starts a new feed consumer.
func NewConsumer(ctx context.Context, remoteURL string, opt *ConsumerOptions, cfn ConsumeFunc) (Consumer, error) {
	remote, err := bfs.NewObject(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	csm, err := NewConsumerForRemote(ctx, remote, opt, cfn)
	if err != nil {
		_ = remote.Close()
		return nil, err
	}
	csm.(*consumer).ownRemote = true
	return csm, nil
}

// NewConsumerForRemote starts a new feed consumer with a remote.
func NewConsumerForRemote(ctx context.Context, remote *bfs.Object, opt *ConsumerOptions, cfn ConsumeFunc) (Consumer, error) {
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
		cfn:    cfn,
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

	cfn  ConsumeFunc
	data atomic.Value

	numRead, lastMod, lastSync int64
}

// Data implements Feed interface.
func (f *consumer) Data() interface{} {
	return f.data.Load()
}

// NumRead implements Feed interface.
func (f *consumer) NumRead() int {
	return int(atomic.LoadInt64(&f.numRead))
}

// LastSync implements Feed interface.
func (f *consumer) LastSync() time.Time {
	return timestamp(atomic.LoadInt64(&f.lastSync)).Time()
}

// LastModified implements Feed interface.
func (f *consumer) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&f.lastMod)).Time()
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
	defer func() {
		atomic.StoreInt64(&f.lastSync, timestampFromTime(time.Now()).Millis())
	}()

	// retrieve original last modified time
	lastMod, err := remoteLastModified(f.ctx, f.remote)
	if err != nil {
		return false, err
	}

	// skip update if not forced or modified
	if lastMod.Millis() == atomic.LoadInt64(&f.lastMod) && !force {
		return false, nil
	}

	// open remote reader
	reader, err := NewReader(f.ctx, f.remote, &f.opt.ReaderOptions)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	// consume feed
	data, err := f.cfn(reader)
	if err != nil {
		return false, err
	}

	// update stores
	f.data.Store(data)
	atomic.StoreInt64(&f.numRead, int64(reader.NumRead()))
	atomic.StoreInt64(&f.lastMod, lastMod.Millis())
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
