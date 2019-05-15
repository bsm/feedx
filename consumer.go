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
	AfterSync func(updated bool, consumer Consumer, err error)
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
	c := &consumer{
		remote: remote,
		opt:    o,
		ctx:    ctx,
		stop:   stop,
		cfn:    cfn,
	}

	// run initial sync
	if _, err := c.sync(true); err != nil {
		_ = c.Close()
		return nil, err
	}

	// start continuous loop
	go c.loop()

	return c, nil
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

// Data implements Consumer interface.
func (c *consumer) Data() interface{} {
	return c.data.Load()
}

// NumRead implements Consumer interface.
func (c *consumer) NumRead() int {
	return int(atomic.LoadInt64(&c.numRead))
}

// LastSync implements Consumer interface.
func (c *consumer) LastSync() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastSync)).Time()
}

// LastModified implements Consumer interface.
func (c *consumer) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastMod)).Time()
}

// Close implements Consumer interface.
func (c *consumer) Close() error {
	c.stop()
	if c.ownRemote {
		return c.remote.Close()
	}
	return nil
}

func (c *consumer) sync(force bool) (bool, error) {
	defer func() {
		atomic.StoreInt64(&c.lastSync, timestampFromTime(time.Now()).Millis())
	}()

	// retrieve original last modified time
	lastMod, err := remoteLastModified(c.ctx, c.remote)
	if err != nil {
		return false, err
	}

	// skip update if not forced or modified
	if lastMod.Millis() == atomic.LoadInt64(&c.lastMod) && !force {
		return false, nil
	}

	// open remote reader
	reader, err := NewReader(c.ctx, c.remote, &c.opt.ReaderOptions)
	if err != nil {
		return false, err
	}
	defer reader.Close()

	// consume feed
	data, err := c.cfn(reader)
	if err != nil {
		return false, err
	}

	// update stores
	c.data.Store(data)
	atomic.StoreInt64(&c.numRead, int64(reader.NumRead()))
	atomic.StoreInt64(&c.lastMod, lastMod.Millis())
	return true, nil
}

func (c *consumer) loop() {
	ticker := time.NewTicker(c.opt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			updated, err := c.sync(false)
			if c.opt.AfterSync != nil {
				c.opt.AfterSync(updated, c, err)
			}
		}
	}
}
