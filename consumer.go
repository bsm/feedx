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
	// the sync state and error (if occurred).
	AfterSync func(*ConsumerSync, error)
}

func (o *ConsumerOptions) norm(name string) {
	o.ReaderOptions.norm(name)
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
}

// ConsumerSync contains the state of the last sync.
type ConsumerSync struct {
	// Consumer exposes the current consumer state.
	Consumer
	// Updated indicates is the sync resulted in an update.
	Updated bool
	// PreviousData references the data before the update.
	// It allows to apply finalizers to data structures created by ConsumeFunc.
	// This is only set when an update happened.
	PreviousData interface{}
}

// ConsumeFunc is a parsing callback which is run by the consumer every sync interval.
type ConsumeFunc func(*Reader) (data interface{}, err error)

// Consumer manages data retrieval from a remote feed.
// It queries the feed in regular intervals, continuously retrieving new updates.
type Consumer interface {
	// Data returns the data as returned by ConsumeFunc on last sync.
	Data() interface{}
	// LastSync returns time of last sync attempt.
	LastSync() time.Time
	// LastConsumed returns time of last feed consumption.
	LastConsumed() time.Time
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
	o.norm(remote.Name())

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

	cfn ConsumeFunc

	consumerState
}

type consumerState struct {
	data                                     atomic.Value
	numRead, lastMod, lastSync, lastConsumed int64
}

// Data implements Consumer interface.
func (c *consumerState) Data() interface{} {
	return c.data.Load()
}

// NumRead implements Consumer interface.
func (c *consumerState) NumRead() int {
	return int(atomic.LoadInt64(&c.numRead))
}

// LastSync implements Consumer interface.
func (c *consumerState) LastSync() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastSync)).Time()
}

// LastConsumed implements Consumer interface.
func (c *consumerState) LastConsumed() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastConsumed)).Time()
}

// LastModified implements Consumer interface.
func (c *consumerState) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastMod)).Time()
}

func (c *consumerState) updateNumRead(n int) {
	atomic.StoreInt64(&c.numRead, int64(n))
}

func (c *consumerState) updateLastSync(t time.Time) {
	atomic.StoreInt64(&c.lastSync, timestampFromTime(t).Millis())
}

func (c *consumerState) updateLastConsumed(t time.Time) {
	atomic.StoreInt64(&c.lastConsumed, timestampFromTime(t).Millis())
}

func (c *consumerState) updateLastModified(t time.Time) {
	atomic.StoreInt64(&c.lastMod, timestampFromTime(t).Millis())
}

func (c *consumerState) storeData(data interface{}) {
	c.data.Store(data)
}

// Close implements Consumer interface.
func (c *consumer) Close() error {
	c.stop()
	if c.ownRemote {
		return c.remote.Close()
	}
	return nil
}

func (c *consumer) sync(force bool) (*ConsumerSync, error) {
	start := time.Now()
	defer func() {
		c.consumerState.updateLastSync(start)
	}()

	// retrieve original last modified time
	lastMod, err := remoteLastModified(c.ctx, c.remote)
	if err != nil {
		return nil, err
	}

	// skip update if not forced or modified
	if !force && lastMod > 0 && lastMod.Millis() == atomic.LoadInt64(&c.lastMod) {
		return &ConsumerSync{Consumer: c}, nil
	}

	// open remote reader
	reader, err := NewReader(c.ctx, c.remote, &c.opt.ReaderOptions)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// consume feed
	data, err := c.cfn(reader)
	if err != nil {
		return nil, err
	}

	// update stores
	previous := c.data.Load()
	c.consumerState.storeData(data)
	c.consumerState.updateNumRead(reader.NumRead())
	c.consumerState.updateLastModified(lastMod.Time())
	c.consumerState.updateLastConsumed(start)
	return &ConsumerSync{
		Consumer:     c,
		Updated:      true,
		PreviousData: previous,
	}, nil
}

func (c *consumer) loop() {
	ticker := time.NewTicker(c.opt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			state, err := c.sync(false)
			if c.opt.AfterSync != nil {
				c.opt.AfterSync(state, err)
			}
		}
	}
}
