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

	// incremental is a flag to denote if feed was written by incremental producer
	incremental bool
}

func (o *ConsumerOptions) norm() {
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
	o.norm()

	ctx, stop := context.WithCancel(ctx)
	c := &consumer{
		remote: remote,
		opt:    o,
		ctx:    ctx,
		stop:   stop,
		cfn:    cfn,
	}

	return c.run()
}

// NewIncrementalConsumer starts a new incremental feed consumer.
func NewIncrementalConsumer(ctx context.Context, bucketURL string, opt *ConsumerOptions, cfn ConsumeFunc) (Consumer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	csm, err := NewIncrementalConsumerForBucket(ctx, bucket, opt, cfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	csm.(*consumer).ownRemote = true
	return csm, nil
}

// NewIncrementalConsumerForBucket starts a new incremental feed consumer with a bucket.
func NewIncrementalConsumerForBucket(ctx context.Context, bucket bfs.Bucket, opt *ConsumerOptions, cfn ConsumeFunc) (Consumer, error) {
	var o ConsumerOptions
	if opt != nil {
		o = *opt
	}
	o.incremental = true
	o.norm()

	ctx, stop := context.WithCancel(ctx)
	c := &consumer{
		remote: bfs.NewObjectFromBucket(bucket, "manifest.json"),
		bucket: bucket,
		opt:    o,
		ctx:    ctx,
		stop:   stop,
		cfn:    cfn,
	}

	return c.run()
}

type consumer struct {
	remote    *bfs.Object
	bucket    bfs.Bucket
	ownRemote bool

	opt  ConsumerOptions
	ctx  context.Context
	stop context.CancelFunc

	cfn  ConsumeFunc
	data atomic.Value

	numRead, lastMod, lastSync, lastConsumed int64
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

// LastConsumed implements Consumer interface.
func (c *consumer) LastConsumed() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastConsumed)).Time()
}

// LastModified implements Consumer interface.
func (c *consumer) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&c.lastMod)).Time()
}

// Close implements Consumer interface.
func (c *consumer) Close() (err error) {
	c.stop()
	if c.ownRemote {
		if e := c.remote.Close(); e != nil {
			err = e
		}
		if c.bucket != nil {
			if e := c.bucket.Close(); e != nil {
				err = e
			}
		}
	}
	return
}

func (c *consumer) run() (Consumer, error) {
	// run initial sync
	if _, err := c.sync(true); err != nil {
		_ = c.Close()
		return nil, err
	}

	// start continuous loop
	go c.loop()

	return c, nil
}

func (c *consumer) sync(force bool) (*ConsumerSync, error) {
	syncTime := timestampFromTime(time.Now()).Millis()
	defer atomic.StoreInt64(&c.lastSync, syncTime)

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
	var reader *Reader
	if c.opt.incremental {
		var remotes []*bfs.Object
		if reader, remotes, err = c.newIncrementalReader(); err != nil {
			return nil, err
		}
		for _, r := range remotes {
			defer r.Close()
		}
	} else {
		if reader, err = NewReader(c.ctx, c.remote, &c.opt.ReaderOptions); err != nil {
			return nil, err
		}
	}
	defer reader.Close()

	// consume feed
	data, err := c.cfn(reader)
	if err != nil {
		return nil, err
	}

	// update stores
	previous := c.data.Load()
	c.data.Store(data)
	atomic.StoreInt64(&c.numRead, reader.NumRead())
	atomic.StoreInt64(&c.lastMod, lastMod.Millis())
	atomic.StoreInt64(&c.lastConsumed, syncTime)
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

func (c *consumer) newIncrementalReader() (*Reader, []*bfs.Object, error) {
	manifest, err := loadManifest(c.ctx, c.remote)
	if err != nil {
		return nil, nil, err
	}

	files := manifest.Files
	remotes := make([]*bfs.Object, 0, len(files))
	for _, file := range files {
		remotes = append(remotes, bfs.NewObjectFromBucket(c.bucket, file))
	}
	return MultiReader(c.ctx, remotes, &c.opt.ReaderOptions), remotes, nil
}
