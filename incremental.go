package feedx

import (
	"context"
	"sort"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// IncrmentalProduceFunc returns a ProduceFunc closure around an incremental mod time.
type IncrementalProduceFunc func(time.Time) ProduceFunc

// IncrementalProducer produces a continuous incremental feed.
type IncrementalProducer struct {
	producerState

	bucket    bfs.Bucket
	manifest  *bfs.Object
	ownBucket bool
	opt       IncrementalProducerOptions

	ctx  context.Context
	stop context.CancelFunc
	ipfn IncrementalProduceFunc
}

// IncrementalProducerOptions configure the producer instance.
type IncrementalProducerOptions struct {
	ProducerOptions
}

func (o *IncrementalProducerOptions) norm(lmfn LastModFunc) {
	if o.Compression == nil {
		o.Compression = GZipCompression
	}
	if o.Format == nil {
		o.Format = ProtobufFormat
	}
	if o.Interval == 0 {
		o.Interval = time.Minute
	}
	o.LastModCheck = lmfn
}

// NewIncrementalProducer inits a new incremental feed producer.
func NewIncrementalProducer(ctx context.Context, bucketURL string, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	p, err := NewIncrementalProducerForBucket(ctx, bucket, opt, lmfn, ipfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	p.ownBucket = true

	return p, nil
}

// NewIncrementalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	var o IncrementalProducerOptions
	if opt != nil {
		o = IncrementalProducerOptions(*opt)
	}
	o.norm(lmfn)

	ctx, stop := context.WithCancel(ctx)
	p := &IncrementalProducer{
		bucket:   bucket,
		manifest: bfs.NewObjectFromBucket(bucket, "manifest.json"),
		ctx:      ctx,
		stop:     stop,
		opt:      o,
		ipfn:     ipfn,
	}

	// run initial push
	if _, err := p.push(); err != nil {
		_ = p.Close()
		return nil, err
	}

	// start continuous loop
	// use error group
	go p.loop()

	return p, nil
}

// Close stops the producer.
func (p *IncrementalProducer) Close() (err error) {
	p.stop()
	if e := p.manifest.Close(); e != nil {
		err = e
	}

	if p.ownBucket {
		if e := p.bucket.Close(); e != nil {
			err = e
		}
	}
	return
}

func (p *IncrementalProducer) loop() {
	ticker := time.NewTicker(p.opt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			state, err := p.push()
			if p.opt.AfterPush != nil {
				p.opt.AfterPush(state, err)
			}
		}
	}
}

func (p *IncrementalProducer) push() (*ProducerPush, error) {
	start := time.Now()
	p.producerState.updateLastPush(start)

	// get last mod time for local records
	localLastMod, err := p.opt.LastModCheck(p.ctx)
	if err != nil {
		return nil, err
	}
	if localLastMod.IsZero() {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	// fetch manifest from remote
	manifest, err := loadManifest(p.ctx, p.manifest)
	if err != nil {
		return nil, err
	}

	// compare manifest LastModified to local last mod.
	remoteLastMod := manifest.LastModified
	if remoteLastMod == timestampFromTime(localLastMod) {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	wopt := p.opt.WriterOptions
	wopt.LastMod = localLastMod

	// write data modified since last remote mod
	numWritten, err := p.writeDataFile(manifest, &wopt)
	if err != nil {
		return nil, err
	}
	// write new manifest to remote
	if err := p.commitManifest(manifest, &WriterOptions{LastMod: wopt.LastMod}); err != nil {
		return nil, err
	}

	p.producerState.updateNumWritten(numWritten)
	p.producerState.updateLastModified(wopt.LastMod)
	return &ProducerPush{producerState: p.producerState, Updated: true}, nil
}

func (p *IncrementalProducer) writeDataFile(m *manifest, wopt *WriterOptions) (int, error) {
	fname := m.newDataFileName(wopt)

	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, fname), wopt)
	defer writer.Discard()

	if err := p.ipfn(wopt.LastMod)(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}

	m.Files = append(m.Files, fname)
	m.LastModified = timestampFromTime(wopt.LastMod)

	return writer.NumWritten(), nil
}

func (p *IncrementalProducer) commitManifest(m *manifest, wopt *WriterOptions) error {
	name := p.manifest.Name()
	wopt.norm(name) // norm sets writer format and compression from name

	writer := NewWriter(p.ctx, p.manifest, wopt)
	defer writer.Discard()

	if err := writer.Encode(m); err != nil {
		return err
	}
	return writer.Commit()
}

// ------------------------------------------------------------------------------------------

// IncrementalConsumeFunc is a parsing callback which is run by the consumer every sync interval.
type IncrementalConsumeFunc func(*ReaderIter) (data interface{}, err error)

// IncrementalConsumerOptions configure the consumer instance.
type IncrementalConsumerOptions struct {
	ConsumerOptions
}

func (o *IncrementalConsumerOptions) norm() {
	if o.Interval == 0 {
		o.Interval = time.Minute
	}
}

// NewIncrementalConsumer starts a new feed consumer.
func NewIncrementalConsumer(ctx context.Context, bucketURL string, opt *IncrementalConsumerOptions, icfn IncrementalConsumeFunc) (Consumer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	csm, err := NewIncrementalConsumerForBucket(ctx, bucket, opt, icfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	csm.(*incrementalConsumer).ownBucket = true
	return csm, nil
}

// NewIncrementalConsumerForBucket starts a new feed consumer with a remote.
func NewIncrementalConsumerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalConsumerOptions, icfn IncrementalConsumeFunc) (Consumer, error) {
	var o IncrementalConsumerOptions
	if opt != nil {
		o = IncrementalConsumerOptions(*opt)
	}
	o.norm()

	ctx, stop := context.WithCancel(ctx)
	c := &incrementalConsumer{
		bucket:   bucket,
		manifest: bfs.NewObjectFromBucket(bucket, "manifest.json"),
		opt:      o,
		ctx:      ctx,
		stop:     stop,
		icfn:     icfn,
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

type incrementalConsumer struct {
	manifest  *bfs.Object
	bucket    bfs.Bucket
	ownBucket bool

	opt  IncrementalConsumerOptions
	ctx  context.Context
	stop context.CancelFunc

	icfn IncrementalConsumeFunc

	consumerState
}

// Close implements Consumer interface.
func (c *incrementalConsumer) Close() (err error) {
	c.stop()
	if e := c.manifest.Close(); e != nil {
		err = e
	}
	if c.ownBucket {
		if e := c.bucket.Close(); e != nil {
			err = e
		}
	}
	return
}

func (c *incrementalConsumer) sync(force bool) (*ConsumerSync, error) {
	start := time.Now()
	defer func() {
		c.consumerState.updateLastSync(start)
	}()

	// retrieve original last modified time
	lastMod, err := remoteLastModified(c.ctx, c.manifest)
	if err != nil {
		return nil, err
	}

	// skip update if not forced or modified
	if !force && lastMod > 0 && lastMod.Millis() == atomic.LoadInt64(&c.lastMod) {
		return &ConsumerSync{Consumer: c}, nil
	}

	// fetch remote manifest
	manifest, err := loadManifest(c.ctx, c.manifest)
	if err != nil {
		return nil, err
	}

	// sort file list
	files := manifest.Files
	sort.Strings(files) // TODO! allow setting sort function in options?

	// set reader options based on file ext.
	if len(files) != 0 {
		c.opt.ReaderOptions.norm(files[0])
	}

	// create reader iterator
	remotes := make([]*bfs.Object, 0, len(files))
	for _, file := range files {
		remotes = append(remotes, bfs.NewObjectFromBucket(c.bucket, file))
	}
	iter := NewReaderIter(c.ctx, remotes, &c.opt.ReaderOptions)

	// consume feed
	data, err := c.icfn(iter)
	if err != nil {
		return nil, err
	}

	// update stores
	previous := c.data.Load()
	c.consumerState.storeData(data)
	c.consumerState.updateNumRead(iter.NumRead())
	c.consumerState.updateLastModified(lastMod.Time())
	c.consumerState.updateLastConsumed(start)
	return &ConsumerSync{
		Consumer:     c,
		Updated:      true,
		PreviousData: previous,
	}, nil
}

func (c *incrementalConsumer) loop() {
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
