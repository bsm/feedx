package feedx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// IncrmentalProduceFunc returns a ProduceFunc closure around an incremental mod time
type IncrementalProduceFunc func(time.Time) ProduceFunc

// IncrementalProducer produces a continuous incremental feed.
type incrementalProducer struct {
	*producerState

	bucket    bfs.Bucket
	manifest  *bfs.Object
	ownBucket bool
	opt       IncrementalProducerOptions

	ctx  context.Context
	stop context.CancelFunc
	ipfn IncrementalProduceFunc
}

// IncrementalProducerOptions configure the producer instance.
type IncrementalProducerOptions ProducerOptions

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
func NewIncrementalProducer(ctx context.Context, bucketURL string, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (Producer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	p, err := NewIncrementalProducerForBucket(ctx, bucket, opt, lmfn, ipfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	p.(*incrementalProducer).ownBucket = true

	return p, nil
}

// NewIncrmentalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalProducerOptions, lmfn LastModFunc, ipfn IncrementalProduceFunc) (Producer, error) {
	var o IncrementalProducerOptions
	if opt != nil {
		o = *opt
	}
	o.norm(lmfn)

	ctx, stop := context.WithCancel(ctx)
	p := &incrementalProducer{
		bucket:        bucket,
		manifest:      bfs.NewObjectFromBucket(bucket, "manifest.json"),
		ctx:           ctx,
		stop:          stop,
		opt:           o,
		ipfn:          ipfn,
		producerState: new(producerState),
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
func (p *incrementalProducer) Close() error {
	p.stop()
	if p.ownBucket {
		return p.bucket.Close()
	}
	return p.manifest.Close()
}

func (p *incrementalProducer) loop() {
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

func (p *incrementalProducer) push() (*ProducerPush, error) {
	start := time.Now()
	atomic.StoreInt64(&p.lastPush, timestampFromTime(start).Millis())

	// get last mod time for local records
	localLastMod, err := p.opt.LastModCheck(p.ctx)
	if err != nil {
		return nil, err
	}
	if localLastMod.IsZero() {
		return &ProducerPush{ProducerState: p}, nil
	}

	// fetch manifest from remote
	manifest, err := LoadManifest(p.ctx, p.manifest)
	if err != nil {
		return nil, err
	}

	// compare manifest LastModified to local last mod.
	remoteLastMod := manifest.LastModified
	if remoteLastMod == timestampFromTime(localLastMod) {
		return &ProducerPush{ProducerState: p}, nil
	}

	wopt := p.opt.WriterOptions
	wopt.LastMod = localLastMod

	// write data modified since last remote mod
	numWritten, err := manifest.WriteDataFile(p.ctx, p.bucket, &wopt, p.ipfn(remoteLastMod.Time()))
	if err != nil {
		return nil, err
	}
	// write new manifest to remote
	if err := manifest.Commit(p.ctx, p.manifest, &WriterOptions{LastMod: wopt.LastMod}); err != nil {
		return nil, err
	}

	atomic.StoreInt64(&p.numWritten, int64(numWritten))
	atomic.StoreInt64(&p.lastMod, timestampFromTime(wopt.LastMod).Millis())
	return &ProducerPush{ProducerState: p, Updated: true}, nil
}
