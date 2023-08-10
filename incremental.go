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
type IncrementalProducer struct {
	*ProducerState

	bucket    bfs.Bucket
	manifest  *bfs.Object
	ownBucket bool
	opt       IncrementalProducerOptions

	ctx  context.Context
	stop context.CancelFunc
	ipfn IncrementalProduceFunc
}

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
		bucket:        bucket,
		manifest:      bfs.NewObjectFromBucket(bucket, "manifest.json"),
		ctx:           ctx,
		stop:          stop,
		opt:           o,
		ipfn:          ipfn,
		ProducerState: new(ProducerState),
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
func (p *IncrementalProducer) Close() error {
	p.stop()
	if p.ownBucket {
		return p.bucket.Close()
	}
	return p.manifest.Close()
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
	atomic.StoreInt64(&p.lastPush, timestampFromTime(start).Millis())

	// get last mod time for local records
	localLastMod, err := p.opt.LastModCheck(p.ctx)
	if err != nil {
		return nil, err
	}
	if localLastMod.IsZero() {
		return &ProducerPush{ProducerState: p.ProducerState}, nil
	}

	// fetch manifest from remote
	manifest, err := loadManifest(p.ctx, p.manifest)
	if err != nil {
		return nil, err
	}

	// compare manifest LastModified to local last mod.
	remoteLastMod := manifest.LastModified
	if remoteLastMod == timestampFromTime(localLastMod) {
		return &ProducerPush{ProducerState: p.ProducerState}, nil
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

	atomic.StoreInt64(&p.numWritten, int64(numWritten))
	atomic.StoreInt64(&p.lastMod, timestampFromTime(wopt.LastMod).Millis())
	return &ProducerPush{ProducerState: p.ProducerState, Updated: true}, nil
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
