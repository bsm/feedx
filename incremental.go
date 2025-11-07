package feedx

import (
	"context"
	"time"

	"github.com/bsm/bfs"
)

// IncrmentalProduceFunc returns a ProduceFunc closure around an incremental version.
type IncrementalProduceFunc func(int64) ProduceFunc

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

func (o *IncrementalProducerOptions) norm(vfn VersionFunc) {
	if o.Compression == nil {
		o.Compression = GZipCompression
	}
	if o.Format == nil {
		o.Format = ProtobufFormat
	}
	if o.Interval == 0 {
		o.Interval = time.Minute
	}
	o.VersionCheck = vfn
}

// NewIncrementalProducer inits a new incremental feed producer.
func NewIncrementalProducer(ctx context.Context, bucketURL string, opt *IncrementalProducerOptions, vfn VersionFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	p, err := NewIncrementalProducerForBucket(ctx, bucket, opt, vfn, ipfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	p.ownBucket = true

	return p, nil
}

// NewIncrementalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalProducerOptions, vfn VersionFunc, ipfn IncrementalProduceFunc) (*IncrementalProducer, error) {
	var o IncrementalProducerOptions
	if opt != nil {
		o = IncrementalProducerOptions(*opt)
	}
	o.norm(vfn)

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
	p.updateLastAttempt(start)

	// get last mod time for local records
	localVersion, err := p.opt.VersionCheck(p.ctx)
	if err != nil {
		return nil, err
	} else if localVersion == 0 {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	// fetch manifest from remote
	manifest, err := loadManifest(p.ctx, p.manifest)
	if err != nil {
		return nil, err
	}

	// compare manifest LastModified to local last mod.
	remoteVersion := manifest.Version
	if remoteVersion == localVersion {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	wopt := p.opt.WriterOptions
	wopt.Version = localVersion

	// write data modified since last remote mod
	numWritten, err := p.writeDataFile(manifest, &wopt)
	if err != nil {
		return nil, err
	}
	// write new manifest to remote
	if err := p.commitManifest(manifest, &WriterOptions{Version: wopt.Version}); err != nil {
		return nil, err
	}

	p.updateNumWritten(numWritten)
	p.updateVersion(wopt.Version)
	return &ProducerPush{producerState: p.producerState, Updated: true}, nil
}

func (p *IncrementalProducer) writeDataFile(m *manifest, wopt *WriterOptions) (int, error) {
	fname := m.newDataFileName(wopt)

	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, fname), wopt)
	defer writer.Discard()

	if err := p.ipfn(wopt.Version)(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}

	m.Files = append(m.Files, fname)
	m.Version = wopt.Version

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
