package feedx

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// Manifest describes the current feed status.
// the current manifest is consumed before each push and a new manifest written after each push.
type Manifest struct {
	// LastModified holds a last-modified time of the records included in Files.
	LastModified timestamp `json:"lastModified"`
	// Generation is a incrementing counter for use in file compaction.
	Generation int `json:"generation"`
	// Files holds a set of incremental data files
	Files []string `json:"files"`
}

// IncrementalProducer produces a continuous incremental feed.
type IncrementalProducer struct {
	*ProducerState

	bucket    bfs.Bucket
	ownBucket bool
	opt       IncrementalProducerOptions

	ctx  context.Context
	stop context.CancelFunc
	pfn  ProduceFunc
}

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
func NewIncrementalProducer(ctx context.Context, bucketURL string, opt *IncrementalProducerOptions, lmfn LastModFunc, pfn ProduceFunc) (*IncrementalProducer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	p, err := NewIncrementalProducerForBucket(ctx, bucket, opt, lmfn, pfn)
	if err != nil {
		_ = bucket.Close()
		return nil, err
	}
	p.ownBucket = true

	return p, nil
}

// NewIncrmentalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(ctx context.Context, bucket bfs.Bucket, opt *IncrementalProducerOptions, lmfn LastModFunc, pfn ProduceFunc) (*IncrementalProducer, error) {
	var o IncrementalProducerOptions
	if opt != nil {
		o = *opt
	}
	o.norm(lmfn)

	ctx, stop := context.WithCancel(ctx)
	p := &IncrementalProducer{
		bucket:        bucket,
		ctx:           ctx,
		stop:          stop,
		opt:           o,
		pfn:           pfn,
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
	return nil
}

func (p *IncrementalProducer) loadManifest() (*Manifest, error) {
	remote := bfs.NewObjectFromBucket(p.bucket, "manifest.json")
	defer remote.Close()

	m := new(Manifest)

	r, err := NewReader(p.ctx, remote, nil)
	if errors.Is(err, bfs.ErrNotFound) {
		return m, nil
	} else if err != nil {
		return nil, err
	}
	defer r.Close()

	if err := r.Decode(m); errors.Is(err, bfs.ErrNotFound) { // some BFS implementations defer Open-ing the S3 object till first Decode call
		return m, nil
	} else if err != nil {
		return nil, err
	}

	return m, nil
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
	manifest, err := p.loadManifest()
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

	// write modified data
	// TODO! set the file extension from WriterOpts format & compression
	fname := "data-" + strconv.Itoa(manifest.Generation) + "-" + localLastMod.Format("2006-01-02-15:04:05.0000") + ".pbz"
	numWritten, err := p.writeData(manifest, fname, remoteLastMod.Time(), &wopt)
	if err != nil {
		return nil, err
	}

	// write new manifest
	if err := p.writeManifest(manifest, fname, localLastMod, &WriterOptions{LastMod: wopt.LastMod}); err != nil {
		return nil, err
	}

	atomic.StoreInt64(&p.numWritten, int64(numWritten))
	atomic.StoreInt64(&p.lastMod, timestampFromTime(wopt.LastMod).Millis())
	return &ProducerPush{ProducerState: p.ProducerState, Updated: true}, nil
}

func (p *IncrementalProducer) writeData(manifest *Manifest, fname string, remoteLastMod time.Time, wopt *WriterOptions) (int, error) {
	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, fname), wopt)
	defer writer.Discard()

	// write data file, it is up to caller to ensure these are incremental changes
	if err := p.pfn(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}
	return writer.NumWritten(), nil
}

func (p *IncrementalProducer) writeManifest(manifest *Manifest, fname string, lastMod time.Time, wopt *WriterOptions) error {
	manifest.Files = append(manifest.Files, fname)
	manifest.LastModified = timestampFromTime(lastMod)

	name := "manifest.json"
	wopt.norm(name) // norm sets writer format and compression from name

	writer := NewWriter(p.ctx, bfs.NewObjectFromBucket(p.bucket, name), wopt)
	defer writer.Discard()

	if err := writer.Encode(manifest); err != nil {
		return err
	}
	return writer.Commit()
}
