package feedx

import (
	"context"
	"errors"

	"github.com/bsm/bfs"
)

// IncrmentalProduceFunc returns a ProduceFunc closure around an incremental version.
type IncrementalProduceFunc func(remoteVersion int64) ProduceFunc

// IncrementalProducer pushes incremental feeds to a remote bucket location.
type IncrementalProducer struct {
	bucket    bfs.Bucket
	object    *bfs.Object
	ownBucket bool
}

// NewIncrementalProducer inits a new incremental feed producer.
func NewIncrementalProducer(ctx context.Context, bucketURL string) (*IncrementalProducer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	pcr := NewIncrementalProducerForBucket(bucket)
	pcr.ownBucket = true
	return pcr, nil
}

// NewIncrementalProducerForRemote starts a new incremental feed producer for a bucket.
func NewIncrementalProducerForBucket(bucket bfs.Bucket) *IncrementalProducer {
	return &IncrementalProducer{
		bucket: bucket,
		object: bfs.NewObjectFromBucket(bucket, "manifest.json"),
	}
}

// Close stops the producer.
func (p *IncrementalProducer) Close() (err error) {
	if e := p.object.Close(); e != nil {
		err = errors.Join(err, e)
	}

	if p.ownBucket && p.bucket != nil {
		if e := p.bucket.Close(); e != nil {
			err = errors.Join(err, e)
		}
		p.bucket = nil
	}
	return
}

func (p *IncrementalProducer) Produce(ctx context.Context, version int64, opt *WriterOptions, pfn IncrementalProduceFunc) (*Status, error) {
	status := Status{LocalVersion: version}

	// fetch manifest from remote object
	mft, err := loadManifest(ctx, p.object)
	if err != nil {
		return nil, err
	}

	// skip if not modified
	remoteVersion := mft.Version
	status.RemoteVersion = remoteVersion
	if skipSync(version, remoteVersion) {
		status.Skipped = true
		return &status, nil
	}

	// set version for writer
	if opt == nil {
		opt = new(WriterOptions)
	}
	opt.Version = version

	// write data modified since last version
	numWritten, err := p.writeDataFile(ctx, mft, version, remoteVersion, opt, pfn)
	if err != nil {
		return nil, err
	}
	// write new manifest to remote
	if err := p.commitManifest(ctx, mft, &WriterOptions{Version: version}); err != nil {
		return nil, err
	}

	status.NumItems = numWritten
	return &status, nil
}

func (p *IncrementalProducer) writeDataFile(ctx context.Context, mft *manifest, version, remoteVersion int64, opt *WriterOptions, pfn IncrementalProduceFunc) (int64, error) {
	fname := mft.newDataFileName(opt)

	obj := bfs.NewObjectFromBucket(p.bucket, fname)
	defer obj.Close()

	writer := NewWriter(ctx, obj, opt)
	defer writer.Discard()

	if err := pfn(remoteVersion)(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}

	mft.Files = append(mft.Files, fname)
	mft.Version = version

	return writer.NumWritten(), nil
}

func (p *IncrementalProducer) commitManifest(ctx context.Context, mft *manifest, opt *WriterOptions) error {
	writer := NewWriter(ctx, p.object, opt)
	defer writer.Discard()

	if err := writer.Encode(mft); err != nil {
		return err
	}
	return writer.Commit()
}
