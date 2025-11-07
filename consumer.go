package feedx

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/bsm/bfs"
)

// ConsumeStatus is returned by Consumer instances.
type ConsumeStatus struct {
	// Skipped indicates the the sync was skipped, because there were no new changes.
	Skipped bool
	// Version indicates the current version of the remote feed.
	Version int64
	// PreviousVersion indicates the last known version to the consumer before the sync.
	PreviousVersion int64
	// NumRead returns the number of items read.
	NumRead int64
}

// ConsumeFunc is a callback invoked by consumers.
type ConsumeFunc func(context.Context, *Reader) error

// Consumer manages data retrieval from a remote feed.
// It queries the feed in regular intervals, continuously retrieving new updates.
type Consumer interface {
	// Consume initiates a sync attempt. It will consume the remote feed only if it has changed since
	// last invocation.
	Consume(context.Context, *ReaderOptions, ConsumeFunc) (*ConsumeStatus, error)

	// Version indicates the most recently consumed version.
	Version() int64

	// Close stops the underlying sync process.
	Close() error
}

// NewConsumer starts a new feed consumer.
func NewConsumer(ctx context.Context, remoteURL string) (Consumer, error) {
	remote, err := bfs.NewObject(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	csm := NewConsumerForRemote(remote)
	csm.(*consumer).ownRemote = true
	return csm, nil
}

// NewConsumerForRemote starts a new feed consumer with a remote.
func NewConsumerForRemote(remote *bfs.Object) Consumer {
	return &consumer{remote: remote}
}

// NewIncrementalConsumer starts a new incremental feed consumer.
func NewIncrementalConsumer(ctx context.Context, bucketURL string) (Consumer, error) {
	bucket, err := bfs.Connect(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	csm := NewIncrementalConsumerForBucket(bucket)
	csm.(*consumer).ownBucket = true
	return csm, nil
}

// NewIncrementalConsumerForBucket starts a new incremental feed consumer with a bucket.
func NewIncrementalConsumerForBucket(bucket bfs.Bucket) Consumer {
	return &consumer{
		remote:    bfs.NewObjectFromBucket(bucket, "manifest.json"),
		ownRemote: true,
		bucket:    bucket,
	}
}

type consumer struct {
	remote    *bfs.Object
	ownRemote bool

	bucket    bfs.Bucket
	ownBucket bool

	version atomic.Int64
}

// Consume implements Consumer interface.
func (c *consumer) Consume(ctx context.Context, opt *ReaderOptions, fn ConsumeFunc) (*ConsumeStatus, error) {
	prevVersion := c.Version()
	status := ConsumeStatus{
		PreviousVersion: prevVersion,
	}

	// retrieve remote mtime
	version, err := fetchRemoteVersion(ctx, c.remote)
	if err != nil {
		return nil, err
	}
	status.Version = version

	// skip sync unless modified
	if prevVersion > 0 && prevVersion == version {
		status.Skipped = true
		return &status, nil
	}

	var reader *Reader
	if c.isIncremental() {
		if reader, err = c.newIncrementalReader(ctx, opt); err != nil {
			return nil, err
		}
	} else {
		if reader, err = NewReader(ctx, c.remote, opt); err != nil {
			return nil, err
		}
	}
	defer reader.Close()

	// consume feed
	if err := fn(ctx, reader); err != nil {
		return nil, err
	}

	status.NumRead = reader.NumRead()
	c.version.Store(version)
	return &status, nil
}

// Version implements Consumer interface.
func (c *consumer) Version() int64 {
	return c.version.Load()
}

// Close implements Consumer interface.
func (c *consumer) Close() (err error) {
	if c.ownRemote && c.remote != nil {
		if e := c.remote.Close(); e != nil {
			err = errors.Join(err, e)
		}
		c.remote = nil
	}
	if c.ownBucket && c.bucket != nil {
		if e := c.bucket.Close(); e != nil {
			err = errors.Join(err, e)
		}
		c.bucket = nil
	}
	return
}

func (c *consumer) isIncremental() bool {
	return c.bucket != nil
}

func (c *consumer) newIncrementalReader(ctx context.Context, opt *ReaderOptions) (*Reader, error) {
	manifest, err := loadManifest(ctx, c.remote)
	if err != nil {
		return nil, err
	}

	files := manifest.Files
	remotes := make([]*bfs.Object, 0, len(files))
	for _, file := range files {
		remotes = append(remotes, bfs.NewObjectFromBucket(c.bucket, file))
	}
	r := MultiReader(ctx, remotes, opt)
	r.ownRemotes = true
	return r, nil
}
