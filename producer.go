package feedx

import (
	"context"

	"github.com/bsm/bfs"
)

// ProduceFunc is a callback which is run by the producer on every iteration.
type ProduceFunc func(*Writer) error

// Producer instances push data feeds to remote locations.
type Producer struct {
	remote    *bfs.Object
	ownRemote bool
}

// NewProducer inits a new feed producer.
func NewProducer(ctx context.Context, remoteURL string) (*Producer, error) {
	remote, err := bfs.NewObject(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	pcr := NewProducerForRemote(remote)
	pcr.ownRemote = true
	return pcr, nil
}

// NewProducerForRemote starts a new feed producer with a remote.
func NewProducerForRemote(remote *bfs.Object) *Producer {
	return &Producer{remote: remote}
}

// Close stops the producer.
func (p *Producer) Close() error {
	if p.ownRemote && p.remote != nil {
		err := p.remote.Close()
		p.remote = nil
		return err
	}
	return nil
}

func (p *Producer) Produce(ctx context.Context, version int64, opt *WriterOptions, pfn ProduceFunc) (*Status, error) {
	status := Status{LocalVersion: version}

	// retrieve previous remote version
	remoteVersion, err := fetchRemoteVersion(ctx, p.remote)
	if err != nil {
		return nil, err
	}
	status.RemoteVersion = remoteVersion

	// skip if not modified
	if skipSync(version, remoteVersion) {
		status.Skipped = true
		return &status, nil
	}

	// set version for writer
	if opt == nil {
		opt = new(WriterOptions)
	}
	opt.Version = version

	// init writer and perform
	writer := NewWriter(ctx, p.remote, opt)
	defer writer.Discard()

	if err := pfn(writer); err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	status.NumItems = writer.NumWritten()
	return &status, nil
}
