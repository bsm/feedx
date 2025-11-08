package feedx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// ProduceFunc is a callback which is run by the producer on every iteration.
type ProduceFunc func(*Writer) error

// VersionFunc is a function to retrieve the current local version.
type VersionFunc func(context.Context) (int64, error)

type producerState struct {
	numWritten, lastAttempt, version int64
}

// LastAttempt returns time of last push attempt.
func (p *producerState) LastAttempt() time.Time {
	return epochToTime(atomic.LoadInt64(&p.lastAttempt))
}

// Version returns the most recent feed version.
func (p *producerState) Version() int64 {
	return atomic.LoadInt64(&p.version)
}

// NumWritten returns the number of values produced during the last push.
func (p *producerState) NumWritten() int {
	return int(atomic.LoadInt64(&p.numWritten))
}

func (p *producerState) updateLastAttempt(t time.Time) {
	atomic.StoreInt64(&p.lastAttempt, timeToEpoch(t))
}

func (p *producerState) updateVersion(v int64) {
	atomic.StoreInt64(&p.version, v)
}

func (p *producerState) updateNumWritten(n int) {
	atomic.StoreInt64(&p.numWritten, int64(n))
}

// ProducerOptions configure the producer instance.
type ProducerOptions struct {
	WriterOptions

	// The interval used by producer to initiate a cycle.
	// Default: 1m
	Interval time.Duration

	// VersionCheck this function will be called before each push attempt
	// to dynamically determine the last modified time.
	VersionCheck VersionFunc

	// AfterPush callbacks are triggered after each push cycle, receiving
	// the push state and error (if occurred).
	AfterPush func(*ProducerPush, error)
}

func (o *ProducerOptions) norm(name string) {
	o.WriterOptions.norm(name)
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
}

// ProducerPush contains the state of the last push.
type ProducerPush struct {
	// Producer exposes the current producer state.
	producerState
	// Updated indicates is the push resulted in an update.
	Updated bool
}

type Producer struct {
	producerState

	remote    *bfs.Object
	ownRemote bool

	opt  ProducerOptions
	ctx  context.Context
	stop context.CancelFunc
	pfn  ProduceFunc
}

// NewProducer inits a new feed producer.
func NewProducer(ctx context.Context, remoteURL string, opt *ProducerOptions, pfn ProduceFunc) (*Producer, error) {
	remote, err := bfs.NewObject(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	p, err := NewProducerForRemote(ctx, remote, opt, pfn)
	if err != nil {
		_ = remote.Close()
		return nil, err
	}
	p.ownRemote = true
	return p, nil
}

// NewProducerForRemote starts a new feed producer with a remote.
func NewProducerForRemote(ctx context.Context, remote *bfs.Object, opt *ProducerOptions, pfn ProduceFunc) (*Producer, error) {
	var o ProducerOptions
	if opt != nil {
		o = *opt
	}
	o.norm(remote.Name())

	ctx, stop := context.WithCancel(ctx)
	p := &Producer{
		remote: remote,
		opt:    o,
		pfn:    pfn,
		ctx:    ctx,
		stop:   stop,
	}

	// run initial push
	if _, err := p.push(); err != nil {
		_ = p.Close()
		return nil, err
	}

	// start continuous loop
	go p.loop()

	return p, nil
}

// Close stops the producer.
func (p *Producer) Close() error {
	p.stop()
	if p.ownRemote {
		return p.remote.Close()
	}
	return nil
}

func (p *Producer) push() (*ProducerPush, error) {
	start := time.Now()
	p.updateLastAttempt(start)

	// setup write options
	wopt := p.opt.WriterOptions
	if p.opt.VersionCheck != nil {
		version, err := p.opt.VersionCheck(p.ctx)
		if err != nil {
			return nil, err
		}
		wopt.Version = version
	}

	// retrieve remote version, skip if not modified
	if ver, err := fetchRemoteVersion(p.ctx, p.remote); err != nil {
		return nil, err
	} else if ver != 0 && ver == wopt.Version {
		return &ProducerPush{producerState: p.producerState}, nil
	}

	writer := NewWriter(p.ctx, p.remote, &wopt)
	defer writer.Discard()

	if err := p.pfn(writer); err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	p.updateNumWritten(writer.NumWritten())
	p.updateVersion(wopt.Version)

	return &ProducerPush{
		producerState: p.producerState,
		Updated:       true,
	}, nil
}

func (p *Producer) loop() {
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
