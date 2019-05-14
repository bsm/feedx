package feedx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// ProduceFunc is a callback which is run by the producer on every iteration.
type ProduceFunc func(FormatPureEncoder) error

// ProducerOptions configure the producer instance.
type ProducerOptions struct {
	WriterOptions

	// The interval used by producer to initiate a cycle.
	// Default: 1m
	Interval time.Duration

	// AfterPush callbacks are triggered after each push cycle, receiving
	// an error (if occurred).
	AfterPush func(error)
}

func (o *ProducerOptions) norm(name string) error {
	o.WriterOptions.norm(name)
	if o.Interval <= 0 {
		o.Interval = time.Minute
	}
	return nil
}

// Producer (continously) produces a feed.
type Producer struct {
	remote    *bfs.Object
	ownRemote bool

	opt  ProducerOptions
	ctx  context.Context
	stop context.CancelFunc
	pfn  ProduceFunc

	numWritten, lastPush, lastMod int64
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
	if err := o.norm(remote.Name()); err != nil {
		return nil, err
	}

	ctx, stop := context.WithCancel(ctx)
	p := &Producer{
		remote: remote,
		opt:    o,
		pfn:    pfn,
		ctx:    ctx,
		stop:   stop,
	}

	// run initial push
	if err := p.push(); err != nil {
		_ = p.Close()
		return nil, err
	}

	// start continuous loop
	go p.loop()

	return p, nil
}

// LastPush returns time of last push attempt.
func (p *Producer) LastPush() time.Time {
	return timestamp(atomic.LoadInt64(&p.lastPush)).Time()
}

// LastModified returns time at which the remote feed was last modified.
func (p *Producer) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&p.lastMod)).Time()
}

// NumWritten returns the number of values produced during the last push.
func (p *Producer) NumWritten() int {
	return int(atomic.LoadInt64(&p.numWritten))
}

// Close stops the producer.
func (p *Producer) Close() error {
	p.stop()
	if p.ownRemote {
		return p.remote.Close()
	}
	return nil
}

func (p *Producer) push() error {
	var now int64
	defer func() {
		if now == 0 {
			now = timestampFromTime(time.Now()).Millis()
		}
		atomic.StoreInt64(&p.lastPush, now)
	}()

	writer, err := NewWriter(p.ctx, p.remote, &p.opt.WriterOptions)
	if err != nil {
		return err
	}
	defer writer.Discard()

	if err := p.pfn(writer); err != nil {
		return err
	}

	if writer.NumWritten() == 0 {
		return nil
	}

	if err := writer.Commit(); err != nil {
		return err
	}

	now = timestampFromTime(time.Now()).Millis()
	atomic.StoreInt64(&p.numWritten, int64(writer.NumWritten()))
	atomic.StoreInt64(&p.lastMod, now)
	return nil
}

func (p *Producer) loop() {
	ticker := time.NewTicker(p.opt.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			if err := p.push(); p.opt.AfterPush != nil {
				p.opt.AfterPush(err)
			}
		}
	}
}
