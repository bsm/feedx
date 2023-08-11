package feedx

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsm/bfs"
)

// ProduceFunc is a callback which is run by the producer on every iteration.
type ProduceFunc func(*Writer) error

// LastModFunc is a function to return local data last modification time.
type LastModFunc func(context.Context) (time.Time, error)

// ProducerState holds current state of producer
type ProducerState struct {
	numWritten, lastPush, lastMod int64
}

// LastPush returns time of last push attempt.
func (p *ProducerState) LastPush() time.Time {
	return timestamp(atomic.LoadInt64(&p.lastPush)).Time()
}

// LastModified returns time at which the remote feed was last modified.
func (p *ProducerState) LastModified() time.Time {
	return timestamp(atomic.LoadInt64(&p.lastMod)).Time()
}

// NumWritten returns the number of values produced during the last push.
func (p *ProducerState) NumWritten() int {
	return int(atomic.LoadInt64(&p.numWritten))
}

func (p *ProducerState) updateLastPush(t time.Time) {
	atomic.StoreInt64(&p.lastPush, timestampFromTime(t).Millis())
}

func (p *ProducerState) updateLastModified(t time.Time) {
	atomic.StoreInt64(&p.lastMod, timestampFromTime(t).Millis())
}

func (p *ProducerState) updateNumWritten(n int) {
	atomic.StoreInt64(&p.numWritten, int64(n))
}

// ProducerOptions configure the producer instance.
type ProducerOptions struct {
	WriterOptions

	// The interval used by producer to initiate a cycle.
	// Default: 1m
	Interval time.Duration

	// LastModCheck this function will be called before each push attempt
	// to dynamically determine the last modified time.
	LastModCheck LastModFunc

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
	*ProducerState
	// Updated indicates is the push resulted in an update.
	Updated bool
}

type Producer struct {
	*ProducerState

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
		remote:        remote,
		opt:           o,
		pfn:           pfn,
		ctx:           ctx,
		stop:          stop,
		ProducerState: new(ProducerState),
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
	p.ProducerState.updateLastPush(start)

	// setup write options
	wopt := p.opt.WriterOptions
	wopt.LastMod = start
	if p.opt.LastModCheck != nil {
		modTime, err := p.opt.LastModCheck(p.ctx)
		if err != nil {
			return nil, err
		}
		wopt.LastMod = modTime
	}

	// retrieve original last modified time, skip if not modified
	if rts, err := remoteLastModified(p.ctx, p.remote); err != nil {
		return nil, err
	} else if rts == timestampFromTime(wopt.LastMod) {
		return &ProducerPush{ProducerState: p.ProducerState}, nil
	}

	writer := NewWriter(p.ctx, p.remote, &wopt)
	defer writer.Discard()

	if err := p.pfn(writer); err != nil {
		return nil, err
	}

	if err := writer.Commit(); err != nil {
		return nil, err
	}

	p.ProducerState.updateNumWritten(writer.NumWritten())
	p.ProducerState.updateLastModified(wopt.LastMod)

	return &ProducerPush{
		ProducerState: p.ProducerState,
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
