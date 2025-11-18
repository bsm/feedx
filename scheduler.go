package feedx

import (
	"context"
	"io"
	"sync"
	"time"
)

// BeforeHook callbacks are run before jobs are started. It receives the local
// version before sync as an argument and may return false to abort the cycle.
type BeforeHook func(version int64) bool

// AfterHook callbacks are run after jobs have finished.
type AfterHook func(*Status, error)

// VersionCheck callbacks return the latest local version.
type VersionCheck func(context.Context) (int64, error)

// Scheduler runs cronjobs in regular intervals.
type Scheduler struct {
	ctx      context.Context
	interval time.Duration

	readerOpt    *ReaderOptions
	writerOpt    *WriterOptions
	versionCheck VersionCheck

	// hooks
	beforeHooks []BeforeHook
	afterHooks  []AfterHook
}

// Every creates a scheduler.
func Every(interval time.Duration) *Scheduler {
	return &Scheduler{ctx: context.Background(), interval: interval}
}

// WithContext sets a custom context for the run.
func (s *Scheduler) WithContext(ctx context.Context) *Scheduler {
	s.ctx = ctx
	return s
}

// BeforeSync adds custom before hooks.
func (s *Scheduler) BeforeSync(hooks ...BeforeHook) *Scheduler {
	s.beforeHooks = append(s.beforeHooks, hooks...)
	return s
}

// AfterSync adds before hooks.
func (s *Scheduler) AfterSync(hooks ...AfterHook) *Scheduler {
	s.afterHooks = append(s.afterHooks, hooks...)
	return s
}

// WithReaderOptions sets custom reader options for consumers.
func (s *Scheduler) WithReaderOptions(opt *ReaderOptions) *Scheduler {
	s.readerOpt = opt
	return s
}

// Consume starts a consumer job.
func (s *Scheduler) Consume(ctx context.Context, remoteURL string, cfn ConsumeFunc) (*CronJob, error) {
	csm, err := NewConsumer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	return s.withClosable(csm, func() (*CronJob, error) {
		return s.ConsumeWith(csm, cfn)
	})
}

// ConsumeWith starts a consumer job with an existing consumer.
func (s *Scheduler) ConsumeWith(csm Consumer, cfn ConsumeFunc) (*CronJob, error) {
	return newCronJob(s.ctx, s.interval, func(ctx context.Context) error {
		version := csm.Version()
		if !s.runBeforeHooks(version) {
			return nil
		}

		status, err := csm.Consume(ctx, s.readerOpt, cfn)
		s.runAfterHooks(status, err)
		return err
	})
}

// WithWriterOptions sets custom writer options for producers.
func (s *Scheduler) WithWriterOptions(opt *WriterOptions) *Scheduler {
	s.writerOpt = opt
	return s
}

// WithVersionCheck sets a custom version check for producers.
func (s *Scheduler) WithVersionCheck(fn VersionCheck) *Scheduler {
	s.versionCheck = fn
	return s
}

// Produce starts a producer job.
func (s *Scheduler) Produce(ctx context.Context, remoteURL string, pfn ProduceFunc) (*CronJob, error) {
	pcr, err := NewProducer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	return s.withClosable(pcr, func() (*CronJob, error) {
		return s.ProduceWith(pcr, pfn)
	})
}

// Produce starts an incremental producer job.
func (s *Scheduler) ProduceIncrementally(ctx context.Context, remoteURL string, pfn IncrementalProduceFunc) (*CronJob, error) {
	pcr, err := NewIncrementalProducer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	return s.withClosable(pcr, func() (*CronJob, error) {
		return s.ProduceIncrementallyWith(pcr, pfn)
	})
}

// ProduceWith starts a producer job with an existing producer.
func (s *Scheduler) ProduceWith(pcr *Producer, pfn ProduceFunc) (*CronJob, error) {
	return s.produce(func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, s.writerOpt, pfn)
	})
}

// ProduceIncrementallyFrom starts an incremental producer job with an existing producer.
func (s *Scheduler) ProduceIncrementallyWith(pcr *IncrementalProducer, pfn IncrementalProduceFunc) (*CronJob, error) {
	return s.produce(func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, s.writerOpt, pfn)
	})
}

func (s *Scheduler) produce(fn func(context.Context, int64) (*Status, error)) (*CronJob, error) {
	return newCronJob(s.ctx, s.interval, func(ctx context.Context) error {
		var version int64
		if s.versionCheck != nil {
			latest, err := s.versionCheck(s.ctx)
			if err != nil {
				s.runAfterHooks(nil, err)
				return err
			}
			version = latest
		}

		if !s.runBeforeHooks(version) {
			return nil
		}

		status, err := fn(ctx, version)
		s.runAfterHooks(status, err)
		return err
	})
}

func (s *Scheduler) runBeforeHooks(version int64) bool {
	for _, hook := range s.beforeHooks {
		if !hook(version) {
			return false
		}
	}
	return true
}

func (s *Scheduler) runAfterHooks(status *Status, err error) {
	for _, hook := range s.afterHooks {
		hook(status, err)
	}
}

func (s *Scheduler) withClosable(closable io.Closer, init func() (*CronJob, error)) (*CronJob, error) {
	job, err := init()
	if err != nil {
		_ = closable.Close()
		return nil, err
	}

	job.closable = closable
	return job, err
}

// CronJob runs in regular intervals until it's stopped.
type CronJob struct {
	closable io.Closer
	cancel   context.CancelFunc
	interval time.Duration
	perform  func(context.Context) error
	wait     sync.WaitGroup
}

func newCronJob(ctx context.Context, interval time.Duration, perform func(context.Context) error) (*CronJob, error) {
	if err := perform(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	job := &CronJob{cancel: cancel, interval: interval, perform: perform}
	go job.loop(ctx)
	return job, nil
}

// Close stops the job and waits until it is complete.
func (j *CronJob) Close() error {
	j.cancel()
	j.wait.Wait()

	if j.closable != nil {
		err := j.closable.Close()
		j.closable = nil
		return err
	}
	return nil
}

func (j *CronJob) loop(ctx context.Context) {
	j.wait.Add(1)
	defer j.wait.Done()

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = j.perform(ctx)
		}
	}
}
