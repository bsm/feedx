package feedx

import (
	"context"
	"sync"
	"time"
)

// BeforeHook callbacks are run before jobs are started.
// Returning false will abort the cycle.
type BeforeHook func() bool

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
func (s *Scheduler) Consume(csm Consumer, cfn ConsumeFunc) *CronJob {
	return newCronJob(s.ctx, s.interval, func(ctx context.Context) {
		for _, hook := range s.beforeHooks {
			if !hook() {
				return
			}
		}

		status, err := csm.Consume(ctx, s.readerOpt, cfn)
		for _, hook := range s.afterHooks {
			hook(status, err)
		}
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
func (s *Scheduler) Produce(pcr *Producer, pfn ProduceFunc) *CronJob {
	return s.produce(func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, s.writerOpt, pfn)
	})
}

// ProduceIncrementally starts an incremental producer job.
func (s *Scheduler) ProduceIncrementally(pcr *IncrementalProducer, pfn IncrementalProduceFunc) *CronJob {
	return s.produce(func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, s.writerOpt, pfn)
	})
}

func (s *Scheduler) produce(fn func(context.Context, int64) (*Status, error)) *CronJob {
	return newCronJob(s.ctx, s.interval, func(ctx context.Context) {
		if !s.runBeforeHooks() {
			return
		}

		var version int64
		if s.versionCheck != nil {
			latest, err := s.versionCheck(s.ctx)
			if err != nil {
				s.runAfterHooks(nil, err)
				return
			}
			version = latest
		}

		status, err := fn(ctx, version)
		s.runAfterHooks(status, err)
	})
}

func (s *Scheduler) runBeforeHooks() bool {
	for _, hook := range s.beforeHooks {
		if !hook() {
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

// CronJob runs in regular intervals until it's stopped.
type CronJob struct {
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
	perform  func(context.Context)
	wait     sync.WaitGroup
}

func newCronJob(ctx context.Context, interval time.Duration, perform func(context.Context)) *CronJob {
	ctx, cancel := context.WithCancel(ctx)

	job := &CronJob{ctx: ctx, cancel: cancel, interval: interval, perform: perform}
	job.perform(ctx) // perform immediately

	job.wait.Add(1)
	go job.loop()
	return job
}

// Stop stops the job and waits until it is complete.
func (j *CronJob) Stop() {
	j.cancel()
	j.wait.Wait()
}

func (j *CronJob) loop() {
	defer j.wait.Done()

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-j.ctx.Done():
			return
		case <-ticker.C:
			j.perform(j.ctx)
		}
	}
}
