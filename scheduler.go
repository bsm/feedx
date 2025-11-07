package feedx

import (
	"context"
	"time"
)

// BeforeConsumeHook functions are run before consume jobs are finished.
// Returning false will abort the consumer cycle.
type BeforeConsumeHook func() bool

// AfterConsumeHook functions are run after consume jobs are finished.
type AfterConsumeHook func(*ConsumeStatus, error)

// Scheduler runs cronjobs in regular intervals.
type Scheduler struct {
	ctx           context.Context
	interval      time.Duration
	readerOptions *ReaderOptions

	// hooks
	beforeConsume []BeforeConsumeHook
	afterConsume  []AfterConsumeHook
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

// WithReaderOptions sets custom reader options for consumers.
func (s *Scheduler) WithReaderOptions(opt *ReaderOptions) *Scheduler {
	s.readerOptions = opt
	return s
}

// BeforeConsume adds custom hooks.
func (s *Scheduler) BeforeConsume(hooks ...BeforeConsumeHook) *Scheduler {
	s.beforeConsume = append(s.beforeConsume, hooks...)
	return s
}

// AfterConsume adds custom hooks.
func (s *Scheduler) AfterConsume(hooks ...AfterConsumeHook) *Scheduler {
	s.afterConsume = append(s.afterConsume, hooks...)
	return s
}

// Consume starts  consumer job.
func (s *Scheduler) Consume(csm Consumer, cfn ConsumeFunc) *CronJob {
	return newCronJob(s.ctx, s.interval, func(ctx context.Context) {
		for _, hook := range s.beforeConsume {
			if !hook() {
				return
			}
		}

		status, err := csm.Consume(ctx, s.readerOptions, cfn)
		for _, hook := range s.afterConsume {
			hook(status, err)
		}
	})
}

// CronJob runs in regular intervals until it's stopped.
type CronJob struct {
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
	perform  func(context.Context)
}

func newCronJob(ctx context.Context, interval time.Duration, perform func(context.Context)) *CronJob {
	ctx, cancel := context.WithCancel(ctx)

	job := &CronJob{ctx: ctx, cancel: cancel, interval: interval, perform: perform}
	job.perform(ctx) // perform immediately

	go job.loop()
	return job
}

// Stop stops the job.
func (j *CronJob) Stop() {
	j.cancel()
}

func (j *CronJob) loop() {
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
