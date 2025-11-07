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

// Schedule runs jobs in regular intervals.
type Schedule struct {
	ctx           context.Context
	interval      time.Duration
	readerOptions *ReaderOptions

	// hooks
	beforeConsume []BeforeConsumeHook
	afterConsume  []AfterConsumeHook
}

// Every creates a periodic schedule.
func Every(interval time.Duration) *Schedule {
	return &Schedule{ctx: context.Background(), interval: interval}
}

// WithContext sets a custom context for the run.
func (s *Schedule) WithContext(ctx context.Context) *Schedule {
	s.ctx = ctx
	return s
}

// WithReaderOptions sets custom reader options for consumers.
func (s *Schedule) WithReaderOptions(opt *ReaderOptions) *Schedule {
	s.readerOptions = opt
	return s
}

// BeforeConsume adds custom hooks.
func (s *Schedule) BeforeConsume(hooks ...BeforeConsumeHook) *Schedule {
	s.beforeConsume = append(s.beforeConsume, hooks...)
	return s
}

// AfterConsume adds custom hooks.
func (s *Schedule) AfterConsume(hooks ...AfterConsumeHook) *Schedule {
	s.afterConsume = append(s.afterConsume, hooks...)
	return s
}

// Consume starts  consumer job.
func (s *Schedule) Consume(csm Consumer, cfn ConsumeFunc) *Job {
	return newJob(s.ctx, s.interval, func(ctx context.Context) {
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

// Job runs in regular intervals and can be stopped.
type Job struct {
	ctx      context.Context
	cancel   context.CancelFunc
	interval time.Duration
	perform  func(context.Context)
}

func newJob(ctx context.Context, interval time.Duration, perform func(context.Context)) *Job {
	ctx, cancel := context.WithCancel(ctx)

	job := &Job{ctx: ctx, cancel: cancel, interval: interval, perform: perform}
	job.perform(ctx) // perform immediately

	go job.loop()
	return job
}

// Stop stops the job.
func (j *Job) Stop() {
	j.cancel()
}

func (j *Job) loop() {
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
