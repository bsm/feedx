package feedx

import (
	"context"
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

// Job is a regular job.
type Job struct {
	versionCheck VersionCheck
	readerOpt    *ReaderOptions
	writerOpt    *WriterOptions
	beforeHooks  []BeforeHook
	afterHooks   []AfterHook
}

// NewJob inits a new job.
func NewJob() *Job {
	return &Job{}
}

// BeforeSync adds custom before hooks.
func (j *Job) BeforeSync(hooks ...BeforeHook) *Job {
	j.beforeHooks = append(j.beforeHooks, hooks...)
	return j
}

// AfterSync adds custom after hooks.
func (j *Job) AfterSync(hooks ...AfterHook) *Job {
	j.afterHooks = append(j.afterHooks, hooks...)
	return j
}

// WithReaderOptions sets custom reader options for consumers.
func (j *Job) WithReaderOptions(opt *ReaderOptions) *Job {
	j.readerOpt = opt
	return j
}

// WithWriterOptions sets custom writer options for producers.
func (j *Job) WithWriterOptions(opt *WriterOptions) *Job {
	j.writerOpt = opt
	return j
}

// WithVersionCheck sets a custom version check for producers.
func (j *Job) WithVersionCheck(fn VersionCheck) *Job {
	j.versionCheck = fn
	return j
}

// Produce starts a producer job.
func (j *Job) Produce(ctx context.Context, remoteURL string, pfn ProduceFunc) (*Status, error) {
	pcr, err := NewProducer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}
	defer pcr.Close()

	return j.ProduceWith(ctx, pcr, pfn)
}

// Produce starts an incremental producer job.
func (j *Job) ProduceIncrementally(ctx context.Context, remoteURL string, pfn IncrementalProduceFunc) (*Status, error) {
	pcr, err := NewIncrementalProducer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}
	defer pcr.Close()

	return j.ProduceIncrementallyWith(ctx, pcr, pfn)
}

// ProduceWith starts a producer job with an existing producer.
func (j *Job) ProduceWith(ctx context.Context, pcr *Producer, pfn ProduceFunc) (*Status, error) {
	return j.produce(ctx, func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, j.writerOpt, pfn)
	})
}

// ProduceIncrementallyFrom starts an incremental producer job with an existing producer.
func (j *Job) ProduceIncrementallyWith(ctx context.Context, pcr *IncrementalProducer, pfn IncrementalProduceFunc) (*Status, error) {
	return j.produce(ctx, func(ctx context.Context, version int64) (*Status, error) {
		return pcr.Produce(ctx, version, j.writerOpt, pfn)
	})
}

// Consume starts a consumer job.
func (j *Job) Consume(ctx context.Context, remoteURL string, cfn ConsumeFunc) (*Status, error) {
	csm, err := NewConsumer(ctx, remoteURL)
	if err != nil {
		return nil, err
	}

	return j.ConsumeWith(ctx, csm, cfn)
}

// ConsumeWith starts a consumer job with an existing consumer.
func (j *Job) ConsumeWith(ctx context.Context, csm Consumer, cfn ConsumeFunc) (*Status, error) {
	return j.runWithHooks(csm.Version(), func() (*Status, error) {
		return csm.Consume(ctx, j.readerOpt, cfn)
	})
}

// RunEvery schedules the Job to run every interval and returns a CronJob.
func (j *Job) RunEvery(interval time.Duration, perform func(*Job) (*Status, error)) *CronJob {
	return newCronJob(j, interval, perform)
}

func (j *Job) produce(ctx context.Context, fn func(context.Context, int64) (*Status, error)) (*Status, error) {
	var version int64
	if j.versionCheck != nil {
		latest, err := j.versionCheck(ctx)
		if err != nil {
			return nil, err
		}
		version = latest
	}

	return j.runWithHooks(version, func() (*Status, error) {
		return fn(ctx, version)
	})
}

func (j *Job) runWithHooks(localVersion int64, fn func() (*Status, error)) (*Status, error) {
	for _, hook := range j.beforeHooks {
		if !hook(localVersion) {
			return &Status{Skipped: true, LocalVersion: localVersion}, nil
		}
	}

	status, err := fn()
	for _, hook := range j.afterHooks {
		hook(status, err)
	}
	return status, err
}

// ----------------------------------------------------------------------------

// CronJob runs in regular intervals until it's stopped.
type CronJob struct {
	cancel   context.CancelFunc
	job      *Job
	interval time.Duration
	perform  func(*Job) (*Status, error)
	wait     sync.WaitGroup
}

func newCronJob(job *Job, interval time.Duration, perform func(*Job) (*Status, error)) *CronJob {
	ctx, cancel := context.WithCancel(context.Background())
	cron := &CronJob{cancel: cancel, job: job, interval: interval, perform: perform}
	go cron.loop(ctx)
	return cron
}

// Close stops the job and waits until it is complete.
func (j *CronJob) Close() error {
	j.cancel()
	j.wait.Wait()
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
		}

		_, _ = j.perform(j.job)
	}
}
