package feedx

import "context"

// Job is a regular job.
type Job struct {
	versionCheck VersionCheck
	writerOpt    *WriterOptions
	beforeHooks  []BeforeHook
}

// NewJob creates a new job.
func NewJob(check VersionCheck) *Job {
	return &Job{}
}

// BeforeSync adds custom before hooks.
func (j *Job) BeforeSync(hooks ...BeforeHook) *Job {
	j.beforeHooks = append(j.beforeHooks, hooks...)
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

func (j *Job) produce(ctx context.Context, fn func(context.Context, int64) (*Status, error)) (*Status, error) {
	var version int64
	if j.versionCheck != nil {
		latest, err := j.versionCheck(ctx)
		if err != nil {
			return nil, err
		}
		version = latest
	}

	if !j.runBeforeHooks(version) {
		return &Status{Skipped: true, LocalVersion: version}, nil
	}

	return fn(ctx, version)
}

func (j *Job) runBeforeHooks(version int64) bool {
	for _, hook := range j.beforeHooks {
		if !hook(version) {
			return false
		}
	}
	return true
}
