package feedx

import (
	"time"
)

func (p *IncrementalProducer) LoadManifest() (*Manifest, error) {
	return p.loadManifest()
}

func TimestampFromTime(t time.Time) timestamp {
	return timestampFromTime(t)
}
