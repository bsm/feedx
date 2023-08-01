package feedx

import (
	"errors"
	"time"

	"github.com/bsm/bfs"
)

func (p *IncrementalProducer) LoadManifest() (*Manifest, error) {
	remote := bfs.NewObjectFromBucket(p.bucket, "manifest.json")
	defer remote.Close()

	m := new(Manifest)

	r, err := NewReader(p.ctx, remote, nil)
	if errors.Is(err, bfs.ErrNotFound) {
		return m, nil
	} else if err != nil {
		return nil, err
	}
	defer r.Close()

	if err := r.Decode(m); errors.Is(err, bfs.ErrNotFound) { // some BFS implementations defer Open-ing the S3 object till first Decode call
		return m, nil
	} else if err != nil {
		return nil, err
	}

	return m, nil
}

func TimestampFromTime(t time.Time) timestamp {
	if n := t.Unix()*1000 + int64(t.Nanosecond()/1e6); n > 0 {
		return timestamp(n)
	}
	return 0
}
