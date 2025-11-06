package feedx

import (
	"context"
	"time"

	"github.com/bsm/bfs"
)

func (c *consumer) TestSync() error {
	_, err := c.sync(false)
	return err
}

func TimestampFromTime(t time.Time) timestamp {
	return timestampFromTime(t)
}

type NoFormat = noFormat

type Manifest manifest

func LoadManifest(ctx context.Context, obj *bfs.Object) (*Manifest, error) {
	m, err := loadManifest(ctx, obj)
	return (*Manifest)(m), err
}
