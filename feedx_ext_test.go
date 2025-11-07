package feedx

import (
	"context"

	"github.com/bsm/bfs"
)

type NoFormat = noFormat

type Manifest manifest

func LoadManifest(ctx context.Context, obj *bfs.Object) (*Manifest, error) {
	m, err := loadManifest(ctx, obj)
	return (*Manifest)(m), err
}
