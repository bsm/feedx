package feedx

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/bsm/bfs"
)

// manifest holds the current feed status.
// the current manifest is consumed before each push and a new manifest written after each push.
type manifest struct {
	// LastModified holds a last-modified time of the records included in Files.
	LastModified timestamp `json:"lastModified"`
	// Generation is a incrementing counter for use in file compaction.
	Generation int `json:"generation"`
	// Files holds a set of data files
	Files []string `json:"files"`
}

func loadManifest(ctx context.Context, obj *bfs.Object) (*manifest, error) {
	m := new(manifest)

	r, err := NewReader(ctx, obj, nil)
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

func (m *manifest) newDataFileName(wopt *WriterOptions) string {
	ts := strings.ReplaceAll(wopt.LastMod.Format("20060102-150405.000"), ".", "")

	formatExt := ".pb"
	if wopt.Format == JSONFormat {
		formatExt = ".json"
	}

	var compressionSuffix string
	if wopt.Compression == GZipCompression {
		compressionSuffix = "z"
	} else if wopt.Compression == FlateCompression {
		compressionSuffix = ".flate"
	}

	return "data-" + strconv.Itoa(m.Generation) + "-" + ts + formatExt + compressionSuffix
}
