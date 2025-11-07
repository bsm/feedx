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
	// Version holds the most recent version of the records included in Files.
	Version int64 `json:"version"`
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
	defer func() { _ = r.Close() }()

	if err := r.Decode(m); errors.Is(err, bfs.ErrNotFound) { // some BFS implementations defer Open-ing the S3 object till first Decode call
		return m, nil
	} else if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *manifest) newDataFileName(wopt *WriterOptions) string {
	version := strings.ReplaceAll(strconv.FormatInt(wopt.Version, 10), ".", "")

	formatExt := ".json"
	switch wopt.Format {
	case ProtobufFormat:
		formatExt = ".pb"
	case CBORFormat:
		formatExt = ".cbor"
	}

	var compressionSuffix string
	switch wopt.Compression {
	case GZipCompression:
		compressionSuffix = "z"
	case FlateCompression:
		compressionSuffix = ".flate"
	case ZstdCompression:
		compressionSuffix = ".zst"
	}

	return "data-" + strconv.Itoa(m.Generation) + "-" + version + formatExt + compressionSuffix
}
