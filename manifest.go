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

// WriteDataFile pushes a new data file to bucket
func (m *manifest) WriteDataFile(ctx context.Context, bucket bfs.Bucket, wopt *WriterOptions, pfn ProduceFunc) (int, error) {
	fname := m.generateFileName(wopt)

	writer := NewWriter(ctx, bfs.NewObjectFromBucket(bucket, fname), wopt)
	defer writer.Discard()

	if err := pfn(writer); err != nil {
		return 0, err
	}
	if err := writer.Commit(); err != nil {
		return 0, err
	}

	m.Files = append(m.Files, fname)
	m.LastModified = timestampFromTime(wopt.LastMod)

	return writer.NumWritten(), nil
}

// Commit writes manifest to remote object
func (m *manifest) Commit(ctx context.Context, obj *bfs.Object, wopt *WriterOptions) error {
	name := obj.Name()
	wopt.norm(name) // norm sets writer format and compression from name

	writer := NewWriter(ctx, obj, wopt)
	defer writer.Discard()

	if err := writer.Encode(m); err != nil {
		return err
	}
	return writer.Commit()
}

func (m *manifest) generateFileName(wopt *WriterOptions) string {
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
