package feedx

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/bsm/bfs"
)

// ErrNotModified is used to signal that something has not been modified.
var ErrNotModified = errors.New("feedx: not modified")

const (
	metaLastModified       = "x-feedx-last-modified"
	metaPusherLastModified = "x-feedx-pusher-last-modified"
)

type millisSinceEpoch int64

func lastModifiedFromTime(t time.Time) millisSinceEpoch {
	if n := t.Unix()*1000 + int64(t.Nanosecond()/1e6); n > 0 {
		return millisSinceEpoch(n)
	}
	return 0
}

func lastModifiedFromObj(ctx context.Context, obj *bfs.Object) (millisSinceEpoch, error) {
	info, err := obj.Head(ctx)
	if err == bfs.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	return lastModifiedFromMeta(info.Metadata), nil
}

func lastModifiedFromMeta(meta map[string]string) millisSinceEpoch {
	ms, _ := strconv.ParseInt(meta[metaLastModified], 10, 64)
	if ms == 0 {
		ms, _ = strconv.ParseInt(meta[metaPusherLastModified], 10, 64)
	}
	return millisSinceEpoch(ms)
}

func (ms millisSinceEpoch) Time() time.Time {
	n := int64(ms)
	return time.Unix(n/1000, n%1000*1e6)
}

func (ms millisSinceEpoch) String() string {
	return strconv.FormatInt(int64(ms), 10)
}
