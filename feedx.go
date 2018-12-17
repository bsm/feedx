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

// Timestamp with millisecond resolution
type timestamp int64

func timestampFromTime(t time.Time) timestamp {
	if n := t.Unix()*1000 + int64(t.Nanosecond()/1e6); n > 0 {
		return timestamp(n)
	}
	return 0
}

func remoteLastModified(ctx context.Context, obj *bfs.Object) (timestamp, error) {
	info, err := obj.Head(ctx)
	if err == bfs.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	millis, _ := strconv.ParseInt(info.Metadata[metaLastModified], 10, 64)
	if millis == 0 {
		millis, _ = strconv.ParseInt(info.Metadata[metaPusherLastModified], 10, 64)
	}
	return timestamp(millis), nil
}

// Millis returns the number of milliseconds since epoch.
func (t timestamp) Millis() int64 { return int64(t) }

// Time returns the time at t.
func (t timestamp) Time() time.Time {
	n := t.Millis()
	return time.Unix(n/1000, n%1000*1e6)
}

// String returns a string of milliseconds.
func (t timestamp) String() string {
	return strconv.FormatInt(int64(t), 10)
}
