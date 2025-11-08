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

const metaVersion = "X-Feedx-Version"

func fetchRemoteVersion(ctx context.Context, obj *bfs.Object) (int64, error) {
	info, err := obj.Head(ctx)
	if err == bfs.ErrNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	version, _ := strconv.ParseInt(info.Metadata.Get(metaVersion), 10, 64)
	return version, nil
}

func epochToTime(epoch int64) time.Time {
	return time.Unix(epoch/1000, epoch%1000*1e6)
}

func timeToEpoch(t time.Time) int64 {
	if n := t.Unix()*1000 + int64(t.Nanosecond()/1e6); n > 0 {
		return n
	}
	return 0
}
