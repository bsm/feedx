package feedx

import (
	"context"
	"errors"
	"strconv"

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

// Status is returned by sync processes.
type Status struct {
	// Skipped indicates the the sync was skipped, because there were no new changes.
	Skipped bool
	// LocalVersion indicates the local version before sync.
	LocalVersion int64
	// RemoteVersion indicates the remote version before sync.
	RemoteVersion int64
	// NumItems returns the number of items processed, either read of written.
	NumItems int64
}

func skipSync(srcVersion, targetVersion int64) bool {
	return (srcVersion != 0 || targetVersion != 0) && srcVersion <= targetVersion
}
