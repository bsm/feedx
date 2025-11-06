package feedx_test

import (
	"context"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
)

func seed() *testdata.MockMessage {
	return &testdata.MockMessage{
		Name:   "Joe",
		Enum:   testdata.MockEnum_FIRST,
		Height: 180,
	}
}

func seedN(n int) []*testdata.MockMessage {
	res := make([]*testdata.MockMessage, 0, n)
	for i := 0; i < n; i++ {
		res = append(res, seed())
	}
	return res
}

func writeN(obj *bfs.Object, numEntries int, lastMod time.Time) error {
	w := feedx.NewWriter(context.Background(), obj, &feedx.WriterOptions{LastMod: lastMod})
	defer w.Discard()

	for i := 0; i < numEntries; i++ {
		if err := w.Encode(seed()); err != nil {
			return err
		}
	}
	return w.Commit()
}

func readMessages(r interface{ Decode(any) error }) ([]*testdata.MockMessage, error) {
	var msgs []*testdata.MockMessage
	for {
		var msg testdata.MockMessage
		err := r.Decode(&msg)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, nil
}
