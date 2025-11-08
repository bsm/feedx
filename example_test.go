package feedx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
)

func ExampleConsumer() {
	ctx := context.TODO()

	// create an mock object
	obj := bfs.NewInMemObject("todos.ndjson")
	defer obj.Close()

	// seed some data
	w, err := obj.Create(ctx, nil)
	if err != nil {
		panic(err)
	}
	defer w.Discard()

	if _, err := w.Write([]byte(`` +
		`{"id":1,"title":"foo","completed":false}` + "\n" +
		`{"id":2,"title":"bar","completed":false}` + "\n",
	)); err != nil {
		panic(err)
	}
	if err := w.Commit(); err != nil {
		panic(err)
	}

	// create a consumer
	csm := feedx.NewConsumerForRemote(obj)
	defer csm.Close()

	// consume data
	var todos []map[string]any
	status, err := csm.Consume(context.TODO(), nil, func(ctx context.Context, r *feedx.Reader) error {
		for {
			var todo map[string]any
			if err := r.Decode(&todo); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			todos = append(todos, todo)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("STATUS skipped:%v version:%v read:%v\n", status.Skipped, status.RemoteVersion, status.NumItems)
	fmt.Printf("DATA   %v\n", todos)

	// Output:
	// STATUS skipped:false version:0 read:2
	// DATA   [map[completed:false id:1 title:foo] map[completed:false id:2 title:bar]]
}

func ExampleScheduler() {
	ctx := context.TODO()

	// create an mock object
	obj := bfs.NewInMemObject("todos.ndjson")
	defer obj.Close()

	// create a consumer
	csm := feedx.NewConsumerForRemote(obj)
	defer csm.Close()

	job := feedx.Every(time.Hour).
		WithContext(ctx).
		BeforeSync(func() bool {
			fmt.Println("[H] Before sync")
			return true
		}).
		AfterSync(func(_ *feedx.Status, err error) {
			fmt.Printf("[H] After sync - error:%v", err)
		}).
		Consume(csm, func(_ context.Context, _ *feedx.Reader) error {
			fmt.Println("[*] Consuming feed")
			return nil
		})
	job.Stop()

	// Output:
	// [H] Before sync
	// [*] Consuming feed
	// [H] After sync - error:<nil>
}
