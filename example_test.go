package feedx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
)

type message = testdata.MockMessage

func Example() {
	ctx := context.TODO()

	// create an mock object
	obj := bfs.NewInMemObject("todos.ndjson")
	defer obj.Close()

	pcr := feedx.NewProducerForRemote(obj)
	defer pcr.Close()

	// produce
	status, err := pcr.Produce(ctx, 101, nil, func(w *feedx.Writer) error {
		return errors.Join(
			w.Encode(&message{Name: "Jane", Height: 175}),
			w.Encode(&message{Name: "Joe", Height: 172}),
		)
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("PRODUCED skipped:%v version:%v->%v items:%v\n", status.Skipped, status.LocalVersion, status.RemoteVersion, status.NumItems)

	// create a consumer
	csm := feedx.NewConsumerForRemote(obj)
	defer csm.Close()

	// consume data
	var msgs []*message
	status, err = csm.Consume(context.TODO(), nil, func(r *feedx.Reader) error {
		for {
			var msg message
			if err := r.Decode(&msg); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			msgs = append(msgs, &msg)
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("CONSUMED skipped:%v version:%v->%v items:%v\n", status.Skipped, status.LocalVersion, status.RemoteVersion, status.NumItems)
	fmt.Printf("DATA     [%q, %q]\n", msgs[0].Name, msgs[1].Name)

	// Output:
	// PRODUCED skipped:false version:101->0 items:2
	// CONSUMED skipped:false version:0->101 items:2
	// DATA     ["Jane", "Joe"]
}

func ExampleScheduler_Consume() {
	ctx := context.TODO()

	// create an mock object
	obj := bfs.NewInMemObject("todos.ndjson")
	defer obj.Close()

	// create a consumer
	csm := feedx.NewConsumerForRemote(obj)
	defer csm.Close()

	job, err := feedx.Every(time.Hour).
		WithContext(ctx).
		BeforeSync(func(_ int64) bool {
			fmt.Println("1. Before sync")
			return true
		}).
		AfterSync(func(_ *feedx.Status, err error) {
			fmt.Printf("3. After sync - error:%v", err)
		}).
		Consume(csm, func(_ *feedx.Reader) error {
			fmt.Println("2. Consuming feed")
			return nil
		})
	if err != nil {
		panic(err)
	}

	job.Stop()

	// Output:
	// 1. Before sync
	// 2. Consuming feed
	// 3. After sync - error:<nil>
}

func ExampleScheduler_Produce() {
	ctx := context.TODO()

	// create an mock object
	obj := bfs.NewInMemObject("todos.ndjson")
	defer obj.Close()

	// create a producer
	pcr := feedx.NewProducerForRemote(obj)
	defer pcr.Close()

	job, err := feedx.Every(time.Hour).
		WithContext(ctx).
		BeforeSync(func(_ int64) bool {
			fmt.Println("2. Before sync")
			return true
		}).
		AfterSync(func(_ *feedx.Status, err error) {
			fmt.Printf("4. After sync - error:%v", err)
		}).
		WithVersionCheck(func(_ context.Context) (int64, error) {
			fmt.Println("1. Retrieve latest version")
			return 101, nil
		}).
		Produce(pcr, func(w *feedx.Writer) error {
			fmt.Println("3. Producing feed")
			return nil
		})
	if err != nil {
		panic(err)
	}

	job.Stop()

	// Output:
	// 1. Retrieve latest version
	// 2. Before sync
	// 3. Producing feed
	// 4. After sync - error:<nil>
}
