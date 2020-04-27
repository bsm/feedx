package feedx_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/feedx"
	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var memStore *bfs.InMem

func init() {
	memStore = bfs.NewInMem()
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		return memStore, nil
	})
}

// ------------------------------------------------------------------------

type Mock_Enum int32

const (
	Mock_UNKNOWN Mock_Enum = 0
	Mock_FIRST   Mock_Enum = 3
)

type MockMessage struct {
	Name   string    `protobuf:"bytes,1,opt,name=name,proto3"`
	Enum   Mock_Enum `protobuf:"varint,2,opt,name=enum,proto3"`
	Height uint32    `protobuf:"varint,3,opt,name=height"`
}

func (m *MockMessage) Reset()         { *m = MockMessage{} }
func (m *MockMessage) String() string { return proto.CompactTextString(m) }
func (*MockMessage) ProtoMessage()    {}

var fixture = MockMessage{
	Name:   "Joe",
	Enum:   Mock_FIRST,
	Height: 180,
}

// ------------------------------------------------------------------------

func writeMulti(obj *bfs.Object, numEntries int) error {
	w := feedx.NewWriter(context.Background(), obj, &feedx.WriterOptions{
		LastMod: time.Unix(1515151515, 123456789),
	})
	defer w.Discard()

	for i := 0; i < numEntries; i++ {
		fix := fixture
		if err := w.Encode(&fix); err != nil {
			return err
		}
	}
	return w.Commit()
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "feedx")
}
