package feedx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/bsm/pbio"
	"google.golang.org/protobuf/proto"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	gio "github.com/gogo/protobuf/io"
	gproto "github.com/gogo/protobuf/proto"
)

var errNoFormat = errors.New("feedx: no format detected")

// Format represents the data format.
type Format interface {
	// NewDecoder wraps a decoder around a reader.
	NewDecoder(io.Reader) (FormatDecoder, error)
	// NewEncoder wraps an encoder around a writer.
	NewEncoder(io.Writer) (FormatEncoder, error)
}

// DetectFormat detects the data format from a URL path or file name.
// May return nil.
func DetectFormat(name string) Format {
	ext := path.Ext(path.Base(name))
	switch ext {
	case ".json":
		return JSONFormat
	case ".pb", ".proto", ".protobuf":
		return ProtobufFormat
	default:
		if name != "" && ext != "" && ext[0] == '.' {
			if ext[len(ext)-1] == 'z' {
				return DetectFormat(name[0 : len(name)-1])
			}
			return DetectFormat(name[0 : len(name)-len(ext)])
		}
	}
	return (*noFormat)(nil)
}

// FormatDecoder methods
type FormatDecoder interface {
	// Decode decodes the next message into an interface.
	Decode(v interface{}) error

	io.Closer
}

// FormatEncoder methods
type FormatEncoder interface {
	// Encode encodes the value to the stream.
	Encode(v interface{}) error

	io.Closer
}

// --------------------------------------------------------------------

type noFormat struct{}

func (*noFormat) NewDecoder(r io.Reader) (FormatDecoder, error) { return nil, errNoFormat }
func (*noFormat) NewEncoder(w io.Writer) (FormatEncoder, error) { return nil, errNoFormat }

// --------------------------------------------------------------------

// JSONFormat provides a Format implemention for JSON.
var JSONFormat = jsonFormat{}

type jsonFormat struct{}

// NewDecoder implements Format.
func (jsonFormat) NewDecoder(r io.Reader) (FormatDecoder, error) {
	return jsonDecoderWrapper{Decoder: json.NewDecoder(r)}, nil
}

// NewEncoder implements Format.
func (jsonFormat) NewEncoder(w io.Writer) (FormatEncoder, error) {
	return jsonEncoderWrapper{Encoder: json.NewEncoder(w)}, nil
}

type jsonDecoderWrapper struct{ *json.Decoder }

func (jsonDecoderWrapper) Close() error { return nil }

type jsonEncoderWrapper struct{ *json.Encoder }

func (jsonEncoderWrapper) Close() error { return nil }

// --------------------------------------------------------------------

// ProtobufFormat provides a Format implemention for Protobuf.
var ProtobufFormat = protobufFormat{}

type protobufFormat struct{}

// NewDecoder implements Format.
func (protobufFormat) NewDecoder(r io.Reader) (FormatDecoder, error) {
	return &protobufWrapper{r: r}, nil
}

// NewEncoder implements Format.
func (protobufFormat) NewEncoder(w io.Writer) (FormatEncoder, error) {
	return &protobufWrapper{w: w}, nil
}

type protobufWrapper struct {
	r   io.Reader
	dec *pbio.Decoder
	pbr gio.Reader

	w   io.Writer
	enc *pbio.Encoder
	pbw gio.Writer
}

func (w *protobufWrapper) Decode(v interface{}) error {
	switch msg := v.(type) {
	case proto.Message:
		if w.dec == nil {
			w.dec = pbio.NewDecoder(w.r)
		}
		return w.dec.Decode(msg)

	case gproto.Message:
		if w.pbr == nil {
			w.pbr = gio.NewDelimitedReader(w.r, 1<<28)
		}
		return w.pbr.ReadMsg(msg)

	default:
		return fmt.Errorf("value %v (%T) is not a proto.Message", v, v)
	}
}

func (w *protobufWrapper) Encode(v interface{}) error {
	switch msg := v.(type) {
	case proto.Message:
		if w.enc == nil {
			w.enc = pbio.NewEncoder(w.w)
		}
		return w.enc.Encode(msg)

	case gproto.Message:
		if w.pbw == nil {
			w.pbw = gio.NewDelimitedWriter(w.w)
		}
		return w.pbw.WriteMsg(msg)

	default:
		return fmt.Errorf("value %v (%T) is not a proto.Message", v, v)
	}
}

func (*protobufWrapper) Close() error {
	return nil
}

// --------------------------------------------------------------------

// ParquetFormat provides a Format implemention for Protobuf.
var ParquetFormat = parquetFormat{}

type parquetFormat struct{}

// NewDecoder implements Format.
func (parquetFormat) NewDecoder(r io.Reader) (FormatDecoder, error) {
	rs, ok := r.(io.ReadSeeker)
	if !ok {
		return nil, fmt.Errorf("value %v is not an io.ReaderSeeker", r)
	}

	pfr, err := goparquet.NewFileReader(rs)
	if err != nil {
		return nil, err
	}

	return &parquetWrapper{
			pfr: pfr,
			ffr: floor.NewReader(pfr)},
		nil
}

func (parquetFormat) NewEncoder(w io.Writer) (FormatEncoder, error) {
	return nil, fmt.Errorf("not implemented")
}

type parquetWrapper struct {
	pfr *goparquet.FileReader
	ffr *floor.Reader
}

func (w *parquetWrapper) Decode(v interface{}) error {
	if w.ffr.Next() {
		return w.ffr.Scan(v)
	}

	if err := w.ffr.Err(); err != nil {
		return err
	}

	return io.EOF
}

func (*parquetWrapper) Encode(v interface{}) error {
	return nil
}

func (w *parquetWrapper) Close() error {
	return w.ffr.Close()
}
