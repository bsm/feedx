package feedx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/bsm/pbio"
	"google.golang.org/protobuf/proto"

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
	return protobufWrapper{dec: pbio.NewDecoder(r)}, nil
}

// NewEncoder implements Format.
func (protobufFormat) NewEncoder(w io.Writer) (FormatEncoder, error) {
	return protobufWrapper{enc: pbio.NewEncoder(w)}, nil
}

// protobufAdapter this is an adapter to switch between
// protobufWrapper (official go protobuf impl) and
// gogoProtobufWrapper (backcompat, deprecated - github.com/gogo/protobuf).
//
// Wrapper is instantiated on the first Encode or Decode call.
// Either .writer or .reader must be set.
type protobufAdapter struct {
	writer io.Writer
	reader io.Reader

	adapter interface {
		FormatDecoder
		FormatEncoder
	}
}

func (a *protobufAdapter) Decode(v interface{}) error {
	a.ensureAdapterFor(v)
	return a.adapter.Decode(v)
}

func (a *protobufAdapter) Encode(v interface{}) error {
	a.ensureAdapterFor(v)
	return a.adapter.Encode(v)
}

func (a *protobufAdapter) Close() error {
	if a.adapter != nil {
		return a.adapter.Close()
	}
	return nil
}

func (a *protobufAdapter) ensureAdapterFor(v interface{}) {
	if a.adapter != nil {
		return
	}

	if _, ok := v.(gproto.Message); ok {
		if a.writer != nil {
			a.adapter = gogoProtobufWrapper{
				Writer: gio.NewDelimitedWriter(a.writer),
			}
			return
		}
		a.adapter = gogoProtobufWrapper{
			Reader: gio.NewDelimitedReader(a.reader, 1<<28),
		}
		return
	}

	if a.writer != nil {
		a.adapter = protobufWrapper{
			enc: pbio.NewEncoder(a.writer),
		}
		return
	}
	a.adapter = protobufWrapper{
		dec: pbio.NewDecoder(a.reader),
	}
}

type protobufWrapper struct {
	dec *pbio.Decoder
	enc *pbio.Encoder
}

func (w protobufWrapper) Decode(v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("value %v (%T) is not a proto.Message", v, v)
	}
	return w.dec.Decode(msg)
}

func (w protobufWrapper) Encode(v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("value %v (%T) is not a proto.Message", v, v)
	}
	return w.enc.Encode(msg)
}

func (protobufWrapper) Close() error {
	return nil
}

// DEPRECATED: prefer official go protobuf impl
type gogoProtobufWrapper struct {
	gio.Reader
	gio.Writer
}

func (w gogoProtobufWrapper) Decode(v interface{}) error {
	msg, ok := v.(gproto.Message)
	if !ok {
		return fmt.Errorf("value %v (%T) is not a gogo/proto.Message", v, v)
	}
	return w.Reader.ReadMsg(msg)
}

func (w gogoProtobufWrapper) Encode(v interface{}) error {
	msg, ok := v.(gproto.Message)
	if !ok {
		return fmt.Errorf("value %v (%T) is not a gogo/proto.Message", v, v)
	}
	return w.WriteMsg(msg)
}

func (gogoProtobufWrapper) Close() error {
	return nil
}
