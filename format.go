package feedx

import (
	"encoding/json"
	"fmt"
	"io"
	"path"

	"github.com/golang/protobuf/proto"

	pbio "github.com/gogo/protobuf/io"
)

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
	return nil
}

// FormatDecoder methods
type FormatDecoder interface {
	// Decode decodes the next message into an interface.
	Decode(v interface{}) error

	io.Closer
}

// FormatPureEncoder methods
type FormatPureEncoder interface {
	// Encode encodes the value to the stream.
	Encode(v interface{}) error
}

// FormatEncoder methods
type FormatEncoder interface {
	FormatPureEncoder
	io.Closer
}

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

const protobufMaxMessageSize = 20 * 1024 * 1024 // 20MB

// ProtobufFormat provides a Format implemention for Protobuf.
var ProtobufFormat = protobufFormat{}

type protobufFormat struct{}

// NewDecoder implements Format.
func (protobufFormat) NewDecoder(r io.Reader) (FormatDecoder, error) {
	rc := pbio.NewDelimitedReader(r, protobufMaxMessageSize)
	return protobufDecoderWrapper{ReadCloser: rc}, nil
}

// NewEncoder implements Format.
func (protobufFormat) NewEncoder(w io.Writer) (FormatEncoder, error) {
	wc := pbio.NewDelimitedWriter(w)
	return protobufEncoderWrapper{WriteCloser: wc}, nil
}

type protobufDecoderWrapper struct{ pbio.ReadCloser }

func (w protobufDecoderWrapper) Decode(v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("feedx: value %v is not a proto.Message", v)
	}
	return w.ReadCloser.ReadMsg(msg)
}

type protobufEncoderWrapper struct{ pbio.WriteCloser }

func (w protobufEncoderWrapper) Encode(v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("feedx: value %v is not a proto.Message", v)
	}
	return w.WriteMsg(msg)
}
