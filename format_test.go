package feedx_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/bsm/feedx"
	"github.com/bsm/feedx/internal/testdata"
)

func TestDetectFormat(t *testing.T) {
	examples := []struct {
		Input string
		Exp   feedx.Format
	}{
		{Input: "/path/to/file.json", Exp: feedx.JSONFormat},
		{Input: "/path/to/file.json.gz", Exp: feedx.JSONFormat},
		{Input: "/path/to/file.json.flate", Exp: feedx.JSONFormat},
		{Input: "/path/to/file.jsonz", Exp: feedx.JSONFormat},

		{Input: "/path/to/file.pb", Exp: feedx.ProtobufFormat},
		{Input: "/path/to/file.pb.gz", Exp: feedx.ProtobufFormat},
		{Input: "/path/to/file.pb.flate", Exp: feedx.ProtobufFormat},
		{Input: "/path/to/file.pbz", Exp: feedx.ProtobufFormat},

		{Input: "/path/to/file.cbor", Exp: feedx.CBORFormat},
		{Input: "/path/to/file.cbor.gz", Exp: feedx.CBORFormat},
		{Input: "/path/to/file.cbor.flate", Exp: feedx.CBORFormat},
		{Input: "/path/to/file.cborz", Exp: feedx.CBORFormat},

		{Input: "", Exp: (*feedx.NoFormat)(nil)},
		{Input: "/path/to/file", Exp: (*feedx.NoFormat)(nil)},
		{Input: "/path/to/file.txt", Exp: (*feedx.NoFormat)(nil)},
	}
	for _, x := range examples {
		if got := feedx.DetectFormat(x.Input); got != x.Exp {
			t.Errorf("expected %s for %q, but got %s", x.Exp, x.Input, got)
		}
	}
}

func TestFormat(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		testFormat(t, feedx.JSONFormat)
	})
	t.Run("protobuf", func(t *testing.T) {
		testFormat(t, feedx.ProtobufFormat)
	})
	t.Run("cbor", func(t *testing.T) {
		testFormat(t, feedx.CBORFormat)
	})
}

func testFormat(t *testing.T, f feedx.Format) {
	t.Helper()

	buf := new(bytes.Buffer)
	enc, err := f.NewEncoder(buf)
	if err != nil {
		t.Fatal("expected no error, got", err)
	}
	defer func() { _ = enc.Close() }()

	if err := enc.Encode(seed()); err != nil {
		t.Fatal("expected no error, got", err)
	}
	if err := enc.Encode(seed()); err != nil {
		t.Fatal("expected no error, got", err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal("expected no error, got", err)
	}

	dec, err := f.NewDecoder(buf)
	if err != nil {
		t.Fatal("expected no error, got", err)
	}
	defer func() { _ = dec.Close() }()

	v1 := new(testdata.MockMessage)
	if err := dec.Decode(v1); err != nil {
		t.Fatal("expected no error, got", err)
	} else if exp, got := "Joe", v1.Name; exp != got {
		t.Errorf("expected %q, got %q", exp, got)
	}

	v2 := new(testdata.MockMessage)
	if err := dec.Decode(v2); err != nil {
		t.Fatal("expected no error, got", err)
	} else if exp, got := "Joe", v2.Name; exp != got {
		t.Errorf("expected %q, got %q", exp, got)
	}

	v3 := new(testdata.MockMessage)
	if err := dec.Decode(v3); !errors.Is(err, io.EOF) {
		t.Error("expected EOF, got", err)
	}

	if err := dec.Close(); err != nil {
		t.Fatal("expected no error, got", err)
	}
}
