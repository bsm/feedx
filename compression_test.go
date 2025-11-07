package feedx_test

import (
	"bytes"
	"testing"

	"github.com/bsm/feedx"
)

func TestDetectCompression(t *testing.T) {
	examples := []struct {
		Input string
		Exp   feedx.Compression
	}{
		{Input: "/path/to/file.json", Exp: feedx.NoCompression},
		{Input: "/path/to/file.json.gz", Exp: feedx.GZipCompression},
		{Input: "/path/to/file.jsonz", Exp: feedx.GZipCompression},
		{Input: "/path/to/file.pb", Exp: feedx.NoCompression},
		{Input: "/path/to/file.pb.gz", Exp: feedx.GZipCompression},
		{Input: "/path/to/file.pbz", Exp: feedx.GZipCompression},
		{Input: "/path/to/file.flate", Exp: feedx.FlateCompression},
		{Input: "/path/to/file.whatever.flate", Exp: feedx.FlateCompression},
		{Input: "/path/to/file.zst", Exp: feedx.ZstdCompression},
		{Input: "", Exp: feedx.NoCompression},
		{Input: "/path/to/file", Exp: feedx.NoCompression},
		{Input: "/path/to/file.txt", Exp: feedx.NoCompression},
	}
	for _, x := range examples {
		if got := feedx.DetectCompression(x.Input); got != x.Exp {
			t.Errorf("expected %s for %q, but got %s", x.Exp, x.Input, got)
		}
	}
}

func TestCompression(t *testing.T) {
	data := bytes.Repeat([]byte("wxyz"), 1024)

	t.Run("no compression", func(t *testing.T) {
		testCompression(t, feedx.NoCompression, data)
	})
	t.Run("gzip", func(t *testing.T) {
		testCompression(t, feedx.GZipCompression, data)
	})
	t.Run("flate", func(t *testing.T) {
		testCompression(t, feedx.FlateCompression, data)
	})
	t.Run("zstd", func(t *testing.T) {
		testCompression(t, feedx.ZstdCompression, data)
	})
}

func testCompression(t *testing.T, c feedx.Compression, data []byte) {
	t.Helper()
	buf := new(bytes.Buffer)

	w, err := c.NewWriter(buf)
	if err != nil {
		t.Fatal("expected no error, got", err)
	}
	defer func() { _ = w.Close() }()

	if _, err := w.Write(data); err != nil {
		t.Fatal("expected no error, got", err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal("expected no error, got", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal("expected no error, got", err)
	}

	r, err := c.NewReader(buf)
	if err != nil {
		t.Fatal("expected no error, got", err)
	}
	defer func() { _ = r.Close() }()

	p := make([]byte, 20)
	if n, err := r.Read(p); err != nil {
		t.Fatal("expected no error, got", err)
	} else if n != 20 {
		t.Errorf("expected to read 20 bytes, got %d", n)
	} else if exp, got := "wxyzwxyzwxyzwxyzwxyz", string(p); exp != got {
		t.Errorf("expected %q, got %q", exp, got)
	}

	if err := r.Close(); err != nil {
		t.Fatal("expected no error, got", err)
	}
}
