package fingerprint

import (
	"bytes"
	"io"
	"testing"
)

func TestSameContentMatchesAcrossSampleSizes(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 1024*1024)
	a, err := ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ReaderAt(bytes.NewReader(data), int64(len(data)), 256*1024)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Compatible(b) {
		t.Fatalf("same content not compatible: %#v %#v", a, b)
	}
}

func TestDifferentContentDoesNotMatch(t *testing.T) {
	aData := bytes.Repeat([]byte("a"), 1024*1024)
	bData := append([]byte(nil), aData...)
	bData[len(bData)/2] = 'b'
	a, _ := ReaderAt(bytes.NewReader(aData), int64(len(aData)), 64*1024)
	b, _ := ReaderAt(bytes.NewReader(bData), int64(len(bData)), 64*1024)
	if a.Compatible(b) {
		t.Fatal("different content matched")
	}
}

type countingReaderAt struct {
	data  []byte
	bytes int64
}

func (r *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.bytes += int64(len(p))
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestPrefixCompatibleUsesBoundedContentAnchors(t *testing.T) {
	data := bytes.Repeat([]byte("abcdefgh"), 2*1024*1024)
	local := bytes.NewReader(data)
	remote := &countingReaderAt{data: data}

	ok, err := PrefixCompatible(local, remote, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("identical prefix did not match")
	}
	if remote.bytes > 1<<20 {
		t.Fatalf("remote verification read %d bytes, want at most 1 MiB", remote.bytes)
	}

	changed := append([]byte(nil), data...)
	anchor := len(changed) / 2
	for i := 0; i < 64*1024; i++ {
		changed[anchor+i] ^= 0xff
	}
	ok, err = PrefixCompatible(bytes.NewReader(data), bytes.NewReader(changed), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("changed prefix matched")
	}
}
