package gvfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverFindsOnlyMTPMounts(t *testing.T) {
	runtimeDir := t.TempDir()
	gvfsDir := filepath.Join(runtimeDir, "gvfs")
	if err := os.MkdirAll(filepath.Join(gvfsDir, "mtp:host=%5Busb%3A001%2C005%5D"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(gvfsDir, "smb-share:server=nas,share=files"), 0o700); err != nil {
		t.Fatal(err)
	}

	mounts, err := Discover(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 1 {
		t.Fatalf("got %d mounts, want 1", len(mounts))
	}
	if mounts[0].Host != "[usb:001,005]" {
		t.Fatalf("host = %q", mounts[0].Host)
	}
	wantRoot := filepath.Join(gvfsDir, "mtp:host=%5Busb%3A001%2C005%5D")
	if mounts[0].Root != wantRoot {
		t.Fatalf("root = %q, want %q", mounts[0].Root, wantRoot)
	}
}

func TestDiscoverMissingGVfsDirectoryIsEmpty(t *testing.T) {
	mounts, err := Discover(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(mounts) != 0 {
		t.Fatalf("got %d mounts", len(mounts))
	}
}

func TestResolveMTPURIUsesMatchingExistingMount(t *testing.T) {
	runtimeDir := t.TempDir()
	root := filepath.Join(runtimeDir, "gvfs", "mtp:host=%5Busb%3A001%2C005%5D")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	mounts, err := Discover(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := ResolveURI("mtp://[usb:001,005]/Internal%20shared%20storage/DCIM/Camera/a.jpg", mounts)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "Internal shared storage", "DCIM", "Camera", "a.jpg")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestResolveFileURI(t *testing.T) {
	got, err := ResolveURI("file:///home/user/My%20Videos/a.mp4", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "/home/user/My Videos/a.mp4" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveMTPURIRejectsTraversal(t *testing.T) {
	mounts := []Mount{{Host: "phone", Root: t.TempDir()}}
	_, err := ResolveURI("mtp://phone/../../etc/passwd", mounts)
	if !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("got %v, want ErrUnsafePath", err)
	}
}

func TestResolveMTPURIRejectsUnknownDevice(t *testing.T) {
	mounts := []Mount{{Host: "phone-a", Root: t.TempDir()}}
	_, err := ResolveURI("mtp://phone-b/DCIM/a.jpg", mounts)
	if !errors.Is(err, ErrMountNotFound) {
		t.Fatalf("got %v, want ErrMountNotFound", err)
	}
}

func TestOpenSourceSupportsRandomAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "movie.bin")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := OpenSource(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if src.Size != 10 {
		t.Fatalf("size = %d", src.Size)
	}
	buf := make([]byte, 4)
	if _, err := src.ReadAt(buf, 5); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if string(buf) != "5678" {
		t.Fatalf("got %q", buf)
	}
}
