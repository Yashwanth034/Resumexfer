package intent

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"resumexfer/internal/gvfs"
	"resumexfer/internal/state"
)

func TestCorrelateExpandsWholeSelectionFromFirstObservedFile(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	paths := make([]string, 200)
	uris := make([]string, 200)
	for i := range paths {
		paths[i] = filepath.Join(src, fmt.Sprintf("file-%03d.bin", i))
		if err := os.WriteFile(paths[i], []byte{byte(i)}, 0o600); err != nil {
			t.Fatal(err)
		}
		uris[i] = fileURI(paths[i])
	}
	in := state.Intent{ID: "job", Direction: "laptop-to-phone", URIs: uris, CreatedAt: time.Now()}

	manifest, err := Correlate(in, nil, paths[0], filepath.Join(dst, filepath.Base(paths[0])))
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Entries) != 200 {
		t.Fatalf("got %d entries, want 200", len(manifest.Entries))
	}
	if manifest.Direction != in.Direction {
		t.Fatalf("direction = %q", manifest.Direction)
	}
	if manifest.DestinationRoot != dst {
		t.Fatalf("destination root = %q, want %q", manifest.DestinationRoot, dst)
	}
	if manifest.Entries[125].Destination != filepath.Join(dst, filepath.Base(paths[125])) {
		t.Fatalf("destination = %q", manifest.Entries[125].Destination)
	}
}

func TestCorrelateReconstructsNestedFolderDestinationRoot(t *testing.T) {
	parent := t.TempDir()
	album := filepath.Join(parent, "Album")
	first := filepath.Join(album, "nested", "a.jpg")
	second := filepath.Join(album, "b.jpg")
	if err := os.MkdirAll(filepath.Dir(first), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("bb"), 0o600); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	in := state.Intent{ID: "folder-job", Direction: "phone-to-laptop", URIs: []string{fileURI(album)}}

	manifest, err := Correlate(in, nil, first, filepath.Join(dst, "Album", "nested", "a.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	entries := entriesBySource(manifest)
	if entries[first].Destination != filepath.Join(dst, "Album", "nested", "a.jpg") {
		t.Fatalf("first destination = %q", entries[first].Destination)
	}
	if entries[second].Destination != filepath.Join(dst, "Album", "b.jpg") {
		t.Fatalf("second destination = %q", entries[second].Destination)
	}
}

func TestCorrelatePhoneToLaptopResolvesMTPSelection(t *testing.T) {
	runtimeDir := t.TempDir()
	root := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	camera := filepath.Join(root, "Internal shared storage", "DCIM", "Camera")
	if err := os.MkdirAll(camera, 0o700); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(camera, "a.jpg")
	second := filepath.Join(camera, "b.jpg")
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	mounts, err := gvfs.Discover(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	in := state.Intent{
		ID:        "download-job",
		Direction: "phone-to-laptop",
		URIs: []string{
			"mtp://phone/Internal%20shared%20storage/DCIM/Camera/a.jpg",
			"mtp://phone/Internal%20shared%20storage/DCIM/Camera/b.jpg",
		},
	}

	manifest, err := Correlate(in, mounts, first, filepath.Join(dst, "a.jpg"))
	if err != nil {
		t.Fatal(err)
	}
	entries := entriesBySource(manifest)
	if len(entries) != 2 {
		t.Fatalf("got %d entries", len(entries))
	}
	if entries[second].Destination != filepath.Join(dst, "b.jpg") {
		t.Fatalf("second destination = %q", entries[second].Destination)
	}
}

func TestCorrelateLaptopToPhoneUsesObservedGVfsDestination(t *testing.T) {
	src := t.TempDir()
	first := filepath.Join(src, "a.bin")
	second := filepath.Join(src, "b.bin")
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("bb"), 0o600); err != nil {
		t.Fatal(err)
	}
	phoneRoot := filepath.Join(t.TempDir(), "gvfs", "mtp:host=phone", "Internal shared storage", "Download")
	if err := os.MkdirAll(phoneRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	in := state.Intent{ID: "upload-job", Direction: "laptop-to-phone", URIs: []string{fileURI(first), fileURI(second)}}

	manifest, err := Correlate(in, nil, first, filepath.Join(phoneRoot, "a.bin"))
	if err != nil {
		t.Fatal(err)
	}
	entries := entriesBySource(manifest)
	if entries[second].Destination != filepath.Join(phoneRoot, "b.bin") {
		t.Fatalf("second destination = %q", entries[second].Destination)
	}
}

func TestCorrelateRejectsObservedSourceOutsideSelection(t *testing.T) {
	src := t.TempDir()
	selected := filepath.Join(src, "selected.bin")
	other := filepath.Join(src, "other.bin")
	if err := os.WriteFile(selected, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := state.Intent{ID: "job", URIs: []string{fileURI(selected)}}

	_, err := Correlate(in, nil, other, filepath.Join(t.TempDir(), "other.bin"))
	if !errors.Is(err, ErrNoSelectionMatch) {
		t.Fatalf("got %v, want ErrNoSelectionMatch", err)
	}
}

func TestCorrelateRejectsAmbiguousNestedSelection(t *testing.T) {
	parent := t.TempDir()
	folder := filepath.Join(parent, "Folder")
	child := filepath.Join(folder, "child.bin")
	if err := os.MkdirAll(folder, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(child, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := state.Intent{ID: "job", URIs: []string{fileURI(folder), fileURI(child)}}

	_, err := Correlate(in, nil, child, filepath.Join(t.TempDir(), "Folder", "child.bin"))
	if !errors.Is(err, ErrAmbiguousSelection) {
		t.Fatalf("got %v, want ErrAmbiguousSelection", err)
	}
}

func fileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func entriesBySource(manifest state.Manifest) map[string]state.ManifestEntry {
	out := make(map[string]state.ManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		out[entry.Source] = entry
	}
	return out
}
