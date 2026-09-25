package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/gvfs"
	"resumexfer/internal/preserve"
	"resumexfer/internal/state"
)

func TestPhoneToLaptopRecoveryContinuesWholeManifest(t *testing.T) {
	const chunk = int64(64 * 1024)
	root := t.TempDir()
	sourceDir := filepath.Join(root, "phone")
	destinationDir := filepath.Join(root, "laptop")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destinationDir, 0o700); err != nil {
		t.Fatal(err)
	}

	contents := [][]byte{
		bytes.Repeat([]byte("a1b2c3d4"), int((5*chunk)/8)),
		bytes.Repeat([]byte("e5f6g7h8"), int((3*chunk)/8)),
		bytes.Repeat([]byte("i9j0k1l2"), int((4*chunk)/8)),
	}
	entries := make([]state.ManifestEntry, len(contents))
	for i, data := range contents {
		source := filepath.Join(sourceDir, string(rune('a'+i))+".bin")
		destination := filepath.Join(destinationDir, filepath.Base(source))
		if err := os.WriteFile(source, data, 0o600); err != nil {
			t.Fatal(err)
		}
		entries[i] = state.ManifestEntry{ID: "entry-" + string(rune('a'+i)), Source: source, Destination: destination, Size: int64(len(data))}
	}

	if err := os.WriteFile(entries[0].Destination, contents[0][:2*chunk], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entries[1].Destination, contents[1], 0o600); err != nil {
		t.Fatal(err)
	}
	completedTime := time.Unix(12345, 0)
	if err := os.Chtimes(entries[1].Destination, completedTime, completedTime); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Entries: entries}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: chunk, CheckpointBytes: 2 * chunk}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}

	for i, entry := range entries {
		got, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, contents[i]) {
			t.Fatalf("entry %d content mismatch", i)
		}
	}
	info, err := os.Stat(entries[1].Destination)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(completedTime) {
		t.Fatalf("already-complete file was rewritten: mtime=%v", info.ModTime())
	}
	manifest, _ := store.Manifest("job")
	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d manifest entries still pending", len(pending))
	}
}

func TestPhoneToLaptopRecoveryUsesPreservedCandidateAfterOriginalDeletion(t *testing.T) {
	const chunk = int64(64 * 1024)
	root := t.TempDir()
	data := bytes.Repeat([]byte("abcdefgh"), int((5*chunk)/8))
	source := filepath.Join(root, "phone", "movie.bin")
	destination := filepath.Join(root, "downloads", "movie.bin")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	manager := preserve.Manager{Store: store, Now: time.Now}
	temp := destination + ".part"
	if err := os.WriteFile(temp, data[:3*chunk], 0o600); err != nil {
		t.Fatal(err)
	}
	preserved, err := manager.Preserve(temp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(temp); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(preserved); err != nil {
		t.Fatal(err)
	}

	entry := state.ManifestEntry{ID: "movie", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: chunk, CheckpointBytes: chunk}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("recovered file mismatch")
	}

	if _, err := os.Stat(preserved); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumed preserved candidate still exists: %v", err)
	}

	if candidates := store.Candidates(); len(candidates) != 0 {
		t.Fatalf("consumed candidate state survived recovery: %#v", candidates)
	}
}

func TestCandidateCleanupNeverDeletesCompletedDestination(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "downloads", "movie.bin")
	data := []byte("verified-completed-file")

	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))

	// Deliberately malformed candidate state: the "partial" path points
	// directly at the real completed destination.
	if err := store.PutCandidate(state.Candidate{
		ID:          "malformed",
		Source:      destination,
		PartialPath: destination,
		Size:        int64(len(data)),
		UpdatedAt:   time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	entry := state.ManifestEntry{
		ID:          "movie",
		Source:      source,
		Destination: destination,
		Size:        int64(len(data)),
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "phone-to-laptop",
		Entries:   []state.ManifestEntry{entry},
	}); err != nil {
		t.Fatal(err)
	}

	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal("completed destination was deleted:", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("completed destination content changed")
	}

	if candidates := store.Candidates(); len(candidates) != 0 {
		t.Fatalf("malformed consumed candidate state survived: %#v", candidates)
	}
}

func TestPhoneToLaptopRecoveryRejectsSameSizeWrongDestination(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "laptop.bin")
	good := bytes.Repeat([]byte("good-data"), 32*1024)
	bad := bytes.Repeat([]byte("bad--data"), 32*1024)
	if len(good) != len(bad) {
		t.Fatal("test fixture sizes differ")
	}
	if err := os.WriteFile(source, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, bad, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	entry := state.ManifestEntry{ID: "conflict", Source: source, Destination: destination, Size: int64(len(good))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	err := runner.RecoverManifest("job")
	if !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("got %v, want ErrDestinationConflict", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, bad) {
		t.Fatal("wrong-content destination was overwritten")
	}
	manifest, _ := store.Manifest("job")
	if manifest.Entries[0].Complete {
		t.Fatal("conflicting entry was marked complete")
	}
}

type zeroReaderAt struct{}

func (zeroReaderAt) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = byte((int64(i) + off) % 251)
	}
	return len(p), nil
}

type countingUploadWriter struct {
	writes     int
	firstWrite int
	maxWrite   int
	bytes      int64
	syncs      int
}

func (w *countingUploadWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		w.firstWrite = len(p)
	}
	if len(p) > w.maxWrite {
		w.maxWrite = len(p)
	}
	w.bytes += int64(len(p))
	return len(p), nil
}

func (w *countingUploadWriter) Sync() error {
	w.syncs++
	return nil
}

func TestManagedUploadStreamUsesLargeWritesWithoutPeriodicDeviceSync(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	const total = int64(48 << 20)
	if err := store.PutManifest(state.Manifest{
		ID:        "fast-upload",
		Direction: "laptop-to-phone",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:   "entry",
			Size: total,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	writer := &countingUploadWriter{}
	runner := Runner{Store: store}
	if err := runner.streamManagedUpload("fast-upload", "entry", zeroReaderAt{}, total, writer, 0); err != nil {
		t.Fatal(err)
	}
	if writer.bytes != total {
		t.Fatalf("wrote %d bytes, want %d", writer.bytes, total)
	}
	if writer.firstWrite != int(managedUploadInitialProgressBytes) {
		t.Fatalf("first write = %d bytes, want %d", writer.firstWrite, managedUploadInitialProgressBytes)
	}
	if writer.maxWrite < 1<<20 {
		t.Fatalf("largest write was only %d bytes; managed path fell back to tiny-copy behavior", writer.maxWrite)
	}
	if writer.syncs != 1 {
		t.Fatalf("device sync count = %d, want exactly one final sync", writer.syncs)
	}
	manifest, _ := store.Manifest("fast-upload")
	if got := manifest.Entries[0].BytesDone; got != total {
		t.Fatalf("progress = %d, want %d", got, total)
	}
}

type failSecondUploadWriter struct {
	writes int
	bytes  int64
}

var errSecondUploadWrite = errors.New("second managed upload write failed")

func (w *failSecondUploadWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes == 2 {
		return 0, errSecondUploadWrite
	}
	w.bytes += int64(len(p))
	return len(p), nil
}

func (w *failSecondUploadWriter) Sync() error { return nil }

func TestManagedUploadPersistsEarlyFreshProgressBeforeSecondWrite(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	const total = int64(32 << 20)
	if err := store.PutManifest(state.Manifest{
		ID:        "early-progress",
		Direction: "laptop-to-phone",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:   "entry",
			Size: total,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	writer := &failSecondUploadWriter{}
	runner := Runner{Store: store}
	err = runner.streamManagedUpload("early-progress", "entry", zeroReaderAt{}, total, writer, 0)
	if !errors.Is(err, errSecondUploadWrite) {
		t.Fatalf("stream error = %v, want %v", err, errSecondUploadWrite)
	}
	if writer.bytes != managedUploadInitialProgressBytes {
		t.Fatalf("first completed write = %d bytes, want %d", writer.bytes, managedUploadInitialProgressBytes)
	}
	manifest, _ := store.Manifest("early-progress")
	if got := manifest.Entries[0].BytesDone; got != managedUploadInitialProgressBytes {
		t.Fatalf("persisted early progress = %d, want %d", got, managedUploadInitialProgressBytes)
	}
}

func TestLaptopToPhoneRecoveryResumesExistingPartialByVerifiedAppend(t *testing.T) {
	const chunk = int64(64 * 1024)
	root := t.TempDir()
	sourceDir := filepath.Join(root, "laptop")
	phoneDir := filepath.Join(root, "phone")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("upload-data-12345"), int((5*chunk)/16))
	source := filepath.Join(sourceDir, "movie.bin")
	destination := filepath.Join(phoneDir, "movie.bin")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	prefix := 2*chunk + chunk/2
	if err := os.WriteFile(destination, data[:prefix], 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	entry := state.ManifestEntry{ID: "movie", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: chunk, CheckpointBytes: chunk}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("phone destination does not match source after resume")
	}
	manifest, _ := store.Manifest("job")
	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d upload entries still pending", len(pending))
	}
}

func TestLaptopToPhoneRecoveryContinuesUnstartedManifestEntries(t *testing.T) {
	const chunk = int64(64 * 1024)
	root := t.TempDir()
	sourceDir := filepath.Join(root, "laptop")
	phoneDir := filepath.Join(root, "phone")
	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	firstData := bytes.Repeat([]byte("first-upload"), int((3*chunk)/12))
	secondData := bytes.Repeat([]byte("second-data-"), int((2*chunk)/12))
	firstSource := filepath.Join(sourceDir, "a.bin")
	secondSource := filepath.Join(sourceDir, "b.bin")
	firstDestination := filepath.Join(phoneDir, "a.bin")
	secondDestination := filepath.Join(phoneDir, "b.bin")
	if err := os.WriteFile(firstSource, firstData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSource, secondData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstDestination, firstData[:chunk], 0o600); err != nil {
		t.Fatal(err)
	}

	entries := []state.ManifestEntry{
		{ID: "a", Source: firstSource, Destination: firstDestination, Size: int64(len(firstData))},
		{ID: "b", Source: secondSource, Destination: secondDestination, Size: int64(len(secondData))},
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Entries: entries}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: chunk, CheckpointBytes: chunk}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	for i, tc := range []struct {
		path string
		data []byte
	}{{firstDestination, firstData}, {secondDestination, secondData}} {
		got, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, tc.data) {
			t.Fatalf("destination %d mismatch", i)
		}
	}
	manifest, _ := store.Manifest("job")
	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d upload entries still pending", len(pending))
	}
}

func TestLaptopToPhoneRecoveryAcceptsAlreadyCompleteDestinationWithoutAppendProbe(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "laptop.bin")
	destination := filepath.Join(root, "phone.bin")
	data := bytes.Repeat([]byte("already-complete"), 4096)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	entry := state.ManifestEntry{ID: "upload", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("job")
	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d upload entries still pending", len(pending))
	}
}

func TestLaptopToPhoneRecoveryResumesExistingPartialByAppend(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "laptop.bin")
	destination := filepath.Join(root, "phone.bin")

	data := bytes.Repeat([]byte("source-data"), 4096)
	partial := append([]byte(nil), data[:len(data)/3]...)

	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, partial, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	entry := state.ManifestEntry{
		ID:          "upload",
		Source:      source,
		Destination: destination,
		Size:        int64(len(data)),
	}

	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "laptop-to-phone",
		Entries:   []state.ManifestEntry{entry},
	}); err != nil {
		t.Fatal(err)
	}

	runner := Runner{
		Store:  store,
		Engine: engine.Engine{Store: store},
	}

	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatalf("append-capable partial recovery failed: %v", err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("append-resumed destination does not match source")
	}

	manifest, _ := store.Manifest("job")
	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d upload entries still pending", len(pending))
	}
}

func TestLaptopToPhoneRecoveryContinuesAllFilesByVerifiedAppend(t *testing.T) {
	root := t.TempDir()
	sourceDir := filepath.Join(root, "laptop")
	phoneDir := filepath.Join(root, "phone")

	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	firstData := bytes.Repeat([]byte("blocked-partial"), 4096)
	secondData := bytes.Repeat([]byte("new-file-data"), 4096)

	firstSource := filepath.Join(sourceDir, "a.bin")
	secondSource := filepath.Join(sourceDir, "b.bin")
	firstDestination := filepath.Join(phoneDir, "a.bin")
	secondDestination := filepath.Join(phoneDir, "b.bin")

	if err := os.WriteFile(firstSource, firstData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSource, secondData, 0o600); err != nil {
		t.Fatal(err)
	}

	partial := append([]byte(nil), firstData[:len(firstData)/3]...)
	if err := os.WriteFile(firstDestination, partial, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))

	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{
			{
				ID:          "a",
				Source:      firstSource,
				Destination: firstDestination,
				Size:        int64(len(firstData)),
			},
			{
				ID:          "b",
				Source:      secondSource,
				Destination: secondDestination,
				Size:        int64(len(secondData)),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	runner := Runner{
		Store: store,
	}

	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}

	gotFirst, err := os.ReadFile(firstDestination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotFirst, firstData) {
		t.Fatal("existing partial did not resume by append")
	}

	gotSecond, err := os.ReadFile(secondDestination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotSecond, secondData) {
		t.Fatal("later unstarted file did not complete")
	}

	manifest, _ := store.Manifest("job")

	if pending := manifest.Pending(); len(pending) != 0 {
		t.Fatalf("%d upload entries still pending", len(pending))
	}
}

func TestLaptopToPhoneRecoveryRejectsSameSizeWrongPartial(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "laptop.bin")
	destination := filepath.Join(root, "phone.bin")
	good := bytes.Repeat([]byte("good-data"), 32*1024)
	bad := bytes.Repeat([]byte("bad--data"), 16*1024)
	if len(bad) >= len(good) {
		t.Fatal("bad fixture must be a partial prefix")
	}
	if err := os.WriteFile(source, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	entry := state.ManifestEntry{ID: "upload", Source: source, Destination: destination, Size: int64(len(good))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store}
	err := runner.RecoverManifest("job")
	if !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("got %v, want ErrDestinationConflict", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, bad) {
		t.Fatal("wrong-content phone partial was overwritten")
	}
	manifest, _ := store.Manifest("job")
	if manifest.Entries[0].Complete {
		t.Fatal("conflicting upload was marked complete")
	}
}

func TestLaptopToPhoneRecoveryRejectsSameSizeWrongDestination(t *testing.T) {
	root := t.TempDir()

	source := filepath.Join(root, "laptop.bin")
	destination := filepath.Join(root, "phone.bin")

	good := bytes.Repeat([]byte("good-upload-data"), 32*1024)
	bad := bytes.Repeat([]byte("wrong-upload-dat"), 32*1024)

	if len(good) != len(bad) {
		t.Fatal("test fixture sizes differ")
	}

	if err := os.WriteFile(source, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, bad, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	entry := state.ManifestEntry{
		ID:          "upload",
		Source:      source,
		Destination: destination,
		Size:        int64(len(good)),
	}

	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "laptop-to-phone",
		Entries:   []state.ManifestEntry{entry},
	}); err != nil {
		t.Fatal(err)
	}

	runner := Runner{
		Store: store,
	}

	err = runner.RecoverManifest("job")
	if !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("got %v, want ErrDestinationConflict", err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bad) {
		t.Fatal("wrong full-size phone destination was overwritten")
	}

	manifest, ok := store.Manifest("job")
	if !ok {
		t.Fatal("manifest disappeared")
	}
	if manifest.Entries[0].Complete {
		t.Fatal("wrong full-size upload was marked complete")
	}
}

func TestManagedUploadRecoveryPersistsProgressAndCompletion(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.bin")
	destination := filepath.Join(root, "phone.bin")
	data := bytes.Repeat([]byte("managed-progress"), 32*1024)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry := state.ManifestEntry{ID: "entry", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "laptop-to-phone",
		Managed:   true,
		Entries:   []state.ManifestEntry{entry},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, CheckpointBytes: 64 * 1024}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.Entries[0].Complete {
		t.Fatal("managed upload did not complete")
	}
	if got := manifest.Entries[0].BytesDone; got != int64(len(data)) {
		t.Fatalf("bytes_done = %d, want %d", got, len(data))
	}
}

func TestManagedDownloadRecoveryPersistsProgressAndCompletion(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "laptop.bin")
	data := bytes.Repeat([]byte("managed-download"), 32*1024)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry := state.ManifestEntry{ID: "entry", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries:   []state.ManifestEntry{entry},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 32 * 1024, CheckpointBytes: 64 * 1024}}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.Entries[0].Complete || manifest.Entries[0].BytesDone != int64(len(data)) {
		t.Fatalf("managed download progress mismatch: %#v", manifest.Entries[0])
	}
}

func TestUploadVerificationRejectsSameSizeCorruption(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.bin")
	destination := filepath.Join(root, "phone.bin")
	good := bytes.Repeat([]byte("verify-good"), 32*1024)
	bad := append([]byte(nil), good...)
	bad[len(bad)/2] ^= 0xff
	if err := os.WriteFile(sourcePath, good, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, bad, 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := gvfs.OpenSource(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	runner := Runner{}
	if err := runner.verifyUploadComplete(source, destination); !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("got %v, want ErrDestinationConflict", err)
	}
}

func TestManagedUploadPausePreservesAndResumeCompletes(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.bin")
	destination := filepath.Join(root, "phone.bin")
	data := bytes.Repeat([]byte("pause-resume"), 1024)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry := state.ManifestEntry{ID: "entry", Source: source, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Managed: true, Paused: true, Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 1024, CheckpointBytes: 2048}}
	if err := runner.RecoverManifest("job"); !errors.Is(err, ErrUserPaused) {
		t.Fatalf("paused recover = %v, want ErrUserPaused", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused transfer unexpectedly wrote destination: %v", err)
	}
	if err := store.SetManifestPaused("job", false); err != nil {
		t.Fatal(err)
	}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed upload data mismatch")
	}
}

func TestManagedPhoneToLaptopPauseResumeAcrossMultipleFiles(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "phone")
	destinationRoot := filepath.Join(root, "laptop")
	if err := os.MkdirAll(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	firstData := bytes.Repeat([]byte("first-multi-pause-block-"), (80<<20)/len("first-multi-pause-block-")+1)
	firstData = firstData[:80<<20]
	secondData := []byte("second-file-after-resume")
	thirdData := []byte("third-file-after-resume")
	fixtures := []struct {
		id   string
		name string
		data []byte
	}{
		{id: "first", name: "first.bin", data: firstData},
		{id: "second", name: "second.bin", data: secondData},
		{id: "third", name: "third.bin", data: thirdData},
	}

	entries := make([]state.ManifestEntry, 0, len(fixtures))
	for _, fixture := range fixtures {
		source := filepath.Join(sourceRoot, fixture.name)
		if err := os.WriteFile(source, fixture.data, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, state.ManifestEntry{
			ID:          fixture.id,
			Source:      source,
			Destination: filepath.Join(destinationRoot, fixture.name),
			Size:        int64(len(fixture.data)),
		})
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:              "multi-pause",
		Direction:       "phone-to-laptop",
		DestinationRoot: destinationRoot,
		Managed:         true,
		Entries:         entries,
	}); err != nil {
		t.Fatal(err)
	}

	paused := false
	runner := Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			if !paused && done >= managedDownloadProgressBytes && done < total {
				paused = true
				return store.SetManifestPaused("multi-pause", true)
			}
			return nil
		},
	}}
	if err := runner.RecoverManifest("multi-pause"); !errors.Is(err, ErrUserPaused) {
		t.Fatalf("multi-file paused recover=%v want=%v", err, ErrUserPaused)
	}
	if !paused {
		t.Fatal("multi-file transfer never reached pause point")
	}

	mid, _ := store.Manifest("multi-pause")
	if !mid.Paused {
		t.Fatalf("multi-file pause state not persisted: %#v", mid)
	}
	if mid.Entries[0].BytesDone <= 0 || mid.Entries[0].BytesDone >= mid.Entries[0].Size || mid.Entries[0].Complete {
		t.Fatalf("first file was not durably paused mid-transfer: %#v", mid.Entries[0])
	}
	for i := 1; i < len(mid.Entries); i++ {
		if mid.Entries[i].BytesDone != 0 || mid.Entries[i].Complete {
			t.Fatalf("later file %d advanced while first file was paused: %#v", i, mid.Entries[i])
		}
		if _, err := os.Stat(mid.Entries[i].Destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("later file %d destination exists before resume: %v", i, err)
		}
	}

	if err := store.SetManifestPaused("multi-pause", false); err != nil {
		t.Fatal(err)
	}
	resumeRunner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := resumeRunner.RecoverManifest("multi-pause"); err != nil {
		t.Fatal(err)
	}

	final, _ := store.Manifest("multi-pause")
	if final.Paused || len(final.Pending()) != 0 {
		t.Fatalf("multi-file resume did not reach terminal state: %#v", final)
	}
	for i, fixture := range fixtures {
		entry := final.Entries[i]
		if !entry.Complete || entry.BytesDone != entry.Size {
			t.Fatalf("resumed entry %d not complete: %#v", i, entry)
		}
		got, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, fixture.data) {
			t.Fatalf("resumed entry %d content mismatch", i)
		}
	}
}

func TestCancelManagedUploadKeepCompletedRemovesOwnedPartialOnly(t *testing.T) {
	root := t.TempDir()
	completedSource := filepath.Join(root, "completed-source.bin")
	completedDestination := filepath.Join(root, "completed-phone.bin")
	pendingSource := filepath.Join(root, "pending-source.bin")
	pendingDestination := filepath.Join(root, "pending-phone.bin")
	completedData := []byte("already-complete")
	pendingData := bytes.Repeat([]byte("cancel-me"), 1024)
	if err := os.WriteFile(completedSource, completedData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(completedDestination, completedData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pendingSource, pendingData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pendingDestination, pendingData[:2048], 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:              "job",
		Direction:       "laptop-to-phone",
		Managed:         true,
		CancelRequested: true,
		CancelMode:      state.CancelModeKeepCompleted,
		Entries: []state.ManifestEntry{
			{ID: "completed", Source: completedSource, Destination: completedDestination, Size: int64(len(completedData)), BytesDone: int64(len(completedData)), Complete: true, CreatedByJob: true},
			{ID: "pending", Source: pendingSource, Destination: pendingDestination, Size: int64(len(pendingData)), BytesDone: 2048, CreatedByJob: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store}
	if err := runner.CancelManifest("job"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(pendingDestination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled upload partial still exists: %v", err)
	}
	if got, err := os.ReadFile(completedDestination); err != nil || !bytes.Equal(got, completedData) {
		t.Fatalf("completed file was not kept: %q err=%v", got, err)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.Cancelled || manifest.CancelRequested || manifest.Entries[1].BytesDone != 0 || !manifest.Entries[0].Complete {
		t.Fatalf("cancel state = %#v", manifest)
	}
}

func TestCancelManagedUploadUndoAllRemovesOnlyJobOwnedFiles(t *testing.T) {
	root := t.TempDir()
	ownedDone := filepath.Join(root, "owned-done.bin")
	ownedPartial := filepath.Join(root, "owned-partial.bin")
	preexisting := filepath.Join(root, "preexisting.bin")
	for path, data := range map[string][]byte{
		ownedDone:    []byte("done"),
		ownedPartial: []byte("partial"),
		preexisting:  []byte("preexisting"),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:              "job",
		Direction:       "laptop-to-phone",
		Managed:         true,
		CancelRequested: true,
		CancelMode:      state.CancelModeUndoAll,
		Entries: []state.ManifestEntry{
			{ID: "owned-done", Destination: ownedDone, Size: 4, BytesDone: 4, Complete: true, CreatedByJob: true},
			{ID: "owned-partial", Destination: ownedPartial, Size: 20, BytesDone: 7, CreatedByJob: true},
			{ID: "preexisting", Destination: preexisting, Size: 11, BytesDone: 11, Complete: true},
		},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store}
	if err := runner.CancelManifest("job"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{ownedDone, ownedPartial} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned transfer file still exists after undo: %s err=%v", path, err)
		}
	}
	if got, err := os.ReadFile(preexisting); err != nil || string(got) != "preexisting" {
		t.Fatalf("preexisting file was changed by undo: %q err=%v", got, err)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.Cancelled || manifest.CancelRequested {
		t.Fatalf("cancel state = %#v", manifest)
	}
}

func TestCancelManagedDownloadModesCleanSidecarAndRespectCompletedFiles(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mode          string
		wantCompleted bool
	}{
		{name: "keep", mode: state.CancelModeKeepCompleted, wantCompleted: true},
		{name: "undo", mode: state.CancelModeUndoAll, wantCompleted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			completed := filepath.Join(root, "completed.bin")
			pending := filepath.Join(root, "pending.bin")
			if err := os.WriteFile(completed, []byte("done"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(pending+engine.PartialSuffix, []byte("partial"), 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := state.Open(filepath.Join(root, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PutTransfer(state.Transfer{ID: "pending", Destination: pending, Size: 20, ChunkSize: 4, Chunks: map[int]state.Chunk{}}); err != nil {
				t.Fatal(err)
			}
			if err := store.PutManifest(state.Manifest{
				ID:              "job",
				Direction:       "phone-to-laptop",
				Managed:         true,
				CancelRequested: true,
				CancelMode:      tc.mode,
				Entries: []state.ManifestEntry{
					{ID: "completed", Destination: completed, Size: 4, BytesDone: 4, Complete: true, CreatedByJob: true},
					{ID: "pending", Destination: pending, Size: 20, BytesDone: 7, CreatedByJob: true},
				},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store}
			if err := runner.CancelManifest("job"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(pending + engine.PartialSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("download sidecar remains after cancel: %v", err)
			}
			if _, ok := store.Transfer("pending"); ok {
				t.Fatal("download transfer state remains after cancel")
			}
			_, err = os.Stat(completed)
			if tc.wantCompleted && err != nil {
				t.Fatalf("completed file should be kept: %v", err)
			}
			if !tc.wantCompleted && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("completed job-owned file should be removed: %v", err)
			}
		})
	}
}

func TestManagedUploadDoesNotAdoptUnknownPreexistingPartial(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source.bin")
	destination := filepath.Join(root, "phone.bin")
	data := bytes.Repeat([]byte("managed-collision"), 128)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data[:128], 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry := state.ManifestEntry{ID: "entry", Source: sourcePath, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "laptop-to-phone", Managed: true, Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store}
	if err := runner.RecoverManifest("job"); !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("managed upload collision = %v, want ErrDestinationConflict", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, data[:128]) {
		t.Fatalf("preexisting destination changed: len=%d err=%v", len(got), err)
	}
}

func TestManagedDownloadDoesNotAdoptUnknownPreexistingPartial(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "phone-source.bin")
	destination := filepath.Join(root, "laptop.bin")
	data := bytes.Repeat([]byte("managed-download-collision"), 128)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, data[:128], 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	entry := state.ManifestEntry{ID: "entry", Source: sourcePath, Destination: destination, Size: int64(len(data))}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Managed: true, Entries: []state.ManifestEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := runner.RecoverManifest("job"); !errors.Is(err, ErrDestinationConflict) {
		t.Fatalf("managed download collision = %v, want ErrDestinationConflict", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, data[:128]) {
		t.Fatalf("preexisting destination changed: len=%d err=%v", len(got), err)
	}
}

func TestManagedUploadCancelModesAcross100Files(t *testing.T) {
	for _, tc := range []struct {
		name              string
		mode              string
		wantCompletedKept bool
	}{
		{name: "keep-completed", mode: state.CancelModeKeepCompleted, wantCompletedKept: true},
		{name: "undo-all", mode: state.CancelModeUndoAll, wantCompletedKept: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			store, err := state.Open(filepath.Join(root, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			entries := make([]state.ManifestEntry, 100)
			for i := range entries {
				path := filepath.Join(root, "phone", fmt.Sprintf("file-%03d.bin", i))
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				complete := i < 60
				payload := []byte("partial")
				if complete {
					payload = []byte("complete")
				}
				if err := os.WriteFile(path, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				entries[i] = state.ManifestEntry{
					ID:           fmt.Sprintf("entry-%03d", i),
					Destination:  path,
					Size:         int64(len([]byte("complete"))),
					BytesDone:    int64(len(payload)),
					Complete:     complete,
					CreatedByJob: true,
				}
			}
			if err := store.PutManifest(state.Manifest{
				ID:              "job",
				Direction:       "laptop-to-phone",
				Managed:         true,
				CancelRequested: true,
				CancelMode:      tc.mode,
				Entries:         entries,
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store}
			if err := runner.CancelManifest("job"); err != nil {
				t.Fatal(err)
			}

			for i, entry := range entries {
				_, err := os.Stat(entry.Destination)
				if i < 60 && tc.wantCompletedKept {
					if err != nil {
						t.Fatalf("completed file %d should remain: %v", i, err)
					}
					continue
				}
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("file %d should be absent after cancel mode %s: %v", i, tc.mode, err)
				}
			}
		})
	}
}

func TestManagedExactExistingDestinationIsRecordedAsDuplicate(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, "source.bin")
			destination := filepath.Join(root, "destination.bin")
			payload := bytes.Repeat([]byte("duplicate-content-"), 4096)
			if err := os.WriteFile(source, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(destination, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := state.Open(filepath.Join(root, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PutManifest(state.Manifest{
				ID:        "duplicate-job",
				Direction: direction,
				Managed:   true,
				Entries: []state.ManifestEntry{{
					ID:          "entry",
					Source:      source,
					Destination: destination,
					Size:        int64(len(payload)),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			if err := runner.RecoverManifest("duplicate-job"); err != nil {
				t.Fatal(err)
			}
			manifest, _ := store.Manifest("duplicate-job")
			entry := manifest.Entries[0]
			if !entry.Complete || !entry.Duplicate || entry.CreatedByJob || entry.BytesDone != entry.Size {
				t.Fatalf("duplicate entry = %#v", entry)
			}
		})
	}
}

func TestManagedDifferentNameExistingContentIsSkippedAsDuplicate(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			destinationRoot := filepath.Join(root, "destination")
			if err := os.MkdirAll(sourceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(destinationRoot, "existing"), 0o700); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("same-content-different-name-"), 8192)
			source := filepath.Join(sourceDir, "incoming-name.bin")
			intended := filepath.Join(destinationRoot, "incoming-name.bin")
			duplicate := filepath.Join(destinationRoot, "existing", "already-there.bin")
			if err := os.WriteFile(source, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(duplicate, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			store, err := state.Open(filepath.Join(root, "state.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := store.PutManifest(state.Manifest{
				ID:              "duplicate-job",
				Direction:       direction,
				DestinationRoot: destinationRoot,
				Managed:         true,
				Entries: []state.ManifestEntry{{
					ID:          "entry",
					Source:      source,
					Destination: intended,
					Size:        int64(len(payload)),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			if err := runner.RecoverManifest("duplicate-job"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(intended); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("duplicate should not be transferred to intended name: %v", err)
			}
			manifest, _ := store.Manifest("duplicate-job")
			entry := manifest.Entries[0]
			if !entry.Complete || !entry.Duplicate || entry.CreatedByJob || entry.BytesDone != entry.Size {
				t.Fatalf("duplicate entry = %#v", entry)
			}
			if entry.DuplicateOf != duplicate {
				t.Fatalf("duplicate_of = %q, want %q", entry.DuplicateOf, duplicate)
			}
		})
	}
}

func TestManagedDifferentNameSameSizeDifferentContentTransfersNormally(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			destinationRoot := filepath.Join(root, "destination")
			if err := os.MkdirAll(sourceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("incoming"), 16384)
			other := bytes.Repeat([]byte("existing"), len(payload)/len("existing"))
			if len(other) < len(payload) {
				other = append(other, bytes.Repeat([]byte{'x'}, len(payload)-len(other))...)
			}
			other = other[:len(payload)]
			source := filepath.Join(sourceDir, "incoming.bin")
			intended := filepath.Join(destinationRoot, "incoming.bin")
			if err := os.WriteFile(source, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destinationRoot, "same-size.bin"), other, 0o600); err != nil {
				t.Fatal(err)
			}
			store, _ := state.Open(filepath.Join(root, "state.json"))
			if err := store.PutManifest(state.Manifest{
				ID: "job", Direction: direction, DestinationRoot: destinationRoot, Managed: true,
				Entries: []state.ManifestEntry{{ID: "entry", Source: source, Destination: intended, Size: int64(len(payload))}},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			if err := runner.RecoverManifest("job"); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(intended)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("intended destination content mismatch")
			}
			manifest, _ := store.Manifest("job")
			if manifest.Entries[0].Duplicate {
				t.Fatalf("different content was incorrectly skipped: %#v", manifest.Entries[0])
			}
		})
	}
}

func TestManagedDuplicateWithinSameJobDifferentNamesTransfersOnlyFirst(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			destinationRoot := filepath.Join(root, "destination")
			if err := os.MkdirAll(sourceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("batch-duplicate-"), 8192)
			firstSource := filepath.Join(sourceDir, "first.bin")
			secondSource := filepath.Join(sourceDir, "second.bin")
			if err := os.WriteFile(firstSource, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(secondSource, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			firstDestination := filepath.Join(destinationRoot, "first.bin")
			secondDestination := filepath.Join(destinationRoot, "second.bin")
			store, _ := state.Open(filepath.Join(root, "state.json"))
			if err := store.PutManifest(state.Manifest{
				ID: "job", Direction: direction, DestinationRoot: destinationRoot, Managed: true,
				Entries: []state.ManifestEntry{
					{ID: "first", Source: firstSource, Destination: firstDestination, Size: int64(len(payload))},
					{ID: "second", Source: secondSource, Destination: secondDestination, Size: int64(len(payload))},
				},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			if err := runner.RecoverManifest("job"); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(firstDestination); err != nil {
				t.Fatalf("first destination missing: %v", err)
			}
			if _, err := os.Stat(secondDestination); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("second duplicate should not have been transferred: %v", err)
			}
			manifest, _ := store.Manifest("job")
			if !manifest.Entries[1].Duplicate || manifest.Entries[1].DuplicateOf != firstDestination {
				t.Fatalf("second entry = %#v", manifest.Entries[1])
			}
		})
	}
}

func TestManagedExactPathConflictWinsOverDifferentNameDuplicate(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			destinationRoot := filepath.Join(root, "destination")
			if err := os.MkdirAll(sourceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("wanted-content"), 8192)
			conflict := bytes.Repeat([]byte("other-content!"), 8192)
			if len(conflict) != len(payload) {
				t.Fatalf("test fixture sizes differ: %d vs %d", len(conflict), len(payload))
			}
			source := filepath.Join(sourceDir, "movie.bin")
			intended := filepath.Join(destinationRoot, "movie.bin")
			duplicate := filepath.Join(destinationRoot, "other-name.bin")
			if err := os.WriteFile(source, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(intended, conflict, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(duplicate, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			store, _ := state.Open(filepath.Join(root, "state.json"))
			if err := store.PutManifest(state.Manifest{
				ID: "job", Direction: direction, DestinationRoot: destinationRoot, Managed: true,
				Entries: []state.ManifestEntry{{ID: "entry", Source: source, Destination: intended, Size: int64(len(payload))}},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			err := runner.RecoverManifest("job")
			if !errors.Is(err, ErrDestinationConflict) {
				t.Fatalf("error = %v, want destination conflict", err)
			}
			manifest, _ := store.Manifest("job")
			if manifest.Entries[0].Duplicate || manifest.Entries[0].Complete {
				t.Fatalf("conflicting exact path was incorrectly treated as duplicate: %#v", manifest.Entries[0])
			}
		})
	}
}

func TestManagedContentDuplicateScanIgnoresResumexferSidecar(t *testing.T) {
	for _, direction := range []string{"laptop-to-phone", "phone-to-laptop"} {
		t.Run(direction, func(t *testing.T) {
			root := t.TempDir()
			sourceDir := filepath.Join(root, "source")
			destinationRoot := filepath.Join(root, "destination")
			if err := os.MkdirAll(sourceDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("sidecar-is-not-final-content"), 4096)
			source := filepath.Join(sourceDir, "fresh.bin")
			intended := filepath.Join(destinationRoot, "fresh.bin")
			if err := os.WriteFile(source, payload, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destinationRoot, "abandoned.bin"+engine.PartialSuffix), payload, 0o600); err != nil {
				t.Fatal(err)
			}
			store, _ := state.Open(filepath.Join(root, "state.json"))
			if err := store.PutManifest(state.Manifest{
				ID: "job", Direction: direction, DestinationRoot: destinationRoot, Managed: true,
				Entries: []state.ManifestEntry{{ID: "entry", Source: source, Destination: intended, Size: int64(len(payload))}},
			}); err != nil {
				t.Fatal(err)
			}
			runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
			if err := runner.RecoverManifest("job"); err != nil {
				t.Fatal(err)
			}
			manifest, _ := store.Manifest("job")
			if manifest.Entries[0].Duplicate {
				t.Fatalf("sidecar was incorrectly treated as duplicate: %#v", manifest.Entries[0])
			}
			if _, err := os.Stat(intended); err != nil {
				t.Fatalf("intended destination missing: %v", err)
			}
		})
	}
}

type flakyReaderAt struct {
	data      []byte
	failures  int
	readCalls int
}

func (f *flakyReaderAt) ReadAt(p []byte, off int64) (int, error) {
	f.readCalls++
	if f.failures > 0 {
		f.failures--
		return 0, syscall.EIO
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestManagedPhoneReaderRetriesTransientMTPRead(t *testing.T) {
	base := &flakyReaderAt{data: []byte("retry-me"), failures: 2}
	reader := retryReaderAt{Reader: base, Attempts: 3, Delay: time.Nanosecond}
	buf := make([]byte, len(base.data))
	n, err := reader.ReadAt(buf, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) || !bytes.Equal(buf, base.data) {
		t.Fatalf("retry read = %q (%d)", buf, n)
	}
	if base.readCalls != 3 {
		t.Fatalf("read calls = %d, want 3", base.readCalls)
	}
}

func TestManagedPhoneToLaptopReportsProgressBeforeLargeCheckpoint(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone-source.bin")
	destination := filepath.Join(root, "laptop-destination.bin")
	data := bytes.Repeat([]byte{0x5a}, 80<<20)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "fast-download",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "entry",
			Source:      source,
			Destination: destination,
			Size:        int64(len(data)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	var samples []int64
	runner := Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			samples = append(samples, done)
			return nil
		},
	}}
	if err := runner.RecoverManifest("fast-download"); err != nil {
		t.Fatal(err)
	}
	seenIntermediate := false
	for _, done := range samples {
		if done >= managedDownloadProgressBytes && done < int64(len(data)) {
			seenIntermediate = true
			break
		}
	}
	if !seenIntermediate {
		t.Fatalf("managed download progress samples = %v", samples)
	}
	manifest, _ := store.Manifest("fast-download")
	if manifest.Entries[0].SourceFingerprint == nil {
		t.Fatal("large managed download did not persist source identity for transport handoff")
	}
}

func TestManagedPhoneToLaptopPauseKeepsDisplayedProgressDurable(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone-pause.bin")
	destination := filepath.Join(root, "laptop-pause.bin")
	const size = int64(80 << 20)
	sourceFile, err := os.OpenFile(source, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceFile.Truncate(size); err != nil {
		sourceFile.Close()
		t.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "durable-pause",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "entry",
			Source:      source,
			Destination: destination,
			Size:        size,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	var displayed int64
	runner := Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			if displayed == 0 && done >= managedDownloadProgressBytes && done < total {
				displayed = done
				return store.SetManifestPaused("durable-pause", true)
			}
			return nil
		},
	}}
	if err := runner.RecoverManifest("durable-pause"); !errors.Is(err, ErrUserPaused) {
		t.Fatalf("paused recover=%v want=%v", err, ErrUserPaused)
	}
	if displayed == 0 {
		t.Fatal("managed download never reported intermediate progress")
	}

	manifest, _ := store.Manifest("durable-pause")
	pausedAt := manifest.Entries[0].BytesDone
	if pausedAt < displayed {
		t.Fatalf("paused progress rolled back: displayed=%d durable=%d", displayed, pausedAt)
	}
	transfer, ok := store.Transfer("entry")
	if !ok {
		t.Fatal("large managed download did not keep engine checkpoint state")
	}
	var durable int64
	for _, chunk := range transfer.Chunks {
		durable += chunk.Size
	}
	if durable != pausedAt {
		t.Fatalf("paused manifest progress=%d but durable checkpoint=%d", pausedAt, durable)
	}

	if err := store.SetManifestPaused("durable-pause", false); err != nil {
		t.Fatal(err)
	}
	var resumeSamples []int64
	resumeRunner := Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			resumeSamples = append(resumeSamples, done)
			return nil
		},
	}}
	if err := resumeRunner.RecoverManifest("durable-pause"); err != nil {
		t.Fatal(err)
	}
	if len(resumeSamples) == 0 || resumeSamples[0] != pausedAt {
		t.Fatalf("resume started at %v want first sample %d", resumeSamples, pausedAt)
	}
	manifest, _ = store.Manifest("durable-pause")
	if len(manifest.Pending()) != 0 || !manifest.Entries[0].Complete {
		t.Fatalf("resumed managed download incomplete: %#v", manifest)
	}
}

func TestLargeManagedDownloadPersistsSourceIdentityBeforeInterruption(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone-large-interrupted.bin")
	destination := filepath.Join(root, "laptop-large-interrupted.bin")
	const size = 80 << 20
	sourceFile, err := os.OpenFile(source, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceFile.Truncate(size); err != nil {
		sourceFile.Close()
		t.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "large-interrupted",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "entry",
			Source:      source,
			Destination: destination,
			Size:        size,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	stop := errors.New("stop large managed download")
	runner := Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			if done >= managedDownloadProgressBytes && done < total {
				return stop
			}
			return nil
		},
	}}
	if err := runner.RecoverManifest("large-interrupted"); !errors.Is(err, stop) {
		t.Fatalf("interrupted recover = %v, want %v", err, stop)
	}

	manifest, _ := store.Manifest("large-interrupted")
	entry := manifest.Entries[0]
	if entry.SourceFingerprint == nil {
		t.Fatal("source identity missing after interrupted large managed download")
	}
	if entry.Complete {
		t.Fatal("interrupted large managed download unexpectedly completed")
	}
}

func TestManagedSmallPhoneToLaptopUsesFastSidecarPath(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone-small.bin")
	destination := filepath.Join(root, "downloads", "phone-small.bin")
	data := bytes.Repeat([]byte("small-fast-path"), 4096)
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "small-fast",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "small-entry",
			Source:      source,
			Destination: destination,
			Size:        int64(len(data)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := runner.RecoverManifest("small-fast"); err != nil {
		t.Fatal(err)
	}
	if transfers := store.Transfers(); len(transfers) != 0 {
		t.Fatalf("small managed download unexpectedly used generic transfer state: %#v", transfers)
	}
	manifest, _ := store.Manifest("small-fast")
	entry := manifest.Entries[0]
	if !entry.Complete || !entry.CreatedByJob || entry.SourceFingerprint == nil {
		t.Fatalf("small fast-path state = %#v", entry)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("small fast-path destination mismatch")
	}
	if _, err := os.Stat(destination + engine.PartialSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sidecar remains after completion: %v", err)
	}
}

func TestManagedSmallPhoneToLaptopResumesOwnedSidecar(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone-small-resume.bin")
	destination := filepath.Join(root, "downloads", "phone-small-resume.bin")
	data := bytes.Repeat([]byte("resume-small-fast-path"), 300000)
	if int64(len(data)) >= managedDownloadDirectLimit {
		t.Fatal("test payload unexpectedly exceeds direct-path limit")
	}
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	partial := destination + engine.PartialSuffix
	prefix := int64(len(data) / 3)
	if err := os.WriteFile(partial, data[:prefix], 0o600); err != nil {
		t.Fatal(err)
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fingerprint.ReaderAt(sourceFile, int64(len(data)), 64*1024)
	_ = sourceFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "small-resume",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:                "resume-entry",
			Source:            source,
			Destination:       destination,
			Size:              int64(len(data)),
			BytesDone:         prefix,
			CreatedByJob:      true,
			SourceFingerprint: &fp,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := runner.RecoverManifest("small-resume"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("resumed small fast-path destination mismatch")
	}
}

func TestRecoveryRespectsCompetingTransportLeaseAndReleasesAfterSuccess(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "laptop.bin")
	payload := bytes.Repeat([]byte("lease-proof"), 4096)
	if err := os.WriteFile(source, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "entry",
			Source:      source,
			Destination: destination,
			Size:        int64(len(payload)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	wifiLease, err := store.AcquireManifestTransport("job", "wifi:test", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4096, CheckpointBytes: 8192}}
	if err := runner.RecoverManifest("job"); !errors.Is(err, state.ErrManifestTransportBusy) {
		t.Fatalf("recovery under competing lease err=%v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("competing transport allowed USB write: %v", err)
	}
	if err := store.ReleaseManifestTransport("job", wifiLease.Owner, wifiLease.Generation); err != nil {
		t.Fatal(err)
	}
	if err := runner.RecoverManifest("job"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("recovered destination mismatch")
	}
	manifest, _ := store.Manifest("job")
	if manifest.TransportOwner != "" || !manifest.TransportLeaseUntil.IsZero() {
		t.Fatalf("successful recovery retained lease: %#v", manifest)
	}
}

func TestManagedRecoveryRejectsChangedExplicitWirelessCandidateBeforeUSBAdoption(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "download", "phone.bin")
	candidatePath := filepath.Join(root, "wireless.part")
	data := bytes.Repeat([]byte{0x4a}, 6<<20)
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	preserved := append([]byte(nil), data[:4<<20]...)
	preserved[1<<20] ^= 0xff // outside the fingerprint anchor windows
	if err := os.WriteFile(candidatePath, preserved, 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "wifi-usb-corrupt",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:                "entry",
			Source:            sourcePath,
			Destination:       destination,
			Size:              int64(len(data)),
			CreatedByJob:      true,
			SourceFingerprint: &fp,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCandidate(state.Candidate{
		ID:          "candidate",
		Direction:   "phone-to-laptop",
		Source:      candidatePath,
		Destination: destination,
		PartialPath: candidatePath,
		Size:        int64(len(data)),
		ChunkSize:   engine.DefaultChunkSize,
		UpdatedAt:   time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Engine: engine.Engine{Store: store}}
	if err := runner.RecoverManifest("wifi-usb-corrupt"); !errors.Is(err, engine.ErrPrefixMismatch) {
		t.Fatalf("recovery error=%v, want prefix mismatch", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination created from changed candidate: %v", err)
	}
	got, err := os.ReadFile(candidatePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, preserved) {
		t.Fatal("changed candidate was modified after rejection")
	}
}
