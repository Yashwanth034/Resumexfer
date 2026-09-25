package preserve

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"resumexfer/internal/state"
)

func TestHardLinkPreservationTracksWritesAndSurvivesOriginalDelete(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	m := Manager{Store: st, Now: time.Now}
	original := filepath.Join(dir, "movie.part")
	if err := os.WriteFile(original, []byte("first"), 0o600); err != nil {
		t.Fatal(err)
	}
	link, err := m.Preserve(original)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(original, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := os.Remove(original); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(link)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("firstsecond")) {
		t.Fatalf("got %q", got)
	}
}

func TestIsTrackedDestinationIgnoresCancelledManifest(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	m := Manager{Store: st, Now: time.Now}
	destination := filepath.Join(dir, "movie.bin")

	if err := st.PutManifest(state.Manifest{
		ID:        "cancelled",
		Direction: "phone-to-laptop",
		Cancelled: true,
		Entries: []state.ManifestEntry{{
			ID:          "entry-cancelled",
			Destination: destination,
			Size:        100,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if m.IsTrackedDestination(destination) {
		t.Fatal("cancelled manifest kept destination tracked")
	}

	if err := st.PutManifest(state.Manifest{
		ID:        "active",
		Direction: "phone-to-laptop",
		Entries: []state.ManifestEntry{{
			ID:          "entry-active",
			Destination: destination,
			Size:        100,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if !m.IsTrackedDestination(destination) {
		t.Fatal("active pending manifest did not track destination")
	}
}

func TestPreserveWhenDiscardsCandidateIfTrackingEndsDuringPreserve(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	m := Manager{Store: st, Now: time.Now}
	original := filepath.Join(dir, "movie.bin")
	if err := os.WriteFile(original, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}

	checks := 0
	link, err := m.preserveWhen(original, func() bool {
		checks++
		return checks == 1
	})
	if err != nil {
		t.Fatal(err)
	}
	if link != "" {
		t.Fatalf("preserved link = %q, want empty after tracking ended", link)
	}
	if got := len(st.Candidates()); got != 0 {
		t.Fatalf("stale candidates = %d, want 0", got)
	}
	cacheDir := filepath.Join(dir, ".resumexfer-cache")
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("stale cache entries = %d, want 0", len(entries))
	}
}

func TestCleanupDeletesStaleKnownTempButNotNormalFile(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	now := time.Unix(200000, 0)
	m := Manager{Store: st, Now: func() time.Time { return now }}
	temp := filepath.Join(dir, "old.part")
	normal := filepath.Join(dir, "keep.mp4")
	os.WriteFile(temp, []byte("partial"), 0o600)
	os.WriteFile(normal, []byte("complete-looking"), 0o600)
	tempLink, _ := m.Preserve(temp)
	normalLink, _ := m.Preserve(normal)
	stale := now.Add(-25 * time.Hour)
	if err := m.SetCandidateTimeForTest(tempLink, stale); err != nil {
		t.Fatal(err)
	}
	if err := m.SetCandidateTimeForTest(normalLink, stale); err != nil {
		t.Fatal(err)
	}

	if err := m.Cleanup(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatal("stale known temp not deleted")
	}
	if _, err := os.Stat(normal); err != nil {
		t.Fatal("normal file was deleted")
	}
	if _, err := os.Stat(tempLink); !os.IsNotExist(err) {
		t.Fatal("stale temp cache link survived")
	}
	if _, err := os.Stat(normalLink); !os.IsNotExist(err) {
		t.Fatal("stale normal cache link survived")
	}
}

func TestCleanupDeletesStaleOwnedEnginePartialAndTransferState(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	now := time.Unix(200000, 0)
	m := Manager{Store: st, Now: func() time.Time { return now }}
	destination := filepath.Join(dir, "movie.mkv")
	partial := destination + ".resumexfer-part"
	if err := os.WriteFile(partial, []byte("partial-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.PutTransfer(state.Transfer{
		ID: "stale-transfer", Destination: destination,
		CreatedAt: now.Add(-26 * time.Hour), UpdatedAt: now.Add(-25 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	if err := m.Cleanup(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatal("stale Resumexfer engine partial survived cleanup")
	}
	if _, ok := st.Transfer("stale-transfer"); ok {
		t.Fatal("stale transfer state survived cleanup")
	}
}

func TestCleanupKeepsFreshOwnedEnginePartial(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	now := time.Unix(200000, 0)
	m := Manager{Store: st, Now: func() time.Time { return now }}
	destination := filepath.Join(dir, "movie.mkv")
	partial := destination + ".resumexfer-part"
	if err := os.WriteFile(partial, []byte("partial-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := st.PutTransfer(state.Transfer{
		ID: "fresh-transfer", Destination: destination,
		CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	if err := m.Cleanup(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("fresh Resumexfer partial was removed: %v", err)
	}
	if _, ok := st.Transfer("fresh-transfer"); !ok {
		t.Fatal("fresh transfer state was removed")
	}
}

func TestWatcherIgnoresUntrackedNewFile(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	m := &Manager{Store: st, Now: time.Now}

	w, err := NewWatcher(m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AddDir(dir); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "unrelated-download.bin")
	if err := os.WriteFile(path, []byte("unrelated"), 0o600); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	if got := len(st.Candidates()); got != 0 {
		t.Fatalf("untracked file created %d preservation candidates, want 0", got)
	}
}

func TestWatcherPreservesTrackedDestinationWithoutMTPBeingConnected(t *testing.T) {
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))

	path := filepath.Join(dir, "incoming.mp4")

	if err := st.PutManifest(state.Manifest{
		ID:        "tracked-job",
		Direction: "phone-to-laptop",
		Entries: []state.ManifestEntry{{
			ID:          "tracked-entry",
			Source:      "/phone/incoming.mp4",
			Destination: path,
			Size:        5,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	m := &Manager{Store: st, Now: time.Now}
	w, err := NewWatcher(m)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if err := w.AddDir(dir); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, candidate := range st.Candidates() {
			if candidate.Source == path {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("tracked destination was not preserved")
}

func TestWatcherFollowsNewNestedDestinationDirectories(t *testing.T) {
	root := t.TempDir()
	store, _ := state.Open(filepath.Join(root, "state.json"))
	manager := &Manager{Store: store, Now: time.Now}
	watcher, err := NewWatcher(manager)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	destinationRoot := filepath.Join(root, "downloads")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := watcher.AddDir(destinationRoot); err != nil {
		t.Fatal(err)
	}

	nested := filepath.Join(destinationRoot, "Album", "nested")
	if err := os.Mkdir(filepath.Join(destinationRoot, "Album"), 0o700); err != nil {
		t.Fatal(err)
	}
	waitForWatchedDir(t, watcher, filepath.Join(destinationRoot, "Album"))
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	waitForWatchedDir(t, watcher, nested)

	file := filepath.Join(nested, "photo.jpg")

	if err := store.PutManifest(state.Manifest{
		ID:        "nested-job",
		Direction: "phone-to-laptop",
		Entries: []state.ManifestEntry{{
			ID:          "nested-entry",
			Source:      "/phone/Album/nested/photo.jpg",
			Destination: file,
			Size:        int64(len("partial-photo")),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(file, []byte("partial-photo"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, candidate := range store.Candidates() {
			if candidate.Source == file {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nested destination file was not preserved")
}

func TestWatcherIgnoresResumexferOwnPartialSidecar(t *testing.T) {
	root := t.TempDir()
	store, _ := state.Open(filepath.Join(root, "state.json"))
	manager := &Manager{Store: store, Now: time.Now}
	watcher, err := NewWatcher(manager)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	if err := watcher.AddDir(root); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "movie.bin.resumexfer-part"), []byte("ours"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if got := len(store.Candidates()); got != 0 {
		t.Fatalf("own sidecar created %d preservation candidates", got)
	}
}

func waitForWatchedDir(t *testing.T, watcher *Watcher, dir string) {
	t.Helper()
	want, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		watcher.mu.RLock()
		found := false
		for _, got := range watcher.dirs {
			if got == want {
				found = true
				break
			}
		}
		watcher.mu.RUnlock()
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("directory was not watched: %s", want)
}
