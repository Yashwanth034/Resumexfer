package state

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestManifestPersists2000EntriesAsOneRecord(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "state.json"))
	entries := make([]ManifestEntry, 2000)
	for i := range entries {
		entries[i] = ManifestEntry{ID: fmt.Sprintf("f-%04d", i), Source: fmt.Sprintf("/src/%d", i), Destination: fmt.Sprintf("/dst/%d", i), Size: int64(i + 1)}
	}
	if err := s.PutManifest(Manifest{ID: "job", Direction: "upload", Entries: entries}); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Manifest("job")
	if !ok || len(got.Entries) != 2000 {
		t.Fatalf("bad manifest: %v %d", ok, len(got.Entries))
	}
}

func TestManifestSkipsCompletedEntries(t *testing.T) {
	s, _ := Open(filepath.Join(t.TempDir(), "state.json"))
	entries := make([]ManifestEntry, 200)
	for i := range entries {
		entries[i] = ManifestEntry{ID: fmt.Sprintf("f-%03d", i)}
	}
	if err := s.PutManifest(Manifest{ID: "job", Entries: entries}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 125; i++ {
		if err := s.MarkManifestEntryComplete("job", entries[i].ID); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := s.Manifest("job")
	pending := got.Pending()
	if len(pending) != 75 {
		t.Fatalf("got %d pending", len(pending))
	}
	if pending[0].ID != "f-125" {
		t.Fatalf("first pending %s", pending[0].ID)
	}
}

func TestManifestReconnectStatePersistsAndEnumerates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, _ := Open(path)
	manifest := Manifest{ID: "job", Direction: "phone-to-laptop", Entries: []ManifestEntry{{ID: "a", Source: "/phone/a", Destination: "/laptop/a"}}}
	if err := s.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestAwaitingReconnect("job", true); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Manifest("job")
	if !ok || !got.AwaitingReconnect {
		t.Fatalf("reconnect state not persisted: %#v, ok=%v", got, ok)
	}
	all := reopened.Manifests()
	if len(all) != 1 || all[0].ID != "job" || !all[0].AwaitingReconnect {
		t.Fatalf("manifest enumeration mismatch: %#v", all)
	}
	all[0].Entries[0].Source = "mutated"
	again, _ := reopened.Manifest("job")
	if again.Entries[0].Source != "/phone/a" {
		t.Fatal("Manifests returned aliased state")
	}
}

func TestManifestUploadStartedPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := s.PutManifest(Manifest{
		ID:        "upload-job",
		Direction: "laptop-to-phone",
		Entries: []ManifestEntry{{
			ID:          "movie",
			Source:      "/laptop/movie.bin",
			Destination: "/phone/movie.bin",
			Size:        123,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.SetManifestUploadStarted("upload-job", true); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	got, ok := reopened.Manifest("upload-job")
	if !ok {
		t.Fatal("manifest disappeared after reopen")
	}
	if !got.UploadStarted {
		t.Fatalf("upload-started state did not persist: %#v", got)
	}
}

func TestManagedManifestProgressPersistsAndCompletionClampsToSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{
		ID:        "managed-job",
		Direction: "laptop-to-phone",
		Entries: []ManifestEntry{{
			ID:   "movie",
			Size: 100,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestManaged("managed-job", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestEntryProgress("managed-job", "movie", 40); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestLastError("managed-job", "temporary"); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Manifest("managed-job")
	if !ok || !got.Managed || got.Entries[0].BytesDone != 40 || got.LastError != "temporary" {
		t.Fatalf("managed state mismatch: %#v", got)
	}

	if err := reopened.SetManifestEntryProgress("managed-job", "movie", 1000); err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkManifestEntryComplete("managed-job", "movie"); err != nil {
		t.Fatal(err)
	}
	got, _ = reopened.Manifest("managed-job")
	if !got.Entries[0].Complete || got.Entries[0].BytesDone != 100 {
		t.Fatalf("completed progress = %#v", got.Entries[0])
	}
}

func TestManagedManifestUserControlsPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{
		ID:        "controlled-job",
		Direction: "laptop-to-phone",
		Managed:   true,
		Entries: []ManifestEntry{{
			ID:   "movie",
			Size: 100,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestPaused("controlled-job", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestCancelRequested("controlled-job", true); err != nil {
		t.Fatal(err)
	}
	if err := s.SetManifestCancelled("controlled-job", true); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Manifest("controlled-job")
	if !ok {
		t.Fatal("controlled manifest disappeared")
	}
	if !got.Cancelled || got.CancelRequested || got.Paused || got.AwaitingReconnect {
		t.Fatalf("unexpected terminal control state: %#v", got)
	}
}

func TestManagedCancelRequestPersistsModeAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{
		ID:        "cancel-mode-job",
		Direction: "laptop-to-phone",
		Managed:   true,
		Paused:    true,
		Entries: []ManifestEntry{{
			ID:           "movie",
			Size:         100,
			CreatedByJob: true,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.RequestManifestCancel("cancel-mode-job", CancelModeUndoAll); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Manifest("cancel-mode-job")
	if !ok {
		t.Fatal("manifest disappeared")
	}
	if !got.CancelRequested || got.CancelMode != CancelModeUndoAll || got.Paused {
		t.Fatalf("cancel request state = %#v", got)
	}
	if !got.Entries[0].CreatedByJob {
		t.Fatalf("entry ownership did not persist: %#v", got.Entries[0])
	}
}

func TestManagedCancelRequestRejectsUnknownMode(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{ID: "job", Managed: true, Entries: []ManifestEntry{{ID: "entry"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestManifestCancel("job", "delete-everything-ever"); err == nil {
		t.Fatal("unknown cancel mode was accepted")
	}
}

func TestManagedManifestCanRecordExactDuplicateSkip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{
		ID:        "duplicate-job",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []ManifestEntry{{
			ID:   "same-file",
			Size: 123,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkManifestEntryDuplicate("duplicate-job", "same-file"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Manifest("duplicate-job")
	if !ok {
		t.Fatal("manifest disappeared")
	}
	entry := got.Entries[0]
	if !entry.Complete || !entry.Duplicate || entry.BytesDone != entry.Size || entry.CreatedByJob {
		t.Fatalf("duplicate state = %#v", entry)
	}
}

func TestDuplicateMatchPathPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, _ := Open(path)
	if err := s.PutManifest(Manifest{
		ID: "job", DestinationRoot: "/dst", Managed: true,
		Entries: []ManifestEntry{{ID: "entry", Source: "/src/copy.bin", Destination: "/dst/copy.bin", Size: 123}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkManifestEntryDuplicateOf("job", "entry", "/dst/original.bin"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, ok := reopened.Manifest("job")
	if !ok || manifest.DestinationRoot != "/dst" {
		t.Fatalf("manifest = %#v, ok=%v", manifest, ok)
	}
	entry := manifest.Entries[0]
	if !entry.Duplicate || entry.DuplicateOf != "/dst/original.bin" || !entry.Complete || entry.BytesDone != 123 {
		t.Fatalf("entry = %#v", entry)
	}
}

func TestManifestTransportLeaseSerializesWritersAndRejectsStaleRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{ID: "job", Managed: true, Entries: []ManifestEntry{{ID: "entry"}}}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1000, 0)
	first, err := s.AcquireManifestTransport("job", "usb:worker-a", now, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation == 0 || first.Owner != "usb:worker-a" || !first.ExpiresAt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("first lease = %#v", first)
	}
	if _, err := s.AcquireManifestTransport("job", "wifi:session-a", now.Add(time.Second), 30*time.Second); !errors.Is(err, ErrManifestTransportBusy) {
		t.Fatalf("competing transport err=%v", err)
	}
	refreshed, err := s.RefreshManifestTransport("job", first.Owner, first.Generation, now.Add(10*time.Second), 45*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !refreshed.ExpiresAt.Equal(now.Add(55 * time.Second)) {
		t.Fatalf("refreshed lease = %#v", refreshed)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	persisted, ok := reopened.Manifest("job")
	if !ok || persisted.TransportOwner != first.Owner || persisted.TransportGeneration != first.Generation || !persisted.TransportLeaseUntil.Equal(refreshed.ExpiresAt) {
		t.Fatalf("persisted manifest lease = %#v ok=%v", persisted, ok)
	}

	second, err := reopened.AcquireManifestTransport("job", "wifi:session-a", refreshed.ExpiresAt.Add(time.Second), 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation == first.Generation || second.Owner != "wifi:session-a" {
		t.Fatalf("replacement lease = %#v first=%#v", second, first)
	}
	if err := reopened.ReleaseManifestTransport("job", first.Owner, first.Generation); !errors.Is(err, ErrManifestTransportLeaseLost) {
		t.Fatalf("stale release err=%v", err)
	}
	current, _ := reopened.Manifest("job")
	if current.TransportOwner != second.Owner || current.TransportGeneration != second.Generation {
		t.Fatalf("stale release changed owner: %#v", current)
	}
	if err := reopened.ReleaseManifestTransport("job", second.Owner, second.Generation); err != nil {
		t.Fatal(err)
	}
	current, _ = reopened.Manifest("job")
	if current.TransportOwner != "" || !current.TransportLeaseUntil.IsZero() || current.TransportGeneration != second.Generation {
		t.Fatalf("released manifest = %#v", current)
	}
}

func TestManifestTransportLeaseOnlyAllowsOneConcurrentOwner(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{ID: "job", Entries: []ManifestEntry{{ID: "entry"}}}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2000, 0)
	var wg sync.WaitGroup
	results := make(chan string, 24)
	for i := 0; i < 24; i++ {
		owner := fmt.Sprintf("transport-%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.AcquireManifestTransport("job", owner, now, time.Minute); err == nil {
				results <- owner
			} else if !errors.Is(err, ErrManifestTransportBusy) {
				t.Errorf("acquire %s: %v", owner, err)
			}
		}()
	}
	wg.Wait()
	close(results)
	var winners []string
	for owner := range results {
		winners = append(winners, owner)
	}
	if len(winners) != 1 {
		t.Fatalf("owners that acquired lease = %#v", winners)
	}
	manifest, _ := s.Manifest("job")
	if manifest.TransportOwner != winners[0] {
		t.Fatalf("manifest owner=%q winners=%#v", manifest.TransportOwner, winners)
	}
}
