package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestStorePersistsTransferAndChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	tr := Transfer{ID: "t1", Source: "/phone/movie.mkv", Destination: "/tmp/movie.mkv", Size: 20 << 30, ChunkSize: 4 << 20, CreatedAt: time.Unix(100, 0), UpdatedAt: time.Unix(100, 0)}
	if err := s.PutTransfer(tr); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckpointChunks("t1", map[int]Chunk{0: {Hash: "a", Size: 4 << 20}, 1: {Hash: "b", Size: 4 << 20}}); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Transfer("t1")
	if !ok {
		t.Fatal("missing transfer")
	}
	if got.Size != tr.Size || len(got.Chunks) != 2 || got.Chunks[1].Hash != "b" {
		t.Fatalf("bad transfer: %#v", got)
	}
}

func TestStorePrunesExpiredEphemeralRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Unix(10, 0)
	fresh := time.Unix(1000, 0)
	if err := s.PutIntent(Intent{ID: "old", CreatedAt: old, UpdatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutIntent(Intent{ID: "fresh", CreatedAt: fresh, UpdatedAt: fresh}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCandidate(Candidate{ID: "cold", UpdatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutCandidate(Candidate{ID: "cfresh", UpdatedAt: fresh}); err != nil {
		t.Fatal(err)
	}

	if err := s.Prune(time.Unix(2000, 0), 1500*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Intent("old"); ok {
		t.Fatal("old intent survived")
	}
	if _, ok := s.Candidate("cold"); ok {
		t.Fatal("old candidate survived")
	}
	if _, ok := s.Intent("fresh"); !ok {
		t.Fatal("fresh intent removed")
	}
	if _, ok := s.Candidate("cfresh"); !ok {
		t.Fatal("fresh candidate removed")
	}
}

func TestStorePrunesExpiredRecoveryState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Unix(10, 0)
	fresh := time.Unix(1000, 0)
	if err := s.PutTransfer(Transfer{ID: "t-old", CreatedAt: old, UpdatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutTransfer(Transfer{ID: "t-fresh", CreatedAt: fresh, UpdatedAt: fresh}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{ID: "m-old", UpdatedAt: old}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutManifest(Manifest{ID: "m-fresh", UpdatedAt: fresh}); err != nil {
		t.Fatal(err)
	}

	if err := s.Prune(time.Unix(2000, 0), 1500*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Transfer("t-old"); ok {
		t.Fatal("old transfer survived")
	}
	if _, ok := s.Manifest("m-old"); ok {
		t.Fatal("old manifest survived")
	}
	if _, ok := s.Transfer("t-fresh"); !ok {
		t.Fatal("fresh transfer removed")
	}
	if _, ok := s.Manifest("m-fresh"); !ok {
		t.Fatal("fresh manifest removed")
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.PutIntent(Intent{ID: "i", URIs: []string{"a"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Intent("i")
	got.URIs[0] = "mutated"
	again, _ := s.Intent("i")
	if again.URIs[0] != "a" {
		t.Fatal("store leaked mutable slice")
	}
}
