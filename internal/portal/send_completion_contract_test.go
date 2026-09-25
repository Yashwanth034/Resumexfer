package portal

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestSendSnapshotWaitsForDeliveryFinalizationAfterBytesReach100Percent(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "movie.bin")
	data := []byte("all-source-bytes-have-been-read")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	s := manager.sessions[info.ID]
	manager.mu.RUnlock()
	if s == nil {
		t.Fatal("send session missing")
	}

	// Reproduce the real UI failure: every source byte is accounted for, but
	// the delivery response has not been finalized yet.
	manager.recordDownload(s, 0, 0, int64(len(data)))
	mid, ok := manager.Snapshot(info.ID)
	if !ok {
		t.Fatal("send session disappeared")
	}
	if mid.BytesDone != mid.TotalBytes {
		t.Fatalf("mid bytes=%d total=%d", mid.BytesDone, mid.TotalBytes)
	}
	if mid.DoneFiles != 0 || mid.Complete {
		t.Fatalf("100%% bytes must not imply finalized delivery: %#v", mid)
	}

	if err := manager.markDeliveryComplete(s, 0); err != nil {
		t.Fatal(err)
	}
	final, ok := manager.Snapshot(info.ID)
	if !ok || !final.Complete || final.DoneFiles != final.TotalFiles || final.DoneFiles != 1 {
		t.Fatalf("finalized send snapshot=%#v ok=%v", final, ok)
	}
}

func TestCompletedSendDeliveryStateSurvivesPortalRestart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "stable.bin")
	data := []byte("persist-completed-delivery")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "portal.json")
	config := Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	}
	manager, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartShare([]string{path})
	if err != nil {
		_ = manager.Close()
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0] + "file/0")
	if err != nil {
		_ = manager.Close()
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	before, ok := manager.Snapshot(info.ID)
	if !ok || !before.Complete || before.DoneFiles != 1 {
		_ = manager.Close()
		t.Fatalf("before restart snapshot=%#v ok=%v", before, ok)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	after, ok := restored.Snapshot(info.ID)
	if !ok || !after.Complete || after.DoneFiles != 1 || after.BytesDone != after.TotalBytes {
		t.Fatalf("restored completed delivery=%#v ok=%v", after, ok)
	}
}

func TestZeroByteSharedFileCompletesOnlyAfterGET(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.txt")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	head, err := http.Head(info.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	afterHead, _ := manager.Snapshot(info.ID)
	if afterHead.Complete || afterHead.DoneFiles != 0 {
		t.Fatalf("HEAD incorrectly finalized zero-byte file: %#v", afterHead)
	}

	resp, err := http.Get(info.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status=%d", resp.StatusCode)
	}
	final, _ := manager.Snapshot(info.ID)
	if !final.Complete || final.DoneFiles != 1 || final.BytesDone != 0 || final.TotalBytes != 0 {
		t.Fatalf("zero-byte delivery snapshot=%#v", final)
	}
}

func TestZipDeliveryFinalizesAllFilesIncludingZeroByteEntries(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first.bin")
	empty := filepath.Join(root, "empty.bin")
	if err := os.WriteFile(first, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartShare([]string{first, empty})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0] + "all.zip")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("zip status=%d", resp.StatusCode)
	}
	final, _ := manager.Snapshot(info.ID)
	if !final.Complete || final.DoneFiles != 2 || final.DoneFiles != final.TotalFiles || final.BytesDone != final.TotalBytes {
		t.Fatalf("zip completion snapshot=%#v", final)
	}
}
