package portal

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/state"
)

func batchFingerprint(t *testing.T, data []byte) fingerprint.Fingerprint {
	t.Helper()
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	return fp
}

func batchFingerprintPtr(fp fingerprint.Fingerprint) *fingerprint.Fingerprint {
	return &fp
}

func TestManifestShareContinuesEveryRemainingLaptopToPhoneEntry(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(sourceRoot, "folder", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	completedData := []byte("already-over-usb")
	firstData := []byte("wifi-first-pending")
	secondData := []byte("wifi-second-pending")
	completedSource := filepath.Join(sourceRoot, "folder", "done.bin")
	firstSource := filepath.Join(sourceRoot, "folder", "dup.bin")
	secondSource := filepath.Join(sourceRoot, "folder", "sub", "dup.bin")
	for path, data := range map[string][]byte{
		completedSource: completedData,
		firstSource:     firstData,
		secondSource:    secondData,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	destinationRoot := filepath.Join(root, "phone")
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := state.Manifest{
		ID:                "manifest-share-batch",
		Direction:         "laptop-to-phone",
		DestinationRoot:   destinationRoot,
		Managed:           true,
		AwaitingReconnect: true,
		Paused:            true,
		Entries: []state.ManifestEntry{
			{
				ID:                "done",
				Source:            completedSource,
				Destination:       filepath.Join(destinationRoot, "folder", "done.bin"),
				Size:              int64(len(completedData)),
				BytesDone:         int64(len(completedData)),
				Complete:          true,
				SourceFingerprint: batchFingerprintPtr(batchFingerprint(t, completedData)),
			},
			{
				ID:                "first",
				Source:            firstSource,
				Destination:       filepath.Join(destinationRoot, "folder", "dup.bin"),
				Size:              int64(len(firstData)),
				BytesDone:         4,
				SourceFingerprint: batchFingerprintPtr(batchFingerprint(t, firstData)),
			},
			{
				ID:                "second",
				Source:            secondSource,
				Destination:       filepath.Join(destinationRoot, "folder", "sub", "dup.bin"),
				Size:              int64(len(secondData)),
				SourceFingerprint: batchFingerprintPtr(batchFingerprint(t, secondData)),
			},
		},
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartManifestShare(manifest.ID)
	if err != nil {
		t.Fatal(err)
	}
	pageResp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	pageBody, _ := io.ReadAll(pageResp.Body)
	pageResp.Body.Close()
	if !bytes.Contains(pageBody, []byte("This interrupted USB job has 2 remaining files")) ||
		!bytes.Contains(pageBody, []byte("Download remaining 2 files as ZIP")) {
		t.Fatalf("managed share page does not explain whole-job ZIP fallback: %q", pageBody)
	}
	if info.TotalFiles != 2 || info.TotalBytes != int64(len(firstData)+len(secondData)) {
		t.Fatalf("manifest share info=%#v", info)
	}
	manager.mu.Lock()
	session := manager.sessions[info.ID]
	manager.mu.Unlock()
	if session == nil || len(session.Entries) != 2 {
		t.Fatalf("manifest share session entries=%v", session)
	}
	gotNames := []string{filepath.ToSlash(session.Entries[0].ZipName), filepath.ToSlash(session.Entries[1].ZipName)}
	sort.Strings(gotNames)
	wantNames := []string{"folder/dup.bin", "folder/sub/dup.bin"}
	sort.Strings(wantNames)
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("manifest share paths=%v want=%v", gotNames, wantNames)
	}
	for _, entry := range session.Entries {
		if entry.ManifestEntryID == "" || entry.Path == completedSource {
			t.Fatalf("manifest share included wrong entry: %#v", entry)
		}
	}

	resp, err := http.Get(info.URLs[0] + "all.zip")
	if err != nil {
		t.Fatal(err)
	}
	archiveBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("zip status=%d body=%q", resp.StatusCode, archiveBytes)
	}
	zr, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		t.Fatal(err)
	}
	zipNames := make([]string, 0, len(zr.File))
	for _, item := range zr.File {
		zipNames = append(zipNames, item.Name)
	}
	sort.Strings(zipNames)
	if !reflect.DeepEqual(zipNames, wantNames) {
		t.Fatalf("zip entries=%v want=%v", zipNames, wantNames)
	}

	updated, _ := store.Manifest(manifest.ID)
	if len(updated.Pending()) != 0 || updated.AwaitingReconnect || updated.Paused {
		t.Fatalf("manifest was not completed by whole-job Wi-Fi handoff: %#v", updated)
	}
	for _, entry := range updated.Entries {
		if !entry.Complete || entry.BytesDone != entry.Size {
			t.Fatalf("entry not complete after Wi-Fi handoff: %#v", entry)
		}
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || snapshot.DoneFiles != 2 || snapshot.BytesDone != snapshot.TotalBytes {
		t.Fatalf("manifest share snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestManifestShareHandles207FileUSBToWiFiJob(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	destinationRoot := filepath.Join(root, "phone")
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	const totalFiles = 207
	const completedUSB = 55
	entries := make([]state.ManifestEntry, 0, totalFiles)
	pendingNames := make(map[string]bool)
	for i := 0; i < totalFiles; i++ {
		rel := filepath.Join("folder", fmt.Sprintf("group-%02d", i%9), fmt.Sprintf("file-%03d.bin", i))
		source := filepath.Join(sourceRoot, rel)
		if err := os.MkdirAll(filepath.Dir(source), 0o755); err != nil {
			t.Fatal(err)
		}
		data := []byte(fmt.Sprintf("payload-%03d", i))
		if err := os.WriteFile(source, data, 0o600); err != nil {
			t.Fatal(err)
		}
		entry := state.ManifestEntry{
			ID:          fmt.Sprintf("entry-%03d", i),
			Source:      source,
			Destination: filepath.Join(destinationRoot, rel),
			Size:        int64(len(data)),
		}
		if i < completedUSB {
			entry.BytesDone = entry.Size
			entry.Complete = true
		} else {
			pendingNames[filepath.ToSlash(rel)] = true
			if i == completedUSB {
				entry.BytesDone = 2
			}
		}
		entries = append(entries, entry)
	}

	manifest := state.Manifest{
		ID:                "manifest-share-207",
		Direction:         "laptop-to-phone",
		DestinationRoot:   destinationRoot,
		Managed:           true,
		AwaitingReconnect: true,
		Paused:            true,
		Entries:           entries,
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartManifestShare(manifest.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantPending := totalFiles - completedUSB
	if info.TotalFiles != wantPending {
		t.Fatalf("pending share files=%d want=%d", info.TotalFiles, wantPending)
	}
	pageResp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(pageResp.Body)
	pageResp.Body.Close()
	if !bytes.Contains(page, []byte("This interrupted USB job has 152 remaining files")) ||
		!bytes.Contains(page, []byte("Download remaining 152 files as ZIP")) {
		t.Fatalf("207-file handoff page did not expose all pending files")
	}

	resp, err := http.Get(info.URLs[0] + "all.zip")
	if err != nil {
		t.Fatal(err)
	}
	archiveBytes, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("207-file zip status=%d", resp.StatusCode)
	}
	zr, err := zip.NewReader(bytes.NewReader(archiveBytes), int64(len(archiveBytes)))
	if err != nil {
		t.Fatal(err)
	}
	if len(zr.File) != wantPending {
		t.Fatalf("207-file zip entries=%d want=%d", len(zr.File), wantPending)
	}
	for _, item := range zr.File {
		if !pendingNames[item.Name] {
			t.Fatalf("ZIP unexpectedly included completed/unknown path %q", item.Name)
		}
		delete(pendingNames, item.Name)
	}
	if len(pendingNames) != 0 {
		t.Fatalf("ZIP missed %d pending files", len(pendingNames))
	}
	updated, _ := store.Manifest(manifest.ID)
	if len(updated.Pending()) != 0 || updated.AwaitingReconnect || updated.Paused {
		t.Fatalf("207-file laptop-to-phone handoff did not finish manifest")
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || !snapshot.Complete || snapshot.DoneFiles != wantPending || snapshot.TotalFiles != wantPending {
		t.Fatalf("207-file share snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestManagedReceiveHandles207FileUSBToWiFiJob(t *testing.T) {
	root := t.TempDir()
	destinationRoot := filepath.Join(root, "destination")
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	const totalFiles = 207
	const completedUSB = 55
	entries := make([]state.ManifestEntry, 0, totalFiles)
	dataByID := make(map[string][]byte, totalFiles)
	currentID := fmt.Sprintf("entry-%03d", completedUSB)
	for i := 0; i < totalFiles; i++ {
		rel := filepath.Join("folder", fmt.Sprintf("group-%02d", i%9), fmt.Sprintf("file-%03d.bin", i))
		data := []byte(fmt.Sprintf("phone-payload-%03d", i))
		id := fmt.Sprintf("entry-%03d", i)
		dataByID[id] = data
		entry := state.ManifestEntry{
			ID:          id,
			Source:      "mtp://phone/" + filepath.ToSlash(rel),
			Destination: filepath.Join(destinationRoot, rel),
			Size:        int64(len(data)),
		}
		if i < completedUSB {
			entry.BytesDone = entry.Size
			entry.Complete = true
		} else if i == completedUSB {
			entry.BytesDone = 3
			entry.CreatedByJob = true
			fp := batchFingerprint(t, data)
			entry.SourceFingerprint = &fp
			if err := os.MkdirAll(filepath.Dir(entry.Destination), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(entry.Destination+engine.PartialSuffix, data[:3], 0o600); err != nil {
				t.Fatal(err)
			}
		}
		entries = append(entries, entry)
	}

	manifest := state.Manifest{
		ID:                "manifest-receive-207",
		Direction:         "phone-to-laptop",
		DestinationRoot:   destinationRoot,
		Managed:           true,
		AwaitingReconnect: true,
		Entries:           entries,
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartManifestReceive(manifest.ID, currentID)
	if err != nil {
		t.Fatal(err)
	}
	wantPending := totalFiles - completedUSB
	if info.TotalFiles != wantPending {
		t.Fatalf("managed receive pending=%d want=%d", info.TotalFiles, wantPending)
	}
	pageResp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(pageResp.Body)
	pageResp.Body.Close()
	if !bytes.Contains(page, []byte("Continue the interrupted 152-file job over Wi-Fi")) ||
		!bytes.Contains(page, []byte("webkitdirectory")) {
		t.Fatalf("207-file receive page did not expose whole-job folder continuation")
	}

	for i := completedUSB; i < totalFiles; i++ {
		id := fmt.Sprintf("entry-%03d", i)
		data := dataByID[id]
		rel := filepath.Join("folder", fmt.Sprintf("group-%02d", i%9), fmt.Sprintf("file-%03d.bin", i))
		fp := batchFingerprint(t, data)
		init := postManagedInitRelative(t, info.URLs[0], filepath.Base(rel), filepath.ToSlash(rel), data, fp, http.StatusOK)
		if i == completedUSB {
			if init.Offset != 3 {
				t.Fatalf("current 207-file entry offset=%d want=3", init.Offset)
			}
			verifyManagedPrefix(t, info.URLs[0], init.UploadID, 3, sha256Hex(data[:3]), http.StatusOK)
			putChunk(t, info.URLs[0], init.UploadID, 3, data[3:], http.StatusOK)
		} else {
			if init.Offset != 0 {
				t.Fatalf("untouched 207-file entry %d offset=%d want=0", i, init.Offset)
			}
			verifyManagedPrefix(t, info.URLs[0], init.UploadID, 0, sha256Hex(nil), http.StatusOK)
			putChunk(t, info.URLs[0], init.UploadID, 0, data, http.StatusOK)
		}
	}

	updated, _ := store.Manifest(manifest.ID)
	if len(updated.Pending()) != 0 || updated.AwaitingReconnect || updated.Paused {
		t.Fatalf("207-file phone-to-laptop handoff did not finish manifest")
	}
	for i := completedUSB; i < totalFiles; i++ {
		rel := filepath.Join("folder", fmt.Sprintf("group-%02d", i%9), fmt.Sprintf("file-%03d.bin", i))
		got, err := os.ReadFile(filepath.Join(destinationRoot, rel))
		if err != nil || !bytes.Equal(got, dataByID[fmt.Sprintf("entry-%03d", i)]) {
			t.Fatalf("207-file destination mismatch at %d err=%v", i, err)
		}
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || snapshot.DoneFiles != wantPending || snapshot.TotalFiles != wantPending || snapshot.BytesDone != snapshot.TotalBytes {
		t.Fatalf("207-file receive snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestManagedReceiveContinuesEveryRemainingPhoneToLaptopEntry(t *testing.T) {
	root := t.TempDir()
	destinationRoot := filepath.Join(root, "destination")
	if err := os.MkdirAll(filepath.Join(destinationRoot, "folder", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	firstData := []byte("phone-first-file-through-usb-then-wifi")
	secondData := []byte("phone-second-file-over-wifi")
	firstDestination := filepath.Join(destinationRoot, "folder", "dup.bin")
	secondDestination := filepath.Join(destinationRoot, "folder", "sub", "dup.bin")
	firstPartial := firstDestination + engine.PartialSuffix
	if err := os.WriteFile(firstPartial, firstData[:8], 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := state.Manifest{
		ID:                "manifest-receive-batch",
		Direction:         "phone-to-laptop",
		DestinationRoot:   destinationRoot,
		Managed:           true,
		AwaitingReconnect: true,
		Entries: []state.ManifestEntry{
			{
				ID:           "first",
				Source:       "mtp://phone/folder/dup.bin",
				Destination:  firstDestination,
				Size:         int64(len(firstData)),
				BytesDone:    8,
				CreatedByJob: true,
			},
			{
				ID:          "second",
				Source:      "mtp://phone/folder/sub/dup.bin",
				Destination: secondDestination,
				Size:        int64(len(secondData)),
			},
		},
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartManifestReceive(manifest.ID, "first")
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalFiles != 2 || info.BytesDone != 8 || info.TotalBytes != int64(len(firstData)+len(secondData)) {
		t.Fatalf("managed batch info=%#v", info)
	}
	resp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	page, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(page, []byte("2-file job")) ||
		!bytes.Contains(page, []byte("id=\"files\" type=\"file\" multiple")) ||
		!bytes.Contains(page, []byte("webkitdirectory")) {
		t.Fatalf("managed multi-file page does not expose batch selection: %q", page)
	}

	firstInit := postManagedInitRelative(t, info.URLs[0], "dup.bin", "folder/dup.bin", firstData, batchFingerprint(t, firstData), http.StatusOK)
	if firstInit.Offset != 8 || !firstInit.NeedsVerification {
		t.Fatalf("first init=%#v", firstInit)
	}
	verifyManagedPrefix(t, info.URLs[0], firstInit.UploadID, 8, sha256Hex(firstData[:8]), http.StatusOK)
	putChunk(t, info.URLs[0], firstInit.UploadID, 8, firstData[8:], http.StatusOK)

	mid, _ := store.Manifest(manifest.ID)
	if !mid.Entries[0].Complete || mid.Entries[1].Complete || !mid.AwaitingReconnect {
		t.Fatalf("job completed too early after first Wi-Fi file: %#v", mid)
	}
	midStatus, ok := manager.Snapshot(info.ID)
	if !ok || midStatus.DoneFiles != 1 || midStatus.TotalFiles != 2 {
		t.Fatalf("mid-session snapshot=%#v ok=%v", midStatus, ok)
	}

	secondInit := postManagedInitRelative(t, info.URLs[0], "dup.bin", "folder/sub/dup.bin", secondData, batchFingerprint(t, secondData), http.StatusOK)
	if secondInit.Offset != 0 || !secondInit.NeedsVerification {
		t.Fatalf("second init=%#v", secondInit)
	}
	verifyManagedPrefix(t, info.URLs[0], secondInit.UploadID, 0, sha256Hex(nil), http.StatusOK)
	putChunk(t, info.URLs[0], secondInit.UploadID, 0, secondData, http.StatusOK)

	firstGot, err := os.ReadFile(firstDestination)
	if err != nil || !bytes.Equal(firstGot, firstData) {
		t.Fatalf("first final=%q err=%v", firstGot, err)
	}
	secondGot, err := os.ReadFile(secondDestination)
	if err != nil || !bytes.Equal(secondGot, secondData) {
		t.Fatalf("second final=%q err=%v", secondGot, err)
	}
	updated, _ := store.Manifest(manifest.ID)
	if len(updated.Pending()) != 0 || updated.AwaitingReconnect || updated.Paused {
		t.Fatalf("managed receive job not terminal after all Wi-Fi files: %#v", updated)
	}
	if updated.Entries[1].SourceFingerprint == nil || !updated.Entries[1].CreatedByJob {
		t.Fatalf("untouched second entry did not acquire durable Wi-Fi identity/ownership: %#v", updated.Entries[1])
	}
	status, ok := manager.Snapshot(info.ID)
	if !ok || status.DoneFiles != status.TotalFiles || status.DoneFiles != 2 || status.BytesDone != status.TotalBytes {
		t.Fatalf("final managed batch status=%#v ok=%v", status, ok)
	}
}

func TestManagedReceiveUntouchedWifiEntrySurvivesSecondDisconnect(t *testing.T) {
	root := t.TempDir()
	destinationRoot := filepath.Join(root, "destination")
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	data := []byte("untouched-on-usb-then-wifi-disconnects-again")
	destination := filepath.Join(destinationRoot, "folder", "new.bin")

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := state.Manifest{
		ID:                "manifest-second-disconnect",
		Direction:         "phone-to-laptop",
		DestinationRoot:   destinationRoot,
		Managed:           true,
		AwaitingReconnect: true,
		Entries: []state.ManifestEntry{{
			ID:          "untouched",
			Source:      "mtp://phone/folder/new.bin",
			Destination: destination,
			Size:        int64(len(data)),
		}},
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	first, err := manager.StartManifestReceive(manifest.ID, "untouched")
	if err != nil {
		t.Fatal(err)
	}
	fp := batchFingerprint(t, data)
	init := postManagedInitRelative(t, first.URLs[0], "new.bin", "folder/new.bin", data, fp, http.StatusOK)
	if init.Offset != 0 || !init.NeedsVerification {
		t.Fatalf("first init=%#v", init)
	}
	verifyManagedPrefix(t, first.URLs[0], init.UploadID, 0, sha256Hex(nil), http.StatusOK)

	cut := int64(11)
	putChunk(t, first.URLs[0], init.UploadID, 0, data[:cut], http.StatusOK)
	mid, _ := store.Manifest(manifest.ID)
	if mid.Entries[0].SourceFingerprint == nil || !mid.Entries[0].CreatedByJob || mid.Entries[0].BytesDone != cut || mid.Entries[0].Complete {
		t.Fatalf("mid-Wi-Fi state not durable: %#v", mid.Entries[0])
	}
	if err := manager.CloseSession(first.ID); err != nil {
		t.Fatal(err)
	}

	second, err := manager.StartManifestReceive(manifest.ID, "untouched")
	if err != nil {
		t.Fatal(err)
	}
	init2 := postManagedInitRelative(t, second.URLs[0], "new.bin", "folder/new.bin", data, fp, http.StatusOK)
	if init2.Offset != cut || !init2.NeedsVerification {
		t.Fatalf("second init=%#v want offset=%d", init2, cut)
	}
	verifyManagedPrefix(t, second.URLs[0], init2.UploadID, cut, sha256Hex(data[:cut]), http.StatusOK)
	putChunk(t, second.URLs[0], init2.UploadID, cut, data[cut:], http.StatusOK)

	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("final file=%q err=%v", got, err)
	}
	final, _ := store.Manifest(manifest.ID)
	if len(final.Pending()) != 0 || final.AwaitingReconnect || !final.Entries[0].Complete || final.Entries[0].BytesDone != int64(len(data)) {
		t.Fatalf("final manifest=%#v", final)
	}
}
