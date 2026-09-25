package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/gvfs"
	"resumexfer/internal/portal"
	"resumexfer/internal/recovery"
	"resumexfer/internal/state"
)

type recordingCleaner struct {
	mu        sync.Mutex
	called    chan struct{}
	retention time.Duration
}

func (c *recordingCleaner) Cleanup(retention time.Duration) error {
	c.mu.Lock()
	c.retention = retention
	c.mu.Unlock()
	select {
	case c.called <- struct{}{}:
	default:
	}
	return nil
}

func TestServerPersistsIntentOverPrivateSocket(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	socket := ControlSocketPath(runtimeDir)
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	server := New(Config{SocketPath: socket, Store: store})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, socket)

	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o", info.Mode().Perm())
	}
	parentInfo, err := os.Stat(filepath.Dir(socket))
	if err != nil {
		t.Fatal(err)
	}
	if parentInfo.Mode().Perm() != 0o700 {
		t.Fatalf("socket dir mode = %o", parentInfo.Mode().Perm())
	}

	in := state.Intent{ID: "intent-1", Direction: "laptop-to-phone", URIs: []string{"file:///tmp/a.bin"}}
	if err := RecordIntent(context.Background(), socket, in); err != nil {
		t.Fatal(err)
	}
	got, ok := store.Intent(in.ID)
	if !ok || got.Direction != in.Direction || len(got.URIs) != 1 {
		t.Fatalf("intent not persisted: %#v %v", got, ok)
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
	if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still exists: %v", err)
	}
}

func TestServerRejectsSecondLiveInstance(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	socket := ControlSocketPath(runtimeDir)
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	first := New(Config{SocketPath: socket, Store: store})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go first.Serve(ctx)
	waitForSocket(t, socket)

	second := New(Config{SocketPath: socket, Store: store})
	err := second.Serve(context.Background())
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("got %v, want ErrAlreadyRunning", err)
	}
}

func TestServerRemovesStaleUnixSocket(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	socket := ControlSocketPath(runtimeDir)
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	addr := &net.UnixAddr{Name: socket, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	server := New(Config{SocketPath: socket, Store: store})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, socket)
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestServerCleanupUsesDefault24HourRetention(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	cleaner := &recordingCleaner{called: make(chan struct{}, 1)}
	server := New(Config{
		SocketPath:      ControlSocketPath(runtimeDir),
		Store:           store,
		Cleaner:         cleaner,
		CleanupInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	select {
	case <-cleaner.called:
	case <-time.After(time.Second):
		t.Fatal("cleanup was not called")
	}
	cleaner.mu.Lock()
	got := cleaner.retention
	cleaner.mu.Unlock()
	if got != 24*time.Hour {
		t.Fatalf("retention = %v", got)
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestObservationCorrelatesAndPersistsWholeManifest(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	src := t.TempDir()
	first := filepath.Join(src, "a.bin")
	second := filepath.Join(src, "b.bin")
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("bb"), 0o600); err != nil {
		t.Fatal(err)
	}
	in := state.Intent{ID: "job", Direction: "laptop-to-phone", URIs: []string{fileURI(first), fileURI(second)}}
	if err := store.PutIntent(in); err != nil {
		t.Fatal(err)
	}

	dst := t.TempDir()
	server := New(Config{
		SocketPath: ControlSocketPath(runtimeDir),
		Store:      store,
		Mounts: func() ([]gvfs.Mount, error) {
			return []gvfs.Mount{{Host: "phone", Root: dst}}, nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)
	if err := Observe(context.Background(), server.socketPath, Observation{
		IntentID:    "job",
		Source:      first,
		Destination: filepath.Join(dst, "a.bin"),
	}); err != nil {
		t.Fatal(err)
	}
	manifest, ok := store.Manifest("job")
	if !ok || len(manifest.Entries) != 2 {
		t.Fatalf("manifest = %#v, ok=%v", manifest, ok)
	}
	if manifest.Entries[1].Destination != filepath.Join(dst, "b.bin") {
		t.Fatalf("destination = %q", manifest.Entries[1].Destination)
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestObservationRejectsUnknownIntent(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	server := New(Config{SocketPath: ControlSocketPath(runtimeDir), Store: store})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	err := Observe(context.Background(), server.socketPath, Observation{IntentID: "missing", Source: "/tmp/a", Destination: "/tmp/b"})
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := store.Manifest("missing"); ok {
		t.Fatal("manifest must not be created")
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func shortRuntimeDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rx-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket did not appear: %s", path)
}

func fileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func TestSystemdUserServiceRunsHeadlessDaemonPrivately(t *testing.T) {
	path := filepath.Join("..", "..", "packaging", "systemd", "resumexfer.service")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{
		"ExecStart=/usr/bin/resumexferd",
		"Restart=on-failure",
		"UMask=0077",
		"NoNewPrivileges=true",
		"WantedBy=default.target",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("service missing %q", want)
		}
	}
}

func TestObservationRejectsLocalToLocalCopy(t *testing.T) {
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "a.bin")
	if err := os.WriteFile(src, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{
		ID:        "local-only",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(src)},
	}); err != nil {
		t.Fatal(err)
	}

	server := New(Config{Store: store, Mounts: func() ([]gvfs.Mount, error) { return nil, nil }})
	err = server.observe(Observation{
		IntentID:    "local-only",
		Source:      src,
		Destination: filepath.Join(t.TempDir(), "a.bin"),
	})
	if err == nil {
		t.Fatal("expected local-to-local observation to be rejected")
	}
	if _, ok := store.Manifest("local-only"); ok {
		t.Fatal("local-to-local copy must not create an Android transfer manifest")
	}
}

func TestObservationAcceptsURIsFromNemo(t *testing.T) {
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	mountRoot := t.TempDir()
	phoneDir := filepath.Join(mountRoot, "Internal")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	phoneFile := filepath.Join(phoneDir, "a.bin")
	if err := os.WriteFile(phoneFile, []byte("phone-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	sourceURI := "mtp://phone/Internal/a.bin"
	if err := store.PutIntent(state.Intent{
		ID:        "uri-job",
		Direction: "phone-to-laptop",
		URIs:      []string{sourceURI},
	}); err != nil {
		t.Fatal(err)
	}
	server := New(Config{
		Store: store,
		Mounts: func() ([]gvfs.Mount, error) {
			return []gvfs.Mount{{Host: "phone", Root: mountRoot}}, nil
		},
	})
	destination := filepath.Join(t.TempDir(), "a.bin")
	if err := server.observe(Observation{
		IntentID:    "uri-job",
		Source:      sourceURI,
		Destination: fileURI(destination),
	}); err != nil {
		t.Fatal(err)
	}
	manifest, ok := store.Manifest("uri-job")
	if !ok || len(manifest.Entries) != 1 {
		t.Fatalf("manifest = %#v, ok=%v", manifest, ok)
	}
	if manifest.Entries[0].Source != phoneFile || manifest.Entries[0].Destination != destination {
		t.Fatalf("entry = %#v", manifest.Entries[0])
	}
}

type recordingRecoverer struct {
	mu     sync.Mutex
	called chan string
	count  int
	err    error
}

func (r *recordingRecoverer) RecoverManifest(id string) error {
	r.mu.Lock()
	r.count++
	err := r.err
	r.mu.Unlock()
	select {
	case r.called <- id:
	default:
	}
	return err
}

func TestAutomaticRecoveryRunsOnlyAfterSourceDisappearsAndReturns(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()
	source := filepath.Join(root, "phone", "a.bin")
	destination := filepath.Join(root, "laptop", "a.bin")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("phone-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "phone-to-laptop",
		Entries: []state.ManifestEntry{{
			ID:          "a",
			Source:      source,
			Destination: destination,
			Size:        int64(len("phone-data")),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{
		SocketPath:       ControlSocketPath(runtimeDir),
		Store:            store,
		Recoverer:        recoverer,
		RecoveryInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	select {
	case id := <-recoverer.called:
		t.Fatalf("recovery ran while source was continuously available: %s", id)
	case <-time.After(40 * time.Millisecond):
	}

	if err := os.Remove(source); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("job")
		if manifest.AwaitingReconnect {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.AwaitingReconnect {
		t.Fatal("disconnect was not persisted")
	}

	if err := os.WriteFile(source, []byte("phone-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-recoverer.called:
		if id != "job" {
			t.Fatalf("recovered %q, want job", id)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery did not run after reconnect")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = store.Manifest("job")
		if !manifest.AwaitingReconnect {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if manifest.AwaitingReconnect {
		t.Fatal("reconnect state was not cleared after successful recovery")
	}

	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestPersistedReconnectStateRecoversAfterDaemonRestart(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()
	source := filepath.Join(root, "phone", "a.bin")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Entries: []state.ManifestEntry{{ID: "a", Source: source, Destination: filepath.Join(root, "a.bin"), Size: 4}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetManifestAwaitingReconnect("job", true); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 1)}
	server := New(Config{SocketPath: ControlSocketPath(runtimeDir), Store: store, Recoverer: recoverer, RecoveryInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)
	select {
	case <-recoverer.called:
	case <-time.After(time.Second):
		t.Fatal("persisted reconnect state did not trigger recovery")
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

type recordingDestinationWatcher struct {
	mu   sync.Mutex
	dirs []string
}

func (w *recordingDestinationWatcher) AddDir(dir string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dirs = append(w.dirs, filepath.Clean(dir))
	return nil
}

func TestWatchDestinationRegistersLocalFolderBeforeMTP(t *testing.T) {
	root := t.TempDir()
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	watcher := &recordingDestinationWatcher{}
	server := New(Config{
		Store:   store,
		Watcher: watcher,
		Mounts:  func() ([]gvfs.Mount, error) { return nil, nil },
	})

	if err := server.handle(request{Action: "watch_destination", Destination: fileURI(root)}); err != nil {
		t.Fatal(err)
	}
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if len(watcher.dirs) != 1 || watcher.dirs[0] != root {
		t.Fatalf("watched dirs = %#v, want [%q]", watcher.dirs, root)
	}
}

func TestPhoneToLaptopObservationStartsWatchingDestinationRoot(t *testing.T) {
	root := t.TempDir()
	mountRoot := filepath.Join(root, "mtp")
	phoneDir := filepath.Join(mountRoot, "Internal")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(phoneDir, "a.bin")
	second := filepath.Join(phoneDir, "b.bin")
	if err := os.WriteFile(first, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	destinationRoot := filepath.Join(root, "Downloads")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutIntent(state.Intent{
		ID:        "job",
		Direction: "phone-to-laptop",
		URIs:      []string{"mtp://phone/Internal/a.bin", "mtp://phone/Internal/b.bin"},
	}); err != nil {
		t.Fatal(err)
	}
	watcher := &recordingDestinationWatcher{}
	server := New(Config{
		Store:   store,
		Watcher: watcher,
		Mounts: func() ([]gvfs.Mount, error) {
			return []gvfs.Mount{{Host: "phone", Root: mountRoot}}, nil
		},
	})
	if err := server.observe(Observation{IntentID: "job", Source: "mtp://phone/Internal/a.bin", Destination: fileURI(filepath.Join(destinationRoot, "a.bin"))}); err != nil {
		t.Fatal(err)
	}
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	if len(watcher.dirs) != 1 || watcher.dirs[0] != destinationRoot {
		t.Fatalf("watched dirs = %#v, want [%q]", watcher.dirs, destinationRoot)
	}
}

func TestCompletedUploadReconcilesWithoutDisconnect(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "laptop.bin")
	destination := filepath.Join(root, "phone", "movie.bin")
	data := []byte("completed-upload")

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
	if err := store.PutManifest(state.Manifest{
		ID:        "completed-upload",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{{
			ID: "entry", Source: source, Destination: destination, Size: int64(len(data)),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	recoverer := &recordingRecoverer{called: make(chan string, 1)}
	server := New(Config{RuntimeDir: root, Store: store, Recoverer: recoverer})

	server.checkRecovery()

	select {
	case id := <-recoverer.called:
		if id != "completed-upload" {
			t.Fatalf("recovered %q, want completed-upload", id)
		}
	default:
		t.Fatal("completed upload was not reconciled while destination remained connected")
	}
}

func TestUploadStartIsPersistedWhenDestinationAppears(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()

	sourceDir := filepath.Join(root, "laptop")
	phoneDir := filepath.Join(root, "phone")
	source := filepath.Join(sourceDir, "movie.bin")
	destination := filepath.Join(phoneDir, "movie.bin")

	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	data := []byte("upload-has-really-started")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{
		ID:        "started-upload",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{{
			ID:          "movie",
			Source:      source,
			Destination: destination,
			Size:        int64(len(data)),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	server := New(Config{
		SocketPath:       ControlSocketPath(runtimeDir),
		Store:            store,
		Recoverer:        &recordingRecoverer{called: make(chan string, 4)},
		RecoveryInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	// A bound manifest by itself must remain unstarted.
	time.Sleep(30 * time.Millisecond)
	manifest, _ := store.Manifest("started-upload")
	if manifest.UploadStarted {
		t.Fatal("upload marked started before destination appeared")
	}

	// This represents Nemo actually beginning the native upload.
	if err := os.WriteFile(destination, data[:8], 0o600); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = store.Manifest("started-upload")
		if manifest.UploadStarted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if !manifest.UploadStarted {
		t.Fatal("destination appeared but upload-started state was not persisted")
	}

	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestUnstartedUploadDoesNotEnterReconnectRecovery(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()

	sourceDir := filepath.Join(root, "laptop")
	phoneDir := filepath.Join(root, "phone")
	source := filepath.Join(sourceDir, "movie.bin")
	destination := filepath.Join(phoneDir, "movie.bin")

	if err := os.MkdirAll(sourceDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("not-pasted-yet"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{
		ID:        "unstarted-upload",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{{
			ID:          "movie",
			Source:      source,
			Destination: destination,
			Size:        int64(len("not-pasted-yet")),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{
		SocketPath:       ControlSocketPath(runtimeDir),
		Store:            store,
		Recoverer:        recoverer,
		RecoveryInterval: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	// Nothing has been pasted, so recovery must not run.
	select {
	case id := <-recoverer.called:
		t.Fatalf("unstarted upload recovered before disconnect: %s", id)
	case <-time.After(40 * time.Millisecond):
	}

	// Simulate the phone destination temporarily disappearing before Paste.
	if err := os.RemoveAll(phoneDir); err != nil {
		t.Fatal(err)
	}

	time.Sleep(40 * time.Millisecond)

	manifest, _ := store.Manifest("unstarted-upload")
	if manifest.AwaitingReconnect {
		t.Fatal("unstarted upload was incorrectly marked awaiting reconnect")
	}

	// Reconnect still must not turn a mere intent into an actual upload.
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-recoverer.called:
		t.Fatalf("unstarted upload recovered after reconnect: %s", id)
	case <-time.After(60 * time.Millisecond):
	}

	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticUploadRecoveryRunsAfterDestinationReconnect(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()
	source := filepath.Join(root, "laptop", "a.bin")
	phoneDir := filepath.Join(root, "phone", "Internal")
	destination := filepath.Join(phoneDir, "a.bin")
	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte("laptop-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("lap"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	if err := store.PutManifest(state.Manifest{
		ID:        "upload-job",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{{
			ID:          "a",
			Source:      source,
			Destination: destination,
			Size:        int64(len("laptop-data")),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{
		SocketPath:       ControlSocketPath(runtimeDir),
		Store:            store,
		Recoverer:        recoverer,
		RecoveryInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, server.socketPath)

	select {
	case id := <-recoverer.called:
		t.Fatalf("upload recovery ran while destination was continuously available: %s", id)
	case <-time.After(40 * time.Millisecond):
	}

	if err := os.RemoveAll(filepath.Join(root, "phone")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("upload-job")
		if manifest.AwaitingReconnect {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	manifest, _ := store.Manifest("upload-job")
	if !manifest.AwaitingReconnect {
		t.Fatal("upload destination disconnect was not persisted")
	}

	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-recoverer.called:
		if id != "upload-job" {
			t.Fatalf("recovered %q, want upload-job", id)
		}
	case <-time.After(time.Second):
		t.Fatal("upload recovery did not run after destination reconnect")
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = store.Manifest("upload-job")
		if !manifest.AwaitingReconnect {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if manifest.AwaitingReconnect {
		t.Fatal("upload reconnect state was not cleared after successful recovery")
	}

	cancel()
	if err := <-serveErr; err != nil {
		t.Fatal(err)
	}
}

func TestWatchDestinationLocalPathDoesNotDiscoverGVFSMounts(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	root := t.TempDir()
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	watcher := &recordingDestinationWatcher{}
	mountCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Watcher:    watcher,
		Mounts: func() ([]gvfs.Mount, error) {
			mountCalls++
			return nil, errors.New("mount discovery must not run for local destination watch")
		},
	})

	if err := server.handle(request{
		Action:      "watch_destination",
		Destination: fileURI(root),
	}); err != nil {
		t.Fatal(err)
	}

	if mountCalls != 0 {
		t.Fatalf("mount discovery calls = %d, want 0", mountCalls)
	}
}

func TestObservationWithGVFSFileURIUsesPathMountWithoutDiscovery(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=realme_RMX2156_FYIBBMAQRKMFJNAE",
	)

	phoneDir := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"Pictures",
	)

	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	phoneFile := filepath.Join(phoneDir, "a.mp4")

	if err := os.WriteFile(phoneFile, []byte("phone-data"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))

	if err := store.PutIntent(state.Intent{
		ID:        "gvfs-file-job",
		Direction: "phone-to-laptop",
		URIs:      []string{fileURI(phoneFile)},
	}); err != nil {
		t.Fatal(err)
	}

	destinationRoot := t.TempDir()
	destination := filepath.Join(destinationRoot, "a.mp4")

	watcher := &recordingDestinationWatcher{}
	mountCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Watcher:    watcher,
		Mounts: func() ([]gvfs.Mount, error) {
			mountCalls++
			return nil, errors.New("mount discovery must not run for GVFS file URI observation")
		},
	})

	if err := server.observe(Observation{
		IntentID:    "gvfs-file-job",
		Source:      fileURI(phoneFile),
		Destination: fileURI(destination),
	}); err != nil {
		t.Fatal(err)
	}

	if mountCalls != 0 {
		t.Fatalf("mount discovery calls = %d, want 0", mountCalls)
	}
}

func TestUploadRecoveryAvailabilityDoesNotDiscoverGVFSMounts(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	phoneDir := filepath.Join(mountRoot, "Internal")

	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(phoneDir, "a.bin")

	if err := os.WriteFile(destination, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	mountCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Mounts: func() ([]gvfs.Mount, error) {
			mountCalls++
			return nil, errors.New("recovery must not enumerate GVFS root")
		},
	})

	manifest := state.Manifest{
		ID:        "upload-job-no-discover",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{{
			ID:          "a",
			Source:      filepath.Join(t.TempDir(), "source.bin"),
			Destination: destination,
			Size:        100,
		}},
	}

	if !server.recoveryEndpointAvailable(manifest) {
		t.Fatal("expected connected GVFS destination to be available")
	}

	if mountCalls != 0 {
		t.Fatalf("mount discovery calls = %d, want 0", mountCalls)
	}
}

func TestBindDestinationCreatesManifestBeforeDestinationFileExists(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=phone",
	)
	phoneDir := filepath.Join(mountRoot, "Internal")

	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	source := filepath.Join(phoneDir, "movie.bin")
	if err := os.WriteFile(source, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}

	destinationRoot := t.TempDir()
	expectedDestination := filepath.Join(destinationRoot, "movie.bin")

	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))

	if err := store.PutIntent(state.Intent{
		ID:        "bind-job",
		Direction: "phone-to-laptop",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}

	watcher := &recordingDestinationWatcher{}
	mountCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Watcher:    watcher,
		Mounts: func() ([]gvfs.Mount, error) {
			mountCalls++
			return nil, nil
		},
	})

	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    "bind-job",
		Destination: fileURI(destinationRoot),
	}); err != nil {
		t.Fatal(err)
	}

	if mountCalls != 0 {
		t.Fatalf("mount discovery calls = %d, want 0", mountCalls)
	}

	var manifest *state.Manifest

	for _, candidate := range store.Manifests() {
		if candidate.ID == "bind-job" {
			copy := candidate
			manifest = &copy
			break
		}
	}

	if manifest == nil {
		t.Fatal("manifest was not created during destination binding")
	}

	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest.Entries))
	}

	if got := manifest.Entries[0].Destination; got != expectedDestination {
		t.Fatalf("destination = %q, want %q", got, expectedDestination)
	}

	if _, err := os.Stat(expectedDestination); !os.IsNotExist(err) {
		t.Fatalf("destination file exists before copy started: err=%v", err)
	}

	watcher.mu.Lock()
	defer watcher.mu.Unlock()

	if len(watcher.dirs) != 1 ||
		watcher.dirs[0] != filepath.Clean(destinationRoot) {
		t.Fatalf(
			"watched dirs = %#v, want [%q]",
			watcher.dirs,
			destinationRoot,
		)
	}
}

func TestExplicitUploadStartPersistsBeforeDestinationFileExists(t *testing.T) {
	root := t.TempDir()

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	intent := state.Intent{
		ID:        "upload-start-job",
		Direction: "laptop-to-phone",
		URIs:      []string{"file:///tmp/source.bin"},
	}

	if err := store.PutIntent(intent); err != nil {
		t.Fatal(err)
	}

	manifest := state.Manifest{
		ID:        "upload-start-job",
		Direction: "laptop-to-phone",
		Entries: []state.ManifestEntry{
			{
				ID:          "entry",
				Source:      "/tmp/source.bin",
				Destination: "/phone/Download/source.bin",
				Size:        123,
			},
		},
	}

	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}

	server := New(Config{
		Store: store,
	})

	err = server.handle(request{
		Action:   "start_upload",
		IntentID: "upload-start-job",
	})

	if err != nil {
		t.Fatalf("start_upload failed: %v", err)
	}

	got, ok := store.Manifest("upload-start-job")
	if !ok {
		t.Fatal("manifest disappeared")
	}

	if !got.UploadStarted {
		t.Fatal("explicit upload start was not persisted")
	}
}

func TestExplicitUploadStartRejectsPhoneToLaptop(t *testing.T) {
	root := t.TempDir()

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.PutIntent(state.Intent{
		ID:        "download-job",
		Direction: "phone-to-laptop",
		URIs:      []string{"mtp://phone/source.bin"},
	}); err != nil {
		t.Fatal(err)
	}

	if err := store.PutManifest(state.Manifest{
		ID:        "download-job",
		Direction: "phone-to-laptop",
		Entries: []state.ManifestEntry{{
			ID:          "entry",
			Source:      "/phone/source.bin",
			Destination: "/tmp/source.bin",
			Size:        123,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	err = New(Config{Store: store}).handle(request{
		Action:   "start_upload",
		IntentID: "download-job",
	})

	if err == nil {
		t.Fatal("phone-to-laptop manifest accepted start_upload")
	}

	got, _ := store.Manifest("download-job")
	if got.UploadStarted {
		t.Fatal("rejected manifest was marked upload_started")
	}
}

func TestStaleConcurrentBindCannotOverwriteNewerDestination(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=phone",
	)

	downloadDir := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"Download",
	)

	if err := os.MkdirAll(downloadDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "proof.bin")

	if err := os.WriteFile(source, []byte("proof"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.PutIntent(state.Intent{
		ID:        "bind-race",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}

	firstAtCommit := make(chan struct{})
	releaseFirst := make(chan struct{})

	var nowMu sync.Mutex
	nowCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Now: func() time.Time {
			nowMu.Lock()
			nowCalls++
			call := nowCalls
			nowMu.Unlock()

			if call == 1 {
				close(firstAtCommit)
				<-releaseFirst
			}

			return time.Unix(int64(call), 0)
		},
	})

	staleDone := make(chan error, 1)

	go func() {
		staleDone <- server.handle(request{
			Action:      "bind_destination",
			IntentID:    "bind-race",
			Destination: "mtp://phone/",
		})
	}()

	select {
	case <-firstAtCommit:
	case <-time.After(2 * time.Second):
		t.Fatal("stale bind never reached commit point")
	}

	newDestination := "mtp://phone/Internal%20shared%20storage/Download"

	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    "bind-race",
		Destination: newDestination,
	}); err != nil {
		t.Fatal(err)
	}

	got, ok := store.Intent("bind-race")
	if !ok {
		t.Fatal("intent disappeared")
	}

	if got.Destination != newDestination {
		t.Fatalf(
			"newer bind did not commit first: got %q, want %q",
			got.Destination,
			newDestination,
		)
	}

	close(releaseFirst)

	if err := <-staleDone; err != nil {
		t.Fatal(err)
	}

	got, ok = store.Intent("bind-race")
	if !ok {
		t.Fatal("intent disappeared after stale bind")
	}

	if got.Destination != newDestination {
		t.Fatalf(
			"stale bind overwrote newer destination: got %q, want %q",
			got.Destination,
			newDestination,
		)
	}

	manifest, ok := store.Manifest("bind-race")
	if !ok {
		t.Fatal("manifest disappeared")
	}

	wantFile := filepath.Join(downloadDir, "proof.bin")

	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest.Entries))
	}

	if got := manifest.Entries[0].Destination; got != wantFile {
		t.Fatalf(
			"stale manifest overwrote newer destination: got %q, want %q",
			got,
			wantFile,
		)
	}
}

func TestUploadStartSurvivesNewerDestinationBindInFlight(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=phone",
	)

	downloadDir := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"Download",
	)

	if err := os.MkdirAll(downloadDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "proof.bin")

	if err := os.WriteFile(source, []byte("proof"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.PutIntent(state.Intent{
		ID:        "start-bind-race",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}

	finalAtCommit := make(chan struct{})
	releaseFinal := make(chan struct{})

	var nowMu sync.Mutex
	nowCalls := 0

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Now: func() time.Time {
			nowMu.Lock()
			nowCalls++
			call := nowCalls
			nowMu.Unlock()

			// Call 1 = initial root bind.
			// Call 2 = final Download bind.
			if call == 2 {
				close(finalAtCommit)
				<-releaseFinal
			}

			return time.Unix(int64(call), 0)
		},
	})

	// Establish the older provisional root manifest first.
	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    "start-bind-race",
		Destination: "mtp://phone/",
	}); err != nil {
		t.Fatal(err)
	}

	finalDestination := "mtp://phone/Internal%20shared%20storage/Download"
	finalDone := make(chan error, 1)

	go func() {
		finalDone <- server.handle(request{
			Action:      "bind_destination",
			IntentID:    "start-bind-race",
			Destination: finalDestination,
		})
	}()

	select {
	case <-finalAtCommit:
	case <-time.After(2 * time.Second):
		t.Fatal("final destination bind never reached commit point")
	}

	// Ctrl+V occurs while the correct Download bind is still in flight.
	if err := server.handle(request{
		Action:   "start_upload",
		IntentID: "start-bind-race",
	}); err != nil {
		t.Fatalf("start_upload failed: %v", err)
	}

	started, ok := store.Manifest("start-bind-race")
	if !ok || !started.UploadStarted {
		t.Fatal("start_upload was not persisted before final bind completed")
	}

	close(releaseFinal)

	if err := <-finalDone; err != nil {
		t.Fatal(err)
	}

	intent, ok := store.Intent("start-bind-race")
	if !ok {
		t.Fatal("intent disappeared")
	}

	if intent.Destination != finalDestination {
		t.Fatalf(
			"destination = %q, want %q",
			intent.Destination,
			finalDestination,
		)
	}

	manifest, ok := store.Manifest("start-bind-race")
	if !ok {
		t.Fatal("manifest disappeared")
	}

	if !manifest.UploadStarted {
		t.Fatal("upload_started was lost when final destination bind committed")
	}

	wantFile := filepath.Join(downloadDir, "proof.bin")

	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest.Entries))
	}

	if got := manifest.Entries[0].Destination; got != wantFile {
		t.Fatalf("destination file = %q, want %q", got, wantFile)
	}
}

func TestStartedUploadDestinationCannotBeRebound(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=phone",
	)

	downloadDir := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"Download",
	)

	dcimDir := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"DCIM",
	)

	if err := os.MkdirAll(downloadDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dcimDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "proof.bin")

	if err := os.WriteFile(source, []byte("proof"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.PutIntent(state.Intent{
		ID:        "started-bind-freeze",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
	})

	downloadURI := "mtp://phone/Internal%20shared%20storage/Download"

	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    "started-bind-freeze",
		Destination: downloadURI,
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.handle(request{
		Action:   "start_upload",
		IntentID: "started-bind-freeze",
	}); err != nil {
		t.Fatal(err)
	}

	// A later navigation callback must not move an upload that already started.
	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    "started-bind-freeze",
		Destination: "mtp://phone/Internal%20shared%20storage/DCIM",
	}); err != nil {
		t.Fatal(err)
	}

	intent, ok := store.Intent("started-bind-freeze")
	if !ok {
		t.Fatal("intent disappeared")
	}

	if intent.Destination != downloadURI {
		t.Fatalf(
			"started upload was rebound: got %q, want %q",
			intent.Destination,
			downloadURI,
		)
	}

	manifest, ok := store.Manifest("started-bind-freeze")
	if !ok {
		t.Fatal("manifest disappeared")
	}

	if !manifest.UploadStarted {
		t.Fatal("started upload lost upload_started")
	}

	wantFile := filepath.Join(downloadDir, "proof.bin")

	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest.Entries))
	}

	if got := manifest.Entries[0].Destination; got != wantFile {
		t.Fatalf(
			"started manifest was rebound: got %q, want %q",
			got,
			wantFile,
		)
	}
}

func TestLaptopToPhoneBindDoesNotRequireMTPDestinationStat(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)

	mountRoot := filepath.Join(
		runtimeDir,
		"gvfs",
		"mtp:host=phone",
	)

	// The GVfs MTP mount itself is known, but deliberately do not create
	// Internal shared storage/Download. A laptop->phone destination bind
	// must not touch/stat that live MTP directory.
	if err := os.MkdirAll(mountRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "proof.bin")
	if err := os.WriteFile(source, []byte("proof"), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	const intentID = "mtp-bind-no-destination-stat"

	if err := store.PutIntent(state.Intent{
		ID:        intentID,
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}

	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
	})

	destinationURI := "mtp://phone/Internal%20shared%20storage/Download"

	if err := server.handle(request{
		Action:      "bind_destination",
		IntentID:    intentID,
		Destination: destinationURI,
	}); err != nil {
		t.Fatalf("MTP destination bind touched/rejected live destination path: %v", err)
	}

	intent, ok := store.Intent(intentID)
	if !ok {
		t.Fatal("intent disappeared")
	}
	if intent.Destination != destinationURI {
		t.Fatalf(
			"destination = %q, want %q",
			intent.Destination,
			destinationURI,
		)
	}

	manifest, ok := store.Manifest(intentID)
	if !ok {
		t.Fatal("manifest was not created")
	}
	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest.Entries))
	}

	wantDestination := filepath.Join(
		mountRoot,
		"Internal shared storage",
		"Download",
		"proof.bin",
	)

	if got := manifest.Entries[0].Destination; got != wantDestination {
		t.Fatalf(
			"manifest destination = %q, want %q",
			got,
			wantDestination,
		)
	}
}

func TestRecoveryOrderingPrioritizesNewestAwaitingReconnect(t *testing.T) {
	old := time.Unix(100, 0)
	middle := time.Unix(200, 0)
	newest := time.Unix(300, 0)
	laterButNotWaiting := time.Unix(400, 0)

	manifests := []state.Manifest{
		{
			ID:                "old-reconnect",
			Direction:         "laptop-to-phone",
			UploadStarted:     true,
			AwaitingReconnect: true,
			UpdatedAt:         old,
		},
		{
			ID:                "not-waiting",
			Direction:         "laptop-to-phone",
			UploadStarted:     true,
			AwaitingReconnect: false,
			UpdatedAt:         laterButNotWaiting,
		},
		{
			ID:                "newest-reconnect",
			Direction:         "laptop-to-phone",
			UploadStarted:     true,
			AwaitingReconnect: true,
			UpdatedAt:         newest,
		},
		{
			ID:                "middle-reconnect",
			Direction:         "phone-to-laptop",
			AwaitingReconnect: true,
			UpdatedAt:         middle,
		},
	}

	got := orderRecoveryManifests(manifests)

	want := []string{
		"newest-reconnect",
		"middle-reconnect",
		"old-reconnect",
		"not-waiting",
	}

	if len(got) != len(want) {
		t.Fatalf("got %d manifests, want %d", len(got), len(want))
	}

	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("position %d = %q, want %q", i, got[i].ID, id)
		}
	}
}

func TestCheckRecoveryAttemptsNewestAwaitingReconnectFirst(t *testing.T) {
	root := t.TempDir()
	phoneDir := filepath.Join(root, "phone")

	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	old := state.Manifest{
		ID:                "old-job",
		Direction:         "laptop-to-phone",
		UploadStarted:     true,
		AwaitingReconnect: true,
		UpdatedAt:         time.Unix(100, 0),
		Entries: []state.ManifestEntry{{
			ID:          "old-entry",
			Source:      filepath.Join(root, "old-source.bin"),
			Destination: filepath.Join(phoneDir, "old.bin"),
			Size:        100,
		}},
	}

	newest := state.Manifest{
		ID:                "newest-job",
		Direction:         "laptop-to-phone",
		UploadStarted:     true,
		AwaitingReconnect: true,
		UpdatedAt:         time.Unix(300, 0),
		Entries: []state.ManifestEntry{{
			ID:          "new-entry",
			Source:      filepath.Join(root, "new-source.bin"),
			Destination: filepath.Join(phoneDir, "new.bin"),
			Size:        100,
		}},
	}

	if err := store.PutManifest(old); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(newest); err != nil {
		t.Fatal(err)
	}

	recoverer := &recordingRecoverer{
		called: make(chan string, 4),
	}

	server := New(Config{
		Store:     store,
		Recoverer: recoverer,
	})

	server.checkRecovery()

	select {
	case first := <-recoverer.called:
		if first != "newest-job" {
			t.Fatalf("first recovery = %q, want newest-job", first)
		}
	default:
		t.Fatal("recovery was not attempted")
	}
}

func TestManagedTransportErrorRemainsRecoverableWhenGVFSMountLooksStale(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	if err := os.MkdirAll(mountRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:                "managed-stale-mount",
		Direction:         "laptop-to-phone",
		Managed:           true,
		UploadStarted:     true,
		AwaitingReconnect: false,
		Entries: []state.ManifestEntry{{
			ID:          "movie",
			Source:      filepath.Join(t.TempDir(), "movie.bin"),
			Destination: filepath.Join(mountRoot, "Internal shared storage", "Download", "movie.bin"),
			Size:        1024,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	recoverer := &recordingRecoverer{
		called: make(chan string, 8),
		err:    &os.PathError{Op: "write", Path: "movie.bin", Err: syscall.EIO},
	}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: recoverer})
	if !server.launchManagedRecovery("managed-stale-mount") {
		t.Fatal("managed recovery did not launch")
	}

	deadline := time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed recovery did not quiesce")
	}

	manifest, _ := store.Manifest("managed-stale-mount")
	if !manifest.AwaitingReconnect {
		t.Fatalf("transport error became terminal instead of reconnectable: %#v", manifest)
	}
	if manifest.LastError != "" {
		t.Fatalf("transport error leaked into terminal LastError: %q", manifest.LastError)
	}

	// The stale GVfs root still exists, so the failed goroutine must not spin
	// immediately. Periodic recovery is allowed to retry later.
	time.Sleep(25 * time.Millisecond)
	recoverer.mu.Lock()
	count := recoverer.count
	recoverer.mu.Unlock()
	if count != 1 {
		t.Fatalf("managed recovery tight-looped after disconnect: count=%d", count)
	}
}

func TestManagedUploadRetriesAndCompletesAfterStaleMountReconnect(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	if err := os.MkdirAll(mountRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceRoot := t.TempDir()
	source := filepath.Join(sourceRoot, "movie.bin")
	payload := []byte("managed reconnect payload")
	if err := os.WriteFile(source, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	destinationDir := filepath.Join(mountRoot, "Internal shared storage", "Download")
	destination := filepath.Join(destinationDir, "movie.bin")

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:                "managed-auto-reconnect",
		Direction:         "laptop-to-phone",
		Managed:           true,
		UploadStarted:     true,
		AwaitingReconnect: false,
		Entries: []state.ManifestEntry{{
			ID:          "movie",
			Source:      source,
			Destination: destination,
			Size:        int64(len(payload)),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if !server.launchManagedRecovery("managed-auto-reconnect") {
		t.Fatal("managed recovery did not launch")
	}

	deadline := time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("managed-auto-reconnect")
	if !manifest.AwaitingReconnect || manifest.LastError != "" {
		t.Fatalf("stale mount failure was not left reconnectable: %#v", manifest)
	}

	// Simulate the phone becoming usable again while the GVfs mount name stays
	// the same. The periodic recovery tick must resume without another paste.
	if err := os.MkdirAll(destinationDir, 0o700); err != nil {
		t.Fatal(err)
	}
	server.checkRecovery()

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = store.Manifest("managed-auto-reconnect")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ = store.Manifest("managed-auto-reconnect")
	if len(manifest.Pending()) != 0 || manifest.AwaitingReconnect || manifest.LastError != "" {
		t.Fatalf("managed transfer did not recover automatically: %#v", manifest)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("recovered destination = %q, want %q", got, payload)
	}
}

func TestManagedTransportInterruptionClassificationDoesNotHidePermanentErrors(t *testing.T) {
	if !isManagedTransportInterruption(&os.PathError{Op: "write", Path: "phone", Err: syscall.EIO}) {
		t.Fatal("EIO should be treated as reconnectable transport loss")
	}
	if isManagedTransportInterruption(os.ErrPermission) {
		t.Fatal("permission failure must remain terminal")
	}
	if isManagedTransportInterruption(recovery.ErrDestinationConflict) {
		t.Fatal("destination conflict must remain terminal")
	}
}

func TestManagedTransferBindsFinalDestinationMarksManagedAndStartsRecovery(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Movies")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	source := filepath.Join(sourceDir, "movie.bin")
	if err := os.WriteFile(source, []byte("movie-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{
		ID:        "managed-job",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(source)},
	}); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{
		RuntimeDir: runtimeDir,
		Store:      store,
		Recoverer:  recoverer,
	})
	if err := server.handle(request{
		Action:      "start_managed_transfer",
		IntentID:    "managed-job",
		Destination: "mtp://phone/Internal%20shared%20storage/Movies",
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case id := <-recoverer.called:
		if id != "managed-job" {
			t.Fatalf("recovered %q, want managed-job", id)
		}
	case <-time.After(time.Second):
		t.Fatal("managed recovery was not started")
	}

	manifest, ok := store.Manifest("managed-job")
	if !ok || !manifest.Managed || !manifest.UploadStarted {
		t.Fatalf("managed upload state = %#v", manifest)
	}
	want := filepath.Join(destinationRoot, "movie.bin")
	if got := manifest.Entries[0].Destination; got != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
	deadline := time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed recovery did not quiesce")
	}
}

func TestManagedTransferRetryAfterVerifiedCompletionIsAccepted(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Movies")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "movie.bin")
	data := []byte("retry-after-complete")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err := store.PutIntent(state.Intent{ID: "managed-complete-retry", Direction: "laptop-to-phone", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, CheckpointBytes: 4}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	destination := "mtp://phone/Internal%20shared%20storage/Movies"

	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-complete-retry", Destination: destination}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("managed-complete-retry")
		if len(manifest.Pending()) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("managed-complete-retry")
	if len(manifest.Pending()) != 0 {
		t.Fatalf("transfer did not complete: %#v", manifest)
	}

	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-complete-retry", Destination: destination}); err != nil {
		t.Fatalf("idempotent retry after completion failed: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(destinationRoot, "movie.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("destination changed on retry: %q", got)
	}
}

func TestManagedTransferRetryPreservesExistingManagedProgress(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Movies")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "movie.bin")
	if err := os.WriteFile(source, []byte("movie-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err := store.PutIntent(state.Intent{ID: "managed-retry", Direction: "laptop-to-phone", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: recoverer})
	destination := "mtp://phone/Internal%20shared%20storage/Movies"

	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-retry", Destination: destination}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("managed-retry")
	if len(manifest.Entries) != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
	entryID := manifest.Entries[0].ID
	if err := store.SetManifestEntryProgress("managed-retry", entryID, 4); err != nil {
		t.Fatal(err)
	}

	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-retry", Destination: destination}); err != nil {
		t.Fatal(err)
	}
	manifest, _ = store.Manifest("managed-retry")
	if got := manifest.Entries[0].BytesDone; got != 4 {
		t.Fatalf("retry reset managed progress to %d, want 4", got)
	}
	if !manifest.Managed || !manifest.UploadStarted {
		t.Fatalf("retry lost managed state: %#v", manifest)
	}
	deadline = time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed retry recovery did not quiesce")
	}
}

func TestManagedTransferFreezesDestinationAgainstLaterNavigationBind(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	movies := filepath.Join(mountRoot, "Internal shared storage", "Movies")
	download := filepath.Join(mountRoot, "Internal shared storage", "Download")
	if err := os.MkdirAll(movies, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(download, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "movie.bin")
	if err := os.WriteFile(source, []byte("movie-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err := store.PutIntent(state.Intent{ID: "managed-freeze", Direction: "laptop-to-phone", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	recoverer := &recordingRecoverer{called: make(chan string, 4)}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: recoverer})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-freeze", Destination: "mtp://phone/Internal%20shared%20storage/Movies"}); err != nil {
		t.Fatal(err)
	}
	if err := server.handle(request{Action: "bind_destination", IntentID: "managed-freeze", Destination: "mtp://phone/Internal%20shared%20storage/Download"}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("managed-freeze")
	want := filepath.Join(movies, "movie.bin")
	if got := manifest.Entries[0].Destination; got != want {
		t.Fatalf("managed destination moved to %q, want %q", got, want)
	}
	deadline := time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed recovery did not quiesce")
	}
}

func TestRecoveryOrderingPrefersManagedWithinSameReconnectPriority(t *testing.T) {
	stamp := time.Unix(500, 0)
	got := orderRecoveryManifests([]state.Manifest{
		{ID: "legacy", AwaitingReconnect: true, UpdatedAt: stamp},
		{ID: "managed", AwaitingReconnect: true, Managed: true, UpdatedAt: stamp},
	})
	if len(got) != 2 || got[0].ID != "managed" {
		t.Fatalf("order = %#v", got)
	}
}

func TestManagedLaptopToPhoneTransferRunsWithoutNativeNemoCopy(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Movies")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(t.TempDir(), "movie.bin")
	data := []byte("managed transfer payload")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err := store.PutIntent(state.Intent{ID: "managed-e2e-up", Direction: "laptop-to-phone", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, CheckpointBytes: 4}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-e2e-up", Destination: "mtp://phone/Internal%20shared%20storage/Movies"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("managed-e2e-up")
		if len(manifest.Pending()) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("managed-e2e-up")
	if len(manifest.Pending()) != 0 {
		t.Fatalf("managed upload did not complete: %#v", manifest)
	}
	got, err := os.ReadFile(filepath.Join(destinationRoot, "movie.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("destination data = %q", got)
	}
	if manifest.Entries[0].BytesDone != int64(len(data)) {
		t.Fatalf("progress = %d, want %d", manifest.Entries[0].BytesDone, len(data))
	}
	if !manifest.Entries[0].CreatedByJob {
		t.Fatalf("managed upload did not record destination ownership: %#v", manifest.Entries[0])
	}
	deadline = time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed upload did not quiesce")
	}
}

func TestManagedPhoneToLaptopTransferRunsWithoutNativeNemoCopy(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	phoneDir := filepath.Join(mountRoot, "Internal shared storage", "Download")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(phoneDir, "clip.bin")
	data := []byte("managed download payload")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	destinationRoot := t.TempDir()
	store, _ := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err := store.PutIntent(state.Intent{ID: "managed-e2e-down", Direction: "phone-to-laptop", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "managed-e2e-down", Destination: fileURI(destinationRoot)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("managed-e2e-down")
		if len(manifest.Pending()) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("managed-e2e-down")
	if len(manifest.Pending()) != 0 {
		t.Fatalf("managed download did not complete: %#v", manifest)
	}
	got, err := os.ReadFile(filepath.Join(destinationRoot, "clip.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("destination data = %q", got)
	}
	if manifest.Entries[0].BytesDone != int64(len(data)) {
		t.Fatalf("progress = %d, want %d", manifest.Entries[0].BytesDone, len(data))
	}
	if !manifest.Entries[0].CreatedByJob {
		t.Fatalf("managed download did not record destination ownership: %#v", manifest.Entries[0])
	}
	deadline = time.Now().Add(time.Second)
	for server.managedRecoveryBusy() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.managedRecoveryBusy() {
		t.Fatal("managed download did not quiesce")
	}
}

func TestManagedLaptopToPhoneHandles100MixedFiles(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Download", "Mixed")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceRoot := t.TempDir()
	exts := []string{"mp4", "mp3", "pdf", "txt"}
	uris := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("mixed file %03d.%s", i, exts[i%len(exts)])
		if i == 99 {
			name = "résumé 最後 099.pdf"
		}
		path := filepath.Join(sourceRoot, name)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("payload-%03d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		uris = append(uris, fileURI(path))
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{ID: "mixed-up-100", Direction: "laptop-to-phone", URIs: uris}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "mixed-up-100", Destination: "mtp://phone/Internal%20shared%20storage/Download/Mixed"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second * managedBatchTimeoutScale)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("mixed-up-100")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("mixed-up-100")
	if len(manifest.Entries) != 100 || len(manifest.Pending()) != 0 {
		t.Fatalf("100-file managed upload incomplete: entries=%d pending=%d", len(manifest.Entries), len(manifest.Pending()))
	}
	for i, entry := range manifest.Entries {
		if !entry.CreatedByJob || !entry.Complete {
			t.Fatalf("entry %d ownership/completion = %#v", i, entry)
		}
		want, err := os.ReadFile(entry.Source)
		if err != nil {
			t.Fatalf("entry %d source unreadable: %v", i, err)
		}
		got, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatalf("entry %d destination unreadable: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("entry %d destination content mismatch", i)
		}
	}
}

func TestManagedPhoneToLaptopHandles100MixedFiles(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	sourceRoot := filepath.Join(mountRoot, "Internal shared storage", "Download", "Mixed")
	if err := os.MkdirAll(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	exts := []string{"mp4", "mp3", "pdf", "txt"}
	uris := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("phone file %03d.%s", i, exts[i%len(exts)])
		if i == 99 {
			name = "résumé 最後 099.pdf"
		}
		path := filepath.Join(sourceRoot, name)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("payload-%03d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		uris = append(uris, fileURI(path))
	}
	destinationRoot := t.TempDir()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{ID: "mixed-down-100", Direction: "phone-to-laptop", URIs: uris}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "mixed-down-100", Destination: fileURI(destinationRoot)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second * managedBatchTimeoutScale)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("mixed-down-100")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("mixed-down-100")
	if len(manifest.Entries) != 100 || len(manifest.Pending()) != 0 {
		t.Fatalf("100-file managed download incomplete: entries=%d pending=%d", len(manifest.Entries), len(manifest.Pending()))
	}
	for i, entry := range manifest.Entries {
		if !entry.CreatedByJob || !entry.Complete {
			t.Fatalf("entry %d ownership/completion = %#v", i, entry)
		}
		want, err := os.ReadFile(entry.Source)
		if err != nil {
			t.Fatalf("entry %d source unreadable: %v", i, err)
		}
		got, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatalf("entry %d destination unreadable: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("entry %d destination content mismatch", i)
		}
	}
}

func TestManagedLaptopToPhoneNestedFolderPreservesPathsAndBytes(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destinationRoot := filepath.Join(mountRoot, "Internal shared storage", "Download", "NestedUSB")
	if err := os.MkdirAll(destinationRoot, 0o700); err != nil {
		t.Fatal(err)
	}

	sourceParent := t.TempDir()
	sourceRoot := filepath.Join(sourceParent, "Album Tree")
	fixtures := map[string][]byte{
		"root.txt":                                  []byte("root-payload"),
		filepath.Join("A", "same.bin"):              []byte("payload-from-A"),
		filepath.Join("B", "same.bin"):              []byte("payload-from-B"),
		filepath.Join("B", "deep", "résumé 最後.txt"): []byte("unicode-payload"),
		filepath.Join("B", "deep", "empty.bin"):     nil,
	}
	for rel, data := range fixtures {
		path := filepath.Join(sourceRoot, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{
		ID:        "nested-usb-up",
		Direction: "laptop-to-phone",
		URIs:      []string{fileURI(sourceRoot)},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{
		Action:      "start_managed_transfer",
		IntentID:    "nested-usb-up",
		Destination: "mtp://phone/Internal%20shared%20storage/Download/NestedUSB",
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second * managedBatchTimeoutScale)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("nested-usb-up")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("nested-usb-up")
	if len(manifest.Entries) != len(fixtures) || len(manifest.Pending()) != 0 {
		t.Fatalf("nested USB upload incomplete: entries=%d pending=%d", len(manifest.Entries), len(manifest.Pending()))
	}

	seen := make(map[string]bool, len(fixtures))
	for _, entry := range manifest.Entries {
		if !entry.Complete || !entry.CreatedByJob || entry.BytesDone != entry.Size {
			t.Fatalf("nested USB upload entry not terminal: %#v", entry)
		}
		rel, err := filepath.Rel(sourceRoot, entry.Source)
		if err != nil {
			t.Fatal(err)
		}
		wantData, ok := fixtures[rel]
		if !ok {
			t.Fatalf("unexpected nested USB upload source %q (rel=%q)", entry.Source, rel)
		}
		wantDestination := filepath.Join(destinationRoot, filepath.Base(sourceRoot), rel)
		if filepath.Clean(entry.Destination) != filepath.Clean(wantDestination) {
			t.Fatalf("nested USB upload path mismatch: got=%q want=%q", entry.Destination, wantDestination)
		}
		gotData, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatalf("nested USB upload destination unreadable %q: %v", entry.Destination, err)
		}
		if !bytes.Equal(gotData, wantData) {
			t.Fatalf("nested USB upload content mismatch for %q", rel)
		}
		seen[rel] = true
	}
	if len(seen) != len(fixtures) {
		t.Fatalf("nested USB upload saw %d files want %d", len(seen), len(fixtures))
	}
}

func TestManagedPhoneToLaptopNestedFolderPreservesPathsAndBytes(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	sourceRoot := filepath.Join(mountRoot, "Internal shared storage", "DCIM", "Nested Album")
	fixtures := map[string][]byte{
		"root.txt":                     []byte("phone-root-payload"),
		filepath.Join("A", "same.bin"): []byte("phone-payload-from-A"),
		filepath.Join("B", "same.bin"): []byte("phone-payload-from-B"),
		filepath.Join("B", "deep", "తెలుగు résumé.txt"): []byte("phone-unicode-payload"),
		filepath.Join("B", "deep", "empty.bin"):         nil,
	}
	for rel, data := range fixtures {
		path := filepath.Join(sourceRoot, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	destinationRoot := t.TempDir()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{
		ID:        "nested-usb-down",
		Direction: "phone-to-laptop",
		URIs:      []string{"mtp://phone/Internal%20shared%20storage/DCIM/Nested%20Album"},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{
		Action:      "start_managed_transfer",
		IntentID:    "nested-usb-down",
		Destination: fileURI(destinationRoot),
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second * managedBatchTimeoutScale)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("nested-usb-down")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("nested-usb-down")
	if len(manifest.Entries) != len(fixtures) || len(manifest.Pending()) != 0 {
		t.Fatalf("nested USB download incomplete: entries=%d pending=%d", len(manifest.Entries), len(manifest.Pending()))
	}

	seen := make(map[string]bool, len(fixtures))
	for _, entry := range manifest.Entries {
		if !entry.Complete || !entry.CreatedByJob || entry.BytesDone != entry.Size {
			t.Fatalf("nested USB download entry not terminal: %#v", entry)
		}
		rel, err := filepath.Rel(sourceRoot, entry.Source)
		if err != nil {
			t.Fatal(err)
		}
		wantData, ok := fixtures[rel]
		if !ok {
			t.Fatalf("unexpected nested USB download source %q (rel=%q)", entry.Source, rel)
		}
		wantDestination := filepath.Join(destinationRoot, filepath.Base(sourceRoot), rel)
		if filepath.Clean(entry.Destination) != filepath.Clean(wantDestination) {
			t.Fatalf("nested USB download path mismatch: got=%q want=%q", entry.Destination, wantDestination)
		}
		gotData, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatalf("nested USB download destination unreadable %q: %v", entry.Destination, err)
		}
		if !bytes.Equal(gotData, wantData) {
			t.Fatalf("nested USB download content mismatch for %q", rel)
		}
		seen[rel] = true
	}
	if len(seen) != len(fixtures) {
		t.Fatalf("nested USB download saw %d files want %d", len(seen), len(fixtures))
	}
}

func TestManagedPauseResumeAndCancelControls(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	source := filepath.Join(root, "source.bin")
	data := []byte("pause-resume-control")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "phone", "source.bin")
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:            "control-job",
		Direction:     "laptop-to-phone",
		Managed:       true,
		UploadStarted: true,
		Entries: []state.ManifestEntry{{
			ID:          "control-entry",
			Source:      source,
			Destination: destination,
			Size:        int64(len(data)),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, ChunkSize: 4, CheckpointBytes: 8}}
	server := New(Config{RuntimeDir: root, Store: store, Recoverer: runner})

	if err := server.handle(request{Action: "pause_managed_transfer", IntentID: "control-job"}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("control-job")
	if !manifest.Paused {
		t.Fatalf("pause control not persisted: %#v", manifest)
	}

	if err := server.handle(request{Action: "resume_managed_transfer", IntentID: "control-job"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		manifest, _ = store.Manifest("control-job")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(manifest.Pending()) != 0 || manifest.Paused {
		t.Fatalf("resume control did not finish: %#v", manifest)
	}

	cancelSource := filepath.Join(root, "cancel-source.bin")
	cancelDestination := filepath.Join(root, "phone", "cancel-source.bin")
	cancelData := []byte("cancel-control-payload")
	if err := os.WriteFile(cancelSource, cancelData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cancelDestination, cancelData[:8], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:            "cancel-job",
		Direction:     "laptop-to-phone",
		Managed:       true,
		UploadStarted: true,
		Entries: []state.ManifestEntry{{
			ID:           "cancel-entry",
			Source:       cancelSource,
			Destination:  cancelDestination,
			Size:         int64(len(cancelData)),
			BytesDone:    8,
			CreatedByJob: true,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := server.handle(request{Action: "cancel_managed_transfer", IntentID: "cancel-job"}); err != nil {
		t.Fatal(err)
	}
	manifest, _ = store.Manifest("cancel-job")
	if !manifest.Cancelled || manifest.CancelRequested {
		t.Fatalf("cancel control did not reach terminal state: %#v", manifest)
	}
	if _, err := os.Stat(cancelDestination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancel left managed partial behind: %v", err)
	}
}

func TestManagedCancelControlPersistsUndoAllModeAndRejectsUnknownMode(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	completed := filepath.Join(root, "phone", "done.bin")
	if err := os.MkdirAll(filepath.Dir(completed), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(completed, []byte("done"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "undo-job",
		Direction: "laptop-to-phone",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:           "done",
			Destination:  completed,
			Size:         4,
			BytesDone:    4,
			Complete:     true,
			CreatedByJob: true,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store}
	server := New(Config{RuntimeDir: root, Store: store, Recoverer: runner})

	if err := server.handle(request{Action: "cancel_managed_transfer", IntentID: "undo-job", CancelMode: state.CancelModeUndoAll}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("undo-job")
	if manifest.CancelMode != state.CancelModeUndoAll || !manifest.Cancelled {
		t.Fatalf("undo mode not persisted: %#v", manifest)
	}
	if _, err := os.Stat(completed); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("undo-all did not remove completed owned file: %v", err)
	}

	if err := store.PutManifest(state.Manifest{ID: "invalid-job", Direction: "phone-to-laptop", Managed: true, Entries: []state.ManifestEntry{{ID: "entry"}}}); err != nil {
		t.Fatal(err)
	}
	if err := server.handle(request{Action: "cancel_managed_transfer", IntentID: "invalid-job", CancelMode: "danger"}); err == nil {
		t.Fatal("unknown cancel mode was accepted")
	}
}

func TestManagedUploadCancelDefersCleanupUntilPhoneReconnect(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	destination := filepath.Join(mountRoot, "Internal shared storage", "Movies", "movie.bin")
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:                "cancel-reconnect",
		Direction:         "laptop-to-phone",
		Managed:           true,
		UploadStarted:     true,
		AwaitingReconnect: true,
		Entries: []state.ManifestEntry{{
			ID:           "movie",
			Destination:  destination,
			Size:         100,
			BytesDone:    40,
			CreatedByJob: true,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})

	if err := server.handle(request{Action: "cancel_managed_transfer", IntentID: "cancel-reconnect", CancelMode: state.CancelModeKeepCompleted}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("cancel-reconnect")
	if !manifest.CancelRequested || manifest.Cancelled || !manifest.AwaitingReconnect {
		t.Fatalf("cancel should wait for reconnect: %#v", manifest)
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	server.checkRecovery()

	manifest, _ = store.Manifest("cancel-reconnect")
	if !manifest.Cancelled || manifest.CancelRequested {
		t.Fatalf("cancel cleanup did not finish after reconnect: %#v", manifest)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned partial remains after reconnect cleanup: %v", err)
	}
}

func TestManagedPhoneMissingFileWithReachableParentIsNotClassifiedAsDisconnect(t *testing.T) {
	root := t.TempDir()
	phoneDir := filepath.Join(root, "phone", "Download")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}
	store, _ := state.Open(filepath.Join(root, "state.json"))
	server := New(Config{RuntimeDir: root, Store: store})
	manifest := state.Manifest{
		ID:        "missing-file",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:     "gone",
			Source: filepath.Join(phoneDir, "gone.mp4"),
			Size:   10,
		}},
	}
	if server.managedErrorNeedsReconnect(manifest, os.ErrNotExist) {
		t.Fatal("reachable phone directory with one missing file was misclassified as a USB disconnect")
	}
}

func TestManagedPhoneToLaptopHandles403MixedFilesWithoutFalseReconnect(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	sourceRoot := filepath.Join(mountRoot, "Internal shared storage", "Download", "LargeBatch")
	if err := os.MkdirAll(sourceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 403; i++ {
		name := fmt.Sprintf("batch file %03d.dat", i)
		if i == 402 {
			name = "తెలుగు résumé 最後 402.dat"
		}
		path := filepath.Join(sourceRoot, name)
		if err := os.WriteFile(path, []byte(fmt.Sprintf("payload-%03d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	destinationRoot := t.TempDir()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutIntent(state.Intent{
		ID:        "mixed-down-403",
		Direction: "phone-to-laptop",
		URIs:      []string{"mtp://phone/Internal%20shared%20storage/Download/LargeBatch"},
	}); err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "mixed-down-403", Destination: fileURI(destinationRoot)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(60 * time.Second * managedBatchTimeoutScale)
	for time.Now().Before(deadline) {
		manifest, _ := store.Manifest("mixed-down-403")
		if len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, _ := store.Manifest("mixed-down-403")
	if manifest.AwaitingReconnect {
		t.Fatalf("connected 403-file batch was falsely marked disconnected: %#v", manifest)
	}
	if manifest.LastError != "" {
		t.Fatalf("connected 403-file batch stopped: %s", manifest.LastError)
	}
	if len(manifest.Entries) != 403 || len(manifest.Pending()) != 0 {
		t.Fatalf("403-file managed download incomplete: entries=%d pending=%d", len(manifest.Entries), len(manifest.Pending()))
	}
	for i, entry := range manifest.Entries {
		if !entry.Complete {
			t.Fatalf("incomplete entry: %#v", entry)
		}
		want, err := os.ReadFile(entry.Source)
		if err != nil {
			t.Fatalf("entry %d source unreadable: %v", i, err)
		}
		got, err := os.ReadFile(entry.Destination)
		if err != nil {
			t.Fatalf("entry %d destination unreadable: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("entry %d destination content mismatch", i)
		}
	}
}

func TestManagedCancelQueuesCleanupWhileAnotherTransportOwnsManifest(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "partial.bin")
	if err := os.WriteFile(destination, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "job",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:           "entry",
			Destination:  destination,
			Size:         100,
			BytesDone:    7,
			CreatedByJob: true,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireManifestTransport("job", "wifi:test", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	runner := &recovery.Runner{Store: store}
	server := New(Config{RuntimeDir: root, Store: store, Recoverer: runner})
	if err := server.handle(request{Action: "cancel_managed_transfer", IntentID: "job", CancelMode: state.CancelModeKeepCompleted}); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("job")
	if !manifest.CancelRequested || manifest.Cancelled {
		t.Fatalf("cancel was not queued behind active transport: %#v", manifest)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("active transport partial was deleted: %v", err)
	}
	if err := store.ReleaseManifestTransport("job", lease.Owner, lease.Generation); err != nil {
		t.Fatal(err)
	}
	server.checkRecovery()
	manifest, _ = store.Manifest("job")
	if !manifest.Cancelled || manifest.CancelRequested {
		t.Fatalf("queued cancel did not finish after transport release: %#v", manifest)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned partial remains after queued cancel: %v", err)
	}
}

func TestLargeOrdinaryWifiUploadAdoptsCheckpointIntoManagedUSBRecovery(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	phoneDir := filepath.Join(mountRoot, "Internal shared storage", "Download")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	const total int64 = 80 << 20
	const sent int64 = 16 << 20
	source := filepath.Join(phoneDir, "large-clip.bin")
	sourceFile, err := os.OpenFile(source, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceFile.Truncate(total); err != nil {
		sourceFile.Close()
		t.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		t.Fatal(err)
	}
	sourceFile, err = os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := fingerprint.ReaderAt(sourceFile, total, 64*1024)
	if err != nil {
		sourceFile.Close()
		t.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		t.Fatal(err)
	}

	destinationRoot := t.TempDir()
	destination := filepath.Join(destinationRoot, "large-clip.bin")
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	portalManager := portal.New(portal.Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer portalManager.Close()

	info, err := portalManager.StartReceive(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	initBody, _ := json.Marshal(map[string]any{
		"name":          "large-clip.bin",
		"relative_path": "large-clip.bin",
		"size":          total,
		"fingerprint":   fp,
	})
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(initBody))
	if err != nil {
		t.Fatal(err)
	}
	var initOut struct {
		UploadID string `json:"upload_id"`
		Offset   int64  `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&initOut); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || initOut.UploadID == "" || initOut.Offset != 0 {
		t.Fatalf("init status=%d response=%+v", resp.StatusCode, initOut)
	}

	req, err := http.NewRequest(
		http.MethodPut,
		fmt.Sprintf("%sapi/upload/%s?offset=0", info.URLs[0], url.PathEscape(initOut.UploadID)),
		bytes.NewReader(make([]byte, int(sent))),
	)
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var uploadOut map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&uploadOut); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload status=%d response=%v", resp.StatusCode, uploadOut)
	}

	partials, err := filepath.Glob(filepath.Join(destinationRoot, ".large-clip.bin.resumexfer-part-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(partials) != 1 {
		t.Fatalf("partial files=%v, want exactly one", partials)
	}
	wirelessPartial := partials[0]
	partialInfo, err := os.Stat(wirelessPartial)
	if err != nil {
		t.Fatal(err)
	}
	if partialInfo.Size() != sent {
		t.Fatalf("wifi partial size=%d want=%d", partialInfo.Size(), sent)
	}

	if err := store.PutIntent(state.Intent{ID: "large-wifi-usb-adopt", Direction: "phone-to-laptop", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	progress := make(chan int64, 32)
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, Progress: func(done, total int64) error {
		progress <- done
		return nil
	}}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner, Portal: portalManager})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "large-wifi-usb-adopt", Destination: fileURI(destinationRoot)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		manifest, ok := store.Manifest("large-wifi-usb-adopt")
		if ok && len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, ok := store.Manifest("large-wifi-usb-adopt")
	if !ok || len(manifest.Pending()) != 0 {
		t.Fatalf("large managed transfer did not complete: %#v", manifest)
	}
	finalInfo, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if finalInfo.Size() != total {
		t.Fatalf("final size=%d want=%d", finalInfo.Size(), total)
	}
	finalFile, err := os.Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	finalFP, err := fingerprint.ReaderAt(finalFile, total, 64*1024)
	closeErr := finalFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !fp.Compatible(finalFP) {
		t.Fatal("large Wi-Fi to USB final fingerprint mismatch")
	}

	select {
	case first := <-progress:
		if first != sent {
			t.Fatalf("first USB recovery progress=%d, want verified %d-byte Wi-Fi checkpoint", first, sent)
		}
	default:
		t.Fatal("large USB recovery reported no adopted progress checkpoint")
	}
	if len(manifest.Entries) != 1 || manifest.Entries[0].BytesDone != total || !manifest.Entries[0].CreatedByJob || manifest.Entries[0].SourceFingerprint == nil {
		t.Fatalf("large adopted manifest metadata incorrect: %#v", manifest.Entries)
	}
	if candidates := store.Candidates(); len(candidates) != 0 {
		t.Fatalf("large adoption candidates remain after completion: %#v", candidates)
	}
	if _, ok := store.Transfer("portal:" + info.ID + ":" + initOut.UploadID); ok {
		t.Fatal("old large wireless engine state remains after adoption")
	}
	if _, err := os.Stat(wirelessPartial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("large wireless preserved partial still exists after completion: %v", err)
	}
}

func TestOrdinaryWifiUploadAdoptsVerifiedCheckpointIntoManagedUSBRecovery(t *testing.T) {
	runtimeDir := shortRuntimeDir(t)
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	phoneDir := filepath.Join(mountRoot, "Internal shared storage", "Download")
	if err := os.MkdirAll(phoneDir, 0o700); err != nil {
		t.Fatal(err)
	}

	const total = 6 << 20
	const sent = 5 << 20
	data := bytes.Repeat([]byte("wifi-to-usb-adoption-0123456789abcdef"), total/36+1)[:total]
	source := filepath.Join(phoneDir, "clip.bin")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	destinationRoot := t.TempDir()
	destination := filepath.Join(destinationRoot, "clip.bin")
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	portalManager := portal.New(portal.Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer portalManager.Close()
	info, err := portalManager.StartReceive(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) == 0 {
		t.Fatal("portal returned no URL")
	}
	base := info.URLs[0]
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	initBody, _ := json.Marshal(map[string]any{
		"name":          "clip.bin",
		"relative_path": "clip.bin",
		"size":          len(data),
		"fingerprint":   fp,
	})
	resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(initBody))
	if err != nil {
		t.Fatal(err)
	}
	var initOut struct {
		UploadID string `json:"upload_id"`
		Offset   int64  `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&initOut); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || initOut.UploadID == "" || initOut.Offset != 0 {
		t.Fatalf("init status=%d response=%+v", resp.StatusCode, initOut)
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%sapi/upload/%s?offset=0", base, url.PathEscape(initOut.UploadID)), bytes.NewReader(data[:sent]))
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var uploadOut map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&uploadOut); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload status=%d response=%v", resp.StatusCode, uploadOut)
	}
	partials, err := filepath.Glob(filepath.Join(destinationRoot, ".clip.bin.resumexfer-part-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(partials) != 1 {
		t.Fatalf("partial files=%v, want exactly one", partials)
	}
	partial := partials[0]
	partialInfo, err := os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	if partialInfo.Size() != sent {
		t.Fatalf("wifi partial size=%d want=%d", partialInfo.Size(), sent)
	}

	if err := store.PutIntent(state.Intent{ID: "wifi-usb-adopt", Direction: "phone-to-laptop", URIs: []string{fileURI(source)}}); err != nil {
		t.Fatal(err)
	}
	progress := make(chan int64, 16)
	runner := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store, Progress: func(done, total int64) error {
		progress <- done
		return nil
	}}}
	server := New(Config{RuntimeDir: runtimeDir, Store: store, Recoverer: runner, Portal: portalManager})
	if err := server.handle(request{Action: "start_managed_transfer", IntentID: "wifi-usb-adopt", Destination: fileURI(destinationRoot)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		manifest, ok := store.Manifest("wifi-usb-adopt")
		if ok && len(manifest.Pending()) == 0 && !server.managedRecoveryBusy() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manifest, ok := store.Manifest("wifi-usb-adopt")
	if !ok || len(manifest.Pending()) != 0 {
		t.Fatalf("managed transfer did not complete: %#v", manifest)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("final file differs from phone source")
	}
	select {
	case first := <-progress:
		if first != 4<<20 {
			t.Fatalf("first USB recovery progress=%d, want verified 4 MiB Wi-Fi checkpoint", first)
		}
	default:
		t.Fatal("USB recovery reported no progress checkpoint")
	}
	if len(manifest.Entries) != 1 || manifest.Entries[0].BytesDone != int64(len(data)) || !manifest.Entries[0].CreatedByJob || manifest.Entries[0].SourceFingerprint == nil {
		t.Fatalf("adopted manifest metadata incorrect: %#v", manifest.Entries)
	}
	if candidates := store.Candidates(); len(candidates) != 0 {
		t.Fatalf("adoption candidates remain after completion: %#v", candidates)
	}
	if _, ok := store.Transfer("portal:" + info.ID + ":" + initOut.UploadID); ok {
		t.Fatal("old wireless engine state remains after adoption")
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wireless preserved partial still exists after completion: %v", err)
	}
}
