package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"resumexfer/internal/daemon"
	"resumexfer/internal/state"
)

func TestResolvePathsUsesXDGDirectories(t *testing.T) {
	env := map[string]string{
		"HOME":            "/home/alice",
		"XDG_STATE_HOME":  "/state/alice",
		"XDG_RUNTIME_DIR": "/run/user/1000",
	}
	paths, err := resolvePaths(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if paths.state != "/state/alice/resumexfer/state.json" {
		t.Fatalf("state = %q", paths.state)
	}
	if paths.portalState != "/state/alice/resumexfer/portal-state.json" {
		t.Fatalf("portal state = %q", paths.portalState)
	}
	if paths.socket != "/run/user/1000/resumexfer/control.sock" {
		t.Fatalf("socket = %q", paths.socket)
	}
}

func TestResolvePathsFallsBackToHomeStateButRequiresRuntimeDir(t *testing.T) {
	env := map[string]string{"HOME": "/home/alice"}
	if _, err := resolvePaths(func(key string) string { return env[key] }); err == nil {
		t.Fatal("expected missing XDG_RUNTIME_DIR error")
	}
	env["XDG_RUNTIME_DIR"] = "/run/user/1000"
	paths, err := resolvePaths(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	if paths.state != "/home/alice/.local/state/resumexfer/state.json" {
		t.Fatalf("state = %q", paths.state)
	}
	if paths.portalState != "/home/alice/.local/state/resumexfer/portal-state.json" {
		t.Fatalf("portal state = %q", paths.portalState)
	}
}

func TestRunWiresAutomaticRecoveryForPersistedReconnect(t *testing.T) {
	root := t.TempDir()
	runtimeDir := filepath.Join(root, "run")
	stateHome := filepath.Join(root, "state")
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateHome, "resumexfer", "state.json")
	store, err := state.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "phone.bin")
	destination := filepath.Join(root, "laptop.bin")
	data := []byte("reconnected phone data")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{ID: "job", Direction: "phone-to-laptop", Entries: []state.ManifestEntry{{ID: "entry", Source: source, Destination: destination, Size: int64(len(data))}}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetManifestAwaitingReconnect("job", true); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"HOME":            root,
		"XDG_STATE_HOME":  stateHome,
		"XDG_RUNTIME_DIR": runtimeDir,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, func(key string) string { return env[key] }) }()

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, readErr := os.ReadFile(destination)
		if readErr == nil && bytes.Equal(got, data) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(got, data) {
		cancel()
		<-done
		t.Fatalf("automatic recovery did not produce destination: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunIgnoresCorruptWirelessPortalState(t *testing.T) {
	root := t.TempDir()
	runtimeDir := filepath.Join(root, "run")
	stateHome := filepath.Join(root, "state")
	if err := os.MkdirAll(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	portalState := filepath.Join(stateHome, "resumexfer", "portal-state.json")
	if err := os.MkdirAll(filepath.Dir(portalState), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(portalState, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"HOME":            root,
		"XDG_STATE_HOME":  stateHome,
		"XDG_RUNTIME_DIR": runtimeDir,
	}
	paths, err := resolvePaths(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, func(key string) string { return env[key] }) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if info, statErr := os.Stat(paths.socket); statErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if info, statErr := os.Stat(paths.socket); statErr != nil || info.Mode()&os.ModeSocket == 0 {
		cancel()
		<-done
		t.Fatalf("USB/control daemon did not start with corrupt wireless state: %v", statErr)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunWiresDestinationPreservationWatcher(t *testing.T) {
	root := t.TempDir()
	runtimeDir := filepath.Join(root, "run")
	stateHome := filepath.Join(root, "state")
	mountRoot := filepath.Join(runtimeDir, "gvfs", "mtp:host=phone")
	phoneDir := filepath.Join(mountRoot, "Internal")
	destinationDir := filepath.Join(root, "Downloads")
	for _, dir := range []string{phoneDir, destinationDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	source := filepath.Join(phoneDir, "a.bin")
	if err := os.WriteFile(source, []byte("phone-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"HOME":            root,
		"XDG_STATE_HOME":  stateHome,
		"XDG_RUNTIME_DIR": runtimeDir,
	}
	paths, err := resolvePaths(func(key string) string { return env[key] })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, func(key string) string { return env[key] }) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if info, statErr := os.Stat(paths.socket); statErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := daemon.RecordIntent(context.Background(), paths.socket, state.Intent{ID: "job", Direction: "phone-to-laptop", URIs: []string{"mtp://phone/Internal/a.bin"}}); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	destination := filepath.Join(destinationDir, "a.bin")
	if err := daemon.Observe(context.Background(), paths.socket, daemon.Observation{IntentID: "job", Source: "mtp://phone/Internal/a.bin", Destination: "file://" + destination}); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	partial := destination + ".part"
	if err := os.WriteFile(partial, []byte("partial"), 0o600); err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	preserved := false
	for time.Now().Before(deadline) {
		store, openErr := state.Open(paths.state)
		if openErr == nil {
			for _, candidate := range store.Candidates() {
				if candidate.Source == partial {
					preserved = true
					break
				}
			}
		}
		if preserved {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !preserved {
		t.Fatal("destination partial was not preserved by production daemon")
	}
}
