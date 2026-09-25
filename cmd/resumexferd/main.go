package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"resumexfer/internal/daemon"
	"resumexfer/internal/engine"
	"resumexfer/internal/netsetup"
	"resumexfer/internal/portal"
	"resumexfer/internal/preserve"
	"resumexfer/internal/recovery"
	"resumexfer/internal/state"
)

type runtimePaths struct {
	state       string
	portalState string
	socket      string
	runtimeDir  string
}

func resolvePaths(getenv func(string) string) (runtimePaths, error) {
	runtimeDir := getenv("XDG_RUNTIME_DIR")
	if runtimeDir == "" {
		return runtimePaths{}, fmt.Errorf("XDG_RUNTIME_DIR is required")
	}
	stateHome := getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home := getenv("HOME")
		if home == "" {
			return runtimePaths{}, fmt.Errorf("HOME is required when XDG_STATE_HOME is unset")
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	return runtimePaths{
		state:       filepath.Join(stateHome, "resumexfer", "state.json"),
		portalState: filepath.Join(stateHome, "resumexfer", "portal-state.json"),
		socket:      daemon.ControlSocketPath(runtimeDir),
		runtimeDir:  runtimeDir,
	}, nil
}

func run(ctx context.Context, getenv func(string) string) error {
	paths, err := resolvePaths(getenv)
	if err != nil {
		return err
	}
	store, err := state.Open(paths.state)
	if err != nil {
		return err
	}
	manager := &preserve.Manager{Store: store}
	watcher, err := preserve.NewWatcher(manager)
	if err != nil {
		return err
	}
	defer watcher.Close()
	recoverer := &recovery.Runner{Store: store, Engine: engine.Engine{Store: store}}
	portalConfig := portal.Config{StatePath: paths.portalState, Store: store}
	portalManager, err := portal.Open(portalConfig)
	if err != nil {
		log.Printf("resumexfer: wireless portal state ignored: %v", err)
		portalManager = portal.New(portalConfig)
	}
	defer portalManager.Close()
	server := daemon.New(daemon.Config{
		SocketPath:   paths.socket,
		RuntimeDir:   paths.runtimeDir,
		Store:        store,
		Cleaner:      manager,
		Recoverer:    recoverer,
		Watcher:      watcher,
		Portal:       portalManager,
		LinkSetupper: netsetup.Manager{},
	})
	return server.Serve(ctx)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Getenv); err != nil {
		log.Fatal(err)
	}
}
