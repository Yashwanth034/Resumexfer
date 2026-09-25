package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"resumexfer/internal/fingerprint"
	"resumexfer/internal/gvfs"
	intentpkg "resumexfer/internal/intent"
	"resumexfer/internal/portal"
	"resumexfer/internal/state"
)

const (
	DefaultRetention        = 24 * time.Hour
	DefaultCleanupInterval  = time.Hour
	DefaultRecoveryInterval = time.Second
	maxRequestBytes         = 1 << 20
)

var ErrAlreadyRunning = errors.New("resumexfer daemon already running")

type Cleaner interface {
	Cleanup(time.Duration) error
}

type Recoverer interface {
	RecoverManifest(string) error
}

type ManagedCanceller interface {
	CancelManifest(string) error
}

type DestinationWatcher interface {
	AddDir(string) error
}

type LinkSetupper interface {
	EnableIPv4LinkLocal(string) error
}

type PortalManager interface {
	StartShare([]string) (portal.SessionInfo, error)
	StartManifestShare(string) (portal.SessionInfo, error)
	StartReceive(string) (portal.SessionInfo, error)
	StartManifestReceive(string, string) (portal.SessionInfo, error)
	HasReceivePartial(string, int64) bool
	AdoptReceivePartial(string, io.ReaderAt, int64, fingerprint.Fingerprint) (portal.ReceiveAdoption, bool, error)
	Snapshot(string) (portal.SessionInfo, bool)
	CloseSession(string) error
	CancelSession(string) error
	SetPaused(string, bool) error
}

type Observation struct {
	IntentID    string `json:"intent_id"`
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type Config struct {
	SocketPath       string
	RuntimeDir       string
	Store            *state.Store
	Cleaner          Cleaner
	CleanupInterval  time.Duration
	RecoveryInterval time.Duration
	Retention        time.Duration
	Recoverer        Recoverer
	Watcher          DestinationWatcher
	Mounts           func() ([]gvfs.Mount, error)
	Now              func() time.Time
	Portal           PortalManager
	LinkSetupper     LinkSetupper
}

type Server struct {
	socketPath         string
	runtimeDir         string
	store              *state.Store
	cleaner            Cleaner
	cleanupInterval    time.Duration
	recoveryInterval   time.Duration
	retention          time.Duration
	recoverer          Recoverer
	watcher            DestinationWatcher
	mounts             func() ([]gvfs.Mount, error)
	now                func() time.Time
	portal             PortalManager
	linkSetupper       LinkSetupper
	bindMu             sync.Mutex
	bindSeq            map[string]uint64
	uploadStartBindSeq map[string]uint64

	managedStartMu sync.Mutex
	managedMu      sync.Mutex
	managedActive  string
}

type request struct {
	IntentID    string        `json:"intent_id,omitempty"`
	Action      string        `json:"action"`
	Intent      *state.Intent `json:"intent,omitempty"`
	Observation *Observation  `json:"observation,omitempty"`
	Destination string        `json:"destination,omitempty"`
	CancelMode  string        `json:"cancel_mode,omitempty"`
	Paths       []string      `json:"paths,omitempty"`
	SessionID   string        `json:"session_id,omitempty"`
	ManifestID  string        `json:"manifest_id,omitempty"`
	EntryID     string        `json:"entry_id,omitempty"`
	Interface   string        `json:"interface,omitempty"`
}

type response struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

func New(config Config) *Server {
	cleanupInterval := config.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}
	recoveryInterval := config.RecoveryInterval
	if recoveryInterval <= 0 {
		recoveryInterval = DefaultRecoveryInterval
	}
	retention := config.Retention
	if retention <= 0 {
		retention = DefaultRetention
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	mounts := config.Mounts
	if mounts == nil && config.RuntimeDir != "" {
		mounts = func() ([]gvfs.Mount, error) { return gvfs.Discover(config.RuntimeDir) }
	}
	return &Server{
		socketPath:         config.SocketPath,
		runtimeDir:         config.RuntimeDir,
		store:              config.Store,
		cleaner:            config.Cleaner,
		cleanupInterval:    cleanupInterval,
		recoveryInterval:   recoveryInterval,
		retention:          retention,
		recoverer:          config.Recoverer,
		watcher:            config.Watcher,
		mounts:             mounts,
		now:                now,
		portal:             config.Portal,
		linkSetupper:       config.LinkSetupper,
		bindSeq:            make(map[string]uint64),
		uploadStartBindSeq: make(map[string]uint64),
	}
}

func ControlSocketPath(runtimeDir string) string {
	return filepath.Join(runtimeDir, "resumexfer", "control.sock")
}

func (s *Server) Serve(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("store required")
	}
	if s.socketPath == "" {
		return fmt.Errorf("socket path required")
	}
	listener, err := listenPrivate(s.socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()

	cleanupDone := make(chan struct{})
	go func() {
		defer close(cleanupDone)
		s.cleanupLoop(ctx)
	}()
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		s.recoveryLoop(ctx)
	}()

	stopAccept := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stopAccept:
		}
	}()
	defer close(stopAccept)

	for {
		conn, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				<-cleanupDone
				<-recoveryDone
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

func listenPrivate(path string) (*net.UnixListener, error) {
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(parent, 0o700); err != nil {
		return nil, err
	}

	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace non-socket control path")
		}
		conn, dialErr := net.DialTimeout("unix", path, 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return nil, ErrAlreadyRunning
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		if isLiveSocket(path) {
			return nil, ErrAlreadyRunning
		}
		return nil, err
	}
	listener.SetUnlinkOnClose(true)
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func isLiveSocket(path string) bool {
	conn, err := net.DialTimeout("unix", path, 50*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *Server) recoveryLoop(ctx context.Context) {
	if s.recoverer == nil {
		return
	}
	ticker := time.NewTicker(s.recoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkRecovery()
		}
	}
}

func orderRecoveryManifests(manifests []state.Manifest) []state.Manifest {
	ordered := append([]state.Manifest(nil), manifests...)

	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i]
		right := ordered[j]

		if left.AwaitingReconnect != right.AwaitingReconnect {
			return left.AwaitingReconnect
		}

		if left.Managed != right.Managed {
			return left.Managed
		}

		if !left.UpdatedAt.Equal(right.UpdatedAt) {
			return left.UpdatedAt.After(right.UpdatedAt)
		}

		return left.ID < right.ID
	})

	return ordered
}

func (s *Server) checkRecovery() {
	manifests := orderRecoveryManifests(s.store.Manifests())

	// A cancelled managed upload may need the phone to reconnect before its
	// owned partial can be removed. Keep cancellation terminal: never restart
	// copying, but retry cleanup as soon as the endpoint is available.
	if !s.managedRecoveryBusy() {
		if canceller, ok := s.recoverer.(ManagedCanceller); ok {
			for _, manifest := range manifests {
				if !manifest.Managed || !manifest.CancelRequested || manifest.Cancelled {
					continue
				}
				if manifest.Direction == "laptop-to-phone" && s.cancelNeedsPhone(manifest) && !s.cancelEndpointAvailable(manifest) {
					continue
				}
				_ = canceller.CancelManifest(manifest.ID)
			}
		}
	}

	// Managed transfers own the transport from the original paste through
	// reconnect recovery. Run at most one at a time so GVfs/MTP never has
	// competing Resumexfer writers and the visible progress stays coherent.
	for _, manifest := range manifests {
		if !manifest.Managed || manifest.Paused || manifest.CancelRequested || manifest.Cancelled || len(manifest.Pending()) == 0 {
			continue
		}
		if manifest.LastError != "" && !manifest.AwaitingReconnect {
			continue
		}
		if manifest.Direction != "phone-to-laptop" && manifest.Direction != "laptop-to-phone" {
			continue
		}

		available := s.recoveryEndpointAvailable(manifest)
		if !available {
			if !manifest.AwaitingReconnect {
				_ = s.store.SetManifestAwaitingReconnect(manifest.ID, true)
			}
			continue
		}

		if s.launchManagedRecovery(manifest.ID) {
			return
		}
		if s.managedRecoveryBusy() {
			return
		}
	}

	if s.managedRecoveryBusy() {
		return
	}

	for _, manifest := range manifests {
		if manifest.Managed {
			continue
		}
		if len(manifest.Pending()) == 0 {
			continue
		}
		if manifest.Direction != "phone-to-laptop" && manifest.Direction != "laptop-to-phone" {
			continue
		}

		available := s.recoveryEndpointAvailable(manifest)

		if manifest.Direction == "laptop-to-phone" {
			complete := false

			// A bound upload is only an intent. It becomes recoverable only
			// after Nemo has actually created a destination file.
			if available {
				started, allComplete := uploadDestinationState(manifest)
				complete = allComplete

				if started && !manifest.UploadStarted {
					if err := s.store.SetManifestUploadStarted(manifest.ID, true); err != nil {
						continue
					}
					manifest.UploadStarted = true
				}
			}

			// Never convert a fresh, unstarted upload intent into reconnect
			// recovery merely because the phone was unavailable.
			if !manifest.UploadStarted {
				continue
			}

			if !manifest.AwaitingReconnect {
				if !available {
					_ = s.store.SetManifestAwaitingReconnect(manifest.ID, true)
					continue
				}

				// Native Nemo may finish normally without any directory
				// monitor on MTP. Verify and reconcile once every pending
				// destination has reached its expected size.
				if complete {
					_ = s.recoverer.RecoverManifest(manifest.ID)
				}
				continue
			}

			if !available {
				continue
			}

			if err := s.recoverer.RecoverManifest(manifest.ID); err != nil {
				continue
			}
			_ = s.store.SetManifestAwaitingReconnect(manifest.ID, false)
			continue
		}

		// phone-to-laptop retains the existing reconnect behavior.
		if !manifest.AwaitingReconnect {
			if !available {
				_ = s.store.SetManifestAwaitingReconnect(manifest.ID, true)
			}
			continue
		}

		if !available {
			continue
		}

		if err := s.recoverer.RecoverManifest(manifest.ID); err != nil {
			continue
		}
		_ = s.store.SetManifestAwaitingReconnect(manifest.ID, false)
	}
}

func uploadDestinationState(manifest state.Manifest) (started bool, complete bool) {
	pending := manifest.Pending()
	if len(pending) == 0 {
		return false, false
	}

	complete = true

	for _, entry := range pending {
		info, err := os.Stat(entry.Destination)
		if err != nil || !info.Mode().IsRegular() {
			complete = false
			continue
		}

		started = true

		if info.Size() != entry.Size {
			complete = false
		}
	}

	return started, complete
}

func (s *Server) recoveryEndpointAvailable(manifest state.Manifest) bool {
	switch manifest.Direction {
	case "phone-to-laptop":
		for _, entry := range manifest.Pending() {
			if _, err := os.Stat(entry.Source); err == nil {
				return true
			}
		}
	case "laptop-to-phone":
		for _, entry := range manifest.Pending() {
			if mount, ok := gvfs.MountFromPath(s.runtimeDir, entry.Destination); ok {
				if info, err := os.Stat(mount.Root); err == nil && info.IsDir() {
					return true
				}
				continue
			}

			if info, err := os.Stat(filepath.Dir(entry.Destination)); err == nil && info.IsDir() {
				return true
			}
		}
	}
	return false
}

func (s *Server) cancelNeedsPhone(manifest state.Manifest) bool {
	mode := manifest.CancelMode
	if mode == "" {
		mode = state.CancelModeKeepCompleted
	}
	for _, entry := range manifest.Entries {
		if !entry.CreatedByJob {
			continue
		}
		if !entry.Complete || mode == state.CancelModeUndoAll {
			return true
		}
	}
	return false
}

func (s *Server) cancelEndpointAvailable(manifest state.Manifest) bool {
	mode := manifest.CancelMode
	if mode == "" {
		mode = state.CancelModeKeepCompleted
	}
	for _, entry := range manifest.Entries {
		if !entry.CreatedByJob || (entry.Complete && mode != state.CancelModeUndoAll) {
			continue
		}
		if mount, ok := gvfs.MountFromPath(s.runtimeDir, entry.Destination); ok {
			if info, err := os.Stat(mount.Root); err == nil && info.IsDir() {
				return true
			}
			continue
		}
		if info, err := os.Stat(filepath.Dir(entry.Destination)); err == nil && info.IsDir() {
			return true
		}
	}
	return false
}

func (s *Server) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

func (s *Server) cleanup() {
	if s.cleaner != nil {
		if err := s.cleaner.Cleanup(s.retention); err != nil {
			return
		}
	}
	_ = s.store.Prune(s.now(), s.retention)
}

func (s *Server) handleConn(conn *net.UnixConn) {
	defer conn.Close()
	decoder := json.NewDecoder(io.LimitReader(conn, maxRequestBytes))
	var req request
	if err := decoder.Decode(&req); err != nil {
		_ = json.NewEncoder(conn).Encode(response{Error: err.Error()})
		return
	}
	if data, handled, err := s.handlePortal(req); handled {
		if err != nil {
			_ = json.NewEncoder(conn).Encode(response{Error: err.Error()})
			return
		}
		_ = json.NewEncoder(conn).Encode(response{OK: true, Data: data})
		return
	}
	if err := s.handle(req); err != nil {
		_ = json.NewEncoder(conn).Encode(response{Error: err.Error()})
		return
	}
	_ = json.NewEncoder(conn).Encode(response{OK: true})
}

func (s *Server) handlePortal(req request) (any, bool, error) {
	switch req.Action {
	case "portal_share":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if len(req.Paths) == 0 {
			return nil, true, fmt.Errorf("at least one shared path required")
		}
		info, err := s.portal.StartShare(req.Paths)
		return info, true, err
	case "portal_share_manifest":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.ManifestID == "" {
			return nil, true, fmt.Errorf("manifest id required")
		}
		info, err := s.portal.StartManifestShare(req.ManifestID)
		return info, true, err
	case "portal_receive":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.Destination == "" {
			return nil, true, fmt.Errorf("receive destination required")
		}
		info, err := s.portal.StartReceive(req.Destination)
		return info, true, err
	case "portal_continue_manifest":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.ManifestID == "" || req.EntryID == "" {
			return nil, true, fmt.Errorf("manifest id and entry id required")
		}
		info, err := s.portal.StartManifestReceive(req.ManifestID, req.EntryID)
		return info, true, err
	case "portal_status":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.SessionID == "" {
			return nil, true, fmt.Errorf("session id required")
		}
		info, ok := s.portal.Snapshot(req.SessionID)
		if !ok {
			return nil, true, fmt.Errorf("portal session %q not found", req.SessionID)
		}
		return info, true, nil
	case "portal_prepare_link":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if s.linkSetupper == nil {
			return nil, true, fmt.Errorf("temporary wired-link setup unavailable")
		}
		if req.SessionID == "" || req.Interface == "" {
			return nil, true, fmt.Errorf("session id and interface required")
		}
		info, ok := s.portal.Snapshot(req.SessionID)
		if !ok {
			return nil, true, fmt.Errorf("portal session %q not found", req.SessionID)
		}
		var selected *portal.LinkInfo
		for i := range info.Links {
			if info.Links[i].Interface == req.Interface {
				selected = &info.Links[i]
				break
			}
		}
		if selected == nil {
			return nil, true, fmt.Errorf("network interface %q is not part of this portal session", req.Interface)
		}
		switch selected.Kind {
		case "ethernet", "usb-network", "thunderbolt-network":
		default:
			return nil, true, fmt.Errorf("network interface %q is not an eligible wired transfer link", req.Interface)
		}
		if selected.State != "connected" {
			return nil, true, fmt.Errorf("network interface %q is not connected", req.Interface)
		}
		if selected.HasIPv4 {
			return nil, true, fmt.Errorf("network interface %q already has IPv4", req.Interface)
		}
		return nil, true, s.linkSetupper.EnableIPv4LinkLocal(req.Interface)
	case "portal_pause", "portal_resume":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.SessionID == "" {
			return nil, true, fmt.Errorf("session id required")
		}
		paused := req.Action == "portal_pause"
		return nil, true, s.portal.SetPaused(req.SessionID, paused)
	case "portal_cancel":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.SessionID == "" {
			return nil, true, fmt.Errorf("session id required")
		}
		return nil, true, s.portal.CancelSession(req.SessionID)
	case "portal_close":
		if s.portal == nil {
			return nil, true, fmt.Errorf("wireless portal unavailable")
		}
		if req.SessionID == "" {
			return nil, true, fmt.Errorf("session id required")
		}
		return nil, true, s.portal.CloseSession(req.SessionID)
	default:
		return nil, false, nil
	}
}

func (s *Server) handle(req request) error {
	switch req.Action {
	case "record_intent":
		if req.Intent == nil {
			return fmt.Errorf("intent required")
		}
		in := *req.Intent
		now := s.now()
		if in.CreatedAt.IsZero() {
			in.CreatedAt = now
		}
		in.UpdatedAt = now
		return s.store.PutIntent(in)
	case "observe":
		if req.Observation == nil {
			return fmt.Errorf("observation required")
		}
		return s.observe(*req.Observation)
	case "bind_destination":
		if req.IntentID == "" || req.Destination == "" {
			return fmt.Errorf("intent id and destination required")
		}
		return s.bindDestination(req.IntentID, req.Destination)

	case "start_managed_transfer":
		if req.IntentID == "" || req.Destination == "" {
			return fmt.Errorf("intent id and destination required")
		}
		return s.startManagedTransfer(req.IntentID, req.Destination)

	case "pause_managed_transfer":
		if req.IntentID == "" {
			return fmt.Errorf("intent id required")
		}
		return s.pauseManagedTransfer(req.IntentID)

	case "resume_managed_transfer":
		if req.IntentID == "" {
			return fmt.Errorf("intent id required")
		}
		return s.resumeManagedTransfer(req.IntentID)

	case "cancel_managed_transfer":
		if req.IntentID == "" {
			return fmt.Errorf("intent id required")
		}
		return s.cancelManagedTransfer(req.IntentID, req.CancelMode)

	case "start_upload":
		if req.IntentID == "" {
			return fmt.Errorf("intent id required")
		}
		return s.startUpload(req.IntentID)

	case "watch_destination":
		if req.Destination == "" {
			return fmt.Errorf("destination required")
		}
		if s.watcher == nil {
			return fmt.Errorf("destination watcher unavailable")
		}

		if strings.HasPrefix(strings.ToLower(req.Destination), "mtp://") {
			return fmt.Errorf("destination watch must be local")
		}

		destination, err := resolveObservation(req.Destination, nil)
		if err != nil {
			return err
		}

		if _, ok := gvfs.MountFromPath(s.runtimeDir, destination); ok {
			return fmt.Errorf("destination watch must be local")
		}
		info, err := os.Stat(destination)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("destination watch must be a directory")
		}
		return s.watcher.AddDir(destination)
	default:
		return fmt.Errorf("unsupported action %q", req.Action)
	}
}

func (s *Server) bindDestination(intentID, rawDestination string) error {
	seq := s.beginBind(intentID)

	in, ok := s.store.Intent(intentID)
	if !ok {
		return fmt.Errorf("intent %q not found", intentID)
	}

	mounts, err := s.mountsForBinding(in, rawDestination)
	if err != nil {
		return err
	}

	destinationRoot, err := resolveObservation(rawDestination, mounts)
	if err != nil {
		return err
	}

	// Do not touch a live GVfs/MTP destination while Nemo may already be
	// writing through the same transport. MTP directory metadata calls can
	// block behind an active transfer or transiently fail with EIO.
	//
	// The fast path is intentionally limited to laptop-to-phone destinations
	// that have already resolved inside a recognized GVfs MTP mount. Other
	// destinations retain normal filesystem directory validation.
	mtpDestination := pathInMount(destinationRoot, mounts)
	if in.Direction != "laptop-to-phone" || !mtpDestination {
		info, err := os.Stat(destinationRoot)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("destination must be a directory")
		}
	}

	manifest, err := intentpkg.BuildManifest(in, mounts, destinationRoot)
	if err != nil {
		return err
	}

	for _, entry := range manifest.Entries {
		if err := validateAndroidBoundary(in.Direction, entry.Source, entry.Destination, mounts); err != nil {
			return err
		}
	}

	in.Destination = rawDestination
	in.UpdatedAt = s.now()

	committed, err := s.commitBind(intentID, seq, in, manifest)
	if err != nil {
		return err
	}
	if !committed {
		return nil
	}

	if in.Direction == "phone-to-laptop" && s.watcher != nil {
		if err := s.watcher.AddDir(destinationRoot); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) startManagedTransfer(intentID, rawDestination string) error {
	s.managedStartMu.Lock()
	defer s.managedStartMu.Unlock()

	if s.recoverer == nil {
		return fmt.Errorf("managed transfer recovery unavailable")
	}
	if err := s.bindDestination(intentID, rawDestination); err != nil {
		return err
	}

	in, ok := s.store.Intent(intentID)
	if !ok {
		return fmt.Errorf("intent %q not found", intentID)
	}
	manifest, ok := s.store.Manifest(intentID)
	if !ok {
		return fmt.Errorf("manifest %q not found", intentID)
	}
	if manifest.Direction != in.Direction {
		return fmt.Errorf("manifest direction changed")
	}
	if manifest.Direction == "phone-to-laptop" {
		if err := s.adoptWirelessReceivePartials(intentID, manifest); err != nil {
			return err
		}
		manifest, _ = s.store.Manifest(intentID)
	}

	// The external Nemo helper may retry if it loses or times out waiting for
	// an acknowledgement. Once a manifest is managed, the same start request
	// is idempotent: never rebuild/reset its durable progress. A retry after
	// verified completion is also success.
	if manifest.Managed {
		if in.Destination != rawDestination {
			return fmt.Errorf("managed destination changed")
		}
		if manifest.Cancelled || manifest.CancelRequested {
			return fmt.Errorf("managed transfer was cancelled")
		}
		if manifest.Paused {
			return nil
		}
		if in.Direction == "laptop-to-phone" && !manifest.UploadStarted {
			if err := s.store.SetManifestUploadStarted(intentID, true); err != nil {
				return err
			}
		}
		if len(manifest.Pending()) == 0 {
			return nil
		}
		if err := s.store.SetManifestLastError(intentID, ""); err != nil {
			return err
		}
		_ = s.store.SetManifestAwaitingReconnect(intentID, false)
		s.launchManagedRecovery(intentID)
		return nil
	}

	if len(manifest.Pending()) == 0 {
		return fmt.Errorf("manifest %q has no pending entries", intentID)
	}

	if in.Direction == "laptop-to-phone" {
		if err := s.store.SetManifestUploadStarted(intentID, true); err != nil {
			return err
		}
	}
	if err := s.store.SetManifestManaged(intentID, true); err != nil {
		return err
	}
	if err := s.store.SetManifestLastError(intentID, ""); err != nil {
		return err
	}
	_ = s.store.SetManifestAwaitingReconnect(intentID, false)

	s.launchManagedRecovery(intentID)
	return nil
}

func (s *Server) adoptWirelessReceivePartials(manifestID string, manifest state.Manifest) error {
	if s.portal == nil || manifest.Direction != "phone-to-laptop" {
		return nil
	}
	for _, entry := range manifest.Pending() {
		if !s.portal.HasReceivePartial(entry.Destination, entry.Size) {
			continue
		}
		source, err := gvfs.OpenSource(entry.Source)
		if err != nil {
			return err
		}
		if entry.Size > 0 && entry.Size != source.Size {
			_ = source.Close()
			return fmt.Errorf("source size changed during transfer")
		}
		fp, err := fingerprint.ReaderAt(source, source.Size, 64*1024)
		if err != nil {
			_ = source.Close()
			return err
		}
		adopted, found, err := s.portal.AdoptReceivePartial(entry.Destination, source, source.Size, fp)
		closeErr := source.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if !found {
			continue
		}
		if adopted.Offset < 0 || adopted.Offset > source.Size {
			return fmt.Errorf("invalid adopted wireless offset")
		}
		if err := s.store.PrepareManifestEntry(manifestID, entry.ID, true, adopted.Fingerprint); err != nil {
			return err
		}
		if err := s.store.SetManifestEntryProgress(manifestID, entry.ID, adopted.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) pauseManagedTransfer(intentID string) error {
	manifest, ok := s.store.Manifest(intentID)
	if !ok || !manifest.Managed {
		return fmt.Errorf("managed manifest %q not found", intentID)
	}
	if manifest.Cancelled || manifest.CancelRequested || len(manifest.Pending()) == 0 {
		return nil
	}
	if err := s.store.SetManifestLastError(intentID, ""); err != nil {
		return err
	}
	_ = s.store.SetManifestAwaitingReconnect(intentID, false)
	return s.store.SetManifestPaused(intentID, true)
}

func (s *Server) resumeManagedTransfer(intentID string) error {
	manifest, ok := s.store.Manifest(intentID)
	if !ok || !manifest.Managed {
		return fmt.Errorf("managed manifest %q not found", intentID)
	}
	if manifest.Cancelled || manifest.CancelRequested || len(manifest.Pending()) == 0 {
		return nil
	}
	if err := s.store.SetManifestPaused(intentID, false); err != nil {
		return err
	}
	if err := s.store.SetManifestLastError(intentID, ""); err != nil {
		return err
	}
	if !s.recoveryEndpointAvailable(manifest) {
		return s.store.SetManifestAwaitingReconnect(intentID, true)
	}
	_ = s.store.SetManifestAwaitingReconnect(intentID, false)
	s.launchManagedRecovery(intentID)
	return nil
}

func (s *Server) cancelManagedTransfer(intentID, mode string) error {
	manifest, ok := s.store.Manifest(intentID)
	if !ok || !manifest.Managed {
		return fmt.Errorf("managed manifest %q not found", intentID)
	}
	if mode == "" {
		mode = state.CancelModeKeepCompleted
	}
	if mode != state.CancelModeKeepCompleted && mode != state.CancelModeUndoAll {
		return fmt.Errorf("invalid cancel mode %q", mode)
	}
	if manifest.Cancelled {
		return nil
	}
	if err := s.store.RequestManifestCancel(intentID, mode); err != nil {
		return err
	}
	manifest, _ = s.store.Manifest(intentID)
	if manifest.Direction == "laptop-to-phone" && s.cancelNeedsPhone(manifest) {
		// Never interpret an unreachable GVfs path as "already deleted". If
		// cancellation happens while the phone is disconnected, keep the request
		// durable and perform cleanup only after the MTP endpoint is available.
		if manifest.AwaitingReconnect || !s.cancelEndpointAvailable(manifest) {
			_ = s.store.SetManifestAwaitingReconnect(intentID, true)
			return nil
		}
	}
	if !s.managedRecoveryBusy() {
		if canceller, ok := s.recoverer.(ManagedCanceller); ok {
			err := canceller.CancelManifest(intentID)
			if errors.Is(err, state.ErrManifestTransportBusy) || errors.Is(err, state.ErrManifestTransportLeaseLost) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (s *Server) managedRecoveryBusy() bool {
	s.managedMu.Lock()
	defer s.managedMu.Unlock()
	return s.managedActive != ""
}

func (s *Server) launchManagedRecovery(intentID string) bool {
	s.managedMu.Lock()
	if s.managedActive != "" {
		s.managedMu.Unlock()
		return false
	}
	s.managedActive = intentID
	s.managedMu.Unlock()

	go func() {
		err := s.recoverer.RecoverManifest(intentID)
		transportCoordinating := errors.Is(err, state.ErrManifestTransportBusy) || errors.Is(err, state.ErrManifestTransportLeaseLost)
		manifest, ok := s.store.Manifest(intentID)
		if ok && manifest.CancelRequested {
			if canceller, supported := s.recoverer.(ManagedCanceller); supported {
				_ = canceller.CancelManifest(intentID)
				manifest, _ = s.store.Manifest(intentID)
			}
		}
		if ok && (manifest.Paused || manifest.CancelRequested || manifest.Cancelled) {
			_ = s.store.SetManifestLastError(intentID, "")
			_ = s.store.SetManifestAwaitingReconnect(intentID, false)
		} else if transportCoordinating {
			_ = s.store.SetManifestLastError(intentID, "")
		} else if err == nil {
			if ok && len(manifest.Pending()) != 0 {
				_ = s.store.SetManifestAwaitingReconnect(intentID, false)
				_ = s.store.SetManifestLastError(intentID, "transfer stopped before completion")
			} else {
				_ = s.store.SetManifestLastError(intentID, "")
				_ = s.store.SetManifestAwaitingReconnect(intentID, false)
			}
		} else if ok {
			if s.managedErrorNeedsReconnect(manifest, err) {
				// GVfs may leave the MTP mount directory visible briefly (or even
				// indefinitely) after a cable pull. Classify transport-level I/O
				// failures as reconnectable even when that stale mount still stats.
				_ = s.store.SetManifestLastError(intentID, "")
				_ = s.store.SetManifestAwaitingReconnect(intentID, true)
			} else {
				_ = s.store.SetManifestAwaitingReconnect(intentID, false)
				_ = s.store.SetManifestLastError(intentID, err.Error())
			}
		}

		s.managedMu.Lock()
		if s.managedActive == intentID {
			s.managedActive = ""
		}
		s.managedMu.Unlock()
		if transportCoordinating {
			return
		}

		// Pick up another queued transfer promptly, but never tight-loop on a
		// stale GVfs mount after disconnect. Awaiting-reconnect jobs are retried
		// by the normal recovery ticker (once per second by default).
		after, stillExists := s.store.Manifest(intentID)
		if !stillExists || !after.AwaitingReconnect {
			s.checkRecovery()
		}
	}()

	return true
}

func (s *Server) managedErrorNeedsReconnect(manifest state.Manifest, err error) bool {
	if err == nil {
		return false
	}
	if manifest.Direction == "phone-to-laptop" && errors.Is(err, os.ErrNotExist) {
		// A missing source while its containing phone directory is still
		// reachable is a real file-level problem, not a cable disconnect. This
		// prevents one vanished item in a large batch from producing the false
		// "Phone disconnected" state seen in physical testing.
		for _, entry := range manifest.Pending() {
			if entry.Source == "" {
				continue
			}
			if info, statErr := os.Stat(filepath.Dir(entry.Source)); statErr == nil && info.IsDir() {
				return false
			}
		}
	}
	return isManagedTransportInterruption(err) || !s.recoveryEndpointAvailable(manifest)
}

func isManagedTransportInterruption(err error) bool {
	if err == nil {
		return false
	}
	for _, target := range []error{
		syscall.EIO,
		syscall.ENOTCONN,
		syscall.ENODEV,
		syscall.ENXIO,
		syscall.ESTALE,
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.ETIMEDOUT,
		syscall.EHOSTUNREACH,
		syscall.EPIPE,
		syscall.ENOENT,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

func (s *Server) startUpload(intentID string) error {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()

	in, ok := s.store.Intent(intentID)
	if !ok {
		return fmt.Errorf("intent %q not found", intentID)
	}
	if in.Direction != "laptop-to-phone" {
		return fmt.Errorf("start_upload requires laptop-to-phone intent")
	}

	manifest, ok := s.store.Manifest(intentID)
	if !ok {
		return fmt.Errorf("manifest %q not found", intentID)
	}
	if manifest.Direction != "laptop-to-phone" {
		return fmt.Errorf("start_upload requires laptop-to-phone manifest")
	}
	if len(manifest.Pending()) == 0 {
		return fmt.Errorf("manifest %q has no pending entries", intentID)
	}

	if err := s.store.SetManifestUploadStarted(intentID, true); err != nil {
		return err
	}

	if s.uploadStartBindSeq == nil {
		s.uploadStartBindSeq = make(map[string]uint64)
	}

	// Freeze the newest destination bind that had already begun when the
	// user pressed Ctrl+V. That bind may still finish, but any bind begun
	// afterward must not move the active upload.
	s.uploadStartBindSeq[intentID] = s.bindSeq[intentID]

	return nil
}

func (s *Server) beginBind(intentID string) uint64 {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()

	if s.bindSeq == nil {
		s.bindSeq = make(map[string]uint64)
	}

	s.bindSeq[intentID]++
	return s.bindSeq[intentID]
}

func (s *Server) commitBind(
	intentID string,
	seq uint64,
	in state.Intent,
	manifest state.Manifest,
) (bool, error) {
	s.bindMu.Lock()
	defer s.bindMu.Unlock()

	if s.bindSeq[intentID] != seq {
		return false, nil
	}

	if current, ok := s.store.Manifest(intentID); ok && current.Managed {
		// A managed transfer owns the destination selected at paste time.
		// Later focus/navigation callbacks must never move it.
		return false, nil
	}

	// start_upload may arrive while a destination bind is doing slow
	// MTP/GVfs work. A bind that had already begun at that moment may
	// still commit, but bindings begun after Ctrl+V must not move the
	// started upload.
	if current, ok := s.store.Manifest(intentID); ok && current.UploadStarted {
		allowedSeq, hasBoundary := s.uploadStartBindSeq[intentID]
		if !hasBoundary || seq > allowedSeq {
			return false, nil
		}

		manifest.UploadStarted = true
	}

	if err := s.store.PutIntent(in); err != nil {
		return false, err
	}
	if err := s.store.PutManifest(manifest); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Server) mountsForBinding(in state.Intent, rawDestination string) ([]gvfs.Mount, error) {
	raws := make([]string, 0, len(in.URIs)+1)
	raws = append(raws, in.URIs...)
	raws = append(raws, rawDestination)

	mounts := make([]gvfs.Mount, 0, 1)
	seen := make(map[string]struct{})

	for _, raw := range raws {
		if mount, ok := gvfs.MountFromURI(s.runtimeDir, raw); ok {
			if _, exists := seen[mount.Root]; !exists {
				seen[mount.Root] = struct{}{}
				mounts = append(mounts, mount)
			}
			continue
		}

		path, err := resolveObservation(raw, nil)
		if err != nil {
			continue
		}

		if mount, ok := gvfs.MountFromPath(s.runtimeDir, path); ok {
			if _, exists := seen[mount.Root]; !exists {
				seen[mount.Root] = struct{}{}
				mounts = append(mounts, mount)
			}
		}
	}

	if len(mounts) == 0 && s.mounts != nil {
		return s.mounts()
	}

	return mounts, nil
}

func (s *Server) observe(observation Observation) error {
	if observation.IntentID == "" || observation.Source == "" || observation.Destination == "" {
		return fmt.Errorf("complete observation required")
	}
	in, ok := s.store.Intent(observation.IntentID)
	if !ok {
		return fmt.Errorf("intent %q not found", observation.IntentID)
	}

	if current, ok := s.store.Manifest(observation.IntentID); ok && current.Managed {
		// Managed transfers own their manifest from start through completion.
		// A late Nemo destination-monitor observation must not replace it.
		return nil
	}

	mounts, err := s.mountsForObservation(in, observation)
	if err != nil {
		return err
	}

	observedSource, err := resolveObservation(observation.Source, mounts)
	if err != nil {
		return err
	}
	observedDestination, err := resolveObservation(observation.Destination, mounts)
	if err != nil {
		return err
	}
	if err := validateAndroidBoundary(in.Direction, observedSource, observedDestination, mounts); err != nil {
		return err
	}
	manifest, err := intentpkg.Correlate(in, mounts, observedSource, observedDestination)
	if err != nil {
		return err
	}
	if err := s.store.PutManifest(manifest); err != nil {
		return err
	}
	if manifest.Direction == "phone-to-laptop" && s.watcher != nil {
		root, err := destinationWatchRoot(manifest)
		if err != nil {
			return err
		}
		if err := s.watcher.AddDir(root); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) mountsForObservation(in state.Intent, observation Observation) ([]gvfs.Mount, error) {
	raws := make([]string, 0, len(in.URIs)+2)
	raws = append(raws, observation.Source, observation.Destination)
	raws = append(raws, in.URIs...)

	var mounts []gvfs.Mount
	seen := make(map[string]struct{})
	needsDiscovery := false

	for _, raw := range raws {
		if strings.HasPrefix(strings.ToLower(raw), "mtp://") {
			needsDiscovery = true
			continue
		}

		path, err := resolveObservation(raw, nil)
		if err != nil {
			continue
		}

		if mount, ok := gvfs.MountFromPath(s.runtimeDir, path); ok {
			if _, exists := seen[mount.Root]; !exists {
				seen[mount.Root] = struct{}{}
				mounts = append(mounts, mount)
			}
		}
	}

	if len(mounts) == 0 {
		needsDiscovery = true
	}

	if needsDiscovery && s.mounts != nil {
		discovered, err := s.mounts()
		if err != nil {
			return nil, err
		}
		for _, mount := range discovered {
			if _, exists := seen[mount.Root]; exists {
				continue
			}
			seen[mount.Root] = struct{}{}
			mounts = append(mounts, mount)
		}
	}

	return mounts, nil
}

func destinationWatchRoot(manifest state.Manifest) (string, error) {
	if len(manifest.Entries) == 0 {
		return "", fmt.Errorf("manifest has no entries")
	}
	root := filepath.Dir(filepath.Clean(manifest.Entries[0].Destination))
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("manifest destination must be absolute")
	}
	for _, entry := range manifest.Entries[1:] {
		dir := filepath.Dir(filepath.Clean(entry.Destination))
		if !filepath.IsAbs(dir) {
			return "", fmt.Errorf("manifest destination must be absolute")
		}
		for !pathWithin(root, dir) {
			parent := filepath.Dir(root)
			if parent == root {
				break
			}
			root = parent
		}
	}
	return root, nil
}

func pathWithin(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func resolveObservation(raw string, mounts []gvfs.Mount) (string, error) {
	if strings.Contains(raw, "://") || strings.HasPrefix(raw, "file:") {
		return gvfs.ResolveURI(raw, mounts)
	}
	if !filepath.IsAbs(raw) {
		return "", fmt.Errorf("observation path must be absolute")
	}
	return filepath.Clean(raw), nil
}

func validateAndroidBoundary(direction, source, destination string, mounts []gvfs.Mount) error {
	sourceMTP := pathInMount(source, mounts)
	destinationMTP := pathInMount(destination, mounts)
	switch direction {
	case "phone-to-laptop":
		if !sourceMTP || destinationMTP {
			return fmt.Errorf("phone-to-laptop observation must cross exactly one MTP boundary")
		}
	case "laptop-to-phone":
		if sourceMTP || !destinationMTP {
			return fmt.Errorf("laptop-to-phone observation must cross exactly one MTP boundary")
		}
	default:
		return fmt.Errorf("unsupported intent direction %q", direction)
	}
	return nil
}

func pathInMount(path string, mounts []gvfs.Mount) bool {
	path = filepath.Clean(path)
	for _, mount := range mounts {
		root := filepath.Clean(mount.Root)
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func RecordIntent(ctx context.Context, socketPath string, in state.Intent) error {
	return send(ctx, socketPath, request{Action: "record_intent", Intent: &in})
}

func Observe(ctx context.Context, socketPath string, observation Observation) error {
	return send(ctx, socketPath, request{Action: "observe", Observation: &observation})
}

func send(ctx context.Context, socketPath string, req request) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return err
	}
	var resp response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return err
	}
	if !resp.OK {
		if resp.Error == "" {
			return fmt.Errorf("daemon request failed")
		}
		return errors.New(resp.Error)
	}
	return nil
}
