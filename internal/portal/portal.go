package portal

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/state"
)

const (
	ModeSend                   = "send"
	ModeReceive                = "receive"
	DefaultSessionTTL          = 24 * time.Hour
	MaxUploadChunk             = 32 << 20
	streamUploadCheckpoint     = 128 << 20
	portalStateSchema          = 1
	persistCheckpoint          = 64 << 20
	downloadTrackInterval      = 8 << 20
	defaultPortalListenAddress = "0.0.0.0:8787"
	defaultTransportLeaseTTL   = 30 * time.Second
	capabilityTokenBytes       = 9
	maxPortalHeaderBytes       = 32 << 10
)

var errPortalSessionClosed = errors.New("portal session closed")

type HostRoute struct {
	Address   string
	Interface string
	Kind      string
}

type RouteInfo struct {
	URL       string `json:"url"`
	Interface string `json:"interface,omitempty"`
	Kind      string `json:"kind"`
}

type LinkInfo struct {
	Interface string `json:"interface"`
	Kind      string `json:"kind"`
	State     string `json:"state"`
	HasIPv4   bool   `json:"has_ipv4"`
}

type Config struct {
	ListenAddress           string
	SessionTTL              time.Duration
	Now                     func() time.Time
	HostAddresses           func() []string
	HostRoutes              func() []HostRoute
	HostLinks               func() []LinkInfo
	StatePath               string
	Store                   *state.Store
	TransportLeaseTTL       time.Duration
	TransportLeaseHeartbeat time.Duration
}

type SessionInfo struct {
	ID          string      `json:"id"`
	Mode        string      `json:"mode"`
	URLs        []string    `json:"urls"`
	Routes      []RouteInfo `json:"routes,omitempty"`
	Links       []LinkInfo  `json:"links,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
	ExpiresAt   time.Time   `json:"expires_at"`
	TotalBytes  int64       `json:"total_bytes,omitempty"`
	BytesDone   int64       `json:"bytes_done,omitempty"`
	TotalFiles  int         `json:"total_files,omitempty"`
	DoneFiles   int         `json:"done_files,omitempty"`
	Complete    bool        `json:"complete,omitempty"`
	Paused      bool        `json:"paused,omitempty"`
	Destination string      `json:"destination,omitempty"`
	ManifestID  string      `json:"manifest_id,omitempty"`
	EntryID     string      `json:"entry_id,omitempty"`
}

type Manager struct {
	mu          sync.RWMutex
	stateFileMu sync.Mutex
	cfg         Config
	listener    net.Listener
	server      *http.Server
	closed      bool
	port        int
	sessions    map[string]*session
	tokenToID   map[string]string
}

type session struct {
	mu sync.RWMutex

	fastMu    sync.Mutex
	fastPeers map[*webrtc.PeerConnection]struct{}

	ID        string
	Token     string
	Mode      string
	CreatedAt time.Time
	ExpiresAt time.Time

	Entries         []shareEntry
	Downloaded      map[int][]byteRange
	Delivered       map[int]bool
	ReceiveRoot     string
	Uploads         map[string]*uploadState
	TotalBytes      int64
	BytesDone       int64
	DoneFiles       int
	Paused          bool
	ReceiveFinished bool
	checkpointBytes int64
	resumeCh        chan struct{}
	closedCh        chan struct{}

	ManifestID      string
	ManifestEntryID string
	TransportOwner  string
	LeaseGeneration uint64
}

type byteRange struct {
	Start int64
	End   int64
}

type shareEntry struct {
	Path            string
	Name            string
	ZipName         string
	Size            int64
	ModTime         time.Time
	Fingerprint     fingerprint.Fingerprint
	ManifestEntryID string
}

type uploadState struct {
	mu sync.Mutex

	ID              string
	Relative        string
	Destination     string
	Partial         string
	Size            int64
	Complete        bool
	Managed         bool
	Verified        bool
	Adopted         bool
	ManifestEntryID string
	Fingerprint     *fingerprint.Fingerprint
}

type uploadInitRequest struct {
	Name         string                   `json:"name"`
	RelativePath string                   `json:"relative_path"`
	Size         int64                    `json:"size"`
	Fingerprint  *fingerprint.Fingerprint `json:"fingerprint,omitempty"`
}

type uploadVerifyChunk struct {
	Offset int64  `json:"offset"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

type uploadInitResponse struct {
	UploadID          string              `json:"upload_id"`
	Offset            int64               `json:"offset"`
	Complete          bool                `json:"complete"`
	NeedsVerification bool                `json:"needs_verification,omitempty"`
	VerifyChunks      []uploadVerifyChunk `json:"verify_chunks,omitempty"`
}

type uploadVerifyRequest struct {
	Offset       int64    `json:"offset"`
	PrefixSHA256 string   `json:"prefix_sha256,omitempty"`
	ChunkSHA256  []string `json:"chunk_sha256,omitempty"`
}

type persistedState struct {
	Schema   int                `json:"schema"`
	Port     int                `json:"port,omitempty"`
	Sessions []persistedSession `json:"sessions,omitempty"`
}

type persistedSession struct {
	ID              string              `json:"id"`
	Token           string              `json:"token"`
	Mode            string              `json:"mode"`
	CreatedAt       time.Time           `json:"created_at"`
	ExpiresAt       time.Time           `json:"expires_at"`
	Entries         []shareEntry        `json:"entries,omitempty"`
	Downloaded      map[int][]byteRange `json:"downloaded,omitempty"`
	Delivered       map[int]bool        `json:"delivered,omitempty"`
	ReceiveRoot     string              `json:"receive_root,omitempty"`
	Uploads         []persistedUpload   `json:"uploads,omitempty"`
	Paused          bool                `json:"paused,omitempty"`
	ReceiveFinished bool                `json:"receive_finished,omitempty"`
	ManifestID      string              `json:"manifest_id,omitempty"`
	ManifestEntryID string              `json:"manifest_entry_id,omitempty"`
	TransportOwner  string              `json:"transport_owner,omitempty"`
}

type persistedUpload struct {
	ID              string                   `json:"id"`
	Relative        string                   `json:"relative"`
	Size            int64                    `json:"size"`
	Complete        bool                     `json:"complete,omitempty"`
	Adopted         bool                     `json:"adopted,omitempty"`
	ManifestEntryID string                   `json:"manifest_entry_id,omitempty"`
	Fingerprint     *fingerprint.Fingerprint `json:"fingerprint,omitempty"`
}

type ReceiveAdoption struct {
	CandidateID string
	Offset      int64
	Fingerprint fingerprint.Fingerprint
}

var ErrAmbiguousReceiveAdoption = errors.New("multiple wireless uploads match managed transfer")

func New(config Config) *Manager {
	return newManager(config)
}

func Open(config Config) (*Manager, error) {
	m := newManager(config)
	if config.StatePath == "" {
		return m, nil
	}
	changed, err := m.load()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	expired := m.expireLocked(m.cfg.Now())
	changed = len(expired) > 0 || changed
	if len(m.sessions) > 0 {
		if err := m.ensureServerLocked(); err != nil {
			m.mu.Unlock()
			_ = m.cleanupSessions(expired)
			return nil, err
		}
		changed = true
	}
	if changed {
		if err := m.persistLocked(); err != nil {
			m.mu.Unlock()
			_ = m.cleanupSessions(expired)
			return nil, err
		}
	}
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return m, nil
}

func newManager(config Config) *Manager {
	if config.ListenAddress == "" {
		config.ListenAddress = defaultPortalListenAddress
	}
	if config.SessionTTL <= 0 {
		config.SessionTTL = DefaultSessionTTL
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.TransportLeaseTTL <= 0 {
		config.TransportLeaseTTL = defaultTransportLeaseTTL
	}
	if config.TransportLeaseHeartbeat <= 0 {
		config.TransportLeaseHeartbeat = config.TransportLeaseTTL / 3
		if config.TransportLeaseHeartbeat > 5*time.Second {
			config.TransportLeaseHeartbeat = 5 * time.Second
		}
		if config.TransportLeaseHeartbeat < 10*time.Millisecond {
			config.TransportLeaseHeartbeat = 10 * time.Millisecond
		}
	}
	return &Manager{
		cfg:       config,
		sessions:  make(map[string]*session),
		tokenToID: make(map[string]string),
	}
}

func (m *Manager) load() (bool, error) {
	b, err := os.ReadFile(m.cfg.StatePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if len(b) == 0 {
		return true, nil
	}
	var persisted persistedState
	if err := json.Unmarshal(b, &persisted); err != nil {
		return false, fmt.Errorf("decode portal state: %w", err)
	}
	if persisted.Schema != portalStateSchema {
		return false, fmt.Errorf("unsupported portal state schema %d", persisted.Schema)
	}
	if persisted.Port >= 1 && persisted.Port <= 65535 {
		m.port = persisted.Port
	}

	dirty := false
	for _, saved := range persisted.Sessions {
		s, keep, err := restoreSession(saved, m.cfg.Store)
		if err != nil {
			return false, fmt.Errorf("restore portal session %q: %w", saved.ID, err)
		}
		if !keep {
			dirty = true
			continue
		}
		if _, exists := m.sessions[s.ID]; exists {
			return false, fmt.Errorf("duplicate portal session id %q", s.ID)
		}
		if _, exists := m.tokenToID[s.Token]; exists {
			return false, fmt.Errorf("duplicate portal capability token")
		}
		m.sessions[s.ID] = s
		m.tokenToID[s.Token] = s.ID
	}
	return dirty, nil
}

func restoreSession(saved persistedSession, store *state.Store) (*session, bool, error) {
	if saved.ID == "" || saved.Token == "" {
		return nil, false, fmt.Errorf("id and token are required")
	}
	if saved.Mode != ModeSend && saved.Mode != ModeReceive {
		return nil, false, fmt.Errorf("invalid mode %q", saved.Mode)
	}
	if saved.CreatedAt.IsZero() || saved.ExpiresAt.IsZero() || !saved.ExpiresAt.After(saved.CreatedAt) {
		return nil, false, fmt.Errorf("invalid session timestamps")
	}
	s := &session{
		ID:              saved.ID,
		Token:           saved.Token,
		Mode:            saved.Mode,
		CreatedAt:       saved.CreatedAt,
		ExpiresAt:       saved.ExpiresAt,
		Paused:          saved.Paused,
		ReceiveFinished: saved.ReceiveFinished,
		closedCh:        make(chan struct{}),
		ManifestID:      saved.ManifestID,
		ManifestEntryID: saved.ManifestEntryID,
		TransportOwner:  saved.TransportOwner,
	}
	if s.Paused {
		s.resumeCh = make(chan struct{})
	}

	if s.Mode == ModeSend {
		if len(saved.Entries) == 0 {
			return nil, false, fmt.Errorf("send session has no files")
		}
		s.Entries = append([]shareEntry(nil), saved.Entries...)
		s.Downloaded = make(map[int][]byteRange)
		s.Delivered = make(map[int]bool)
		for index, delivered := range saved.Delivered {
			if delivered && (index < 0 || index >= len(s.Entries)) {
				return nil, false, fmt.Errorf("invalid delivered file index")
			}
		}
		for index, entry := range s.Entries {
			if entry.Path == "" || !filepath.IsAbs(entry.Path) || entry.Size < 0 || entry.ZipName == "" {
				return nil, false, fmt.Errorf("invalid shared file entry")
			}
			if !validShareFingerprint(entry.Fingerprint, entry.Size) {
				return nil, false, nil
			}
			s.TotalBytes += entry.Size
			for _, r := range saved.Downloaded[index] {
				if r.Start < 0 {
					r.Start = 0
				}
				if r.End > entry.Size {
					r.End = entry.Size
				}
				if r.End > r.Start {
					s.Downloaded[index] = mergeRange(s.Downloaded[index], r)
				}
			}
			covered := coveredBytes(s.Downloaded[index])
			s.BytesDone += covered
			if saved.Delivered[index] {
				if entry.Size > 0 && covered < entry.Size {
					return nil, false, fmt.Errorf("delivered shared file is missing recorded bytes")
				}
				s.Delivered[index] = true
				s.DoneFiles++
			}
		}
		s.checkpointBytes = s.BytesDone
		return s, true, nil
	}

	if saved.ReceiveRoot == "" || !filepath.IsAbs(saved.ReceiveRoot) {
		return nil, false, fmt.Errorf("invalid receive root")
	}
	root := filepath.Clean(saved.ReceiveRoot)
	rootInfo, err := os.Stat(root)
	if errors.Is(err, os.ErrNotExist) || (err == nil && !rootInfo.IsDir()) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	s.ReceiveRoot = root
	s.Uploads = make(map[string]*uploadState)
	for _, savedUpload := range saved.Uploads {
		if savedUpload.ID == "" || savedUpload.Size < 0 {
			return nil, false, fmt.Errorf("invalid upload state")
		}
		rel, err := cleanRelativePath(savedUpload.Relative)
		if err != nil {
			return nil, false, err
		}
		if _, exists := s.Uploads[rel]; exists {
			return nil, false, fmt.Errorf("duplicate upload path %q", rel)
		}
		destination, err := safeJoin(root, rel)
		if err != nil {
			return nil, false, err
		}
		managed := s.ManifestID != ""
		if !managed && (savedUpload.Fingerprint == nil || !validUploadFingerprint(*savedUpload.Fingerprint, savedUpload.Size)) {
			return nil, false, nil
		}
		if !managed && savedUpload.Adopted && store != nil {
			if _, ok := store.Candidate(portalCandidateID(saved.ID, savedUpload.ID)); ok {
				continue
			}
		}
		partial := partialPath(destination, s.ID, savedUpload.ID)
		if managed {
			partial = destination + engine.PartialSuffix
		}
		entryID := savedUpload.ManifestEntryID
		if managed && entryID == "" {
			// Backward compatibility with older single-entry persisted sessions.
			entryID = s.ManifestEntryID
		}
		u := &uploadState{
			ID:              savedUpload.ID,
			Relative:        rel,
			Destination:     destination,
			Partial:         partial,
			Size:            savedUpload.Size,
			Managed:         managed,
			Adopted:         false,
			ManifestEntryID: entryID,
		}
		if !managed {
			fp := *savedUpload.Fingerprint
			u.Fingerprint = &fp
		}
		partialInfo, partialErr := os.Stat(u.Partial)
		if partialErr != nil && !errors.Is(partialErr, os.ErrNotExist) {
			return nil, false, partialErr
		}
		if partialErr == nil && (!partialInfo.Mode().IsRegular() || partialInfo.Size() > u.Size) {
			return nil, false, fmt.Errorf("invalid partial upload %q", rel)
		}
		destinationInfo, destinationErr := os.Stat(u.Destination)
		if destinationErr != nil && !errors.Is(destinationErr, os.ErrNotExist) {
			return nil, false, destinationErr
		}
		if destinationErr == nil && destinationInfo.Mode().IsRegular() && destinationInfo.Size() == u.Size && errors.Is(partialErr, os.ErrNotExist) {
			u.Complete = true
			s.BytesDone += u.Size
			s.DoneFiles++
		} else if savedUpload.Complete && destinationErr == nil && destinationInfo.Mode().IsRegular() && destinationInfo.Size() == u.Size {
			u.Complete = true
			s.BytesDone += u.Size
			s.DoneFiles++
		} else if partialErr == nil {
			s.BytesDone += partialInfo.Size()
		}
		s.TotalBytes += u.Size
		s.Uploads[rel] = u
	}
	s.checkpointBytes = s.BytesDone
	return s, true, nil
}

func (m *Manager) persistLocked() error {
	path := strings.TrimSpace(m.cfg.StatePath)
	if path == "" {
		return nil
	}
	state := persistedState{Schema: portalStateSchema, Port: m.port}
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		state.Sessions = append(state.Sessions, persistedSessionSnapshot(m.sessions[id]))
	}
	m.stateFileMu.Lock()
	defer m.stateFileMu.Unlock()
	return writePersistedStateFile(path, state)
}

func writePersistedStateFile(path string, state persistedState) error {
	if len(state.Sessions) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".resumexfer-portal-state-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(name)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func (m *Manager) persistPausedState(id string, paused bool) error {
	path := strings.TrimSpace(m.cfg.StatePath)
	if path == "" {
		return nil
	}
	m.stateFileMu.Lock()
	defer m.stateFileMu.Unlock()

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var state persistedState
	if err := json.Unmarshal(b, &state); err != nil {
		return err
	}
	for i := range state.Sessions {
		if state.Sessions[i].ID == id {
			state.Sessions[i].Paused = paused
			return writePersistedStateFile(path, state)
		}
	}
	return nil
}

func (m *Manager) persistRemovedSession(id string) error {
	path := strings.TrimSpace(m.cfg.StatePath)
	if path == "" {
		return nil
	}
	m.stateFileMu.Lock()
	defer m.stateFileMu.Unlock()

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var state persistedState
	if err := json.Unmarshal(b, &state); err != nil {
		return err
	}
	kept := state.Sessions[:0]
	for _, saved := range state.Sessions {
		if saved.ID != id {
			kept = append(kept, saved)
		}
	}
	state.Sessions = kept
	return writePersistedStateFile(path, state)
}

func persistedSessionSnapshot(s *session) persistedSession {
	// Never hold the session lock while waiting on an upload lock. Active
	// streams update session progress while holding their upload lock, so the
	// opposite order here can deadlock persistence against a live transfer.
	s.mu.RLock()
	out := persistedSession{
		ID:              s.ID,
		Token:           s.Token,
		Mode:            s.Mode,
		CreatedAt:       s.CreatedAt,
		ExpiresAt:       s.ExpiresAt,
		Entries:         append([]shareEntry(nil), s.Entries...),
		ReceiveRoot:     s.ReceiveRoot,
		Paused:          s.Paused,
		ReceiveFinished: s.ReceiveFinished,
		ManifestID:      s.ManifestID,
		ManifestEntryID: s.ManifestEntryID,
		TransportOwner:  s.TransportOwner,
	}
	if len(s.Downloaded) > 0 {
		out.Downloaded = make(map[int][]byteRange, len(s.Downloaded))
		for index, ranges := range s.Downloaded {
			out.Downloaded[index] = append([]byteRange(nil), ranges...)
		}
	}
	if len(s.Delivered) > 0 {
		out.Delivered = make(map[int]bool, len(s.Delivered))
		for index, delivered := range s.Delivered {
			if delivered {
				out.Delivered[index] = true
			}
		}
	}
	uploads := make(map[string]*uploadState, len(s.Uploads))
	for rel, u := range s.Uploads {
		uploads[rel] = u
	}
	s.mu.RUnlock()

	if len(uploads) > 0 {
		rels := make([]string, 0, len(uploads))
		for rel := range uploads {
			rels = append(rels, rel)
		}
		sort.Strings(rels)
		for _, rel := range rels {
			u := uploads[rel]
			u.mu.Lock()
			saved := persistedUpload{ID: u.ID, Relative: u.Relative, Size: u.Size, Complete: u.Complete, Adopted: u.Adopted, ManifestEntryID: u.ManifestEntryID}
			if !u.Managed && u.Fingerprint != nil && validUploadFingerprint(*u.Fingerprint, u.Size) {
				fp := *u.Fingerprint
				saved.Fingerprint = &fp
			}
			u.mu.Unlock()
			out.Uploads = append(out.Uploads, saved)
		}
	}
	return out
}

func (m *Manager) persist() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persistLocked()
}

func (m *Manager) StartShare(paths []string) (SessionInfo, error) {
	entries, total, err := collectShareEntries(paths)
	if err != nil {
		return SessionInfo{}, err
	}
	if len(entries) == 0 {
		return SessionInfo{}, fmt.Errorf("no regular files selected")
	}

	now := m.cfg.Now()
	s := &session{
		ID:         randomID("share"),
		Token:      randomToken(),
		Mode:       ModeSend,
		CreatedAt:  now,
		ExpiresAt:  now.Add(m.cfg.SessionTTL),
		Entries:    entries,
		Downloaded: make(map[int][]byteRange),
		Delivered:  make(map[int]bool),
		TotalBytes: total,
		closedCh:   make(chan struct{}),
	}

	m.mu.Lock()
	if err := m.ensureServerLocked(); err != nil {
		m.mu.Unlock()
		return SessionInfo{}, err
	}
	expired := m.expireLocked(now)
	m.sessions[s.ID] = s
	m.tokenToID[s.Token] = s.ID
	if err := m.persistLocked(); err != nil {
		delete(m.sessions, s.ID)
		delete(m.tokenToID, s.Token)
		s.close()
		m.mu.Unlock()
		_ = m.cleanupSessions(expired)
		return SessionInfo{}, err
	}
	info := m.infoLocked(s)
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return info, nil
}

func (m *Manager) StartManifestShare(manifestID string) (SessionInfo, error) {
	if m.cfg.Store == nil {
		return SessionInfo{}, fmt.Errorf("state store required for managed share")
	}
	manifest, ok := m.cfg.Store.Manifest(manifestID)
	if !ok {
		return SessionInfo{}, fmt.Errorf("manifest %q not found", manifestID)
	}
	if !manifest.Managed || manifest.Direction != "laptop-to-phone" {
		return SessionInfo{}, fmt.Errorf("manifest is not a managed laptop-to-phone transfer")
	}
	pending := manifest.Pending()
	if len(pending) == 0 {
		return SessionInfo{}, fmt.Errorf("manifest has no pending entries")
	}

	root := filepath.Clean(manifest.DestinationRoot)
	entries := make([]shareEntry, 0, len(pending))
	var total int64
	for _, item := range pending {
		if strings.TrimSpace(item.Source) == "" || !filepath.IsAbs(item.Source) {
			return SessionInfo{}, fmt.Errorf("manifest entry %q has invalid source", item.ID)
		}
		info, err := os.Lstat(item.Source)
		if err != nil {
			return SessionInfo{}, err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return SessionInfo{}, fmt.Errorf("manifest entry %q source is not a regular file", item.ID)
		}
		if info.Size() != item.Size {
			return SessionInfo{}, fmt.Errorf("manifest entry %q source size changed", item.ID)
		}
		zipName := filepath.Base(item.Source)
		if root != "" && filepath.IsAbs(root) && item.Destination != "" && filepath.IsAbs(item.Destination) {
			if rel, err := filepath.Rel(root, item.Destination); err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				zipName = rel
			}
		}
		entry, err := makeShareEntry(item.Source, filepath.Base(item.Source), zipName, info)
		if err != nil {
			return SessionInfo{}, err
		}
		if item.SourceFingerprint != nil && !item.SourceFingerprint.Compatible(entry.Fingerprint) {
			return SessionInfo{}, fmt.Errorf("manifest entry %q source changed", item.ID)
		}
		entry.ManifestEntryID = item.ID
		entries = append(entries, entry)
		total += item.Size
	}

	now := m.cfg.Now()
	s := &session{
		ID:         randomID("share"),
		Token:      randomToken(),
		Mode:       ModeSend,
		CreatedAt:  now,
		ExpiresAt:  now.Add(m.cfg.SessionTTL),
		Entries:    entries,
		Downloaded: make(map[int][]byteRange),
		Delivered:  make(map[int]bool),
		TotalBytes: total,
		closedCh:   make(chan struct{}),
		ManifestID: manifestID,
	}

	m.mu.Lock()
	expired := m.expireLocked(now)
	for _, existing := range m.sessions {
		if existing.Mode != ModeSend || existing.ManifestID != manifestID {
			continue
		}
		if err := m.ensureServerLocked(); err != nil {
			m.mu.Unlock()
			_ = m.cleanupSessions(expired)
			return SessionInfo{}, err
		}
		result := m.infoLocked(existing)
		m.mu.Unlock()
		_ = m.cleanupSessions(expired)
		return result, nil
	}
	if err := m.ensureServerLocked(); err != nil {
		m.mu.Unlock()
		_ = m.cleanupSessions(expired)
		return SessionInfo{}, err
	}
	m.sessions[s.ID] = s
	m.tokenToID[s.Token] = s.ID
	if err := m.persistLocked(); err != nil {
		delete(m.sessions, s.ID)
		delete(m.tokenToID, s.Token)
		s.close()
		m.mu.Unlock()
		_ = m.cleanupSessions(expired)
		return SessionInfo{}, err
	}
	result := m.infoLocked(s)
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return result, nil
}

func (m *Manager) StartReceive(destination string) (SessionInfo, error) {
	root, err := filepath.Abs(destination)
	if err != nil {
		return SessionInfo{}, err
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return SessionInfo{}, err
	}
	if !rootInfo.IsDir() {
		return SessionInfo{}, fmt.Errorf("receive destination must be a directory")
	}

	now := m.cfg.Now()
	s := &session{
		ID:          randomID("receive"),
		Token:       randomToken(),
		Mode:        ModeReceive,
		CreatedAt:   now,
		ExpiresAt:   now.Add(m.cfg.SessionTTL),
		ReceiveRoot: filepath.Clean(root),
		Uploads:     make(map[string]*uploadState),
		closedCh:    make(chan struct{}),
	}

	m.mu.Lock()
	if err := m.ensureServerLocked(); err != nil {
		m.mu.Unlock()
		return SessionInfo{}, err
	}
	expired := m.expireLocked(now)
	m.sessions[s.ID] = s
	m.tokenToID[s.Token] = s.ID
	if err := m.persistLocked(); err != nil {
		delete(m.sessions, s.ID)
		delete(m.tokenToID, s.Token)
		s.close()
		m.mu.Unlock()
		_ = m.cleanupSessions(expired)
		return SessionInfo{}, err
	}
	info := m.infoLocked(s)
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return info, nil
}

func (m *Manager) StartManifestReceive(manifestID, entryID string) (SessionInfo, error) {
	if m.cfg.Store == nil {
		return SessionInfo{}, fmt.Errorf("state store required for managed receive")
	}
	manifest, ok := m.cfg.Store.Manifest(manifestID)
	if !ok {
		return SessionInfo{}, fmt.Errorf("manifest %q not found", manifestID)
	}
	if !manifest.Managed || manifest.Direction != "phone-to-laptop" {
		return SessionInfo{}, fmt.Errorf("manifest is not a managed phone-to-laptop transfer")
	}
	preferred, preferredOK := manifestEntry(manifest, entryID)
	if entryID != "" && !preferredOK {
		return SessionInfo{}, fmt.Errorf("manifest entry %q not found", entryID)
	}

	pending := manifest.Pending()
	if len(pending) == 0 {
		return SessionInfo{}, fmt.Errorf("manifest has no pending entries")
	}

	root := filepath.Clean(manifest.DestinationRoot)
	if root == "." || root == "" || !filepath.IsAbs(root) {
		if !preferredOK || preferred.Destination == "" || !filepath.IsAbs(preferred.Destination) {
			return SessionInfo{}, fmt.Errorf("manifest destination root is unavailable")
		}
		root = filepath.Dir(filepath.Clean(preferred.Destination))
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return SessionInfo{}, err
	}
	if !rootInfo.IsDir() {
		return SessionInfo{}, fmt.Errorf("manifest destination root is not a directory")
	}

	now := m.cfg.Now()
	id := randomID("receive")
	sess := &session{
		ID:              id,
		Token:           randomToken(),
		Mode:            ModeReceive,
		CreatedAt:       now,
		ExpiresAt:       now.Add(m.cfg.SessionTTL),
		ReceiveRoot:     root,
		Uploads:         make(map[string]*uploadState),
		closedCh:        make(chan struct{}),
		ManifestID:      manifestID,
		ManifestEntryID: entryID,
		TransportOwner:  "wifi-portal:" + id,
	}

	for _, entry := range pending {
		if entry.Size < 0 || entry.Destination == "" || !filepath.IsAbs(entry.Destination) {
			return SessionInfo{}, fmt.Errorf("manifest entry %q has invalid destination metadata", entry.ID)
		}
		rel, err := filepath.Rel(root, filepath.Clean(entry.Destination))
		if err != nil {
			return SessionInfo{}, err
		}
		rel, err = cleanRelativePath(rel)
		if err != nil {
			return SessionInfo{}, fmt.Errorf("manifest entry %q is outside destination root", entry.ID)
		}
		if _, exists := sess.Uploads[rel]; exists {
			return SessionInfo{}, fmt.Errorf("duplicate managed destination path %q", rel)
		}
		if _, err := os.Stat(entry.Destination); err == nil {
			return SessionInfo{}, fmt.Errorf("destination already exists for manifest entry %q", entry.ID)
		} else if !errors.Is(err, os.ErrNotExist) {
			return SessionInfo{}, err
		}
		if err := os.MkdirAll(filepath.Dir(entry.Destination), 0o755); err != nil {
			return SessionInfo{}, err
		}

		partial := entry.Destination + engine.PartialSuffix
		partialSize := int64(0)
		if info, err := os.Stat(partial); err == nil {
			if !entry.CreatedByJob || !info.Mode().IsRegular() || info.Size() > entry.Size {
				return SessionInfo{}, fmt.Errorf("managed partial conflicts with manifest entry %q", entry.ID)
			}
			partialSize = info.Size()
		} else if !errors.Is(err, os.ErrNotExist) {
			return SessionInfo{}, err
		}
		if durable, _, ok := checkpointVerificationProof(m.cfg.Store, entry.ID, entry.Size); ok {
			if durable > partialSize {
				return SessionInfo{}, fmt.Errorf("managed partial for entry %q is shorter than its durable checkpoint", entry.ID)
			}
			if durable < partialSize {
				if err := os.Truncate(partial, durable); err != nil {
					return SessionInfo{}, err
				}
				partialSize = durable
			}
		}

		uploadID := randomID("upload")
		sess.Uploads[rel] = &uploadState{
			ID:              uploadID,
			Relative:        rel,
			Destination:     entry.Destination,
			Partial:         partial,
			Size:            entry.Size,
			Managed:         true,
			ManifestEntryID: entry.ID,
		}
		sess.TotalBytes += entry.Size
		sess.BytesDone += partialSize
	}
	if len(sess.Uploads) == 0 {
		return SessionInfo{}, fmt.Errorf("manifest has no resumable entries")
	}

	m.mu.Lock()
	expiredSessions := m.expireLocked(now)
	for _, existing := range m.sessions {
		if existing.Mode != ModeReceive || existing.ManifestID != manifestID {
			continue
		}
		if err := m.ensureServerLocked(); err != nil {
			m.mu.Unlock()
			_ = m.cleanupSessions(expiredSessions)
			return SessionInfo{}, err
		}
		if len(expiredSessions) > 0 {
			if err := m.persistLocked(); err != nil {
				m.mu.Unlock()
				_ = m.cleanupSessions(expiredSessions)
				return SessionInfo{}, err
			}
		}
		result := m.infoLocked(existing)
		m.mu.Unlock()
		_ = m.cleanupSessions(expiredSessions)
		return result, nil
	}
	if err := m.ensureServerLocked(); err != nil {
		m.mu.Unlock()
		_ = m.cleanupSessions(expiredSessions)
		return SessionInfo{}, err
	}
	m.sessions[sess.ID] = sess
	m.tokenToID[sess.Token] = sess.ID
	if err := m.persistLocked(); err != nil {
		delete(m.sessions, sess.ID)
		delete(m.tokenToID, sess.Token)
		sess.close()
		m.mu.Unlock()
		_ = m.cleanupSessions(expiredSessions)
		return SessionInfo{}, err
	}
	result := m.infoLocked(sess)
	m.mu.Unlock()
	_ = m.cleanupSessions(expiredSessions)
	return result, nil
}

func (m *Manager) Snapshot(id string) (SessionInfo, bool) {
	m.mu.Lock()
	expired := m.expireLocked(m.cfg.Now())
	if len(expired) > 0 {
		_ = m.persistLocked()
	}
	s, ok := m.sessions[id]
	var info SessionInfo
	if ok {
		info = m.infoLocked(s)
	}
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return info, ok
}

func (m *Manager) CloseSession(id string) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return os.ErrNotExist
	}
	s.close()
	delete(m.sessions, id)
	delete(m.tokenToID, s.Token)
	persistErr := m.persistLocked()
	m.mu.Unlock()

	cleanupErr := m.cleanupSession(s)
	if persistErr != nil {
		return persistErr
	}
	return cleanupErr
}

// CancelSession invalidates the capability link immediately and lets active
// transfer handlers unwind before cleanup. Unlike CloseSession, it never waits
// for a large in-flight upload/download to release its file lock before the
// control request can return.
func (m *Manager) CancelSession(id string) error {
	m.mu.Lock()
	s, ok := m.sessions[id]
	if !ok {
		m.mu.Unlock()
		return os.ErrNotExist
	}
	s.close()
	delete(m.sessions, id)
	delete(m.tokenToID, s.Token)
	m.mu.Unlock()

	go func() {
		_ = m.persistRemovedSession(id)
		_ = m.cleanupSession(s)
	}()
	return nil
}

func (m *Manager) SetPaused(id string, paused bool) error {
	m.mu.Lock()
	expired := m.expireLocked(m.cfg.Now())
	s, ok := m.sessions[id]
	if ok {
		// Pause/resume is a live control path. Apply it while the session is
		// still registered, then release the manager lock before persistence.
		// Persisting can wait on an active upload's lock and must never make
		// the UI control itself appear frozen.
		s.setPaused(paused)
	}
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	for _, expiredSession := range expired {
		expiredID := expiredSession.ID
		go func() { _ = m.persistRemovedSession(expiredID) }()
	}

	if !ok {
		return os.ErrNotExist
	}
	go func() { _ = m.persistPausedState(id, paused) }()
	return nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	persistErr := m.persistLocked()
	m.closed = true
	srv := m.server
	m.server = nil
	m.listener = nil
	m.mu.Unlock()

	if srv == nil {
		return persistErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdownErr := srv.Shutdown(ctx)
	if persistErr != nil {
		return persistErr
	}
	return shutdownErr
}

func (m *Manager) ensureServerLocked() error {
	if m.closed {
		return fmt.Errorf("portal is closed")
	}
	if m.listener != nil {
		return nil
	}
	address := m.cfg.ListenAddress
	preferred := address
	if m.port > 0 {
		if host, port, err := net.SplitHostPort(address); err == nil && port == "0" {
			preferred = net.JoinHostPort(host, strconv.Itoa(m.port))
		}
	}
	listener, err := net.Listen("tcp", preferred)
	if err != nil && preferred != address {
		listener, err = net.Listen("tcp", address)
	}
	if err != nil {
		if host, port, splitErr := net.SplitHostPort(address); splitErr == nil && port != "0" {
			listener, err = net.Listen("tcp", net.JoinHostPort(host, "0"))
		}
	}
	if err != nil {
		return err
	}
	if _, port, splitErr := net.SplitHostPort(listener.Addr().String()); splitErr == nil {
		if parsed, parseErr := strconv.Atoi(port); parseErr == nil {
			m.port = parsed
		}
	}
	srv := &http.Server{
		Handler:           m,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    maxPortalHeaderBytes,
	}
	m.listener = listener
	m.server = srv
	go func() {
		_ = srv.Serve(listener)
	}()
	return nil
}

func sendProgressLocked(s *session) (int64, int) {
	var bytesDone int64
	doneFiles := 0
	for index, entry := range s.Entries {
		ranges := s.Downloaded[index]
		covered := coveredBytes(ranges)
		if covered < 0 {
			covered = 0
		}
		if covered > entry.Size {
			covered = entry.Size
		}
		bytesDone += covered
		if s.Delivered[index] {
			doneFiles++
		}
	}
	return bytesDone, doneFiles
}

func (m *Manager) infoLocked(s *session) SessionInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	routes := m.routesLocked(s.Token)
	info := SessionInfo{
		ID:          s.ID,
		Mode:        s.Mode,
		URLs:        routeURLs(routes),
		Routes:      routes,
		Links:       m.linksLocked(routes),
		CreatedAt:   s.CreatedAt,
		ExpiresAt:   s.ExpiresAt,
		TotalBytes:  s.TotalBytes,
		BytesDone:   s.BytesDone,
		TotalFiles:  len(s.Entries),
		DoneFiles:   s.DoneFiles,
		Paused:      s.Paused,
		Destination: s.ReceiveRoot,
		ManifestID:  s.ManifestID,
		EntryID:     s.ManifestEntryID,
	}
	if s.Mode == ModeReceive {
		info.TotalFiles = len(s.Uploads)
		info.Complete = s.ReceiveFinished && info.TotalFiles > 0 && info.DoneFiles >= info.TotalFiles
	} else {
		info.BytesDone, info.DoneFiles = sendProgressLocked(s)
		info.Complete = info.TotalFiles > 0 && info.DoneFiles >= info.TotalFiles
	}
	return info
}

func (m *Manager) hostRoutes() []HostRoute {
	if m.cfg.HostRoutes != nil {
		return m.cfg.HostRoutes()
	}
	if m.cfg.HostAddresses != nil {
		hosts := m.cfg.HostAddresses()
		routes := make([]HostRoute, 0, len(hosts))
		for _, host := range hosts {
			routes = append(routes, HostRoute{Address: host, Kind: "network"})
		}
		return routes
	}
	return localNetworkRoutes()
}

func (m *Manager) routesLocked(token string) []RouteInfo {
	if m.listener == nil {
		return nil
	}
	_, port, err := net.SplitHostPort(m.listener.Addr().String())
	if err != nil {
		return nil
	}
	routes := m.hostRoutes()
	if len(routes) == 0 {
		routes = []HostRoute{{Address: "127.0.0.1", Kind: "loopback"}}
	}
	seen := make(map[string]struct{})
	out := make([]RouteInfo, 0, len(routes))
	for _, route := range routes {
		host := strings.TrimSpace(route.Address)
		if host == "" {
			continue
		}
		url := "http://" + net.JoinHostPort(host, port) + "/" + token + "/"
		if _, ok := seen[url]; ok {
			continue
		}
		seen[url] = struct{}{}
		kind := strings.TrimSpace(route.Kind)
		if kind == "" {
			kind = "network"
		}
		out = append(out, RouteInfo{URL: url, Interface: route.Interface, Kind: kind})
	}
	return out
}

func (m *Manager) linksLocked(routes []RouteInfo) []LinkInfo {
	if m.cfg.HostLinks != nil {
		return append([]LinkInfo(nil), m.cfg.HostLinks()...)
	}
	if m.cfg.HostRoutes != nil || m.cfg.HostAddresses != nil {
		seen := make(map[string]struct{})
		var links []LinkInfo
		for _, route := range routes {
			if route.Interface == "" {
				continue
			}
			key := route.Interface + "\x00" + route.Kind
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			links = append(links, LinkInfo{Interface: route.Interface, Kind: route.Kind, State: "connected", HasIPv4: true})
		}
		return links
	}
	hostRoutes := make([]HostRoute, 0, len(routes))
	for _, route := range routes {
		if route.Interface == "" {
			continue
		}
		hostRoutes = append(hostRoutes, HostRoute{Address: "configured", Interface: route.Interface, Kind: route.Kind})
	}
	return networkLinksFromSysfs("/sys/class/net", hostRoutes)
}

func routeURLs(routes []RouteInfo) []string {
	urls := make([]string, 0, len(routes))
	for _, route := range routes {
		urls = append(urls, route.URL)
	}
	return urls
}

func (m *Manager) expireLocked(now time.Time) []*session {
	var expired []*session
	for id, s := range m.sessions {
		if !s.ExpiresAt.After(now) {
			s.close()
			delete(m.sessions, id)
			delete(m.tokenToID, s.Token)
			expired = append(expired, s)
		}
	}
	return expired
}

func setPortalSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("X-Permitted-Cross-Domain-Policies", "none")
	w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'; base-uri 'none'; object-src 'none'")
	w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=(), serial=(), bluetooth=()")
}

func (m *Manager) portalHostAllowed(hostport string) bool {
	host, port, err := net.SplitHostPort(strings.TrimSpace(hostport))
	if err != nil {
		return false
	}
	if m.port > 0 && port != strconv.Itoa(m.port) {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	for _, route := range m.hostRoutes() {
		routeIP := net.ParseIP(strings.TrimSpace(route.Address))
		if routeIP != nil && routeIP.Equal(ip) {
			return true
		}
	}
	if m.listener != nil {
		listenerHost, _, splitErr := net.SplitHostPort(m.listener.Addr().String())
		if splitErr == nil {
			listenerIP := net.ParseIP(listenerHost)
			if listenerIP != nil && !listenerIP.IsUnspecified() && listenerIP.Equal(ip) {
				return true
			}
		}
	}
	return false
}

func portalRemoteAllowed(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err != nil {
		return false
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()
}

func (m *Manager) portalOriginAllowed(origin string) bool {
	origin = strings.TrimSpace(origin)
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	return m.portalHostAllowed(u.Host)
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setPortalSecurityHeaders(w)
	if !portalRemoteAllowed(r.RemoteAddr) {
		http.Error(w, "local network access required", http.StatusForbidden)
		return
	}
	if !m.portalHostAllowed(r.Host) {
		http.Error(w, "invalid local portal host", http.StatusMisdirectedRequest)
		return
	}
	origin := r.Header.Get("Origin")
	if !m.portalOriginAllowed(origin) {
		http.Error(w, "invalid portal origin", http.StatusForbidden)
		return
	}
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin")
	}

	token, rest, ok := splitSessionPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	s, ok := m.sessionForToken(token)
	if !ok {
		http.Error(w, "This Resumexfer link is no longer active.", http.StatusGone)
		return
	}
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if rest == "" {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		m.servePage(w, r, s)
		return
	}

	if rest == "api/status" {
		m.serveStatus(w, r, s)
		return
	}
	if rest == "api/pause" || rest == "api/resume" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := m.SetPaused(s.ID, rest == "api/pause"); err != nil {
			http.Error(w, err.Error(), http.StatusGone)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"paused": rest == "api/pause"})
		return
	}
	if rest == "api/cancel" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := m.CancelSession(s.ID); err != nil {
			http.Error(w, err.Error(), http.StatusGone)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"cancelled": true})
		return
	}
	if rest == "api/finish" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := m.finishReceiveBatch(s); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"complete": true})
		return
	}

	if s.Mode == ModeSend {
		m.serveSend(w, r, s, rest)
		return
	}
	m.serveReceive(w, r, s, rest)
}

func (m *Manager) sessionForToken(token string) (*session, bool) {
	m.mu.Lock()
	expired := m.expireLocked(m.cfg.Now())
	if len(expired) > 0 {
		_ = m.persistLocked()
	}
	id, ok := m.tokenToID[token]
	var s *session
	if ok {
		s, ok = m.sessions[id]
	}
	m.mu.Unlock()
	_ = m.cleanupSessions(expired)
	return s, ok
}

func (m *Manager) servePage(w http.ResponseWriter, r *http.Request, s *session) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}
	if s.Mode == ModeSend {
		data := struct {
			Files        []shareEntry
			Single       bool
			Managed      bool
			PendingFiles int
		}{
			Files:        s.Entries,
			Single:       len(s.Entries) == 1,
			Managed:      s.ManifestID != "",
			PendingFiles: len(s.Entries),
		}
		_ = sendTemplate.Execute(w, data)
		return
	}
	s.mu.RLock()
	managed := s.ManifestID != ""
	pendingFiles := len(s.Uploads)
	s.mu.RUnlock()
	data := struct {
		Managed      bool
		MultiManaged bool
		PendingFiles int
	}{
		Managed:      managed,
		MultiManaged: managed && pendingFiles > 1,
		PendingFiles: pendingFiles,
	}
	_ = receiveTemplate.Execute(w, data)
}

func (m *Manager) serveStatus(w http.ResponseWriter, r *http.Request, s *session) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	totalFiles := len(s.Uploads)
	bytesDone := s.BytesDone
	doneFiles := s.DoneFiles
	complete := s.ReceiveFinished && totalFiles > 0 && doneFiles >= totalFiles
	if s.Mode == ModeSend {
		totalFiles = len(s.Entries)
		bytesDone, doneFiles = sendProgressLocked(s)
		complete = totalFiles > 0 && doneFiles >= totalFiles
	}
	status := map[string]any{
		"mode":        s.Mode,
		"total_bytes": s.TotalBytes,
		"bytes_done":  bytesDone,
		"done_files":  doneFiles,
		"total_files": totalFiles,
		"paused":      s.Paused,
		"complete":    complete,
	}
	token := s.Token
	s.mu.RUnlock()
	m.mu.RLock()
	routes := m.routesLocked(token)
	status["urls"] = routeURLs(routes)
	status["routes"] = routes
	status["links"] = m.linksLocked(routes)
	m.mu.RUnlock()
	writeJSON(w, http.StatusOK, status)
}

func (m *Manager) serveSend(w http.ResponseWriter, r *http.Request, s *session, rest string) {
	if rest == "api/fast/offer" {
		m.serveFastOffer(w, r, s)
		return
	}
	if strings.HasPrefix(rest, "file/") {
		m.serveSharedFile(w, r, s, strings.TrimPrefix(rest, "file/"))
		return
	}
	if rest == "all.zip" {
		m.serveZip(w, r, s)
		return
	}
	http.NotFound(w, r)
}

func (m *Manager) serveSharedFile(w http.ResponseWriter, r *http.Request, s *session, rawIndex string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	index, err := strconv.Atoi(rawIndex)
	if err != nil || index < 0 || index >= len(s.Entries) {
		http.NotFound(w, r)
		return
	}
	entry := s.Entries[index]
	file, err := os.Open(entry.Path)
	if err != nil {
		http.Error(w, "shared file is unavailable", http.StatusGone)
		return
	}
	defer file.Close()
	matches, err := shareEntryMatches(file, entry)
	if err != nil || !matches {
		http.Error(w, "shared file changed after sharing started", http.StatusConflict)
		return
	}
	if contentType := mime.TypeByExtension(strings.ToLower(filepath.Ext(entry.Name))); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if disposition := mime.FormatMediaType("attachment", map[string]string{"filename": entry.Name}); disposition != "" {
		w.Header().Set("Content-Disposition", disposition)
	}
	tracked := &trackingResponseWriter{
		ResponseWriter: w,
		wait:           func() error { return s.waitUntilResumed(r.Context()) },
		record:         func(start, end int64) { m.recordDownload(s, index, start, end) },
	}
	http.ServeContent(tracked, r, entry.Name, entry.ModTime, file)
	if r.Method == http.MethodGet {
		// A successful non-range GET is an explicit whole-file delivery
		// boundary. Record the full interval before finalizing so status cannot
		// remain at 100% bytes with a stale delivered-file counter.
		if r.Header.Get("Range") == "" && entry.Size > 0 {
			m.recordDownload(s, index, 0, entry.Size)
		}
		_ = m.markDeliveryComplete(s, index)
	}
}

func validShareFingerprint(fp fingerprint.Fingerprint, size int64) bool {
	return fp.Size == size && fp.SampleSize > 0 && fp.First != "" && fp.Middle != "" && fp.Last != ""
}

func shareEntryMatches(file *os.File, entry shareEntry) (bool, error) {
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != entry.Size || !info.ModTime().Equal(entry.ModTime) {
		return false, nil
	}
	if !validShareFingerprint(entry.Fingerprint, entry.Size) {
		return false, nil
	}
	current, err := fingerprint.ReaderAt(file, info.Size(), entry.Fingerprint.SampleSize)
	if err != nil {
		return false, err
	}
	return entry.Fingerprint.Compatible(current), nil
}

func (m *Manager) serveZip(w http.ResponseWriter, r *http.Request, s *session) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	for _, entry := range s.Entries {
		file, err := os.Open(entry.Path)
		if err != nil {
			http.Error(w, "shared file is unavailable", http.StatusGone)
			return
		}
		matches, validateErr := shareEntryMatches(file, entry)
		file.Close()
		if validateErr != nil || !matches {
			http.Error(w, "shared file changed after sharing started", http.StatusConflict)
			return
		}
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="resumexfer-share.zip"`)
	zw := zip.NewWriter(w)
	for index, entry := range s.Entries {
		file, err := os.Open(entry.Path)
		if err != nil {
			return
		}
		matches, validateErr := shareEntryMatches(file, entry)
		if validateErr != nil || !matches {
			file.Close()
			return
		}
		// Local-network transfers favor throughput over compression.
		// Most large media is already compressed, and Store avoids wasting CPU.
		header := &zip.FileHeader{Name: filepath.ToSlash(entry.ZipName), Method: zip.Store}
		header.SetModTime(entry.ModTime)
		writer, err := zw.CreateHeader(header)
		if err != nil {
			file.Close()
			return
		}
		tracked := &trackingReadSeeker{
			ReadSeeker: file,
			wait:       func() error { return s.waitUntilResumed(r.Context()) },
			record:     func(start, end int64) { m.recordDownload(s, index, start, end) },
		}
		_, copyErr := io.Copy(writer, tracked)
		file.Close()
		if copyErr != nil {
			_ = zw.Close()
			return
		}
		if entry.Size > 0 {
			m.recordDownload(s, index, 0, entry.Size)
		}
	}
	if err := zw.Close(); err != nil {
		return
	}
	for index := range s.Entries {
		if err := m.markDeliveryComplete(s, index); err != nil {
			return
		}
	}
}

func (m *Manager) finishReceiveBatch(s *session) error {
	if s == nil || s.Mode != ModeReceive {
		return fmt.Errorf("receive session required")
	}

	s.mu.RLock()
	if s.ReceiveFinished {
		s.mu.RUnlock()
		return nil
	}
	uploads := make([]*uploadState, 0, len(s.Uploads))
	for _, u := range s.Uploads {
		uploads = append(uploads, u)
	}
	closedCh := s.closedCh
	s.mu.RUnlock()

	select {
	case <-closedCh:
		return errPortalSessionClosed
	default:
	}
	if len(uploads) == 0 {
		return fmt.Errorf("no files were uploaded")
	}
	for _, u := range uploads {
		u.mu.Lock()
		complete := u.Complete
		adopted := u.Adopted
		u.mu.Unlock()
		if adopted {
			return fmt.Errorf("upload continued by another transport")
		}
		if !complete {
			return fmt.Errorf("one or more files are still incomplete")
		}
	}

	s.mu.Lock()
	select {
	case <-s.closedCh:
		s.mu.Unlock()
		return errPortalSessionClosed
	default:
	}
	s.ReceiveFinished = true
	s.mu.Unlock()
	return m.persist()
}

func (m *Manager) serveReceive(w http.ResponseWriter, r *http.Request, s *session, rest string) {
	switch {
	case rest == "api/init":
		m.initUpload(w, r, s)
	case strings.HasPrefix(rest, "api/verify/"):
		m.verifyManagedUpload(w, r, s, strings.TrimPrefix(rest, "api/verify/"))
	case strings.HasPrefix(rest, "api/stream/"):
		m.streamUpload(w, r, s, strings.TrimPrefix(rest, "api/stream/"))
	case strings.HasPrefix(rest, "api/upload/"):
		m.writeUploadChunk(w, r, s, strings.TrimPrefix(rest, "api/upload/"))
	default:
		http.NotFound(w, r)
	}
}

func (m *Manager) initUpload(w http.ResponseWriter, r *http.Request, s *session) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.waitUntilResumed(r.Context()); err != nil {
		http.Error(w, "portal session closed", http.StatusGone)
		return
	}
	s.mu.RLock()
	finished := s.ReceiveFinished
	s.mu.RUnlock()
	if finished {
		http.Error(w, "receive batch is already complete", http.StatusGone)
		return
	}
	var req uploadInitRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid upload metadata", http.StatusBadRequest)
		return
	}
	if req.Size < 0 {
		http.Error(w, "invalid file size", http.StatusBadRequest)
		return
	}
	if s.ManifestID != "" {
		m.initManagedUpload(w, s, req)
		return
	}
	if req.Fingerprint == nil || !validUploadFingerprint(*req.Fingerprint, req.Size) {
		http.Error(w, "valid source fingerprint required", http.StatusBadRequest)
		return
	}
	rel := req.RelativePath
	if strings.TrimSpace(rel) == "" {
		rel = req.Name
	}
	rel, err := cleanRelativePath(rel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	destination, err := safeJoin(s.ReceiveRoot, rel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := os.Stat(destination); err == nil {
		http.Error(w, "a file with this name already exists", http.StatusConflict)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		http.Error(w, "cannot inspect destination", http.StatusInternalServerError)
		return
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		http.Error(w, "cannot create destination folder", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	select {
	case <-s.closedCh:
		s.mu.Unlock()
		http.Error(w, "portal session closed", http.StatusGone)
		return
	default:
	}
	if existing, ok := s.Uploads[rel]; ok {
		if existing.Size != req.Size || existing.Destination != destination || existing.Fingerprint == nil || !existing.Fingerprint.Compatible(*req.Fingerprint) {
			s.mu.Unlock()
			http.Error(w, "upload metadata changed", http.StatusConflict)
			return
		}
		s.mu.Unlock()
		offset, complete, err := m.prepareGenericUpload(s, existing)
		if err != nil {
			if errors.Is(err, errPortalSessionClosed) {
				http.Error(w, "portal session closed", http.StatusGone)
				return
			}
			http.Error(w, "cannot verify resumable upload", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, uploadInitResponse{UploadID: existing.ID, Offset: offset, Complete: complete})
		return
	}

	uploadID := randomID("upload")
	partial := partialPath(destination, s.ID, uploadID)
	fp := *req.Fingerprint
	u := &uploadState{ID: uploadID, Relative: rel, Destination: destination, Partial: partial, Size: req.Size, Fingerprint: &fp}
	s.Uploads[rel] = u
	s.TotalBytes += req.Size
	s.mu.Unlock()
	if err := m.persist(); err != nil {
		s.mu.Lock()
		delete(s.Uploads, rel)
		s.TotalBytes -= req.Size
		s.mu.Unlock()
		http.Error(w, "cannot persist resumable upload", http.StatusInternalServerError)
		return
	}
	offset, complete, err := m.prepareGenericUpload(s, u)
	if err != nil {
		if errors.Is(err, errPortalSessionClosed) {
			http.Error(w, "portal session closed", http.StatusGone)
			return
		}
		http.Error(w, "cannot prepare resumable upload", http.StatusInternalServerError)
		return
	}
	if req.Size == 0 && !complete {
		if err := m.finalizeEmptyUpload(s, u); err != nil {
			if errors.Is(err, errPortalSessionClosed) {
				http.Error(w, "portal session closed", http.StatusGone)
				return
			}
			http.Error(w, "cannot finalize empty upload", http.StatusConflict)
			return
		}
		complete = true
		if err := m.persist(); err != nil {
			http.Error(w, "cannot persist completed upload", http.StatusInternalServerError)
			return
		}
	}
	writeJSON(w, http.StatusOK, uploadInitResponse{UploadID: uploadID, Offset: offset, Complete: complete})
}

func (m *Manager) initManagedUpload(w http.ResponseWriter, s *session, req uploadInitRequest) {
	if req.Fingerprint == nil || !validUploadFingerprint(*req.Fingerprint, req.Size) {
		http.Error(w, "valid source fingerprint required", http.StatusBadRequest)
		return
	}
	manifest, entry, u, err := m.managedUploadForRequest(s, req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if manifest.Cancelled || manifest.CancelRequested {
		http.Error(w, "managed transfer is no longer resumable", http.StatusConflict)
		return
	}
	if entry.Complete {
		u.mu.Lock()
		u.Complete = true
		u.Verified = false
		u.mu.Unlock()
		syncManagedSessionFromManifest(s, manifest)
		writeJSON(w, http.StatusOK, uploadInitResponse{UploadID: u.ID, Offset: entry.Size, Complete: true})
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ManifestEntryID != entry.ID || u.Size != entry.Size || u.Destination != entry.Destination || u.Partial != entry.Destination+engine.PartialSuffix {
		http.Error(w, "managed upload metadata changed", http.StatusConflict)
		return
	}
	if _, err := os.Stat(u.Destination); err == nil {
		http.Error(w, "destination already exists", http.StatusConflict)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		http.Error(w, "cannot inspect destination", http.StatusInternalServerError)
		return
	}
	offset, err := managedPartialSize(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	fp := *req.Fingerprint
	u.Fingerprint = &fp
	u.Verified = false
	response := uploadInitResponse{UploadID: u.ID, Offset: offset, NeedsVerification: true}
	if durable, proof, ok := checkpointVerificationProof(m.cfg.Store, entry.ID, entry.Size); ok && durable == offset {
		response.VerifyChunks = proof
	}
	writeJSON(w, http.StatusOK, response)
}

func (m *Manager) verifyManagedUpload(w http.ResponseWriter, r *http.Request, s *session, uploadID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	u := findUploadByID(s, uploadID)
	if u == nil || !u.Managed || s.ManifestID == "" {
		http.NotFound(w, r)
		return
	}
	var req uploadVerifyRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, 64<<10))
	if err := decoder.Decode(&req); err != nil || req.Offset < 0 {
		http.Error(w, "invalid prefix verification", http.StatusBadRequest)
		return
	}
	if len(req.ChunkSHA256) == 0 {
		if !validSHA256(req.PrefixSHA256) {
			http.Error(w, "invalid prefix verification", http.StatusBadRequest)
			return
		}
	} else {
		for _, sum := range req.ChunkSHA256 {
			if !validSHA256(sum) {
				http.Error(w, "invalid checkpoint verification", http.StatusBadRequest)
				return
			}
		}
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if s.isClosed() {
		http.Error(w, "portal session closed", http.StatusGone)
		return
	}
	if u.Fingerprint == nil {
		http.Error(w, "source identity must be checked before prefix verification", http.StatusConflict)
		return
	}
	manifest, entry, err := m.managedEntryForUpload(s, u)
	if err != nil || entry.SourceFingerprint == nil || !entry.SourceFingerprint.Compatible(*u.Fingerprint) {
		http.Error(w, "selected source identity changed", http.StatusConflict)
		return
	}
	if entry.Complete {
		u.Complete = true
		u.Verified = false
		syncManagedSessionFromManifest(s, manifest)
		writeJSON(w, http.StatusOK, map[string]any{"offset": entry.Size, "complete": true})
		return
	}
	if err := m.acquireManagedLease(s); err != nil {
		if errors.Is(err, state.ErrManifestTransportBusy) {
			writeJSON(w, http.StatusConflict, map[string]any{"transport_busy": true})
			return
		}
		http.Error(w, "cannot acquire managed transport lease", http.StatusInternalServerError)
		return
	}
	heartbeat := m.startManagedLeaseHeartbeat(s)
	heartbeatStopped := false
	defer func() {
		if !heartbeatStopped {
			_ = heartbeat.stopAndCheck()
		}
	}()
	release := true
	defer func() {
		if release {
			_ = m.releaseManagedLease(s)
		}
	}()

	if _, err := os.Stat(u.Destination); err == nil {
		http.Error(w, "destination already exists", http.StatusConflict)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		http.Error(w, "cannot inspect destination", http.StatusInternalServerError)
		return
	}
	current, err := managedPartialSize(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if current != req.Offset {
		writeJSON(w, http.StatusConflict, map[string]any{"expected_offset": current})
		return
	}
	if len(req.ChunkSHA256) > 0 {
		durable, proof, ok := checkpointVerificationProof(m.cfg.Store, entry.ID, entry.Size)
		if !ok || durable != current || len(proof) != len(req.ChunkSHA256) {
			http.Error(w, "saved checkpoint proof is unavailable", http.StatusConflict)
			return
		}
		for i, checkpoint := range proof {
			if !strings.EqualFold(req.ChunkSHA256[i], checkpoint.SHA256) {
				http.Error(w, "selected source does not match the saved USB checkpoint", http.StatusConflict)
				return
			}
			actualHash, err := sha256Range(u.Partial, checkpoint.Offset, checkpoint.Size)
			if err != nil {
				http.Error(w, "cannot verify preserved partial", http.StatusInternalServerError)
				return
			}
			if !strings.EqualFold(actualHash, checkpoint.SHA256) {
				http.Error(w, "preserved partial failed its saved USB checkpoint", http.StatusConflict)
				return
			}
		}
	} else {
		actualHash, err := sha256Prefix(u.Partial, current)
		if err != nil {
			http.Error(w, "cannot verify preserved partial", http.StatusInternalServerError)
			return
		}
		if !strings.EqualFold(actualHash, req.PrefixSHA256) {
			http.Error(w, "preserved partial does not match the selected source", http.StatusConflict)
			return
		}
	}
	if err := heartbeat.check(); err != nil {
		http.Error(w, "managed transport lease was lost during verification", http.StatusConflict)
		return
	}
	if err := m.cfg.Store.SetManifestEntryProgress(s.ManifestID, u.ManifestEntryID, current); err != nil {
		http.Error(w, "cannot checkpoint verified progress", http.StatusInternalServerError)
		return
	}
	u.Verified = true
	if err := heartbeat.stopAndCheck(); err != nil {
		heartbeatStopped = true
		u.Verified = false
		http.Error(w, "managed transport lease was lost during verification", http.StatusConflict)
		return
	}
	heartbeatStopped = true
	if current == u.Size {
		if err := m.finalizeManagedUpload(s, u); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"offset": current, "complete": true})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"offset": current, "complete": false})
}

func (m *Manager) managedEntryForUpload(s *session, u *uploadState) (state.Manifest, state.ManifestEntry, error) {
	if m.cfg.Store == nil || s == nil || u == nil || s.ManifestID == "" || u.ManifestEntryID == "" {
		return state.Manifest{}, state.ManifestEntry{}, fmt.Errorf("managed transfer state unavailable")
	}
	manifest, ok := m.cfg.Store.Manifest(s.ManifestID)
	if !ok {
		return state.Manifest{}, state.ManifestEntry{}, fmt.Errorf("manifest no longer exists")
	}
	if !manifest.Managed || manifest.Direction != "phone-to-laptop" {
		return state.Manifest{}, state.ManifestEntry{}, fmt.Errorf("manifest is not a managed phone-to-laptop transfer")
	}
	entry, ok := manifestEntry(manifest, u.ManifestEntryID)
	if !ok {
		return state.Manifest{}, state.ManifestEntry{}, fmt.Errorf("manifest entry no longer exists")
	}
	return manifest, entry, nil
}

func manifestEntry(manifest state.Manifest, entryID string) (state.ManifestEntry, bool) {
	for _, entry := range manifest.Entries {
		if entry.ID == entryID {
			return entry, true
		}
	}
	return state.ManifestEntry{}, false
}

func (m *Manager) managedUploadForRequest(s *session, req uploadInitRequest) (state.Manifest, state.ManifestEntry, *uploadState, error) {
	if m.cfg.Store == nil || s == nil || s.ManifestID == "" {
		return state.Manifest{}, state.ManifestEntry{}, nil, fmt.Errorf("managed transfer state unavailable")
	}
	if req.Fingerprint == nil {
		return state.Manifest{}, state.ManifestEntry{}, nil, fmt.Errorf("source identity is required")
	}
	manifest, ok := m.cfg.Store.Manifest(s.ManifestID)
	if !ok {
		return state.Manifest{}, state.ManifestEntry{}, nil, fmt.Errorf("manifest no longer exists")
	}
	if !manifest.Managed || manifest.Direction != "phone-to-laptop" {
		return state.Manifest{}, state.ManifestEntry{}, nil, fmt.Errorf("manifest is not a managed phone-to-laptop transfer")
	}

	rawHint := strings.TrimSpace(req.RelativePath)
	if rawHint == "" {
		rawHint = strings.TrimSpace(req.Name)
	}
	hint := ""
	if rawHint != "" {
		if clean, err := cleanRelativePath(rawHint); err == nil {
			hint = filepath.ToSlash(clean)
		}
	}

	s.mu.RLock()
	uploads := make([]*uploadState, 0, len(s.Uploads))
	for _, u := range s.Uploads {
		uploads = append(uploads, u)
	}
	s.mu.RUnlock()

	type candidate struct {
		u     *uploadState
		entry state.ManifestEntry
		score int
	}
	candidates := make([]candidate, 0, len(uploads))
	for _, u := range uploads {
		if u == nil || !u.Managed || u.ManifestEntryID == "" {
			continue
		}
		entry, ok := manifestEntry(manifest, u.ManifestEntryID)
		if !ok || entry.Complete || req.Size != entry.Size {
			continue
		}
		score := 0
		if hint != "" && filepath.ToSlash(u.Relative) == hint {
			score = 3
		} else if req.Name != "" && filepath.Base(u.Relative) == req.Name {
			score = 2
		}
		if entry.SourceFingerprint != nil {
			if !entry.SourceFingerprint.Compatible(*req.Fingerprint) {
				continue
			}
		} else if score == 0 {
			// An untouched manifest entry has no durable source identity yet.
			// Require at least an exact path/name match before accepting the
			// browser-selected file as that entry's initial fingerprint.
			continue
		}
		candidates = append(candidates, candidate{u: u, entry: entry, score: score})
	}
	if len(candidates) == 0 {
		return manifest, state.ManifestEntry{}, nil, fmt.Errorf("selected source does not match any remaining interrupted file")
	}

	bestScore := -1
	bestIndex := -1
	tied := false
	for i, item := range candidates {
		if item.score > bestScore {
			bestScore = item.score
			bestIndex = i
			tied = false
		} else if item.score == bestScore {
			tied = true
		}
	}
	if len(candidates) > 1 && tied {
		return manifest, state.ManifestEntry{}, nil, fmt.Errorf("multiple interrupted files match this selection; choose the original folder so paths can be matched")
	}
	chosen := candidates[bestIndex]
	if chosen.entry.SourceFingerprint == nil {
		fp := *req.Fingerprint
		if err := m.cfg.Store.PrepareManifestEntry(s.ManifestID, chosen.entry.ID, chosen.entry.CreatedByJob, fp); err != nil {
			return manifest, state.ManifestEntry{}, nil, fmt.Errorf("cannot persist selected source identity: %w", err)
		}
		chosen.entry.SourceFingerprint = &fp
	}
	return manifest, chosen.entry, chosen.u, nil
}

func (m *Manager) ensureManagedUploadOwned(s *session, u *uploadState) error {
	_, entry, err := m.managedEntryForUpload(s, u)
	if err != nil {
		return err
	}
	if entry.CreatedByJob {
		return nil
	}
	return m.cfg.Store.SetManifestEntryCreatedByJob(s.ManifestID, u.ManifestEntryID, true)
}

func managedUploads(s *session) []*uploadState {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*uploadState, 0, len(s.Uploads))
	for _, u := range s.Uploads {
		if u != nil && u.Managed {
			out = append(out, u)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Relative < out[j].Relative })
	return out
}

func syncManagedSessionFromManifest(s *session, manifest state.Manifest) {
	uploads := managedUploads(s)
	if len(uploads) == 0 {
		return
	}
	var total int64
	var done int64
	doneFiles := 0
	allComplete := true
	for _, u := range uploads {
		entry, ok := manifestEntry(manifest, u.ManifestEntryID)
		if !ok {
			allComplete = false
			continue
		}
		total += entry.Size
		bytesDone := entry.BytesDone
		if entry.Complete {
			bytesDone = entry.Size
			doneFiles++
		} else {
			allComplete = false
		}
		if bytesDone < 0 {
			bytesDone = 0
		}
		if bytesDone > entry.Size {
			bytesDone = entry.Size
		}
		done += bytesDone
	}
	s.mu.Lock()
	s.TotalBytes = total
	s.BytesDone = done
	s.DoneFiles = doneFiles
	s.ReceiveFinished = allComplete
	s.mu.Unlock()
}

func managedPartialSize(u *uploadState) (int64, error) {
	info, err := os.Stat(u.Partial)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > u.Size {
		return 0, fmt.Errorf("managed partial is invalid")
	}
	return info.Size(), nil
}

func sha256Prefix(path string, size int64) (string, error) {
	h := sha256.New()
	if size == 0 {
		return hex.EncodeToString(h.Sum(nil)), nil
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != size {
		return "", fmt.Errorf("partial size changed during verification")
	}
	if _, err := io.CopyN(h, f, size); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256Range(path string, offset, size int64) (string, error) {
	if offset < 0 || size <= 0 {
		return "", fmt.Errorf("invalid verification range")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || offset+size > info.Size() {
		return "", fmt.Errorf("partial size changed during verification")
	}
	h := sha256.New()
	if _, err := io.CopyN(h, io.NewSectionReader(f, offset, size), size); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func checkpointVerificationProof(store *state.Store, transferID string, expectedSize int64) (int64, []uploadVerifyChunk, bool) {
	if store == nil || transferID == "" || expectedSize < 0 {
		return 0, nil, false
	}
	tr, ok := store.Transfer(transferID)
	if !ok || tr.Size != expectedSize || tr.ChunkSize <= 0 || len(tr.Chunks) == 0 {
		return 0, nil, false
	}
	durable := int64(0)
	indices := make([]int, 0, len(tr.Chunks))
	count := engine.ChunkCount(tr.Size, tr.ChunkSize)
	missing := false
	for index := 0; index < count; index++ {
		chunk, exists := tr.Chunks[index]
		if !exists {
			missing = true
			continue
		}
		if missing {
			return 0, nil, false
		}
		expected := tr.ChunkSize
		if remaining := tr.Size - durable; remaining < expected {
			expected = remaining
		}
		if chunk.Size != expected || !validSHA256(chunk.Hash) {
			return 0, nil, false
		}
		indices = append(indices, index)
		durable += chunk.Size
	}
	if durable <= 0 || len(indices) == 0 {
		return 0, nil, false
	}
	positions := []int{0, len(indices) / 2, len(indices) - 1}
	seen := map[int]bool{}
	proof := make([]uploadVerifyChunk, 0, 3)
	for _, position := range positions {
		index := indices[position]
		if seen[index] {
			continue
		}
		seen[index] = true
		chunk := tr.Chunks[index]
		proof = append(proof, uploadVerifyChunk{
			Offset: int64(index) * tr.ChunkSize,
			Size:   chunk.Size,
			SHA256: strings.ToLower(chunk.Hash),
		})
	}
	return durable, proof, len(proof) > 0
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validUploadFingerprint(fp fingerprint.Fingerprint, size int64) bool {
	return fp.Size == size && fp.SampleSize > 0 && validSHA256(fp.First) && validSHA256(fp.Middle) && validSHA256(fp.Last)
}

func (m *Manager) acquireManagedLease(s *session) error {
	if m.cfg.Store == nil || s.ManifestID == "" || s.TransportOwner == "" {
		return fmt.Errorf("managed transport lease unavailable")
	}
	lease, err := m.cfg.Store.AcquireManifestTransport(s.ManifestID, s.TransportOwner, m.cfg.Now(), m.cfg.TransportLeaseTTL)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.LeaseGeneration = lease.Generation
	s.mu.Unlock()
	return nil
}

func (m *Manager) releaseManagedLease(s *session) error {
	if m.cfg.Store == nil || s.ManifestID == "" || s.TransportOwner == "" {
		return nil
	}
	s.mu.RLock()
	generation := s.LeaseGeneration
	s.mu.RUnlock()
	if generation == 0 {
		return nil
	}
	err := m.cfg.Store.ReleaseManifestTransport(s.ManifestID, s.TransportOwner, generation)
	if err == nil || errors.Is(err, state.ErrManifestTransportLeaseLost) {
		s.mu.Lock()
		if s.LeaseGeneration == generation {
			s.LeaseGeneration = 0
		}
		s.mu.Unlock()
	}
	if errors.Is(err, state.ErrManifestTransportLeaseLost) {
		return nil
	}
	return err
}

type managedLeaseHeartbeat struct {
	mu   sync.Mutex
	err  error
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func (h *managedLeaseHeartbeat) setError(err error) {
	h.mu.Lock()
	if h.err == nil {
		h.err = err
	}
	h.mu.Unlock()
}

func (h *managedLeaseHeartbeat) check() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

func (h *managedLeaseHeartbeat) stopAndCheck() error {
	if h == nil {
		return nil
	}
	h.once.Do(func() { close(h.stop) })
	<-h.done
	return h.check()
}

func (m *Manager) startManagedLeaseHeartbeat(s *session) *managedLeaseHeartbeat {
	h := &managedLeaseHeartbeat{stop: make(chan struct{}), done: make(chan struct{})}
	s.mu.RLock()
	generation := s.LeaseGeneration
	manifestID := s.ManifestID
	owner := s.TransportOwner
	s.mu.RUnlock()
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(m.cfg.TransportLeaseHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				if _, err := m.cfg.Store.RefreshManifestTransport(manifestID, owner, generation, m.cfg.Now(), m.cfg.TransportLeaseTTL); err != nil {
					h.setError(err)
					return
				}
			}
		}
	}()
	return h
}

func (m *Manager) finalizeManagedUpload(s *session, u *uploadState) error {
	if u == nil || u.ManifestEntryID == "" {
		return fmt.Errorf("managed upload entry is unavailable")
	}
	if err := m.ensureManagedUploadOwned(s, u); err != nil {
		return err
	}
	if u.Size == 0 {
		if _, err := os.Stat(u.Partial); errors.Is(err, os.ErrNotExist) {
			f, createErr := os.OpenFile(u.Partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if createErr != nil {
				return createErr
			}
			if closeErr := f.Close(); closeErr != nil {
				return closeErr
			}
		} else if err != nil {
			return err
		}
	}
	if err := promoteNoReplace(u.Partial, u.Destination); err != nil {
		return fmt.Errorf("cannot finalize managed upload: %w", err)
	}
	if err := m.cfg.Store.SetManifestEntryProgress(s.ManifestID, u.ManifestEntryID, u.Size); err != nil {
		return err
	}
	if err := m.cfg.Store.MarkManifestEntryComplete(s.ManifestID, u.ManifestEntryID); err != nil {
		return err
	}
	if err := m.cleanupManagedCandidatesForDestination(u.Destination); err != nil {
		return err
	}
	_ = m.cfg.Store.DeleteTransfer(u.ManifestEntryID)
	u.Complete = true
	u.Verified = false

	manifest, ok := m.cfg.Store.Manifest(s.ManifestID)
	if !ok {
		return fmt.Errorf("manifest no longer exists")
	}
	syncManagedSessionFromManifest(s, manifest)

	if len(manifest.Pending()) == 0 {
		if err := m.cfg.Store.SetManifestAwaitingReconnect(s.ManifestID, false); err != nil {
			return err
		}
		if err := m.cfg.Store.SetManifestPaused(s.ManifestID, false); err != nil {
			return err
		}
	}
	return m.releaseManagedLease(s)
}

func (m *Manager) cleanupManagedCandidatesForDestination(destination string) error {
	if m.cfg.Store == nil {
		return nil
	}
	destination = filepath.Clean(destination)
	for _, candidate := range m.cfg.Store.Candidates() {
		if candidate.ID == "" || candidate.PartialPath == "" || candidate.Source == "" {
			continue
		}
		if portalCandidateDestination(candidate) != destination {
			continue
		}
		partial := filepath.Clean(candidate.PartialPath)
		if partial != destination {
			if err := os.Remove(partial); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if err := m.cfg.Store.DeleteCandidate(candidate.ID); err != nil {
			return err
		}
	}
	return nil
}

func portalCandidateDestination(candidate state.Candidate) string {
	if strings.TrimSpace(candidate.Destination) != "" {
		return filepath.Clean(candidate.Destination)
	}
	clean := filepath.Clean(candidate.Source)
	lower := strings.ToLower(clean)
	for _, suffix := range []string{".partial", ".crdownload", ".part", ".tmp"} {
		if strings.HasSuffix(lower, suffix) {
			return clean[:len(clean)-len(suffix)]
		}
	}
	return clean
}

func (m *Manager) releaseManagedSession(s *session) error {
	if s == nil || s.ManifestID == "" {
		return nil
	}
	uploads := managedUploads(s)
	for _, u := range uploads {
		u.mu.Lock()
	}
	defer func() {
		for i := len(uploads) - 1; i >= 0; i-- {
			uploads[i].mu.Unlock()
		}
	}()
	return m.releaseManagedLease(s)
}

func (m *Manager) cleanupSessions(sessions []*session) error {
	var firstErr error
	for _, s := range sessions {
		if err := m.cleanupSession(s); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (m *Manager) cleanupSession(s *session) error {
	if s == nil {
		return nil
	}
	if s.ManifestID != "" {
		return m.releaseManagedSession(s)
	}
	if s.Mode != ModeReceive {
		return nil
	}

	s.mu.RLock()
	uploads := make([]*uploadState, 0, len(s.Uploads))
	for _, u := range s.Uploads {
		uploads = append(uploads, u)
	}
	s.mu.RUnlock()

	var firstErr error
	for _, u := range uploads {
		u.mu.Lock()
		if !u.Managed && !u.Complete && !u.Adopted {
			removed := true
			if err := os.Remove(u.Partial); err != nil && !errors.Is(err, os.ErrNotExist) {
				removed = false
				if firstErr == nil {
					firstErr = err
				}
			}
			if removed && m.cfg.Store != nil {
				if err := m.cfg.Store.DeleteTransfer(portalTransferID(s.ID, u.ID)); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
		u.mu.Unlock()
	}
	return firstErr
}

type pausingUploadReader struct {
	ctx context.Context
	s   *session
	r   io.Reader
}

func (r *pausingUploadReader) Read(p []byte) (int, error) {
	if err := r.s.waitUntilResumed(r.ctx); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (m *Manager) streamUpload(w http.ResponseWriter, r *http.Request, s *session, uploadID string) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	u := findUploadByID(s, uploadID)
	if u == nil {
		http.NotFound(w, r)
		return
	}
	if u.Managed {
		writeJSON(w, http.StatusConflict, map[string]any{"chunked_required": true})
		return
	}
	if err := s.waitUntilResumed(r.Context()); err != nil {
		http.Error(w, "portal session closed", http.StatusGone)
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	u.mu.Lock()
	locked := true
	defer func() {
		if locked {
			u.mu.Unlock()
		}
	}()
	select {
	case <-s.closedCh:
		http.Error(w, "portal session closed", http.StatusGone)
		return
	default:
	}
	if u.Adopted {
		http.Error(w, "upload continued by another transport", http.StatusConflict)
		return
	}
	if u.Complete {
		writeJSON(w, http.StatusOK, map[string]any{"offset": u.Size, "complete": true})
		return
	}

	current, _, err := uploadOffsetUnlocked(u)
	if err != nil {
		http.Error(w, "cannot inspect upload", http.StatusInternalServerError)
		return
	}
	if current != offset {
		writeJSON(w, http.StatusConflict, map[string]any{"expected_offset": current})
		return
	}
	remaining := u.Size - current
	if remaining < 0 {
		http.Error(w, "partial upload exceeds expected size", http.StatusConflict)
		return
	}
	if remaining == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"offset": current, "complete": true})
		return
	}
	if r.ContentLength > remaining {
		http.Error(w, "upload body exceeds remaining file size", http.StatusRequestEntityTooLarge)
		return
	}

	reader := &pausingUploadReader{
		ctx: r.Context(),
		s:   s,
		r:   io.LimitReader(r.Body, remaining),
	}
	lastReported := current
	updateProgress := func(done int64) error {
		if done < lastReported {
			return fmt.Errorf("upload progress moved backwards")
		}
		s.mu.Lock()
		s.BytesDone += done - lastReported
		s.mu.Unlock()
		lastReported = done
		return nil
	}

	newOffset := current
	complete := false
	if m.cfg.Store != nil {
		newOffset, complete, err = (engine.Engine{Store: m.cfg.Store}).StreamRemoteUpload(
			portalTransferID(s.ID, u.ID),
			u.Partial,
			current,
			reader,
			streamUploadCheckpoint,
			updateProgress,
		)
	} else {
		file, openErr := os.OpenFile(u.Partial, os.O_CREATE|os.O_WRONLY, 0o600)
		if openErr != nil {
			err = openErr
		} else {
			if _, seekErr := file.Seek(current, io.SeekStart); seekErr != nil {
				err = seekErr
			} else {
				buf := make([]byte, 1<<20)
				for newOffset < u.Size {
					if waitErr := s.waitUntilResumed(r.Context()); waitErr != nil {
						err = waitErr
						break
					}
					want := int64(len(buf))
					if left := u.Size - newOffset; left < want {
						want = left
					}
					n, readErr := reader.Read(buf[:int(want)])
					if n > 0 {
						written, writeErr := file.Write(buf[:n])
						if writeErr == nil && written != n {
							writeErr = io.ErrShortWrite
						}
						if writeErr != nil {
							err = writeErr
							break
						}
						newOffset += int64(written)
						if progressErr := updateProgress(newOffset); progressErr != nil {
							err = progressErr
							break
						}
					}
					if readErr != nil {
						if errors.Is(readErr, io.EOF) && newOffset == u.Size {
							break
						}
						err = readErr
						break
					}
				}
			}
			if syncErr := file.Sync(); err == nil && syncErr != nil {
				err = syncErr
			}
			if closeErr := file.Close(); err == nil && closeErr != nil {
				err = closeErr
			}
			complete = err == nil && newOffset == u.Size
		}
	}

	if newOffset != lastReported {
		s.mu.Lock()
		s.BytesDone += newOffset - lastReported
		if s.BytesDone < 0 {
			s.BytesDone = 0
		}
		s.mu.Unlock()
		lastReported = newOffset
	}
	if err != nil {
		u.mu.Unlock()
		locked = false
		_ = m.persist()
		if errors.Is(err, context.Canceled) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"expected_offset": newOffset,
				"retryable":       true,
			})
			return
		}
		if strings.Contains(err.Error(), "portal session closed") {
			http.Error(w, "portal session closed", http.StatusGone)
			return
		}
		http.Error(w, "stream upload failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !complete {
		u.mu.Unlock()
		locked = false
		_ = m.persist()
		writeJSON(w, http.StatusConflict, map[string]any{
			"expected_offset": newOffset,
			"retryable":       true,
		})
		return
	}

	if err := promoteNoReplace(u.Partial, u.Destination); err != nil {
		http.Error(w, "cannot finalize upload: "+err.Error(), http.StatusConflict)
		return
	}
	u.Complete = true
	s.mu.Lock()
	s.DoneFiles++
	s.mu.Unlock()
	if m.cfg.Store != nil {
		if err := (engine.Engine{Store: m.cfg.Store}).CompleteRemoteUpload(portalTransferID(s.ID, u.ID)); err != nil {
			http.Error(w, "cannot clear completed upload state", http.StatusInternalServerError)
			return
		}
	}
	u.mu.Unlock()
	locked = false
	if err := m.persist(); err != nil {
		http.Error(w, "cannot persist completed upload", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"offset": newOffset, "complete": true, "streamed": true})
}

func (m *Manager) writeUploadChunk(w http.ResponseWriter, r *http.Request, s *session, uploadID string) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	u := findUploadByID(s, uploadID)
	if u == nil {
		http.NotFound(w, r)
		return
	}
	if err := s.waitUntilResumed(r.Context()); err != nil {
		http.Error(w, "portal session closed", http.StatusGone)
		return
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	u.mu.Lock()
	locked := true
	defer func() {
		if locked {
			u.mu.Unlock()
		}
	}()
	select {
	case <-s.closedCh:
		http.Error(w, "portal session closed", http.StatusGone)
		return
	default:
	}
	if u.Adopted {
		http.Error(w, "upload continued by another transport", http.StatusConflict)
		return
	}
	if u.Complete {
		writeJSON(w, http.StatusOK, map[string]any{"offset": u.Size, "complete": true})
		return
	}
	var heartbeat *managedLeaseHeartbeat
	if u.Managed {
		if !u.Verified {
			http.Error(w, "preserved prefix must be verified before upload", http.StatusConflict)
			return
		}
		if err := m.acquireManagedLease(s); err != nil {
			if errors.Is(err, state.ErrManifestTransportBusy) {
				writeJSON(w, http.StatusConflict, map[string]any{"transport_busy": true})
				return
			}
			http.Error(w, "cannot acquire managed transport lease", http.StatusInternalServerError)
			return
		}
		heartbeat = m.startManagedLeaseHeartbeat(s)
		defer heartbeat.stopAndCheck()
		manifest, entry, err := m.managedEntryForUpload(s, u)
		if err != nil || manifest.Cancelled || manifest.CancelRequested {
			_ = m.releaseManagedLease(s)
			http.Error(w, "managed transfer is no longer resumable", http.StatusConflict)
			return
		}
		if entry.Complete {
			if err := heartbeat.stopAndCheck(); err != nil {
				http.Error(w, "managed transport lease was lost during upload", http.StatusConflict)
				return
			}
			heartbeat = nil
			_ = m.releaseManagedLease(s)
			u.Complete = true
			u.Verified = false
			syncManagedSessionFromManifest(s, manifest)
			writeJSON(w, http.StatusOK, map[string]any{"offset": entry.Size, "complete": true})
			return
		}
		if _, err := os.Stat(u.Destination); err == nil {
			_ = m.releaseManagedLease(s)
			http.Error(w, "destination already exists", http.StatusConflict)
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			http.Error(w, "cannot inspect destination", http.StatusInternalServerError)
			return
		}
	}
	current, _, err := uploadOffsetUnlocked(u)
	if err != nil {
		http.Error(w, "cannot inspect upload", http.StatusInternalServerError)
		return
	}
	if current != offset {
		if u.Managed {
			u.Verified = false
			_ = m.releaseManagedLease(s)
		}
		writeJSON(w, http.StatusConflict, map[string]any{"expected_offset": current})
		return
	}
	remaining := u.Size - current
	if remaining < 0 {
		http.Error(w, "partial upload exceeds expected size", http.StatusConflict)
		return
	}
	limit := int64(MaxUploadChunk)
	if remaining < limit {
		limit = remaining
	}
	payload, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		http.Error(w, "cannot read upload chunk", http.StatusBadRequest)
		return
	}
	if int64(len(payload)) > limit {
		http.Error(w, "upload chunk is too large", http.StatusRequestEntityTooLarge)
		return
	}
	if len(payload) == 0 && current < u.Size {
		http.Error(w, "empty upload chunk", http.StatusBadRequest)
		return
	}
	if u.Managed && len(payload) > 0 {
		if err := m.ensureManagedUploadOwned(s, u); err != nil {
			http.Error(w, "cannot persist managed upload ownership", http.StatusInternalServerError)
			return
		}
	}
	if heartbeat != nil {
		if err := heartbeat.check(); err != nil {
			http.Error(w, "managed transport lease was lost during upload", http.StatusConflict)
			return
		}
	}
	newOffset := current
	complete := false
	if !u.Managed && m.cfg.Store != nil {
		newOffset, complete, err = (engine.Engine{Store: m.cfg.Store}).AppendRemoteUpload(
			portalTransferID(s.ID, u.ID), u.Partial, current, payload,
		)
		if err != nil {
			http.Error(w, "cannot checkpoint upload chunk: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		file, openErr := os.OpenFile(u.Partial, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if openErr != nil {
			http.Error(w, "cannot open upload destination", http.StatusInternalServerError)
			return
		}
		written, writeErr := file.Write(payload)
		if writeErr == nil && written != len(payload) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = file.Sync()
		}
		closeErr := file.Close()
		if writeErr != nil {
			http.Error(w, "cannot write upload chunk", http.StatusInternalServerError)
			return
		}
		if closeErr != nil {
			http.Error(w, "cannot close upload chunk", http.StatusInternalServerError)
			return
		}
		newOffset = current + int64(written)
		complete = newOffset == u.Size
	}
	if heartbeat != nil {
		if err := heartbeat.check(); err != nil {
			http.Error(w, "managed transport lease was lost during upload", http.StatusConflict)
			return
		}
	}

	s.mu.Lock()
	s.BytesDone += newOffset - current
	s.mu.Unlock()
	if u.Managed {
		if err := m.cfg.Store.SetManifestEntryProgress(s.ManifestID, u.ManifestEntryID, newOffset); err != nil {
			http.Error(w, "cannot checkpoint managed progress", http.StatusInternalServerError)
			return
		}
		if complete {
			if heartbeat != nil {
				if err := heartbeat.stopAndCheck(); err != nil {
					http.Error(w, "managed transport lease was lost during upload", http.StatusConflict)
					return
				}
				heartbeat = nil
			}
			if err := m.finalizeManagedUpload(s, u); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
		} else {
			if heartbeat != nil {
				if err := heartbeat.stopAndCheck(); err != nil {
					http.Error(w, "managed transport lease was lost during upload", http.StatusConflict)
					return
				}
				heartbeat = nil
			}
			if err := m.releaseManagedLease(s); err != nil {
				http.Error(w, "cannot release managed transport lease", http.StatusInternalServerError)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"offset": newOffset, "complete": complete})
		return
	}
	if complete {
		if err := promoteNoReplace(u.Partial, u.Destination); err != nil {
			http.Error(w, "cannot finalize upload: "+err.Error(), http.StatusConflict)
			return
		}
		u.Complete = true
		s.mu.Lock()
		s.DoneFiles++
		s.mu.Unlock()
		if m.cfg.Store != nil {
			if err := (engine.Engine{Store: m.cfg.Store}).CompleteRemoteUpload(portalTransferID(s.ID, u.ID)); err != nil {
				http.Error(w, "cannot clear completed upload state", http.StatusInternalServerError)
				return
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"offset": newOffset, "complete": complete})
}

type trackingResponseWriter struct {
	http.ResponseWriter
	wait   func() error
	record func(start, end int64)
}

func (w *trackingResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	limited, ok := src.(*io.LimitedReader)
	if !ok {
		return io.Copy(w.ResponseWriter, src)
	}
	file, ok := limited.R.(*os.File)
	if !ok {
		return io.Copy(w.ResponseWriter, src)
	}
	readerFrom, fastPath := w.ResponseWriter.(io.ReaderFrom)

	var total int64
	for limited.N > 0 {
		if w.wait != nil {
			if err := w.wait(); err != nil {
				return total, err
			}
		}
		start, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return total, err
		}
		chunk := int64(downloadTrackInterval)
		if limited.N < chunk {
			chunk = limited.N
		}
		part := &io.LimitedReader{R: file, N: chunk}
		var n int64
		var copyErr error
		if fastPath {
			n, copyErr = readerFrom.ReadFrom(part)
		} else {
			n, copyErr = io.Copy(w.ResponseWriter, part)
		}
		err = copyErr
		if n > 0 {
			total += n
			limited.N -= n
			if w.record != nil {
				w.record(start, start+n)
			}
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}

type trackingReadSeeker struct {
	io.ReadSeeker
	pos         int64
	recordStart int64
	haveRecord  bool
	wait        func() error
	record      func(start, end int64)
}

func (r *trackingReadSeeker) flushRecord(end int64) {
	if r.record == nil || !r.haveRecord || end <= r.recordStart {
		return
	}
	r.record(r.recordStart, end)
	r.recordStart = end
}

func (r *trackingReadSeeker) Read(p []byte) (int, error) {
	if r.wait != nil {
		if err := r.wait(); err != nil {
			return 0, err
		}
	}
	start := r.pos
	n, err := r.ReadSeeker.Read(p)
	if n > 0 {
		if !r.haveRecord {
			r.recordStart = start
			r.haveRecord = true
		}
		r.pos += int64(n)
		if r.pos-r.recordStart >= downloadTrackInterval || errors.Is(err, io.EOF) {
			r.flushRecord(r.pos)
		}
	} else if errors.Is(err, io.EOF) {
		r.flushRecord(r.pos)
	}
	return n, err
}

func (r *trackingReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.flushRecord(r.pos)
	pos, err := r.ReadSeeker.Seek(offset, whence)
	if err == nil {
		r.pos = pos
		r.recordStart = pos
		r.haveRecord = false
	}
	return pos, err
}

func (s *session) setPaused(paused bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if paused == s.Paused {
		return
	}
	if paused {
		s.Paused = true
		s.resumeCh = make(chan struct{})
		return
	}
	s.Paused = false
	if s.resumeCh != nil {
		close(s.resumeCh)
		s.resumeCh = nil
	}
}

func (s *session) paused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Paused
}

func (s *session) close() {
	s.mu.Lock()
	select {
	case <-s.closedCh:
		s.mu.Unlock()
		return
	default:
		close(s.closedCh)
	}
	if s.resumeCh != nil {
		close(s.resumeCh)
		s.resumeCh = nil
	}
	s.Paused = false
	s.mu.Unlock()
	s.closeFastPeers()
}

func (s *session) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	select {
	case <-s.closedCh:
		return true
	default:
		return false
	}
}

func (s *session) waitUntilResumed(ctx context.Context) error {
	for {
		s.mu.RLock()
		paused := s.Paused
		resumeCh := s.resumeCh
		closedCh := s.closedCh
		s.mu.RUnlock()
		if !paused {
			select {
			case <-closedCh:
				return fmt.Errorf("portal session closed")
			default:
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closedCh:
			return fmt.Errorf("portal session closed")
		case <-resumeCh:
		}
	}
}

func (m *Manager) recordDownload(s *session, index int, start, end int64) {
	if s.recordDownload(index, start, end) {
		_ = m.persist()
	}
}

func (m *Manager) markDeliveryComplete(s *session, index int) error {
	if s == nil {
		return fmt.Errorf("portal session is unavailable")
	}
	s.mu.Lock()
	if index < 0 || index >= len(s.Entries) {
		s.mu.Unlock()
		return fmt.Errorf("shared file index is invalid")
	}
	if s.Delivered == nil {
		s.Delivered = make(map[int]bool)
	}
	if s.Delivered[index] {
		s.mu.Unlock()
		return nil
	}
	entry := s.Entries[index]
	covered := coveredBytes(s.Downloaded[index])
	if entry.Size > 0 && covered < entry.Size {
		s.mu.Unlock()
		return fmt.Errorf("shared file delivery finalized before all bytes were acknowledged")
	}
	s.Delivered[index] = true
	_, doneFiles := sendProgressLocked(s)
	s.DoneFiles = doneFiles
	manifestID := s.ManifestID
	manifestEntryID := entry.ManifestEntryID
	s.mu.Unlock()

	if manifestID != "" && manifestEntryID != "" && m.cfg.Store != nil {
		if err := m.cfg.Store.SetManifestEntryProgress(manifestID, manifestEntryID, entry.Size); err != nil {
			return err
		}
		if err := m.cfg.Store.MarkManifestEntryComplete(manifestID, manifestEntryID); err != nil {
			return err
		}
		if manifest, ok := m.cfg.Store.Manifest(manifestID); ok && len(manifest.Pending()) == 0 {
			if err := m.cfg.Store.SetManifestAwaitingReconnect(manifestID, false); err != nil {
				return err
			}
			if err := m.cfg.Store.SetManifestPaused(manifestID, false); err != nil {
				return err
			}
		}
	}
	return m.persist()
}

func (s *session) recordDownload(index int, start, end int64) bool {
	if index < 0 || index >= len(s.Entries) || end <= start {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.Entries[index]
	if start < 0 {
		start = 0
	}
	if end > entry.Size {
		end = entry.Size
	}
	if end <= start {
		return false
	}
	before := coveredBytes(s.Downloaded[index])
	updated := mergeRange(s.Downloaded[index], byteRange{Start: start, End: end})
	after := coveredBytes(updated)
	s.Downloaded[index] = updated
	if after > before {
		s.BytesDone += after - before
	}
	reachedEnd := entry.Size > 0 && before < entry.Size && after >= entry.Size
	persistNeeded := s.BytesDone-s.checkpointBytes >= persistCheckpoint || reachedEnd
	if persistNeeded {
		s.checkpointBytes = s.BytesDone
	}
	return persistNeeded
}

func mergeRange(existing []byteRange, added byteRange) []byteRange {
	all := append(append([]byteRange(nil), existing...), added)
	if len(all) <= 1 {
		return all
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Start == all[j].Start {
			return all[i].End < all[j].End
		}
		return all[i].Start < all[j].Start
	})
	out := make([]byteRange, 0, len(all))
	for _, current := range all {
		if current.End <= current.Start {
			continue
		}
		if len(out) == 0 || current.Start > out[len(out)-1].End {
			out = append(out, current)
			continue
		}
		if current.End > out[len(out)-1].End {
			out[len(out)-1].End = current.End
		}
	}
	return out
}

func coveredBytes(ranges []byteRange) int64 {
	var total int64
	for _, r := range ranges {
		if r.End > r.Start {
			total += r.End - r.Start
		}
	}
	return total
}

func findUploadByID(s *session, id string) *uploadState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.Uploads {
		if u.ID == id {
			return u
		}
	}
	return nil
}

func uploadOffset(u *uploadState) (int64, bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return uploadOffsetUnlocked(u)
}

func uploadOffsetUnlocked(u *uploadState) (int64, bool, error) {
	if u.Complete {
		return u.Size, true, nil
	}
	info, err := os.Stat(u.Partial)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > u.Size {
		return 0, false, fmt.Errorf("invalid partial upload")
	}
	return info.Size(), false, nil
}

func collectShareEntries(paths []string) ([]shareEntry, int64, error) {
	if len(paths) == 0 {
		return nil, 0, fmt.Errorf("at least one path is required")
	}
	entries := make([]shareEntry, 0)
	var total int64
	for _, raw := range paths {
		if strings.TrimSpace(raw) == "" {
			return nil, 0, fmt.Errorf("shared path is empty")
		}
		path, err := filepath.Abs(raw)
		if err != nil {
			return nil, 0, err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return nil, 0, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, 0, fmt.Errorf("symbolic links are not shared")
		}
		base := filepath.Base(path)
		if info.Mode().IsRegular() {
			entry, err := makeShareEntry(path, base, base, info)
			if err != nil {
				return nil, 0, err
			}
			entries = append(entries, entry)
			total += info.Size()
			continue
		}
		if !info.IsDir() {
			return nil, 0, fmt.Errorf("unsupported shared item %q", path)
		}
		err = filepath.WalkDir(path, func(current string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if current == path {
				return nil
			}
			if d.Type()&os.ModeSymlink != 0 {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if d.IsDir() {
				return nil
			}
			childInfo, err := d.Info()
			if err != nil {
				return err
			}
			if !childInfo.Mode().IsRegular() {
				return nil
			}
			rel, err := filepath.Rel(path, current)
			if err != nil {
				return err
			}
			zipName := filepath.Join(base, rel)
			entry, err := makeShareEntry(current, filepath.Base(current), zipName, childInfo)
			if err != nil {
				return err
			}
			entries = append(entries, entry)
			total += childInfo.Size()
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
	}
	return entries, total, nil
}

func makeShareEntry(path, name, zipName string, info os.FileInfo) (shareEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return shareEntry{}, err
	}
	defer file.Close()
	fp, err := fingerprint.ReaderAt(file, info.Size(), 64*1024)
	if err != nil {
		return shareEntry{}, err
	}
	return shareEntry{
		Path:        path,
		Name:        name,
		ZipName:     zipName,
		Size:        info.Size(),
		ModTime:     info.ModTime(),
		Fingerprint: fp,
	}, nil
}

func safeJoin(root, rel string) (string, error) {
	clean, err := cleanRelativePath(rel)
	if err != nil {
		return "", err
	}
	root = filepath.Clean(root)
	destination := filepath.Join(root, filepath.FromSlash(clean))
	relToRoot, err := filepath.Rel(root, destination)
	if err != nil || relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("upload path escapes destination")
	}
	return destination, nil
}

func cleanRelativePath(rel string) (string, error) {
	rel = strings.ReplaceAll(strings.TrimSpace(rel), "\\", "/")
	if rel == "" || strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("invalid relative path")
	}
	parts := strings.Split(rel, "/")
	cleanParts := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		if part == ".." || strings.ContainsRune(part, 0) {
			return "", fmt.Errorf("invalid relative path")
		}
		cleanParts = append(cleanParts, part)
	}
	if len(cleanParts) == 0 {
		return "", fmt.Errorf("invalid relative path")
	}
	return strings.Join(cleanParts, "/"), nil
}

func portalTransferID(sessionID, uploadID string) string {
	return "portal:" + sessionID + ":" + uploadID
}

func portalCandidateID(sessionID, uploadID string) string {
	return "portal-candidate:" + sessionID + ":" + uploadID
}

func hashReaderAtPrefix(source io.ReaderAt, size int64) (string, error) {
	if size < 0 {
		return "", fmt.Errorf("negative prefix size")
	}
	h := sha256.New()
	if size > 0 {
		if _, err := io.Copy(h, io.NewSectionReader(source, 0, size)); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (m *Manager) HasReceivePartial(destination string, size int64) bool {
	destination = filepath.Clean(destination)
	m.mu.RLock()
	sessions := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.RUnlock()
	for _, s := range sessions {
		s.mu.RLock()
		if s.Mode != ModeReceive || s.ManifestID != "" {
			s.mu.RUnlock()
			continue
		}
		for _, u := range s.Uploads {
			u.mu.Lock()
			match := !u.Managed && !u.Complete && !u.Adopted && filepath.Clean(u.Destination) == destination && u.Size == size
			u.mu.Unlock()
			if match {
				s.mu.RUnlock()
				return true
			}
		}
		s.mu.RUnlock()
	}
	return false
}

func (m *Manager) AdoptReceivePartial(destination string, source io.ReaderAt, size int64, sourceFingerprint fingerprint.Fingerprint) (ReceiveAdoption, bool, error) {
	if m.cfg.Store == nil {
		return ReceiveAdoption{}, false, nil
	}
	if source == nil || size < 0 || !validUploadFingerprint(sourceFingerprint, size) {
		return ReceiveAdoption{}, false, fmt.Errorf("valid source identity required")
	}
	destination = filepath.Clean(destination)

	type match struct {
		s *session
		u *uploadState
	}
	m.mu.RLock()
	sessions := make([]*session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.RUnlock()

	matches := make([]match, 0, 1)
	for _, s := range sessions {
		s.mu.RLock()
		if s.Mode != ModeReceive || s.ManifestID != "" {
			s.mu.RUnlock()
			continue
		}
		for _, u := range s.Uploads {
			u.mu.Lock()
			compatible := !u.Managed && !u.Complete && !u.Adopted && filepath.Clean(u.Destination) == destination && u.Size == size && u.Fingerprint != nil && u.Fingerprint.Compatible(sourceFingerprint)
			u.mu.Unlock()
			if compatible {
				matches = append(matches, match{s: s, u: u})
			}
		}
		s.mu.RUnlock()
	}
	if len(matches) == 0 {
		return ReceiveAdoption{}, false, nil
	}
	if len(matches) > 1 {
		return ReceiveAdoption{}, false, ErrAmbiguousReceiveAdoption
	}

	selected := matches[0]
	s, u := selected.s, selected.u
	u.mu.Lock()
	select {
	case <-s.closedCh:
		u.mu.Unlock()
		return ReceiveAdoption{}, false, nil
	default:
	}
	if u.Managed || u.Complete || u.Adopted || filepath.Clean(u.Destination) != destination || u.Size != size || u.Fingerprint == nil || !u.Fingerprint.Compatible(sourceFingerprint) {
		u.mu.Unlock()
		return ReceiveAdoption{}, false, nil
	}
	durable, err := (engine.Engine{Store: m.cfg.Store}).PrepareRemoteUpload(portalTransferID(s.ID, u.ID), u.Size, u.Destination, u.Partial, *u.Fingerprint)
	if err != nil {
		u.mu.Unlock()
		return ReceiveAdoption{}, false, err
	}
	if durable > 0 {
		partialHash, err := sha256Prefix(u.Partial, durable)
		if err != nil {
			u.mu.Unlock()
			return ReceiveAdoption{}, false, err
		}
		sourceHash, err := hashReaderAtPrefix(source, durable)
		if err != nil {
			u.mu.Unlock()
			return ReceiveAdoption{}, false, err
		}
		if partialHash != sourceHash {
			u.mu.Unlock()
			return ReceiveAdoption{}, false, engine.ErrPrefixMismatch
		}
	}
	u.Adopted = true
	u.mu.Unlock()

	if err := m.persist(); err != nil {
		u.mu.Lock()
		u.Adopted = false
		u.mu.Unlock()
		return ReceiveAdoption{}, false, err
	}
	transferID := portalTransferID(s.ID, u.ID)
	transfer, ok := m.cfg.Store.Transfer(transferID)
	if !ok || transfer.ChunkSize <= 0 {
		u.mu.Lock()
		u.Adopted = false
		u.mu.Unlock()
		_ = m.persist()
		return ReceiveAdoption{}, false, fmt.Errorf("wireless transfer checkpoint state unavailable")
	}
	candidateID := portalCandidateID(s.ID, u.ID)
	candidate := state.Candidate{
		ID: candidateID, Direction: "phone-to-laptop", Source: u.Partial, Destination: u.Destination, PartialPath: u.Partial, Size: u.Size, ChunkSize: transfer.ChunkSize, UpdatedAt: m.cfg.Now(),
	}
	if err := m.cfg.Store.PutCandidate(candidate); err != nil {
		u.mu.Lock()
		u.Adopted = false
		u.mu.Unlock()
		_ = m.persist()
		return ReceiveAdoption{}, false, err
	}

	s.mu.Lock()
	if current, ok := s.Uploads[u.Relative]; ok && current == u {
		delete(s.Uploads, u.Relative)
		s.TotalBytes -= u.Size
		s.BytesDone -= durable
		if s.TotalBytes < 0 {
			s.TotalBytes = 0
		}
		if s.BytesDone < 0 {
			s.BytesDone = 0
		}
		s.checkpointBytes = s.BytesDone
	}
	s.mu.Unlock()
	_ = m.cfg.Store.DeleteTransfer(transferID)
	if err := m.persist(); err != nil {
		return ReceiveAdoption{}, true, err
	}
	return ReceiveAdoption{CandidateID: candidateID, Offset: durable, Fingerprint: sourceFingerprint}, true, nil
}

func (m *Manager) finalizeEmptyUpload(s *session, u *uploadState) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if s.isClosed() {
		return errPortalSessionClosed
	}
	if u.Adopted {
		return fmt.Errorf("upload continued by another transport")
	}
	if u.Complete {
		return nil
	}
	if err := os.WriteFile(u.Partial, nil, 0o600); err != nil {
		return err
	}
	if err := promoteNoReplace(u.Partial, u.Destination); err != nil {
		return err
	}
	if m.cfg.Store != nil {
		if err := (engine.Engine{Store: m.cfg.Store}).CompleteRemoteUpload(portalTransferID(s.ID, u.ID)); err != nil {
			return err
		}
	}
	u.Complete = true
	s.mu.Lock()
	s.DoneFiles++
	s.mu.Unlock()
	return nil
}

func (m *Manager) prepareGenericUpload(s *session, u *uploadState) (int64, bool, error) {
	u.mu.Lock()
	if s.isClosed() {
		u.mu.Unlock()
		return 0, false, errPortalSessionClosed
	}
	if u.Adopted {
		u.mu.Unlock()
		return 0, false, fmt.Errorf("upload continued by another transport")
	}
	current, complete, err := uploadOffsetUnlocked(u)
	if err != nil || complete || m.cfg.Store == nil {
		u.mu.Unlock()
		return current, complete, err
	}
	if u.Fingerprint == nil {
		u.mu.Unlock()
		return 0, false, fmt.Errorf("source fingerprint unavailable")
	}
	fp := *u.Fingerprint
	durable, err := (engine.Engine{Store: m.cfg.Store}).PrepareRemoteUpload(
		portalTransferID(s.ID, u.ID), u.Size, u.Destination, u.Partial, fp,
	)
	u.mu.Unlock()
	if err != nil {
		return 0, false, err
	}
	if durable != current {
		s.mu.Lock()
		s.BytesDone += durable - current
		if s.BytesDone < 0 {
			s.BytesDone = 0
		}
		s.checkpointBytes = s.BytesDone
		s.mu.Unlock()
	}
	return durable, false, nil
}

func partialPath(destination, sessionID, uploadID string) string {
	base := filepath.Base(destination)
	name := fmt.Sprintf(".%s.resumexfer-part-%s-%s", base, shortID(sessionID), shortID(uploadID))
	return filepath.Join(filepath.Dir(destination), name)
}

func promoteNoReplace(partial, destination string) error {
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Link(partial, destination); err == nil {
		return os.Remove(partial)
	}

	source, err := os.Open(partial)
	if err != nil {
		return err
	}
	defer source.Close()
	destinationFile, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	copyOK := false
	defer func() {
		_ = destinationFile.Close()
		if !copyOK {
			_ = os.Remove(destination)
		}
	}()
	if _, err := io.Copy(destinationFile, source); err != nil {
		return err
	}
	if err := destinationFile.Sync(); err != nil {
		return err
	}
	if err := destinationFile.Close(); err != nil {
		return err
	}
	copyOK = true
	return os.Remove(partial)
}

func splitSessionPath(path string) (string, string, bool) {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "", "", false
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return "", "", false
	}
	for _, part := range parts {
		if part == "" {
			return "", "", false
		}
	}

	// Accept the older /s/<token>/... form so existing persisted sessions and
	// already-open browser tabs keep working after the compact URL rollout.
	tokenIndex := 0
	if parts[0] == "s" {
		if len(parts) < 2 || parts[1] == "" {
			return "", "", false
		}
		tokenIndex = 1
	}
	token := parts[tokenIndex]
	restStart := tokenIndex + 1
	rest := ""
	if restStart < len(parts) {
		rest = strings.Join(parts[restStart:], "/")
	}
	return token, rest, true
}

func randomID(prefix string) string {
	return prefix + "-" + randomHex(12)
}

func randomToken() string {
	// Capability URLs are the authorization boundary for the zero-install
	// local portal. Nine CSPRNG bytes encode to a compact 12-character URL-safe
	// token (72 bits). Combined with local-network-only access, short expiry,
	// host/origin validation and cancellation, this remains impractical to
	// guess online while keeping the phone link human-sized.
	b := make([]byte, capabilityTokenBytes)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[len(id)-12:]
	}
	return id
}

func localIPv4Addresses() []string {
	routes := localNetworkRoutes()
	out := make([]string, 0, len(routes))
	for _, route := range routes {
		out = append(out, route.Address)
	}
	return out
}

func localNetworkRoutes() []HostRoute {
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []HostRoute
	seen := make(map[string]struct{})
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 || ignoredPortalInterface(iface.Name) {
			continue
		}
		kind := classifyNetworkInterface("/sys/class/net", iface.Name)
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}
			if ip == nil || ip.IsLoopback() || ip.IsUnspecified() || ip.IsMulticast() {
				continue
			}
			ip4 := ip.To4()
			if ip4 == nil {
				continue
			}
			host := ip4.String()
			if _, ok := seen[host]; ok {
				continue
			}
			seen[host] = struct{}{}
			out = append(out, HostRoute{Address: host, Interface: iface.Name, Kind: kind})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, right := networkRouteRank(out[i].Kind), networkRouteRank(out[j].Kind)
		if left != right {
			return left < right
		}
		if out[i].Interface != out[j].Interface {
			return out[i].Interface < out[j].Interface
		}
		return out[i].Address < out[j].Address
	})
	return out
}

func networkLinksFromSysfs(sysClassNet string, routes []HostRoute) []LinkInfo {
	entries, err := os.ReadDir(sysClassNet)
	if err != nil {
		return nil
	}
	addressed := make(map[string]bool)
	for _, route := range routes {
		if route.Interface != "" && strings.TrimSpace(route.Address) != "" {
			addressed[route.Interface] = true
		}
	}
	links := make([]LinkInfo, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == "lo" || ignoredPortalInterface(name) {
			continue
		}
		base := filepath.Join(sysClassNet, name)
		info, err := os.Stat(base)
		if err != nil || !info.IsDir() {
			continue
		}
		kind := classifyNetworkInterface(sysClassNet, name)
		state := networkLinkState(base)
		links = append(links, LinkInfo{Interface: name, Kind: kind, State: state, HasIPv4: addressed[name]})
	}
	sort.SliceStable(links, func(i, j int) bool {
		left, right := networkRouteRank(links[i].Kind), networkRouteRank(links[j].Kind)
		if left != right {
			return left < right
		}
		return links[i].Interface < links[j].Interface
	})
	return links
}

func networkLinkState(base string) string {
	if value, err := os.ReadFile(filepath.Join(base, "carrier")); err == nil {
		switch strings.TrimSpace(string(value)) {
		case "1":
			return "connected"
		case "0":
			return "disconnected"
		}
	}
	if value, err := os.ReadFile(filepath.Join(base, "operstate")); err == nil {
		state := strings.TrimSpace(string(value))
		switch state {
		case "up":
			return "connected"
		case "down", "lowerlayerdown", "notpresent":
			return "disconnected"
		case "dormant", "testing", "unknown":
			return state
		}
	}
	return "unknown"
}

func ignoredPortalInterface(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	if lower == "" {
		return true
	}
	if lower == "docker0" {
		return true
	}
	for _, prefix := range []string{
		"br-",
		"veth",
		"podman",
		"cni",
		"flannel",
		"kube-",
		"virbr",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func classifyNetworkInterface(sysClassNet, name string) string {
	base := filepath.Join(sysClassNet, name)
	if info, err := os.Stat(filepath.Join(base, "wireless")); err == nil && info.IsDir() {
		return "wifi"
	}
	if driver, err := filepath.EvalSymlinks(filepath.Join(base, "device", "driver")); err == nil {
		driverName := strings.ToLower(filepath.Base(driver))
		if driverName == "thunderbolt-net" {
			return "thunderbolt-network"
		}
	}
	device := filepath.Join(base, "device")
	resolved, err := filepath.EvalSymlinks(device)
	if err != nil {
		lowerName := strings.ToLower(name)
		if isIndexedInterfaceName(lowerName, "thunderbolt") {
			return "thunderbolt-network"
		}
		if isIndexedInterfaceName(lowerName, "usb") {
			return "usb-network"
		}
		return "network"
	}
	lower := strings.ToLower(filepath.ToSlash(resolved))
	if strings.Contains(lower, "/thunderbolt") {
		return "thunderbolt-network"
	}
	if strings.Contains(lower, "/usb") {
		return "usb-network"
	}
	return "ethernet"
}

func isIndexedInterfaceName(name, prefix string) bool {
	if !strings.HasPrefix(name, prefix) || len(name) == len(prefix) {
		return false
	}
	for _, r := range name[len(prefix):] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func networkRouteRank(kind string) int {
	switch kind {
	case "thunderbolt-network", "usb-network", "ethernet":
		return 0
	case "wifi":
		return 1
	case "network":
		return 2
	default:
		return 3
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

var sendTemplate = template.Must(template.New("send").Parse(sendPageHTML))

var receiveTemplate = template.Must(template.New("receive").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Resumexfer</title><style>
body{font-family:system-ui,sans-serif;max-width:720px;margin:32px auto;padding:0 18px;color:#1f2328}.box{border:1px solid #d0d7de;border-radius:12px;padding:18px}button{padding:9px 13px;border:0;border-radius:8px;background:#0969da;color:#fff}.muted{color:#656d76}progress{width:100%;height:18px}
</style></head><body><h1>Resumexfer</h1><div class="box">{{if .Managed}}{{if .MultiManaged}}<p>Continue the interrupted {{.PendingFiles}}-file job over Wi-Fi. Choose all remaining original files, or choose the original folder so Resumexfer can preserve paths and verify any saved USB progress.</p>{{else}}<p>Continue the interrupted transfer over Wi-Fi. Choose the same original file from this device; Resumexfer will verify the existing USB progress before continuing.</p>{{end}}{{else}}<p>Send files to the nearby computer. Nothing needs to be installed on this device.</p>{{end}}
{{if .Managed}}{{if .MultiManaged}}<input id="files" type="file" multiple><p><input id="folder" type="file" webkitdirectory multiple> <span class="muted">Recommended for a folder transfer</span></p>{{else}}<input id="files" type="file">{{end}}{{else}}<input id="files" type="file" multiple><p><input id="folder" type="file" webkitdirectory multiple> <span class="muted">Optional: choose a whole folder</span></p>{{end}}<p><button id="send">{{if .Managed}}Continue transfer{{else}}Send files{{end}}</button> <button id="pauseTransfer" type="button">Pause</button> <button id="cancelTransfer" type="button">Cancel</button></p><progress id="p" value="0" max="1"></progress><p id="status" class="muted">{{if .Managed}}{{if .MultiManaged}}Choose the remaining files or original folder to verify and continue the whole job.{{else}}Choose the original file to verify and continue.{{end}}{{else}}Choose files or a folder to begin.{{end}}</p></div>
<script>
const input=document.getElementById('files'),folderInput=document.getElementById('folder'),button=document.getElementById('send'),pauseControl=document.getElementById('pauseTransfer'),cancelControl=document.getElementById('cancelTransfer'),status=document.getElementById('status'),bar=document.getElementById('p');
const CHUNK=32*1024*1024,HASH_CHUNK=1024*1024,ANCHOR=64*1024;
let routes=[new URL('./',window.location.href).href],preferred=routes[0],serverPaused=false,cancelled=false;
function mergeRoutes(values){for(const value of values||[]){try{const base=new URL(value,window.location.href).href;if(!routes.includes(base))routes.push(base)}catch(e){}}}
async function routeFetch(path,options={},timeoutMs=45000){const ordered=[preferred,...routes.filter(item=>item!==preferred)];let lastError=null;for(const base of ordered){const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeoutMs);try{const response=await fetch(new URL(path,base),{...options,signal:controller.signal});clearTimeout(timer);preferred=base;return response}catch(e){clearTimeout(timer);lastError=e}}throw lastError||new Error('No Resumexfer route is reachable')}
async function jsonFetch(path,options={},timeoutMs=45000){const r=await routeFetch(path,options,timeoutMs);const text=await r.text();let body={};try{body=text?JSON.parse(text):{}}catch(e){}if(!r.ok){const err=new Error(body.error||text||('HTTP '+r.status));err.response=body;err.status=r.status;throw err}return body}
async function refreshRoutes(){if(cancelled)return;try{const info=await jsonFetch('api/status',{},2500);mergeRoutes(info.urls);serverPaused=!!info.paused;pauseControl.textContent=serverPaused?'Resume':'Pause';if(serverPaused)status.textContent='Paused…'}catch(e){}finally{if(!cancelled)setTimeout(refreshRoutes,1500)}}
async function sessionControl(path){return jsonFetch(path,{method:'POST'},5000)}
pauseControl.onclick=async()=>{if(cancelled)return;pauseControl.disabled=true;const next=!serverPaused;try{await sessionControl(next?'api/pause':'api/resume');serverPaused=next;pauseControl.textContent=serverPaused?'Resume':'Pause';status.textContent=serverPaused?'Paused.':'Resuming…'}catch(e){status.textContent='Could not change transfer state: '+e.message}finally{if(!cancelled)pauseControl.disabled=false}};
cancelControl.onclick=async()=>{if(cancelled)return;cancelled=true;pauseControl.disabled=true;cancelControl.disabled=true;button.disabled=true;status.textContent='Cancelling…';try{await sessionControl('api/cancel')}catch(e){}status.textContent='Cancelled.'};
class SHA256{
constructor(){this.h=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);this.buffer=new Uint8Array(64);this.bufferLength=0;this.bytesHashed=0;this.finished=false;this.w=new Uint32Array(64)}
update(data){if(this.finished)throw new Error('SHA-256 already finalized');if(!(data instanceof Uint8Array))data=new Uint8Array(data);this.bytesHashed+=data.length;let pos=0;if(this.bufferLength){while(this.bufferLength<64&&pos<data.length)this.buffer[this.bufferLength++]=data[pos++];if(this.bufferLength===64){this.compress(this.buffer);this.bufferLength=0}}while(pos+64<=data.length){this.compress(data.subarray(pos,pos+64));pos+=64}while(pos<data.length)this.buffer[this.bufferLength++]=data[pos++];return this}
compress(chunk){const w=this.w;for(let i=0;i<16;i++){const j=i*4;w[i]=((chunk[j]<<24)|(chunk[j+1]<<16)|(chunk[j+2]<<8)|chunk[j+3])>>>0}for(let i=16;i<64;i++){const x=w[i-15],y=w[i-2],s0=((x>>>7)|(x<<25))^((x>>>18)|(x<<14))^(x>>>3),s1=((y>>>17)|(y<<15))^((y>>>19)|(y<<13))^(y>>>10);w[i]=(w[i-16]+s0+w[i-7]+s1)>>>0}let a=this.h[0],b=this.h[1],c=this.h[2],d=this.h[3],e=this.h[4],f=this.h[5],g=this.h[6],h=this.h[7];for(let i=0;i<64;i++){const s1=((e>>>6)|(e<<26))^((e>>>11)|(e<<21))^((e>>>25)|(e<<7)),ch=(e&f)^((~e)&g),t1=(h+s1+ch+SHA256.K[i]+w[i])>>>0,s0=((a>>>2)|(a<<30))^((a>>>13)|(a<<19))^((a>>>22)|(a<<10)),maj=(a&b)^(a&c)^(b&c),t2=(s0+maj)>>>0;h=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0}this.h[0]=(this.h[0]+a)>>>0;this.h[1]=(this.h[1]+b)>>>0;this.h[2]=(this.h[2]+c)>>>0;this.h[3]=(this.h[3]+d)>>>0;this.h[4]=(this.h[4]+e)>>>0;this.h[5]=(this.h[5]+f)>>>0;this.h[6]=(this.h[6]+g)>>>0;this.h[7]=(this.h[7]+h)>>>0}
hex(){if(!this.finished)this.finish();let out='';for(const value of this.h)out+=value.toString(16).padStart(8,'0');return out}
finish(){const bytes=this.bytesHashed;this.buffer[this.bufferLength++]=0x80;if(this.bufferLength>56){while(this.bufferLength<64)this.buffer[this.bufferLength++]=0;this.compress(this.buffer);this.bufferLength=0}while(this.bufferLength<56)this.buffer[this.bufferLength++]=0;const high=Math.floor(bytes/0x20000000)>>>0,low=(bytes*8)>>>0;for(let i=0;i<4;i++)this.buffer[56+i]=(high>>>(24-i*8))&255;for(let i=0;i<4;i++)this.buffer[60+i]=(low>>>(24-i*8))&255;this.compress(this.buffer);this.bufferLength=0;this.finished=true}
}
SHA256.K=new Uint32Array([0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2]);
function hexBytes(bytes){let out='';for(const value of bytes)out+=value.toString(16).padStart(2,'0');return out}
async function hashBlob(blob){const bytes=new Uint8Array(await blob.arrayBuffer());if(globalThis.crypto&&globalThis.crypto.subtle){try{return hexBytes(new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256',bytes)))}catch(e){}}return new SHA256().update(bytes).hex()}
async function fingerprintFile(file){const n=Math.min(file.size,ANCHOR),middle=file.size>n?Math.floor((file.size-n)/2):0,last=file.size>n?file.size-n:0;return {size:file.size,sample_size:ANCHOR,first:await hashBlob(file.slice(0,n)),middle:await hashBlob(file.slice(middle,middle+n)),last:await hashBlob(file.slice(last,last+n))}}
async function prefixSHA256(file,size){const hash=new SHA256();if(size===0)return hash.hex();for(let offset=0;offset<size;offset+=HASH_CHUNK){const end=Math.min(size,offset+HASH_CHUNK);hash.update(new Uint8Array(await file.slice(offset,end).arrayBuffer()));bar.value=file.size?end/file.size:0;status.textContent='Verifying existing progress for '+file.name+' — '+Math.round((end/size)*100)+'%'}return hash.hex()}
async function checkpointHashes(file,chunks){const hashes=[];for(let i=0;i<chunks.length;i++){const chunk=chunks[i]||{},start=Number(chunk.offset),size=Number(chunk.size);if(!Number.isFinite(start)||!Number.isFinite(size)||start<0||size<=0||start+size>file.size)throw new Error('Saved checkpoint range is invalid');hashes.push(await hashBlob(file.slice(start,start+size)));status.textContent='Verifying saved USB checkpoint — '+(i+1)+' / '+chunks.length}return hashes}
function sleep(ms){return new Promise(resolve=>setTimeout(resolve,ms))}
async function verifyManagedPrefix(file,init,startOffset){let offset=Number.isFinite(startOffset)?startOffset:(init.offset||0),changes=0;for(;;){const proof=Array.isArray(init.verify_chunks)&&offset===Number(init.offset||0)?init.verify_chunks:[],body={offset};if(proof.length){body.chunk_sha256=await checkpointHashes(file,proof)}else{body.prefix_sha256=await prefixSHA256(file,offset)}try{return await jsonFetch('api/verify/'+encodeURIComponent(init.upload_id),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)},120000)}catch(e){if(e.status===409&&e.response.transport_busy){status.textContent='USB is active; waiting to continue over Wi-Fi…';await sleep(1000);continue}if(e.status===409&&Number.isFinite(e.response.expected_offset)){offset=e.response.expected_offset;status.textContent='USB progress changed; verifying the new resume point…';if(++changes>12)throw new Error('Resume point kept changing; retry after the USB transfer has stopped');continue}throw e}}}
function formatUploadSpeed(bytes,ms){if(ms<=0)return '0 MB/s';return (bytes/(1024*1024)/(ms/1000)).toFixed(1)+' MB/s'}
function xhrUploadOnce(base,path,blob,start,total,name){return new Promise((resolve,reject)=>{const xhr=new XMLHttpRequest(),startedAt=performance.now();xhr.open('PUT',new URL(path,base).href,true);xhr.timeout=30*60*1000;xhr.setRequestHeader('Content-Type','application/octet-stream');xhr.upload.onprogress=e=>{const sent=e.loaded||0,done=start+sent;bar.value=total?done/total:1;status.textContent='Sending '+name+' — '+Math.round(bar.value*100)+'% • '+formatUploadSpeed(sent,performance.now()-startedAt)};xhr.onload=()=>{let body={};try{body=xhr.responseText?JSON.parse(xhr.responseText):{}}catch(e){}if(xhr.status>=200&&xhr.status<300){preferred=base;resolve(body);return}const err=new Error(body.error||xhr.responseText||('HTTP '+xhr.status));err.response=body;err.status=xhr.status;reject(err)};xhr.onerror=()=>reject(new Error('Wireless upload connection failed'));xhr.ontimeout=()=>reject(new Error('Wireless upload timed out'));xhr.send(blob)})}
async function streamUpload(init,file,offset){const path='api/stream/'+encodeURIComponent(init.upload_id)+'?offset='+offset,blob=file.slice(offset),ordered=[preferred,...routes.filter(item=>item!==preferred)];let lastError=null;for(const base of ordered){try{return await xhrUploadOnce(base,path,blob,offset,file.size,file.name)}catch(e){lastError=e;if(e.status)throw e}}throw lastError||new Error('No Resumexfer route is reachable')}
async function upload(file){const rel=file.webkitRelativePath||file.name;status.textContent='Checking '+file.name+'…';const fingerprint=await fingerprintFile(file),metadata=JSON.stringify({name:file.name,relative_path:rel,size:file.size,fingerprint});let init=await jsonFetch('api/init',{method:'POST',headers:{'Content-Type':'application/json'},body:metadata}),offset=init.offset||0,managed=!!init.needs_verification;if(init.complete){bar.value=1;return}if(managed){const verified=await verifyManagedPrefix(file,init);offset=verified.offset||0;if(verified.complete){bar.value=1;return}while(offset<file.size){const end=Math.min(file.size,offset+CHUNK),chunk=file.slice(offset,end);try{const out=await jsonFetch('api/upload/'+encodeURIComponent(init.upload_id)+'?offset='+offset,{method:'PUT',body:chunk},90000);offset=out.offset;if(out.complete){bar.value=1;return}}catch(e){if(e.status===409&&e.response.transport_busy){status.textContent='USB is active; waiting to continue over Wi-Fi…';await sleep(1000);continue}if(e.status===409&&Number.isFinite(e.response.expected_offset)){const reverified=await verifyManagedPrefix(file,init,e.response.expected_offset);offset=reverified.offset||0;if(reverified.complete){bar.value=1;return}continue}throw e}bar.value=file.size?offset/file.size:1;serverPaused=false;status.textContent='Sending '+file.name+' — '+Math.round(bar.value*100)+'%'}return}
while(offset<file.size){try{const out=await streamUpload(init,file,offset);offset=Number(out.offset||offset);if(out.complete){bar.value=1;status.textContent='Received '+file.name+'.';return}}catch(e){if(e.status===409&&Number.isFinite(e.response.expected_offset)){offset=e.response.expected_offset;status.textContent='Resuming '+file.name+' from '+Math.round((offset/file.size)*100)+'%…';continue}if(!e.status){status.textContent='Connection changed; finding the saved resume point…';await sleep(500);init=await jsonFetch('api/init',{method:'POST',headers:{'Content-Type':'application/json'},body:metadata});offset=init.offset||0;if(init.complete){bar.value=1;return}continue}throw e}}
}

button.onclick=async()=>{if(cancelled)return;button.disabled=true;try{const selected=[...input.files,...(folderInput?[...folderInput.files]:[])],files=[],seen=new Set();for(const file of selected){const key=(file.webkitRelativePath||file.name)+'\0'+file.size+'\0'+file.lastModified;if(!seen.has(key)){seen.add(key);files.push(file)}}if(!files.length){status.textContent='Choose at least one file or folder.';return}for(let i=0;i<files.length;i++){bar.value=0;status.textContent='Preparing '+files[i].name+'…';await upload(files[i])}await sessionControl('api/finish');bar.value=1;status.textContent='Completed.'}catch(e){status.textContent=cancelled?'Cancelled.':'Transfer interrupted: '+e.message+' — keep this page open and press Send files to resume if needed.'}finally{if(!cancelled)button.disabled=false}};
refreshRoutes();
</script></body></html>`))
