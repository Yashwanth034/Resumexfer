package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"resumexfer/internal/fingerprint"
)

type Chunk struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type Transfer struct {
	ID                string                   `json:"id"`
	Source            string                   `json:"source"`
	Destination       string                   `json:"destination"`
	Size              int64                    `json:"size"`
	ChunkSize         int64                    `json:"chunk_size"`
	Chunks            map[int]Chunk            `json:"chunks,omitempty"`
	SourceFingerprint *fingerprint.Fingerprint `json:"source_fingerprint,omitempty"`
	CreatedAt         time.Time                `json:"created_at"`
	UpdatedAt         time.Time                `json:"updated_at"`
}

type Intent struct {
	ID          string    `json:"id"`
	Direction   string    `json:"direction,omitempty"`
	URIs        []string  `json:"uris,omitempty"`
	Destination string    `json:"destination,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ManifestEntry struct {
	ID                string                   `json:"id"`
	Source            string                   `json:"source,omitempty"`
	Destination       string                   `json:"destination,omitempty"`
	Size              int64                    `json:"size,omitempty"`
	BytesDone         int64                    `json:"bytes_done,omitempty"`
	Complete          bool                     `json:"complete,omitempty"`
	CreatedByJob      bool                     `json:"created_by_job,omitempty"`
	Duplicate         bool                     `json:"duplicate,omitempty"`
	DuplicateOf       string                   `json:"duplicate_of,omitempty"`
	SourceFingerprint *fingerprint.Fingerprint `json:"source_fingerprint,omitempty"`
}

const (
	CancelModeKeepCompleted = "keep_completed"
	CancelModeUndoAll       = "undo_all"
)

type Manifest struct {
	ID                  string          `json:"id"`
	Direction           string          `json:"direction,omitempty"`
	DestinationRoot     string          `json:"destination_root,omitempty"`
	Entries             []ManifestEntry `json:"entries"`
	UploadStarted       bool            `json:"upload_started,omitempty"`
	AwaitingReconnect   bool            `json:"awaiting_reconnect,omitempty"`
	Managed             bool            `json:"managed,omitempty"`
	Paused              bool            `json:"paused,omitempty"`
	CancelRequested     bool            `json:"cancel_requested,omitempty"`
	CancelMode          string          `json:"cancel_mode,omitempty"`
	Cancelled           bool            `json:"cancelled,omitempty"`
	LastError           string          `json:"last_error,omitempty"`
	TransportOwner      string          `json:"transport_owner,omitempty"`
	TransportLeaseUntil time.Time       `json:"transport_lease_until,omitempty"`
	TransportGeneration uint64          `json:"transport_generation,omitempty"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

var (
	ErrManifestTransportBusy      = errors.New("manifest transport is owned by another transport")
	ErrManifestTransportLeaseLost = errors.New("manifest transport lease is no longer owned")
)

type TransportLease struct {
	Owner      string
	Generation uint64
	ExpiresAt  time.Time
}

func (m Manifest) Pending() []ManifestEntry {
	out := make([]ManifestEntry, 0, len(m.Entries))
	for _, e := range m.Entries {
		if !e.Complete {
			out = append(out, e)
		}
	}
	return out
}

type Candidate struct {
	ID          string    `json:"id"`
	Direction   string    `json:"direction,omitempty"`
	Source      string    `json:"source,omitempty"`
	Destination string    `json:"destination,omitempty"`
	PartialPath string    `json:"partial_path,omitempty"`
	Size        int64     `json:"size,omitempty"`
	ChunkSize   int64     `json:"chunk_size,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type data struct {
	Transfers  map[string]Transfer  `json:"transfers"`
	Intents    map[string]Intent    `json:"intents"`
	Candidates map[string]Candidate `json:"candidates"`
	Manifests  map[string]Manifest  `json:"manifests"`
}

type Store struct {
	mu   sync.RWMutex
	path string
	data data
}

func Open(path string) (*Store, error) {
	s := &Store{path: path, data: newData()}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(b, &s.data); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	s.ensureMaps()
	return s, nil
}

func newData() data {
	return data{Transfers: map[string]Transfer{}, Intents: map[string]Intent{}, Candidates: map[string]Candidate{}, Manifests: map[string]Manifest{}}
}

func (s *Store) ensureMaps() {
	if s.data.Transfers == nil {
		s.data.Transfers = map[string]Transfer{}
	}
	if s.data.Intents == nil {
		s.data.Intents = map[string]Intent{}
	}
	if s.data.Candidates == nil {
		s.data.Candidates = map[string]Candidate{}
	}
	if s.data.Manifests == nil {
		s.data.Manifests = map[string]Manifest{}
	}
}

func (s *Store) PutTransfer(v Transfer) error {
	if v.ID == "" {
		return fmt.Errorf("transfer id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v = cloneTransfer(v)
	if v.Chunks == nil {
		v.Chunks = map[int]Chunk{}
	}
	s.data.Transfers[v.ID] = v
	return s.saveLocked()
}

func (s *Store) CheckpointChunks(id string, chunks map[int]Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Transfers[id]
	if !ok {
		return fmt.Errorf("transfer %q not found", id)
	}
	if v.Chunks == nil {
		v.Chunks = map[int]Chunk{}
	}
	for i, c := range chunks {
		v.Chunks[i] = c
	}
	v.UpdatedAt = time.Now()
	s.data.Transfers[id] = v
	return s.saveLocked()
}

func (s *Store) Transfer(id string) (Transfer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Transfers[id]
	return cloneTransfer(v), ok
}

func (s *Store) Transfers() []Transfer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Transfer, 0, len(s.data.Transfers))
	for _, v := range s.data.Transfers {
		out = append(out, cloneTransfer(v))
	}
	return out
}

func (s *Store) PutIntent(v Intent) error {
	if v.ID == "" {
		return fmt.Errorf("intent id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Intents[v.ID] = cloneIntent(v)
	return s.saveLocked()
}

func (s *Store) Intent(id string) (Intent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Intents[id]
	return cloneIntent(v), ok
}

func (s *Store) PutCandidate(v Candidate) error {
	if v.ID == "" {
		return fmt.Errorf("candidate id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Candidates[v.ID] = v
	return s.saveLocked()
}

func (s *Store) Candidate(id string) (Candidate, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Candidates[id]
	return v, ok
}

func (s *Store) Prune(now time.Time, retention time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := now.Add(-retention)
	for id, v := range s.data.Intents {
		stamp := v.UpdatedAt
		if stamp.IsZero() {
			stamp = v.CreatedAt
		}
		if !stamp.IsZero() && stamp.Before(cutoff) {
			delete(s.data.Intents, id)
		}
	}
	for id, v := range s.data.Candidates {
		if !v.UpdatedAt.IsZero() && v.UpdatedAt.Before(cutoff) {
			delete(s.data.Candidates, id)
		}
	}
	for id, v := range s.data.Transfers {
		stamp := v.UpdatedAt
		if stamp.IsZero() {
			stamp = v.CreatedAt
		}
		if !stamp.IsZero() && stamp.Before(cutoff) {
			delete(s.data.Transfers, id)
		}
	}
	for id, v := range s.data.Manifests {
		if !v.UpdatedAt.IsZero() && v.UpdatedAt.Before(cutoff) {
			delete(s.data.Manifests, id)
		}
	}
	return s.saveLocked()
}

func (s *Store) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".resumexfer-state-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	ok := false
	defer func() {
		tmp.Close()
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
	if err := os.Rename(name, s.path); err != nil {
		return err
	}
	ok = true
	return nil
}

func cloneTransfer(v Transfer) Transfer {
	out := v
	if v.SourceFingerprint != nil {
		fp := *v.SourceFingerprint
		out.SourceFingerprint = &fp
	}
	if v.Chunks != nil {
		out.Chunks = make(map[int]Chunk, len(v.Chunks))
		for k, c := range v.Chunks {
			out.Chunks[k] = c
		}
	}
	return out
}
func cloneIntent(v Intent) Intent { out := v; out.URIs = append([]string(nil), v.URIs...); return out }

func (s *Store) DeleteTransfer(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Transfers, id)
	return s.saveLocked()
}

func (s *Store) PutManifest(v Manifest) error {
	if v.ID == "" {
		return fmt.Errorf("manifest id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Manifests[v.ID] = cloneManifest(v)
	return s.saveLocked()
}

func (s *Store) Manifest(id string) (Manifest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Manifests[id]
	return cloneManifest(v), ok
}

func (s *Store) Manifests() []Manifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Manifest, 0, len(s.data.Manifests))
	for _, v := range s.data.Manifests {
		out = append(out, cloneManifest(v))
	}
	return out
}

func (s *Store) AcquireManifestTransport(id, owner string, now time.Time, ttl time.Duration) (TransportLease, error) {
	if owner == "" {
		return TransportLease{}, fmt.Errorf("transport owner required")
	}
	if now.IsZero() {
		return TransportLease{}, fmt.Errorf("transport lease time required")
	}
	if ttl <= 0 {
		return TransportLease{}, fmt.Errorf("transport lease ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return TransportLease{}, fmt.Errorf("manifest %q not found", id)
	}
	if v.TransportOwner != "" && v.TransportLeaseUntil.After(now) {
		if v.TransportOwner != owner {
			return TransportLease{}, ErrManifestTransportBusy
		}
		v.TransportLeaseUntil = now.Add(ttl)
		v.UpdatedAt = now
		s.data.Manifests[id] = v
		if err := s.saveLocked(); err != nil {
			return TransportLease{}, err
		}
		return TransportLease{Owner: owner, Generation: v.TransportGeneration, ExpiresAt: v.TransportLeaseUntil}, nil
	}

	v.TransportGeneration++
	if v.TransportGeneration == 0 {
		v.TransportGeneration = 1
	}
	v.TransportOwner = owner
	v.TransportLeaseUntil = now.Add(ttl)
	v.UpdatedAt = now
	s.data.Manifests[id] = v
	if err := s.saveLocked(); err != nil {
		return TransportLease{}, err
	}
	return TransportLease{Owner: owner, Generation: v.TransportGeneration, ExpiresAt: v.TransportLeaseUntil}, nil
}

func (s *Store) RefreshManifestTransport(id, owner string, generation uint64, now time.Time, ttl time.Duration) (TransportLease, error) {
	if owner == "" || generation == 0 {
		return TransportLease{}, fmt.Errorf("transport lease identity required")
	}
	if now.IsZero() {
		return TransportLease{}, fmt.Errorf("transport lease time required")
	}
	if ttl <= 0 {
		return TransportLease{}, fmt.Errorf("transport lease ttl must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return TransportLease{}, fmt.Errorf("manifest %q not found", id)
	}
	if v.TransportOwner != owner || v.TransportGeneration != generation || !v.TransportLeaseUntil.After(now) {
		return TransportLease{}, ErrManifestTransportLeaseLost
	}
	v.TransportLeaseUntil = now.Add(ttl)
	v.UpdatedAt = now
	s.data.Manifests[id] = v
	if err := s.saveLocked(); err != nil {
		return TransportLease{}, err
	}
	return TransportLease{Owner: owner, Generation: generation, ExpiresAt: v.TransportLeaseUntil}, nil
}

func (s *Store) ReleaseManifestTransport(id, owner string, generation uint64) error {
	if owner == "" || generation == 0 {
		return fmt.Errorf("transport lease identity required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if v.TransportOwner != owner || v.TransportGeneration != generation {
		return ErrManifestTransportLeaseLost
	}
	v.TransportOwner = ""
	v.TransportLeaseUntil = time.Time{}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestUploadStarted(id string, started bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}

	if v.UploadStarted == started {
		return nil
	}
	v.UploadStarted = started
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestManaged(id string, managed bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if v.Managed == managed {
		return nil
	}
	v.Managed = managed
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestPaused(id string, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if v.Paused == paused {
		return nil
	}
	v.Paused = paused
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestCancelRequested(id string, requested bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	v.CancelRequested = requested
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func validCancelMode(mode string) bool {
	return mode == CancelModeKeepCompleted || mode == CancelModeUndoAll
}

func (s *Store) RequestManifestCancel(id, mode string) error {
	if !validCancelMode(mode) {
		return fmt.Errorf("invalid cancel mode %q", mode)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	v.Paused = false
	v.CancelMode = mode
	v.CancelRequested = true
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestCancelled(id string, cancelled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	v.Cancelled = cancelled
	if cancelled {
		v.CancelRequested = false
		v.Paused = false
		v.AwaitingReconnect = false
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestLastError(id, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if v.LastError == message {
		return nil
	}
	v.LastError = message
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestEntryProgress(id, entryID string, bytesDone int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID != entryID {
			continue
		}
		if bytesDone < 0 {
			bytesDone = 0
		}
		if v.Entries[i].Size > 0 && bytesDone > v.Entries[i].Size {
			bytesDone = v.Entries[i].Size
		}
		if v.Entries[i].BytesDone == bytesDone {
			return nil
		}
		v.Entries[i].BytesDone = bytesDone
		found = true
		break
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) PrepareManifestEntry(id, entryID string, createdByJob bool, fp fingerprint.Fingerprint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID != entryID {
			continue
		}
		copyFP := fp
		v.Entries[i].CreatedByJob = createdByJob
		v.Entries[i].SourceFingerprint = &copyFP
		found = true
		break
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestEntryCreatedByJob(id, entryID string, created bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID != entryID {
			continue
		}
		if v.Entries[i].CreatedByJob == created {
			return nil
		}
		v.Entries[i].CreatedByJob = created
		found = true
		break
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) ResetManifestEntry(id, entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID != entryID {
			continue
		}
		v.Entries[i].BytesDone = 0
		v.Entries[i].Complete = false
		v.Entries[i].Duplicate = false
		v.Entries[i].DuplicateOf = ""
		found = true
		break
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) MarkManifestEntryDuplicate(id, entryID string) error {
	return s.MarkManifestEntryDuplicateOf(id, entryID, "")
}

func (s *Store) MarkManifestEntryDuplicateOf(id, entryID, duplicateOf string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID != entryID {
			continue
		}
		v.Entries[i].Complete = true
		v.Entries[i].Duplicate = true
		v.Entries[i].DuplicateOf = duplicateOf
		v.Entries[i].BytesDone = v.Entries[i].Size
		v.Entries[i].CreatedByJob = false
		found = true
		break
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) SetManifestAwaitingReconnect(id string, awaiting bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if v.AwaitingReconnect == awaiting {
		return nil
	}
	v.AwaitingReconnect = awaiting
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) MarkManifestEntryComplete(id, entryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data.Manifests[id]
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	found := false
	for i := range v.Entries {
		if v.Entries[i].ID == entryID {
			if v.Entries[i].Complete && v.Entries[i].BytesDone == v.Entries[i].Size {
				return nil
			}
			v.Entries[i].Complete = true
			v.Entries[i].BytesDone = v.Entries[i].Size
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("manifest entry %q not found", entryID)
	}
	v.UpdatedAt = time.Now()
	s.data.Manifests[id] = v
	return s.saveLocked()
}

func (s *Store) DeleteManifest(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Manifests, id)
	return s.saveLocked()
}

func cloneManifest(v Manifest) Manifest {
	out := v
	out.Entries = append([]ManifestEntry(nil), v.Entries...)
	for i := range out.Entries {
		if out.Entries[i].SourceFingerprint != nil {
			fp := *out.Entries[i].SourceFingerprint
			out.Entries[i].SourceFingerprint = &fp
		}
	}
	return out
}

func (s *Store) Candidates() []Candidate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Candidate, 0, len(s.data.Candidates))
	for _, v := range s.data.Candidates {
		out = append(out, v)
	}
	return out
}

func (s *Store) DeleteCandidate(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Candidates, id)
	return s.saveLocked()
}
