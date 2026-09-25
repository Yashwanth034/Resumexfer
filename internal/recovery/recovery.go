package recovery

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/gvfs"
	"resumexfer/internal/state"
)

var (
	ErrDestinationConflict = errors.New("destination conflicts with selected source")
	ErrAmbiguousCandidate  = errors.New("multiple preserved candidates match destination")
	ErrSourceChanged       = errors.New("source size changed since transfer intent")
	ErrUserPaused          = errors.New("transfer paused by user")
	ErrUserCancelled       = errors.New("transfer cancelled by user")
)

const (
	managedUploadBufferBytes          int64 = 8 << 20
	managedUploadInitialProgressBytes int64 = 1 << 20
	managedUploadProgressBytes        int64 = 16 << 20
	managedDownloadChunkBytes         int64 = 8 << 20
	managedDownloadProgressBytes      int64 = 16 << 20
	managedDownloadCheckpointBytes    int64 = 256 << 20
	managedDownloadDirectLimit        int64 = 64 << 20
	defaultTransportLeaseTTL                = 30 * time.Second
	defaultTransportHeartbeat               = 5 * time.Second
)

var transportLeaseCounter atomic.Uint64

type managedUploadWriter interface {
	Write([]byte) (int, error)
	Sync() error
}

type retryReaderAt struct {
	Reader   io.ReaderAt
	Attempts int
	Delay    time.Duration
}

func (r retryReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if r.Reader == nil {
		return 0, fmt.Errorf("reader required")
	}
	attempts := r.Attempts
	if attempts <= 0 {
		attempts = 3
	}
	delay := r.Delay
	if delay <= 0 {
		delay = 75 * time.Millisecond
	}
	total := 0
	for total < len(p) {
		n, err := r.Reader.ReadAt(p[total:], off+int64(total))
		total += n
		if err == nil {
			if total == len(p) {
				return total, nil
			}
			continue
		}
		if errors.Is(err, io.EOF) && total == len(p) {
			return total, io.EOF
		}
		if !retryablePhoneReadError(err) {
			return total, err
		}
		succeeded := false
		for attempt := 1; attempt < attempts; attempt++ {
			time.Sleep(delay)
			n, retryErr := r.Reader.ReadAt(p[total:], off+int64(total))
			total += n
			if retryErr == nil {
				succeeded = true
				break
			}
			if errors.Is(retryErr, io.EOF) && total == len(p) {
				return total, io.EOF
			}
			if !retryablePhoneReadError(retryErr) {
				return total, retryErr
			}
			err = retryErr
		}
		if !succeeded {
			return total, err
		}
	}
	return total, nil
}

func retryablePhoneReadError(err error) bool {
	for _, target := range []error{
		syscall.EIO,
		syscall.ENOENT,
		syscall.EBUSY,
		syscall.EAGAIN,
		syscall.ENOTCONN,
		syscall.ENODEV,
		syscall.ETIMEDOUT,
		syscall.ECONNRESET,
	} {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

type Runner struct {
	Store                   *state.Store
	Engine                  engine.Engine
	TransportLeaseTTL       time.Duration
	TransportLeaseHeartbeat time.Duration
	transportGuard          *transportGuard
}

type transportGuard struct {
	lost atomic.Bool
}

func (g *transportGuard) check() error {
	if g != nil && g.lost.Load() {
		return state.ErrManifestTransportLeaseLost
	}
	return nil
}

type contentDuplicateFinder struct {
	root   string
	built  bool
	bySize map[int64][]string
	seen   map[string]struct{}
}

func newContentDuplicateFinder(manifest state.Manifest) *contentDuplicateFinder {
	if manifest.DestinationRoot == "" {
		// Manifests created before content-wide duplicate detection do not carry
		// the user-selected destination root. Keep those resumable jobs fully
		// backward compatible instead of guessing a scan scope.
		return &contentDuplicateFinder{}
	}
	return &contentDuplicateFinder{root: filepath.Clean(manifest.DestinationRoot)}
}

func (f *contentDuplicateFinder) ensureIndex() error {
	if f == nil || f.built {
		return nil
	}
	if f.root == "" {
		f.built = true
		f.bySize = map[int64][]string{}
		f.seen = map[string]struct{}{}
		return nil
	}
	info, err := os.Stat(f.root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("duplicate scan root is not a directory")
	}
	f.bySize = make(map[int64][]string)
	f.seen = make(map[string]struct{})
	err = filepath.WalkDir(f.root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		base := entry.Name()
		if strings.HasSuffix(base, engine.PartialSuffix) || strings.HasPrefix(base, ".resumexfer-seek-probe-") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		clean := filepath.Clean(path)
		if _, exists := f.seen[clean]; exists {
			return nil
		}
		f.seen[clean] = struct{}{}
		f.bySize[info.Size()] = append(f.bySize[info.Size()], clean)
		return nil
	})
	if err != nil {
		return err
	}
	f.built = true
	return nil
}

func (f *contentDuplicateFinder) record(path string, size int64) {
	if f == nil || !f.built || path == "" {
		return
	}
	clean := filepath.Clean(path)
	if _, exists := f.seen[clean]; exists {
		return
	}
	f.seen[clean] = struct{}{}
	f.bySize[size] = append(f.bySize[size], clean)
}

func (f *contentDuplicateFinder) find(source io.ReaderAt, size int64, excludedPath string) (string, bool, error) {
	if f == nil || source == nil || size < 0 {
		return "", false, nil
	}
	if err := f.ensureIndex(); err != nil {
		return "", false, err
	}
	candidates := f.bySize[size]
	if len(candidates) == 0 {
		return "", false, nil
	}
	excludedPath = filepath.Clean(excludedPath)
	sourceReader := retryReaderAt{Reader: source, Attempts: 3, Delay: 75 * time.Millisecond}
	sourceFP, err := fingerprint.ReaderAt(sourceReader, size, 64*1024)
	if err != nil {
		return "", false, err
	}
	for _, candidatePath := range candidates {
		if filepath.Clean(candidatePath) == excludedPath {
			continue
		}
		candidate, err := gvfs.OpenSource(candidatePath)
		if err != nil {
			return "", false, err
		}
		candidateReader := retryReaderAt{Reader: candidate, Attempts: 3, Delay: 75 * time.Millisecond}
		candidateFP, fpErr := fingerprint.ReaderAt(candidateReader, size, 64*1024)
		if fpErr != nil {
			_ = candidate.Close()
			return "", false, fpErr
		}
		if !sourceFP.Compatible(candidateFP) {
			if err := candidate.Close(); err != nil {
				return "", false, err
			}
			continue
		}
		equal, compareErr := exactContentEqual(sourceReader, candidateReader, size)
		closeErr := candidate.Close()
		if compareErr != nil {
			return "", false, compareErr
		}
		if closeErr != nil {
			return "", false, closeErr
		}
		if equal {
			return candidatePath, true, nil
		}
	}
	return "", false, nil
}

func (r Runner) RecoverManifest(id string) error {
	if r.Store == nil {
		return fmt.Errorf("store required")
	}
	return r.withManifestTransport(id, "usb-recovery", func(leased Runner) error {
		return leased.recoverManifest(id)
	})
}

func (r Runner) recoverManifest(id string) error {
	manifest, ok := r.Store.Manifest(id)
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if manifest.Direction != "phone-to-laptop" && manifest.Direction != "laptop-to-phone" {
		return fmt.Errorf("unsupported recovery direction %q", manifest.Direction)
	}

	resumeEngine := r.Engine
	if resumeEngine.Store == nil {
		resumeEngine.Store = r.Store
	}

	var firstErr error
	duplicateFinder := newContentDuplicateFinder(manifest)
	for _, entry := range manifest.Entries {
		if entry.Complete {
			continue
		}
		if err := r.controlError(id); err != nil {
			if errors.Is(err, ErrUserCancelled) {
				return r.cancelManifest(id)
			}
			return err
		}
		var err error
		if manifest.Direction == "laptop-to-phone" {
			err = r.recoverUploadEntry(id, entry, manifest.Managed, duplicateFinder)
		} else {
			entryEngine := resumeEngine
			if manifest.Managed {
				entryEngine.ChunkSize = managedDownloadChunkBytes
				entryEngine.ProgressBytes = managedDownloadProgressBytes
				entryEngine.CheckpointBytes = managedDownloadCheckpointBytes
			}
			baseProgress := entryEngine.Progress
			entryEngine.Progress = func(done, total int64) error {
				if err := r.controlError(id); err != nil {
					return err
				}
				if err := r.Store.SetManifestAwaitingReconnect(id, false); err != nil {
					return err
				}
				if baseProgress != nil {
					if err := baseProgress(done, total); err != nil {
						return err
					}
				}
				return r.Store.SetManifestEntryProgress(id, entry.ID, done)
			}
			err = r.recoverEntry(id, entryEngine, entry, manifest.Managed, duplicateFinder)
		}
		if err != nil {
			if errors.Is(err, ErrUserCancelled) {
				return r.cancelManifest(id)
			}
			if errors.Is(err, ErrUserPaused) {
				// Large managed downloads use the generic chunk engine. The
				// engine checkpoints any already-written pending chunks when
				// Pause interrupts progress, so publish that durable offset
				// before returning. This keeps the paused UI and the next
				// resume point identical without adding fsyncs to normal flow.
				if manifest.Managed && manifest.Direction == "phone-to-laptop" {
					if transfer, ok := r.Store.Transfer(entry.ID); ok {
						var durable int64
						for _, chunk := range transfer.Chunks {
							durable += chunk.Size
						}
						if durable > 0 {
							if progressErr := r.Store.SetManifestEntryProgress(id, entry.ID, durable); progressErr != nil {
								return progressErr
							}
						}
					}
				}
				return err
			}
			if manifest.Managed {
				return err
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := r.Store.MarkManifestEntryComplete(id, entry.ID); err != nil {
			return err
		}
		duplicateFinder.record(entry.Destination, entry.Size)
		if err := r.cleanupCandidatesForDestination(entry.Destination); err != nil {
			return err
		}
	}
	return firstErr
}

func (r Runner) controlError(id string) error {
	if err := r.transportGuard.check(); err != nil {
		return err
	}
	manifest, ok := r.Store.Manifest(id)
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}
	if manifest.CancelRequested {
		return ErrUserCancelled
	}
	if manifest.Paused {
		return ErrUserPaused
	}
	return nil
}

func (r Runner) withManifestTransport(id, kind string, fn func(Runner) error) error {
	if r.Store == nil {
		return fmt.Errorf("store required")
	}
	if fn == nil {
		return fmt.Errorf("transport operation required")
	}
	ttl := r.TransportLeaseTTL
	if ttl <= 0 {
		ttl = defaultTransportLeaseTTL
	}
	heartbeat := r.TransportLeaseHeartbeat
	if heartbeat <= 0 || heartbeat >= ttl {
		heartbeat = defaultTransportHeartbeat
		if heartbeat >= ttl {
			heartbeat = ttl / 3
		}
	}
	if heartbeat <= 0 {
		heartbeat = time.Millisecond
	}

	owner := fmt.Sprintf("%s:%d:%d", kind, os.Getpid(), transportLeaseCounter.Add(1))
	lease, err := r.Store.AcquireManifestTransport(id, owner, time.Now(), ttl)
	if err != nil {
		return err
	}
	guard := &transportGuard{}
	leased := r
	leased.transportGuard = guard

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				refreshed, refreshErr := r.Store.RefreshManifestTransport(id, owner, lease.Generation, now, ttl)
				if refreshErr != nil {
					guard.lost.Store(true)
					return
				}
				lease = refreshed
			}
		}
	}()

	operationErr := fn(leased)
	close(stop)
	<-done
	if guard.lost.Load() && operationErr == nil {
		operationErr = state.ErrManifestTransportLeaseLost
	}
	releaseErr := r.Store.ReleaseManifestTransport(id, owner, lease.Generation)
	if operationErr != nil {
		return operationErr
	}
	if releaseErr != nil {
		return releaseErr
	}
	return nil
}

func (r Runner) CancelManifest(id string) error {
	if r.Store == nil {
		return fmt.Errorf("store required")
	}
	return r.withManifestTransport(id, "usb-cancel", func(leased Runner) error {
		return leased.cancelManifest(id)
	})
}

func (r Runner) cancelManifest(id string) error {
	manifest, ok := r.Store.Manifest(id)
	if !ok {
		return fmt.Errorf("manifest %q not found", id)
	}

	mode := manifest.CancelMode
	if mode == "" {
		mode = state.CancelModeKeepCompleted
	}
	if mode != state.CancelModeKeepCompleted && mode != state.CancelModeUndoAll {
		return fmt.Errorf("invalid cancel mode %q", mode)
	}

	for _, entry := range manifest.Entries {
		removeFinal := entry.CreatedByJob && (!entry.Complete || mode == state.CancelModeUndoAll)
		removed := false

		if manifest.Direction == "laptop-to-phone" {
			if removeFinal {
				if err := os.Remove(entry.Destination); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				removed = true
			}
		} else {
			// The local sidecar is always Resumexfer-owned transfer state. Remove
			// it for every incomplete entry regardless of whether the eventual
			// final destination is owned by this job.
			if !entry.Complete {
				partial := entry.Destination + engine.PartialSuffix
				if err := os.Remove(partial); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if err := r.Store.DeleteTransfer(entry.ID); err != nil {
					return err
				}
				removed = true
			}
			if removeFinal {
				if err := os.Remove(entry.Destination); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				removed = true
			}
		}

		if removed {
			if err := r.Store.ResetManifestEntry(id, entry.ID); err != nil {
				return err
			}
		}
	}

	if err := r.Store.SetManifestLastError(id, ""); err != nil {
		return err
	}
	return r.Store.SetManifestCancelled(id, true)
}

func (r Runner) cleanupCandidatesForDestination(destination string) error {
	destination = filepath.Clean(destination)

	for _, candidate := range r.Store.Candidates() {
		if candidate.ID == "" || candidate.PartialPath == "" || candidate.Source == "" {
			continue
		}
		if candidateDestination(candidate) != destination {
			continue
		}

		partial := filepath.Clean(candidate.PartialPath)

		// Never remove the completed destination itself, even if state is malformed.
		if partial != destination {
			if err := os.Remove(partial); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}

		if err := r.Store.DeleteCandidate(candidate.ID); err != nil {
			return err
		}
	}

	return nil
}

func (r Runner) recoverUploadEntry(manifestID string, entry state.ManifestEntry, managed bool, duplicates *contentDuplicateFinder) error {
	source, err := gvfs.OpenSource(entry.Source)
	if err != nil {
		return err
	}
	defer source.Close()
	if entry.Size > 0 && entry.Size != source.Size {
		return ErrSourceChanged
	}

	if info, statErr := os.Stat(entry.Destination); statErr == nil {
		if !info.Mode().IsRegular() || info.Size() > source.Size {
			return ErrDestinationConflict
		}
		if info.Size() == source.Size {
			destination, openErr := os.Open(entry.Destination)
			if openErr != nil {
				return openErr
			}
			compatible, compareErr := exactContentEqual(destination, source, source.Size)
			closeErr := destination.Close()
			if compareErr != nil {
				return compareErr
			}
			if closeErr != nil {
				return closeErr
			}
			if !compatible {
				return ErrDestinationConflict
			}
			if managed && !entry.CreatedByJob {
				if err := r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, entry.Destination); err != nil {
					return err
				}
				return nil
			}
			if err := r.Store.SetManifestAwaitingReconnect(manifestID, false); err != nil {
				return err
			}
			if err := r.Store.SetManifestEntryProgress(manifestID, entry.ID, source.Size); err != nil {
				return err
			}
			return nil
		}
		if managed && !entry.CreatedByJob {
			// Managed transfers must never silently append into an unrelated
			// pre-existing same-name destination. That would make "Undo entire
			// transfer" impossible to implement safely on MTP, where truncating
			// back to an arbitrary original size is not reliably supported.
			return ErrDestinationConflict
		}
	} else if errors.Is(statErr, os.ErrNotExist) {
		if managed && !entry.CreatedByJob {
			if duplicateOf, found, findErr := duplicates.find(source, source.Size, entry.Destination); findErr != nil {
				return findErr
			} else if found {
				return r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, duplicateOf)
			}
		}
		if err := r.writeNewUpload(manifestID, entry, source); err != nil {
			return err
		}
		return r.verifyUploadComplete(source, entry.Destination)
	} else {
		return statErr
	}

	// Existing interrupted uploads are resumed by verified append.
	//
	// GVfs/MTP may allow an existing file to be reopened with O_APPEND
	// while rejecting O_RDWR/O_WRONLY reopen. Verify the preserved prefix
	// read-only first, then append only the missing suffix.
	existing, err := os.Open(entry.Destination)
	if err != nil {
		return err
	}

	info, err := existing.Stat()
	if err != nil {
		existing.Close()
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > source.Size {
		existing.Close()
		return ErrDestinationConflict
	}

	verifiedSize := info.Size()
	if err := r.Store.SetManifestEntryProgress(manifestID, entry.ID, verifiedSize); err != nil {
		existing.Close()
		return err
	}

	if verifiedSize > 0 {
		compatible, compareErr := fingerprint.PrefixCompatible(existing, source, verifiedSize)
		closeErr := existing.Close()

		if compareErr != nil {
			return compareErr
		}
		if closeErr != nil {
			return closeErr
		}
		if !compatible {
			return ErrDestinationConflict
		}
	} else if err := existing.Close(); err != nil {
		return err
	}

	destination, err := os.OpenFile(entry.Destination, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer destination.Close()

	// Fail closed if another writer changed the destination between
	// verification and reopening it for append.
	current, err := destination.Stat()
	if err != nil {
		return err
	}
	if !current.Mode().IsRegular() || current.Size() != verifiedSize {
		return ErrDestinationConflict
	}

	if err := r.streamManagedUpload(manifestID, entry.ID, source, source.Size, destination, verifiedSize); err != nil {
		return err
	}
	if err := r.controlError(manifestID); err != nil {
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	return r.verifyUploadComplete(source, entry.Destination)
}

func (r Runner) verifyUploadComplete(source *gvfs.Source, path string) error {
	destination, err := os.Open(path)
	if err != nil {
		return err
	}
	defer destination.Close()
	info, err := destination.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != source.Size {
		return ErrDestinationConflict
	}
	compatible, err := fingerprint.PrefixCompatible(destination, source, source.Size)
	if err != nil {
		return err
	}
	if !compatible {
		return ErrDestinationConflict
	}
	return nil
}

func (r Runner) writeNewUpload(manifestID string, entry state.ManifestEntry, source *gvfs.Source) error {
	path := entry.Destination
	manifest, ok := r.Store.Manifest(manifestID)
	if !ok {
		return fmt.Errorf("manifest %q not found", manifestID)
	}
	if manifest.Managed && manifest.DestinationRoot != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
	}
	destination, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	owned := false
	defer func() {
		_ = destination.Close()
		if !owned {
			_ = os.Remove(path)
		}
	}()
	if err := r.Store.SetManifestEntryCreatedByJob(manifestID, entry.ID, true); err != nil {
		return err
	}
	owned = true

	return r.streamManagedUpload(manifestID, entry.ID, source, source.Size, destination, 0)
}

func (r Runner) streamManagedUpload(manifestID, entryID string, source io.ReaderAt, size int64, destination managedUploadWriter, start int64) error {
	if r.Store == nil {
		return fmt.Errorf("store required")
	}
	if source == nil {
		return fmt.Errorf("source required")
	}
	if destination == nil {
		return fmt.Errorf("destination required")
	}
	if start < 0 || start > size {
		return fmt.Errorf("invalid upload offset")
	}

	// Reaching this point means the destination was successfully opened for
	// writing. A stale GVfs mount can survive a cable pull, so clear the
	// reconnect state only after the real write endpoint is usable.
	if err := r.Store.SetManifestAwaitingReconnect(manifestID, false); err != nil {
		return err
	}
	if err := r.Store.SetManifestEntryProgress(manifestID, entryID, start); err != nil {
		return err
	}

	bufferSize := managedUploadBufferBytes
	if remaining := size - start; remaining > 0 && remaining < bufferSize {
		bufferSize = remaining
	}
	if bufferSize <= 0 {
		bufferSize = 1
	}
	buf := make([]byte, int(bufferSize))
	off := start
	lastProgress := start

	for off < size {
		if err := r.controlError(manifestID); err != nil {
			if errors.Is(err, ErrUserPaused) {
				// Pause is an explicit durability boundary. Do not force device
				// syncs during normal streaming; MTP/GVfs sync is expensive and
				// was the main throughput bottleneck.
				_ = destination.Sync()
				_ = r.Store.SetManifestEntryProgress(manifestID, entryID, off)
			}
			return err
		}

		n := size - off
		if start == 0 && off == 0 && n > managedUploadInitialProgressBytes {
			n = managedUploadInitialProgressBytes
		} else if n > int64(len(buf)) {
			n = int64(len(buf))
		}
		read, err := source.ReadAt(buf[:int(n)], off)
		if err != nil && !(errors.Is(err, io.EOF) && int64(read) == n) {
			return err
		}
		if int64(read) != n {
			return io.ErrUnexpectedEOF
		}

		written, err := destination.Write(buf[:read])
		if err != nil {
			return err
		}
		if written != read {
			return io.ErrShortWrite
		}
		off += int64(written)

		// Persist one small first sample so a fresh transfer immediately shows
		// real progress/speed. After that, retain the existing coarse cadence
		// to avoid turning local state fsyncs into a throughput bottleneck.
		firstFreshProgress := start == 0 && lastProgress == 0 && off > 0
		if off < size && (firstFreshProgress || off-lastProgress >= managedUploadProgressBytes) {
			if err := r.Store.SetManifestEntryProgress(manifestID, entryID, off); err != nil {
				return err
			}
			lastProgress = off
		}
	}

	// One final durability sync is sufficient for a completed managed upload.
	if err := destination.Sync(); err != nil {
		return err
	}
	return r.Store.SetManifestEntryProgress(manifestID, entryID, size)
}

func (r Runner) recoverEntry(manifestID string, resumeEngine engine.Engine, entry state.ManifestEntry, managed bool, duplicates *contentDuplicateFinder) error {
	var source *gvfs.Source
	var err error
	openAttempts := 1
	if managed {
		openAttempts = 5
	}
	for attempt := 0; attempt < openAttempts; attempt++ {
		source, err = gvfs.OpenSource(entry.Source)
		if err == nil {
			break
		}
		if !managed || !retryablePhoneReadError(err) || attempt+1 >= openAttempts {
			return err
		}
		time.Sleep(time.Duration(75*(1<<attempt)) * time.Millisecond)
	}
	defer source.Close()
	if entry.Size > 0 && entry.Size != source.Size {
		return ErrSourceChanged
	}
	var sourceReader io.ReaderAt = source
	if managed {
		sourceReader = retryReaderAt{Reader: source, Attempts: 3, Delay: 75 * time.Millisecond}

		// Small managed downloads have their own crash-safe direct path. Let
		// that path fingerprint and persist ownership once, rather than first
		// doing the same durable state write here and then repeating it there.
		if _, hasLegacyTransfer := r.Store.Transfer(entry.ID); !hasLegacyTransfer && source.Size <= managedDownloadDirectLimit {
			_, hasCandidate, candidateErr := r.candidateForDestination(entry.Destination)
			if candidateErr != nil {
				return candidateErr
			}
			if !hasCandidate {
				return r.recoverManagedSmallDownload(manifestID, entry, sourceReader, source.Size, duplicates)
			}
		}

		currentFingerprint, fpErr := fingerprint.ReaderAt(sourceReader, source.Size, 64*1024)
		if fpErr != nil {
			return fpErr
		}
		if entry.SourceFingerprint != nil {
			if !entry.SourceFingerprint.Compatible(currentFingerprint) {
				return ErrSourceChanged
			}
		} else {
			if err := r.Store.PrepareManifestEntry(manifestID, entry.ID, entry.CreatedByJob, currentFingerprint); err != nil {
				return err
			}
			fp := currentFingerprint
			entry.SourceFingerprint = &fp
		}
	}

	if _, ok := r.Store.Transfer(entry.ID); ok {
		if managed && !entry.CreatedByJob {
			if _, err := os.Stat(entry.Destination); errors.Is(err, os.ErrNotExist) {
				if err := r.Store.SetManifestEntryCreatedByJob(manifestID, entry.ID, true); err != nil {
					return err
				}
			}
		}
		return resumeEngine.Resume(entry.ID, sourceReader, source.Size, entry.Destination)
	}

	info, err := os.Stat(entry.Destination)
	switch {
	case err == nil:
		if !info.Mode().IsRegular() {
			return ErrDestinationConflict
		}
		switch {
		case info.Size() == source.Size:
			destination, openErr := os.Open(entry.Destination)
			if openErr != nil {
				return openErr
			}
			var compatible bool
			var compareErr error
			if managed {
				compatible, compareErr = exactContentEqual(destination, sourceReader, source.Size)
			} else {
				compatible, compareErr = fingerprint.PrefixCompatible(destination, source, source.Size)
			}
			closeErr := destination.Close()
			if compareErr != nil {
				return compareErr
			}
			if closeErr != nil {
				return closeErr
			}
			if !compatible {
				return ErrDestinationConflict
			}
			if managed && !entry.CreatedByJob {
				if err := r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, entry.Destination); err != nil {
					return err
				}
			}
			return nil
		case info.Size() < source.Size:
			if managed && !entry.CreatedByJob {
				return ErrDestinationConflict
			}
			return resumeEngine.ResumeFromPreserved(entry.ID, sourceReader, source.Size, entry.Destination, entry.Destination)
		default:
			return ErrDestinationConflict
		}
	case errors.Is(err, os.ErrNotExist):
		if managed && !entry.CreatedByJob {
			if duplicateOf, found, findErr := duplicates.find(sourceReader, source.Size, entry.Destination); findErr != nil {
				return findErr
			} else if found {
				return r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, duplicateOf)
			}
		}
		candidate, found, candidateErr := r.candidateForDestination(entry.Destination)
		if candidateErr != nil {
			return candidateErr
		}
		if managed && !entry.CreatedByJob {
			if err := r.Store.SetManifestEntryCreatedByJob(manifestID, entry.ID, true); err != nil {
				return err
			}
		}
		if found {
			if candidate.Destination != "" && candidate.ChunkSize > 0 {
				preserved, openErr := os.Open(candidate.PartialPath)
				if openErr != nil {
					return openErr
				}
				info, statErr := preserved.Stat()
				if statErr != nil {
					_ = preserved.Close()
					return statErr
				}
				if !info.Mode().IsRegular() || info.Size() > source.Size || (info.Size() > 0 && info.Size()%candidate.ChunkSize != 0) {
					_ = preserved.Close()
					return engine.ErrCorruptPartial
				}
				equal, compareErr := exactContentEqual(preserved, sourceReader, info.Size())
				closeErr := preserved.Close()
				if compareErr != nil {
					return compareErr
				}
				if closeErr != nil {
					return closeErr
				}
				if !equal {
					return engine.ErrPrefixMismatch
				}
			}
			candidateEngine := resumeEngine
			if candidate.ChunkSize > 0 {
				candidateEngine.ChunkSize = candidate.ChunkSize
			}
			return candidateEngine.ResumeFromPreserved(entry.ID, sourceReader, source.Size, entry.Destination, candidate.PartialPath)
		}
		return resumeEngine.Resume(entry.ID, sourceReader, source.Size, entry.Destination)
	default:
		return err
	}
}

func (r Runner) recoverManagedSmallDownload(manifestID string, entry state.ManifestEntry, source io.ReaderAt, size int64, duplicates *contentDuplicateFinder) error {
	if r.Store == nil {
		return fmt.Errorf("store required")
	}
	if source == nil {
		return fmt.Errorf("source required")
	}
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	partialPath := entry.Destination + engine.PartialSuffix

	if info, err := os.Stat(entry.Destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() != size {
			return ErrDestinationConflict
		}
		destination, openErr := os.Open(entry.Destination)
		if openErr != nil {
			return openErr
		}
		equal, compareErr := exactContentEqual(destination, source, size)
		closeErr := destination.Close()
		if compareErr != nil {
			return compareErr
		}
		if closeErr != nil {
			return closeErr
		}
		if !equal {
			return ErrDestinationConflict
		}
		if !entry.CreatedByJob {
			return r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, entry.Destination)
		}
		// Recovery may restart after the final file was promoted but before the
		// owned sidecar name was removed. Clear that terminal artifact now.
		if err := os.Remove(partialPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return r.Store.SetManifestEntryProgress(manifestID, entry.ID, size)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	if !entry.CreatedByJob {
		if duplicateOf, found, findErr := duplicates.find(source, size, entry.Destination); findErr != nil {
			return findErr
		} else if found {
			return r.Store.MarkManifestEntryDuplicateOf(manifestID, entry.ID, duplicateOf)
		}
	}

	currentFingerprint, err := fingerprint.ReaderAt(source, size, 64*1024)
	if err != nil {
		return err
	}
	if entry.SourceFingerprint != nil && !entry.SourceFingerprint.Compatible(currentFingerprint) {
		return ErrSourceChanged
	}

	if err := os.MkdirAll(filepath.Dir(entry.Destination), 0o755); err != nil {
		return err
	}
	partialInfo, statErr := os.Stat(partialPath)
	start := int64(0)
	var destination *os.File

	switch {
	case statErr == nil:
		if !entry.CreatedByJob || !partialInfo.Mode().IsRegular() || partialInfo.Size() > size {
			return ErrDestinationConflict
		}
		if entry.SourceFingerprint == nil {
			if err := r.Store.PrepareManifestEntry(manifestID, entry.ID, true, currentFingerprint); err != nil {
				return err
			}
		}
		start = partialInfo.Size()
		if start > 0 {
			partial, openErr := os.Open(partialPath)
			if openErr != nil {
				return openErr
			}
			compatible, compareErr := exactContentEqual(partial, source, start)
			closeErr := partial.Close()
			if compareErr != nil {
				return compareErr
			}
			if closeErr != nil {
				return closeErr
			}
			if !compatible {
				return engine.ErrCorruptPartial
			}
		}
		destination, err = os.OpenFile(partialPath, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			return err
		}
	case errors.Is(statErr, os.ErrNotExist):
		destination, err = os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if err := r.Store.PrepareManifestEntry(manifestID, entry.ID, true, currentFingerprint); err != nil {
			_ = destination.Close()
			_ = os.Remove(partialPath)
			return err
		}
	default:
		return statErr
	}

	closed := false
	defer func() {
		if !closed {
			_ = destination.Close()
		}
	}()

	if err := r.streamManagedDownload(manifestID, entry.ID, source, size, destination, start); err != nil {
		return err
	}
	if err := r.controlError(manifestID); err != nil {
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	closed = true

	// Promote without overwriting a file that appeared after the initial
	// destination check. Prefer a same-filesystem hard link for atomic
	// no-replace semantics. Filesystems such as FAT/exFAT do not support hard
	// links, so fall back to an O_EXCL final file and copy at most the direct
	// path limit (64 MiB) from our owned sidecar.
	if err := promoteManagedSmallDownload(partialPath, entry.Destination); err != nil {
		return err
	}
	if err := syncLocalDir(filepath.Dir(entry.Destination)); err != nil {
		return err
	}
	// recoverManifest immediately persists Complete=true and BytesDone=size.
	// Avoid a redundant full state-file fsync here for every small file.
	return nil
}

func promoteManagedSmallDownload(partialPath, destination string) error {
	if err := os.Link(partialPath, destination); err == nil {
		return os.Remove(partialPath)
	} else if errors.Is(err, os.ErrExist) {
		return ErrDestinationConflict
	} else if !errors.Is(err, syscall.EPERM) && !errors.Is(err, syscall.ENOTSUP) && !errors.Is(err, syscall.EOPNOTSUPP) {
		return err
	}

	source, err := os.Open(partialPath)
	if err != nil {
		return err
	}
	defer source.Close()

	final, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrDestinationConflict
		}
		return err
	}
	keepFinal := false
	defer func() {
		_ = final.Close()
		if !keepFinal {
			_ = os.Remove(destination)
		}
	}()

	buf := make([]byte, 8<<20)
	if _, err := io.CopyBuffer(final, source, buf); err != nil {
		return err
	}
	if err := final.Sync(); err != nil {
		return err
	}
	if err := final.Close(); err != nil {
		return err
	}
	if err := os.Remove(partialPath); err != nil {
		return err
	}
	keepFinal = true
	return nil
}

func (r Runner) streamManagedDownload(manifestID, entryID string, source io.ReaderAt, size int64, destination *os.File, start int64) error {
	if start < 0 || start > size {
		return fmt.Errorf("invalid download offset")
	}
	if err := r.Store.SetManifestAwaitingReconnect(manifestID, false); err != nil {
		return err
	}
	if err := r.Store.SetManifestEntryProgress(manifestID, entryID, start); err != nil {
		return err
	}
	bufferSize := managedDownloadChunkBytes
	if remaining := size - start; remaining > 0 && remaining < bufferSize {
		bufferSize = remaining
	}
	if bufferSize <= 0 {
		bufferSize = 1
	}
	buf := make([]byte, int(bufferSize))
	off := start
	lastProgress := start
	for off < size {
		if err := r.controlError(manifestID); err != nil {
			if errors.Is(err, ErrUserPaused) {
				_ = destination.Sync()
				_ = r.Store.SetManifestEntryProgress(manifestID, entryID, off)
			}
			return err
		}
		n := size - off
		if n > int64(len(buf)) {
			n = int64(len(buf))
		}
		read, err := source.ReadAt(buf[:int(n)], off)
		if err != nil && !(errors.Is(err, io.EOF) && int64(read) == n) {
			return err
		}
		if int64(read) != n {
			return io.ErrUnexpectedEOF
		}
		written, err := destination.Write(buf[:read])
		if err != nil {
			return err
		}
		if written != read {
			return io.ErrShortWrite
		}
		off += int64(written)
		if off < size && off-lastProgress >= managedDownloadProgressBytes {
			if err := r.Store.SetManifestEntryProgress(manifestID, entryID, off); err != nil {
				return err
			}
			lastProgress = off
		}
	}
	if err := destination.Sync(); err != nil {
		return err
	}
	// The caller promotes the fully-synced sidecar and immediately marks the
	// entry complete, which persists BytesDone=size. A separate durable
	// progress write here only doubles state-file fsync work for small files.
	return nil
}

func syncLocalDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func exactContentEqual(left, right io.ReaderAt, size int64) (bool, error) {
	if size < 0 {
		return false, fmt.Errorf("negative size")
	}
	const block = int64(1 << 20)
	leftBuf := make([]byte, block)
	rightBuf := make([]byte, block)
	for off := int64(0); off < size; {
		n := size - off
		if n > block {
			n = block
		}
		ln, lerr := left.ReadAt(leftBuf[:int(n)], off)
		rn, rerr := right.ReadAt(rightBuf[:int(n)], off)
		if lerr != nil && !(errors.Is(lerr, io.EOF) && int64(ln) == n) {
			return false, lerr
		}
		if rerr != nil && !(errors.Is(rerr, io.EOF) && int64(rn) == n) {
			return false, rerr
		}
		if ln != int(n) || rn != int(n) {
			return false, io.ErrUnexpectedEOF
		}
		if !bytes.Equal(leftBuf[:ln], rightBuf[:rn]) {
			return false, nil
		}
		off += n
	}
	return true, nil
}

func (r Runner) candidateForDestination(destination string) (state.Candidate, bool, error) {
	destination = filepath.Clean(destination)
	matches := make([]state.Candidate, 0, 1)
	for _, candidate := range r.Store.Candidates() {
		if candidate.PartialPath == "" || candidate.Source == "" {
			continue
		}
		if candidateDestination(candidate) != destination {
			continue
		}
		info, err := os.Stat(candidate.PartialPath)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		matches = append(matches, candidate)
	}
	if len(matches) == 0 {
		return state.Candidate{}, false, nil
	}
	if len(matches) > 1 {
		return state.Candidate{}, false, ErrAmbiguousCandidate
	}
	return matches[0], true, nil
}

func candidateDestination(candidate state.Candidate) string {
	if strings.TrimSpace(candidate.Destination) != "" {
		return filepath.Clean(candidate.Destination)
	}
	return candidateTarget(candidate.Source)
}

func candidateTarget(path string) string {
	clean := filepath.Clean(path)
	lower := strings.ToLower(clean)
	for _, suffix := range []string{".partial", ".crdownload", ".part", ".tmp"} {
		if strings.HasSuffix(lower, suffix) {
			return clean[:len(clean)-len(suffix)]
		}
	}
	return clean
}
