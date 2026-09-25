package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"resumexfer/internal/fingerprint"
	"resumexfer/internal/state"
)

const (
	DefaultChunkSize       int64 = 4 << 20
	DefaultCheckpointBytes int64 = 64 << 20
	PartialSuffix                = ".resumexfer-part"
)

var (
	ErrCorruptPartial            = errors.New("partial file failed chunk verification")
	ErrPrefixMismatch            = errors.New("preserved prefix does not match source")
	ErrAdoptionConflict          = errors.New("preserved prefix adoption conflicts with existing resume state")
	ErrSourceChanged             = errors.New("source content changed during resumable transfer")
	ErrSourceIdentityUnavailable = errors.New("source identity unavailable for existing transfer")
)

type Engine struct {
	Store           *state.Store
	ChunkSize       int64
	CheckpointBytes int64
	ProgressBytes   int64
	Progress        func(done, total int64) error
}

func ChunkCount(size, chunkSize int64) int {
	if size <= 0 {
		return 0
	}
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return int((size + chunkSize - 1) / chunkSize)
}

func (e Engine) ResumeFromPreserved(id string, source io.ReaderAt, size int64, destination, preservedPath string) error {
	if e.Store == nil {
		return fmt.Errorf("store required")
	}
	if id == "" {
		return fmt.Errorf("transfer id required")
	}
	if source == nil {
		return fmt.Errorf("source required")
	}
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	if _, ok := e.Store.Transfer(id); ok {
		return ErrAdoptionConflict
	}

	preserved, err := os.Open(preservedPath)
	if err != nil {
		return err
	}
	defer preserved.Close()
	info, err := preserved.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("preserved prefix is not a regular file")
	}
	prefixSize := info.Size()
	if prefixSize <= 0 {
		return e.Resume(id, source, size, destination)
	}
	if prefixSize > size {
		return ErrPrefixMismatch
	}

	compatible, err := fingerprint.PrefixCompatible(preserved, source, prefixSize)
	if err != nil {
		return err
	}
	if !compatible {
		return ErrPrefixMismatch
	}

	chunkSize := e.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	durableChunks := int(prefixSize / chunkSize)
	if durableChunks == 0 {
		return e.Resume(id, source, size, destination)
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	partialPath := destination + PartialSuffix
	partial, err := os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if errors.Is(err, os.ErrExist) {
		return ErrAdoptionConflict
	}
	if err != nil {
		return err
	}
	keepPartial := false
	defer func() {
		_ = partial.Close()
		if !keepPartial {
			_ = os.Remove(partialPath)
		}
	}()

	if _, err := io.CopyN(partial, io.NewSectionReader(preserved, 0, prefixSize), prefixSize); err != nil {
		return err
	}
	if err := partial.Sync(); err != nil {
		return err
	}

	chunks := make(map[int]state.Chunk, durableChunks)
	for i := 0; i < durableChunks; i++ {
		off := int64(i) * chunkSize
		buf := make([]byte, chunkSize)
		if _, err := io.ReadFull(io.NewSectionReader(preserved, off, chunkSize), buf); err != nil {
			return err
		}
		sum := sha256.Sum256(buf)
		chunks[i] = state.Chunk{Hash: hex.EncodeToString(sum[:]), Size: chunkSize}
	}

	sourceFingerprint, err := fingerprint.ReaderAt(source, size, 64*1024)
	if err != nil {
		return err
	}
	now := time.Now()
	transfer := state.Transfer{
		ID:                id,
		Destination:       destination,
		Size:              size,
		ChunkSize:         chunkSize,
		Chunks:            chunks,
		SourceFingerprint: &sourceFingerprint,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := e.Store.PutTransfer(transfer); err != nil {
		return err
	}
	if e.Progress != nil {
		if err := e.Progress(int64(durableChunks)*chunkSize, size); err != nil {
			return err
		}
	}
	keepPartial = true
	return e.Resume(id, source, size, destination)
}

// PrepareRemoteUpload binds a remotely supplied byte stream to the same
// durable chunk map used by local/MTP transfers. Only bytes covered by
// verified, checkpointed chunks are adopted after a restart; an uncheckpointed
// tail is discarded so callers never resume from a UI/physical byte count
// alone.
func (e Engine) PrepareRemoteUpload(id string, size int64, destination, partialPath string, sourceFingerprint fingerprint.Fingerprint) (int64, error) {
	if e.Store == nil {
		return 0, fmt.Errorf("store required")
	}
	if id == "" {
		return 0, fmt.Errorf("transfer id required")
	}
	if size < 0 {
		return 0, fmt.Errorf("negative size")
	}
	if destination == "" || partialPath == "" {
		return 0, fmt.Errorf("destination and partial path required")
	}
	if sourceFingerprint.Size != size || sourceFingerprint.SampleSize <= 0 {
		return 0, fmt.Errorf("valid source fingerprint required")
	}

	chunkSize := e.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	tr, ok := e.Store.Transfer(id)
	if !ok {
		now := time.Now()
		tr = state.Transfer{
			ID:                id,
			Destination:       destination,
			Size:              size,
			ChunkSize:         chunkSize,
			Chunks:            map[int]state.Chunk{},
			SourceFingerprint: &sourceFingerprint,
			CreatedAt:         now,
			UpdatedAt:         now,
		}
		if err := e.Store.PutTransfer(tr); err != nil {
			return 0, err
		}
	} else {
		if tr.Size != size || tr.Destination != destination || tr.ChunkSize != chunkSize {
			return 0, fmt.Errorf("transfer metadata changed")
		}
		if tr.SourceFingerprint == nil {
			return 0, ErrSourceIdentityUnavailable
		}
		if !tr.SourceFingerprint.Compatible(sourceFingerprint) {
			return 0, ErrSourceChanged
		}
	}

	durable, err := contiguousDurablePrefix(tr)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(partialPath), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(partialPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if !info.Mode().IsRegular() || info.Size() < durable || info.Size() > size {
		return 0, ErrCorruptPartial
	}
	if err := verifyRecorded(f, tr, size, chunkSize); err != nil {
		return 0, err
	}
	if info.Size() != durable {
		if err := f.Truncate(durable); err != nil {
			return 0, err
		}
		if err := f.Sync(); err != nil {
			return 0, err
		}
	}
	return durable, nil
}

// AppendRemoteUpload durably appends remote bytes and checkpoints every newly
// complete engine chunk. The final short chunk is checkpointed only when the
// complete file has arrived.
func (e Engine) AppendRemoteUpload(id, partialPath string, expectedOffset int64, payload []byte) (int64, bool, error) {
	if e.Store == nil {
		return 0, false, fmt.Errorf("store required")
	}
	tr, ok := e.Store.Transfer(id)
	if !ok {
		return 0, false, fmt.Errorf("transfer %q not found", id)
	}
	if partialPath == "" {
		return 0, false, fmt.Errorf("partial path required")
	}
	if expectedOffset < 0 || expectedOffset > tr.Size {
		return 0, false, fmt.Errorf("invalid expected offset")
	}
	if int64(len(payload)) > tr.Size-expectedOffset {
		return 0, false, fmt.Errorf("payload exceeds transfer size")
	}

	if err := os.MkdirAll(filepath.Dir(partialPath), 0o755); err != nil {
		return 0, false, err
	}
	f, err := os.OpenFile(partialPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, false, err
	}
	if !info.Mode().IsRegular() || info.Size() != expectedOffset {
		return info.Size(), false, fmt.Errorf("remote upload offset changed")
	}
	durable, err := contiguousDurablePrefix(tr)
	if err != nil {
		return expectedOffset, false, err
	}
	if durable > expectedOffset {
		return expectedOffset, false, ErrAdoptionConflict
	}
	if len(payload) > 0 {
		if _, err := f.WriteAt(payload, expectedOffset); err != nil {
			return expectedOffset, false, err
		}
	}
	newOffset := expectedOffset + int64(len(payload))
	if err := f.Truncate(newOffset); err != nil {
		return expectedOffset, false, err
	}
	if err := f.Sync(); err != nil {
		return expectedOffset, false, err
	}

	checkpointEnd := (newOffset / tr.ChunkSize) * tr.ChunkSize
	if newOffset == tr.Size {
		checkpointEnd = newOffset
	}
	pending := map[int]state.Chunk{}
	for off := durable; off < checkpointEnd; {
		index := int(off / tr.ChunkSize)
		n := min64(tr.ChunkSize, tr.Size-off)
		if off+n > checkpointEnd {
			break
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(io.NewSectionReader(f, off, n), buf); err != nil {
			return expectedOffset, false, err
		}
		sum := sha256.Sum256(buf)
		pending[index] = state.Chunk{Hash: hex.EncodeToString(sum[:]), Size: n}
		off += n
	}
	if len(pending) > 0 {
		if err := e.Store.CheckpointChunks(id, pending); err != nil {
			return expectedOffset, false, err
		}
		for index, chunk := range pending {
			tr.Chunks[index] = chunk
		}
	}

	complete := newOffset == tr.Size
	if complete {
		latest, ok := e.Store.Transfer(id)
		if !ok {
			return newOffset, false, fmt.Errorf("transfer disappeared")
		}
		if len(latest.Chunks) != ChunkCount(tr.Size, tr.ChunkSize) {
			return newOffset, false, fmt.Errorf("incomplete chunk map")
		}
		if err := verifyRecorded(f, latest, tr.Size, tr.ChunkSize); err != nil {
			return newOffset, false, err
		}
	}
	return newOffset, complete, nil
}

// StreamRemoteUpload appends a remote stream without buffering the entire HTTP
// request in memory. It hashes the same engine-sized chunks used by resumable
// recovery and batches durable state/fsync work so a browser upload can keep
// the network pipe full. A short read checkpoints every complete chunk that
// arrived before the interruption; at most one engine chunk is retried.
func (e Engine) StreamRemoteUpload(
	id, partialPath string,
	expectedOffset int64,
	source io.Reader,
	checkpointBytes int64,
	progress func(int64) error,
) (int64, bool, error) {
	if e.Store == nil {
		return 0, false, fmt.Errorf("store required")
	}
	tr, ok := e.Store.Transfer(id)
	if !ok {
		return 0, false, fmt.Errorf("transfer %q not found", id)
	}
	if partialPath == "" {
		return 0, false, fmt.Errorf("partial path required")
	}
	if source == nil {
		return 0, false, fmt.Errorf("source required")
	}
	if expectedOffset < 0 || expectedOffset > tr.Size {
		return 0, false, fmt.Errorf("invalid expected offset")
	}
	if checkpointBytes <= 0 {
		checkpointBytes = 128 << 20
	}

	durable, err := contiguousDurablePrefix(tr)
	if err != nil {
		return expectedOffset, false, err
	}
	if durable != expectedOffset {
		return durable, false, fmt.Errorf("remote upload offset changed")
	}
	if expectedOffset < tr.Size && expectedOffset%tr.ChunkSize != 0 {
		return expectedOffset, false, ErrCorruptPartial
	}

	if err := os.MkdirAll(filepath.Dir(partialPath), 0o755); err != nil {
		return expectedOffset, false, err
	}
	f, err := os.OpenFile(partialPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return expectedOffset, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return expectedOffset, false, err
	}
	if !info.Mode().IsRegular() || info.Size() != expectedOffset {
		return info.Size(), false, fmt.Errorf("remote upload offset changed")
	}

	pending := make(map[int]state.Chunk)
	pendingBytes := int64(0)
	current := expectedOffset

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := f.Sync(); err != nil {
			return err
		}
		if err := e.Store.CheckpointChunks(id, pending); err != nil {
			return err
		}
		for index, chunk := range pending {
			tr.Chunks[index] = chunk
		}
		pending = make(map[int]state.Chunk)
		pendingBytes = 0
		durable = current
		return nil
	}

	buf := make([]byte, tr.ChunkSize)
	for current < tr.Size {
		want := min64(tr.ChunkSize, tr.Size-current)
		n, readErr := io.ReadFull(source, buf[:int(want)])
		if int64(n) == want {
			if _, err := f.WriteAt(buf[:n], current); err != nil {
				return durable, false, err
			}
			sum := sha256.Sum256(buf[:n])
			index := int(current / tr.ChunkSize)
			pending[index] = state.Chunk{Hash: hex.EncodeToString(sum[:]), Size: int64(n)}
			current += int64(n)
			pendingBytes += int64(n)
			if progress != nil {
				if err := progress(current); err != nil {
					if flushErr := flush(); flushErr != nil {
						return durable, false, flushErr
					}
					return durable, false, err
				}
			}
			if pendingBytes >= checkpointBytes || current == tr.Size {
				if err := flush(); err != nil {
					return durable, false, err
				}
			}
			continue
		}

		// Do not commit a partial engine chunk. Persist all complete chunks that
		// preceded it so a retry only re-sends the unfinished chunk.
		if err := flush(); err != nil {
			return durable, false, err
		}
		if readErr == nil {
			readErr = io.ErrUnexpectedEOF
		}
		return durable, false, readErr
	}

	latest, ok := e.Store.Transfer(id)
	if !ok {
		return current, false, fmt.Errorf("transfer disappeared")
	}
	if len(latest.Chunks) != ChunkCount(tr.Size, tr.ChunkSize) {
		return current, false, fmt.Errorf("incomplete chunk map")
	}
	if err := verifyRecorded(f, latest, tr.Size, tr.ChunkSize); err != nil {
		return current, false, err
	}
	return current, true, nil
}

// CompleteRemoteUpload forgets durable transfer state only after the caller has
// safely promoted the verified partial to its final no-replace destination.
func (e Engine) CompleteRemoteUpload(id string) error {
	if e.Store == nil {
		return fmt.Errorf("store required")
	}
	tr, ok := e.Store.Transfer(id)
	if !ok {
		return fmt.Errorf("transfer %q not found", id)
	}
	if len(tr.Chunks) != ChunkCount(tr.Size, tr.ChunkSize) {
		return fmt.Errorf("remote transfer is not fully checkpointed")
	}
	return e.Store.DeleteTransfer(id)
}

func contiguousDurablePrefix(tr state.Transfer) (int64, error) {
	if tr.ChunkSize <= 0 {
		return 0, ErrCorruptPartial
	}
	durable := int64(0)
	count := ChunkCount(tr.Size, tr.ChunkSize)
	for index := 0; index < count; index++ {
		chunk, ok := tr.Chunks[index]
		if !ok {
			for later := index + 1; later < count; later++ {
				if _, exists := tr.Chunks[later]; exists {
					return 0, ErrCorruptPartial
				}
			}
			break
		}
		expected := min64(tr.ChunkSize, tr.Size-durable)
		if chunk.Size != expected {
			return 0, ErrCorruptPartial
		}
		durable += chunk.Size
	}
	return durable, nil
}

func (e Engine) Resume(id string, source io.ReaderAt, size int64, destination string) error {
	if e.Store == nil {
		return fmt.Errorf("store required")
	}
	if id == "" {
		return fmt.Errorf("transfer id required")
	}
	if size < 0 {
		return fmt.Errorf("negative size")
	}
	chunkSize := e.ChunkSize
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	checkpointBytes := e.CheckpointBytes
	if checkpointBytes <= 0 {
		checkpointBytes = DefaultCheckpointBytes
	}
	if checkpointBytes < chunkSize {
		checkpointBytes = chunkSize
	}
	progressBytes := e.ProgressBytes
	if progressBytes <= 0 {
		progressBytes = checkpointBytes
	}
	if progressBytes < chunkSize {
		progressBytes = chunkSize
	}

	tr, ok := e.Store.Transfer(id)
	if !ok {
		sourceFingerprint, err := fingerprint.ReaderAt(source, size, 64*1024)
		if err != nil {
			return err
		}
		now := time.Now()
		tr = state.Transfer{ID: id, Destination: destination, Size: size, ChunkSize: chunkSize, Chunks: map[int]state.Chunk{}, SourceFingerprint: &sourceFingerprint, CreatedAt: now, UpdatedAt: now}
		if err := e.Store.PutTransfer(tr); err != nil {
			return err
		}
	} else {
		if tr.Size != size || tr.Destination != destination || tr.ChunkSize != chunkSize {
			return fmt.Errorf("transfer metadata changed")
		}
		if tr.SourceFingerprint == nil {
			return ErrSourceIdentityUnavailable
		}
		currentFingerprint, err := fingerprint.ReaderAt(source, size, tr.SourceFingerprint.SampleSize)
		if err != nil {
			return err
		}
		if !tr.SourceFingerprint.Compatible(currentFingerprint) {
			return ErrSourceChanged
		}
	}

	durableDone := int64(0)
	for _, chunk := range tr.Chunks {
		durableDone += chunk.Size
	}
	reportedDone := durableDone
	if e.Progress != nil {
		if err := e.Progress(durableDone, size); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	partial := destination + PartialSuffix
	f, err := os.OpenFile(partial, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := verifyRecorded(f, tr, size, chunkSize); err != nil {
		return err
	}

	pending := map[int]state.Chunk{}
	pendingBytes := int64(0)
	for i := 0; i < ChunkCount(size, chunkSize); i++ {
		if _, done := tr.Chunks[i]; done {
			continue
		}
		off := int64(i) * chunkSize
		n := min64(chunkSize, size-off)
		buf := make([]byte, n)
		if _, err := io.ReadFull(io.NewSectionReader(source, off, n), buf); err != nil {
			return err
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			return err
		}
		sum := sha256.Sum256(buf)
		pending[i] = state.Chunk{Hash: hex.EncodeToString(sum[:]), Size: n}
		pendingBytes += n
		if e.Progress != nil {
			currentDone := durableDone + pendingBytes
			if currentDone-reportedDone >= progressBytes {
				if err := e.Progress(currentDone, size); err != nil {
					// An interactive pause/cancel can arrive between normal
					// durability checkpoints. Persist the bytes that are
					// already written and hashed before returning so callers
					// never have to resume behind the last physical write.
					if len(pending) > 0 {
						if checkpointErr := checkpoint(e.Store, f, id, pending); checkpointErr != nil {
							return checkpointErr
						}
						for k, c := range pending {
							tr.Chunks[k] = c
						}
						durableDone += pendingBytes
						pending = map[int]state.Chunk{}
						pendingBytes = 0
					}
					return err
				}
				reportedDone = currentDone
			}
		}
		if pendingBytes >= checkpointBytes {
			if err := checkpoint(e.Store, f, id, pending); err != nil {
				return err
			}
			for k, c := range pending {
				tr.Chunks[k] = c
			}
			durableDone += pendingBytes
			if e.Progress != nil && durableDone > reportedDone {
				if err := e.Progress(durableDone, size); err != nil {
					return err
				}
				reportedDone = durableDone
			}
			pending = map[int]state.Chunk{}
			pendingBytes = 0
		}
	}
	if len(pending) > 0 {
		if err := checkpoint(e.Store, f, id, pending); err != nil {
			return err
		}
		for k, c := range pending {
			tr.Chunks[k] = c
		}
		durableDone += pendingBytes
		if e.Progress != nil && durableDone > reportedDone {
			if err := e.Progress(durableDone, size); err != nil {
				return err
			}
			reportedDone = durableDone
		}
	}

	latest, ok := e.Store.Transfer(id)
	if !ok {
		return fmt.Errorf("transfer disappeared")
	}
	if len(latest.Chunks) != ChunkCount(size, chunkSize) {
		return fmt.Errorf("incomplete chunk map")
	}
	if err := verifyRecorded(f, latest, size, chunkSize); err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(partial, destination); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(destination)); err != nil {
		return err
	}
	if e.Progress != nil {
		if err := e.Progress(size, size); err != nil {
			return err
		}
	}
	return e.Store.DeleteTransfer(id)
}

func checkpoint(s *state.Store, f *os.File, id string, chunks map[int]state.Chunk) error {
	if err := f.Sync(); err != nil {
		return err
	}
	return s.CheckpointChunks(id, chunks)
}

func verifyRecorded(f *os.File, tr state.Transfer, size, chunkSize int64) error {
	for i, c := range tr.Chunks {
		off := int64(i) * chunkSize
		if off < 0 || off >= size {
			return ErrCorruptPartial
		}
		expected := min64(chunkSize, size-off)
		if c.Size != expected {
			return ErrCorruptPartial
		}
		buf := make([]byte, expected)
		if _, err := io.ReadFull(io.NewSectionReader(f, off, expected), buf); err != nil {
			return ErrCorruptPartial
		}
		sum := sha256.Sum256(buf)
		if hex.EncodeToString(sum[:]) != c.Hash {
			return ErrCorruptPartial
		}
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
