package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"resumexfer/internal/fingerprint"
	"resumexfer/internal/state"
)

type recordingReader struct {
	data               []byte
	failAt             int64
	preflightAllowance int
	reads              []readSpan
}

func (r *recordingReader) ReadAt(p []byte, off int64) (int, error) {
	r.reads = append(r.reads, readSpan{off: off, n: len(p)})
	if len(r.reads) > r.preflightAllowance && r.failAt >= 0 && off >= r.failAt {
		return 0, errors.New("disconnected")
	}
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestInterruptedTransferReopensAndReadsOnlyMissingChunks(t *testing.T) {
	const chunk = int64(64 * 1024)
	data := bytes.Repeat([]byte("abcdefgh"), int((5*chunk)/8))
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "movie.bin")

	first := &recordingReader{data: data, failAt: 3 * chunk, preflightAllowance: 6}
	e := Engine{Store: st, ChunkSize: chunk, CheckpointBytes: 2 * chunk}
	if err := e.Resume("x", first, int64(len(data)), dest); err == nil {
		t.Fatal("expected interruption")
	}

	reopened, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	second := &recordingReader{data: data, failAt: -1}
	e.Store = reopened
	if err := e.Resume("x", second, int64(len(data)), dest); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("final file mismatch")
	}
	var durableVerificationBytes int64
	for _, span := range second.reads {
		if span.off < 2*chunk {
			durableVerificationBytes += int64(span.n)
		}
	}
	if durableVerificationBytes > 64*1024 {
		t.Fatalf("second run read %d bytes from durable prefix, want at most 64 KiB identity verification", durableVerificationBytes)
	}
}

func TestFinalizeRejectsCorruptedCompletedChunk(t *testing.T) {
	const chunk = int64(32 * 1024)
	data := bytes.Repeat([]byte("z"), int(2*chunk))
	dir := t.TempDir()
	st, _ := state.Open(filepath.Join(dir, "state.json"))
	dest := filepath.Join(dir, "file.bin")
	part := dest + PartialSuffix
	e := Engine{Store: st, ChunkSize: chunk, CheckpointBytes: chunk}

	interrupted := &recordingReader{data: data, failAt: chunk, preflightAllowance: 6}
	if err := e.Resume("c", interrupted, int64(len(data)), dest); err == nil {
		t.Fatal("expected interruption")
	}

	f, err := os.OpenFile(part, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("CORRUPT"), 0); err != nil {
		t.Fatal(err)
	}
	f.Close()

	healthy := &recordingReader{data: data, failAt: -1}
	err = e.Resume("c", healthy, int64(len(data)), dest)
	if !errors.Is(err, ErrCorruptPartial) {
		t.Fatalf("got %v, want ErrCorruptPartial", err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupted partial promoted")
	}
}

func TestChunkCountHandles200GiB(t *testing.T) {
	size := int64(200) << 30
	got := ChunkCount(size, 4<<20)
	if got != 51200 {
		t.Fatalf("got %d", got)
	}
}

type measuredReader struct {
	data  []byte
	reads []readSpan
}

type readSpan struct {
	off int64
	n   int
}

func (r *measuredReader) ReadAt(p []byte, off int64) (int, error) {
	r.reads = append(r.reads, readSpan{off: off, n: len(p)})
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func TestResumeFromPreservedPrefixReusesVerifiedChunks(t *testing.T) {
	const chunk = int64(64 * 1024)
	data := bytes.Repeat([]byte("abcdefgh"), int((6*chunk)/8))
	dir := t.TempDir()
	store, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "movie.bin")
	preserved := filepath.Join(dir, "wifi.partial")
	prefixBytes := 3 * chunk
	if err := os.WriteFile(preserved, data[:prefixBytes], 0o600); err != nil {
		t.Fatal(err)
	}

	source := &measuredReader{data: data}
	e := Engine{Store: store, ChunkSize: chunk, CheckpointBytes: 2 * chunk}
	if err := e.ResumeFromPreserved("cross-transport", source, int64(len(data)), destination, preserved); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("final file mismatch")
	}

	var verificationBytes int64
	for _, span := range source.reads {
		if span.off < prefixBytes {
			verificationBytes += int64(span.n)
			continue
		}
		if span.off < 3*chunk {
			t.Fatalf("missing-chunk fetch restarted inside verified prefix at %d", span.off)
		}
	}
	if verificationBytes > 1<<20 {
		t.Fatalf("prefix verification read %d bytes, want at most 1 MiB", verificationBytes)
	}
}

func TestResumeFromPreservedPrefixRejectsWrongContentBeforeAdoption(t *testing.T) {
	const chunk = int64(64 * 1024)
	data := bytes.Repeat([]byte("abcdefgh"), int((5*chunk)/8))
	wrong := append([]byte(nil), data[:3*chunk]...)
	for i := chunk; i < 2*chunk; i++ {
		wrong[i] ^= 0xff
	}

	dir := t.TempDir()
	store, _ := state.Open(filepath.Join(dir, "state.json"))
	destination := filepath.Join(dir, "same-name.bin")
	preserved := filepath.Join(dir, "same-name.partial")
	if err := os.WriteFile(preserved, wrong, 0o600); err != nil {
		t.Fatal(err)
	}

	e := Engine{Store: store, ChunkSize: chunk, CheckpointBytes: chunk}
	err := e.ResumeFromPreserved("wrong-content", bytes.NewReader(data), int64(len(data)), destination, preserved)
	if !errors.Is(err, ErrPrefixMismatch) {
		t.Fatalf("got %v, want ErrPrefixMismatch", err)
	}
	if _, err := os.Stat(destination + PartialSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatched prefix was adopted: %v", err)
	}
	if _, ok := store.Transfer("wrong-content"); ok {
		t.Fatal("mismatched prefix created durable transfer state")
	}
}

func TestResumeRejectsSameSizeSourceChangedAfterInterruption(t *testing.T) {
	const chunk = int64(64 * 1024)
	original := bytes.Repeat([]byte("abcdefgh"), int((5*chunk)/8))
	changed := append([]byte(nil), original...)
	for i := len(changed) - int(chunk/2); i < len(changed); i++ {
		changed[i] ^= 0xff
	}

	dir := t.TempDir()
	store, _ := state.Open(filepath.Join(dir, "state.json"))
	destination := filepath.Join(dir, "movie.bin")
	first := &recordingReader{data: original, failAt: 3 * chunk, preflightAllowance: 6}
	e := Engine{Store: store, ChunkSize: chunk, CheckpointBytes: 2 * chunk}
	if err := e.Resume("identity", first, int64(len(original)), destination); err == nil {
		t.Fatal("expected first transfer interruption")
	}

	reopened, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	e.Store = reopened
	second := &recordingReader{data: changed, failAt: -1}
	if err := e.Resume("identity", second, int64(len(changed)), destination); err == nil {
		t.Fatal("same-size changed source was accepted")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("changed source was promoted to final destination")
	}
}

func TestResumeReportsDurableProgressThroughCompletion(t *testing.T) {
	const chunk = int64(32 * 1024)
	data := bytes.Repeat([]byte("progress-data"), int((5*chunk)/13))
	dir := t.TempDir()
	store, err := state.Open(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var progress []int64
	e := Engine{
		Store:           store,
		ChunkSize:       chunk,
		CheckpointBytes: 2 * chunk,
		Progress: func(done, total int64) error {
			if total != int64(len(data)) {
				t.Fatalf("total = %d, want %d", total, len(data))
			}
			progress = append(progress, done)
			return nil
		},
	}
	destination := filepath.Join(dir, "movie.bin")
	if err := e.Resume("progress", bytes.NewReader(data), int64(len(data)), destination); err != nil {
		t.Fatal(err)
	}
	if len(progress) < 2 {
		t.Fatalf("progress callbacks = %v", progress)
	}
	for i := 1; i < len(progress); i++ {
		if progress[i] < progress[i-1] {
			t.Fatalf("progress went backwards: %v", progress)
		}
	}
	if got := progress[len(progress)-1]; got != int64(len(data)) {
		t.Fatalf("final progress = %d, want %d", got, len(data))
	}
}

func TestProgressErrorFlushesPendingCheckpoint(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	source := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	destination := filepath.Join(root, "out.bin")
	stop := errors.New("pause transfer")
	e := Engine{
		Store:           store,
		ChunkSize:       4,
		CheckpointBytes: 32,
		ProgressBytes:   8,
		Progress: func(done, total int64) error {
			if done >= 16 && done < total {
				return stop
			}
			return nil
		},
	}
	if err := e.Resume("pause-checkpoint", bytes.NewReader(source), int64(len(source)), destination); !errors.Is(err, stop) {
		t.Fatalf("resume error=%v want=%v", err, stop)
	}
	tr, ok := store.Transfer("pause-checkpoint")
	if !ok {
		t.Fatal("transfer state disappeared after interrupted progress callback")
	}
	var durable int64
	for _, chunk := range tr.Chunks {
		durable += chunk.Size
	}
	if durable != 16 {
		t.Fatalf("durable bytes=%d want=16", durable)
	}
	info, err := os.Stat(destination + PartialSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 16 {
		t.Fatalf("partial size=%d want=16", info.Size())
	}
}

func TestProgressCanAdvanceMoreFrequentlyThanDurabilityCheckpoints(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	source := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	destination := filepath.Join(root, "out.bin")
	var samples []int64
	e := Engine{
		Store:           store,
		ChunkSize:       4,
		CheckpointBytes: 32,
		ProgressBytes:   8,
		Progress: func(done, total int64) error {
			samples = append(samples, done)
			return nil
		},
	}
	if err := e.Resume("progress-cadence", bytes.NewReader(source), int64(len(source)), destination); err != nil {
		t.Fatal(err)
	}
	seenEarly := false
	for _, done := range samples {
		if done >= 8 && done < 32 {
			seenEarly = true
			break
		}
	}
	if !seenEarly {
		t.Fatalf("progress samples %v never advanced before the 32-byte checkpoint", samples)
	}
}

func TestRemoteUploadRestartsFromVerifiedDurableChunks(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "state.json")
	store, err := state.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("abcdefghijk")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "movie.bin")
	partial := filepath.Join(root, ".movie.remote-part")
	e := Engine{Store: store, ChunkSize: 4}

	offset, err := e.PrepareRemoteUpload("remote-1", int64(len(data)), destination, partial, fp)
	if err != nil || offset != 0 {
		t.Fatalf("prepare offset=%d err=%v", offset, err)
	}
	newOffset, complete, err := e.AppendRemoteUpload("remote-1", partial, 0, data[:6])
	if err != nil || newOffset != 6 || complete {
		t.Fatalf("append offset=%d complete=%v err=%v", newOffset, complete, err)
	}
	transfer, ok := store.Transfer("remote-1")
	if !ok || len(transfer.Chunks) != 1 || transfer.Chunks[0].Size != 4 {
		t.Fatalf("checkpointed transfer=%#v ok=%v", transfer, ok)
	}

	reopened, err := state.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	e = Engine{Store: reopened, ChunkSize: 4}
	offset, err = e.PrepareRemoteUpload("remote-1", int64(len(data)), destination, partial, fp)
	if err != nil || offset != 4 {
		t.Fatalf("restart prepare offset=%d err=%v", offset, err)
	}
	if info, err := os.Stat(partial); err != nil || info.Size() != 4 {
		t.Fatalf("partial after restart info=%#v err=%v", info, err)
	}
	if err := e.CompleteRemoteUpload("remote-1"); err == nil {
		t.Fatal("incomplete remote transfer was allowed to complete")
	}

	newOffset, complete, err = e.AppendRemoteUpload("remote-1", partial, offset, data[offset:])
	if err != nil || newOffset != int64(len(data)) || !complete {
		t.Fatalf("final append offset=%d complete=%v err=%v", newOffset, complete, err)
	}
	content, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(content, data) {
		t.Fatalf("partial content=%q err=%v", content, err)
	}
	transfer, ok = reopened.Transfer("remote-1")
	if !ok || len(transfer.Chunks) != ChunkCount(int64(len(data)), 4) {
		t.Fatalf("final transfer=%#v ok=%v", transfer, ok)
	}
	if err := e.CompleteRemoteUpload("remote-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.Transfer("remote-1"); ok {
		t.Fatal("completed remote transfer state was not removed")
	}
}

func TestStreamRemoteUploadCompletesExactBytes(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("stream-upload-"), 1024)
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "stream.bin")
	partial := filepath.Join(root, ".stream.remote-part")
	e := Engine{Store: store, ChunkSize: 1024}
	offset, err := e.PrepareRemoteUpload("stream-1", int64(len(data)), destination, partial, fp)
	if err != nil || offset != 0 {
		t.Fatalf("prepare offset=%d err=%v", offset, err)
	}
	var progress []int64
	newOffset, complete, err := e.StreamRemoteUpload(
		"stream-1",
		partial,
		0,
		bytes.NewReader(data),
		8<<10,
		func(done int64) error {
			progress = append(progress, done)
			return nil
		},
	)
	if err != nil || !complete || newOffset != int64(len(data)) {
		t.Fatalf("stream offset=%d complete=%v err=%v", newOffset, complete, err)
	}
	got, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("streamed bytes mismatch len=%d err=%v", len(got), err)
	}
	tr, ok := store.Transfer("stream-1")
	if !ok || len(tr.Chunks) != ChunkCount(int64(len(data)), 1024) {
		t.Fatalf("stream transfer=%#v ok=%v", tr, ok)
	}
	if len(progress) == 0 || progress[len(progress)-1] != int64(len(data)) {
		t.Fatalf("progress=%v", progress)
	}
	if err := e.CompleteRemoteUpload("stream-1"); err != nil {
		t.Fatal(err)
	}
}

type failingUploadReader struct {
	data []byte
	err  error
	done bool
}

func (r *failingUploadReader) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	if !r.done {
		r.done = true
		return 0, r.err
	}
	return 0, io.EOF
}

func TestStreamRemoteUploadCheckpointsCompleteChunksOnInterruption(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("abcdefghijklmnop")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(root, ".stream-part")
	e := Engine{Store: store, ChunkSize: 4}
	if _, err := e.PrepareRemoteUpload("stream-interrupt", int64(len(data)), filepath.Join(root, "out.bin"), partial, fp); err != nil {
		t.Fatal(err)
	}
	stopErr := errors.New("network stopped")
	reader := &failingUploadReader{data: append([]byte(nil), data[:10]...), err: stopErr}
	offset, complete, err := e.StreamRemoteUpload("stream-interrupt", partial, 0, reader, 64, nil)
	if !errors.Is(err, stopErr) || complete || offset != 8 {
		t.Fatalf("interrupted offset=%d complete=%v err=%v", offset, complete, err)
	}
	info, err := os.Stat(partial)
	if err != nil || info.Size() != 8 {
		t.Fatalf("partial info=%#v err=%v", info, err)
	}
	tr, ok := store.Transfer("stream-interrupt")
	if !ok || len(tr.Chunks) != 2 {
		t.Fatalf("durable transfer=%#v ok=%v", tr, ok)
	}
	reopened, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	e = Engine{Store: reopened, ChunkSize: 4}
	offset, err = e.PrepareRemoteUpload("stream-interrupt", int64(len(data)), filepath.Join(root, "out.bin"), partial, fp)
	if err != nil || offset != 8 {
		t.Fatalf("restart offset=%d err=%v", offset, err)
	}
}

func TestRemoteUploadRejectsChangedSourceIdentityBeforeAdoptingPartial(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	original := []byte("abcdefgh")
	changed := []byte("ABCDEFGH")
	originalFP, err := fingerprint.ReaderAt(bytes.NewReader(original), int64(len(original)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	changedFP, err := fingerprint.ReaderAt(bytes.NewReader(changed), int64(len(changed)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(root, ".remote-part")
	e := Engine{Store: store, ChunkSize: 4}
	if _, err := e.PrepareRemoteUpload("remote-identity", int64(len(original)), filepath.Join(root, "movie.bin"), partial, originalFP); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.AppendRemoteUpload("remote-identity", partial, 0, original[:4]); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(partial)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.PrepareRemoteUpload("remote-identity", int64(len(changed)), filepath.Join(root, "movie.bin"), partial, changedFP); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("changed identity err=%v", err)
	}
	after, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("partial changed after rejected identity: before=%q after=%q err=%v", before, after, err)
	}
}

func TestRemoteUploadRejectsCorruptCheckpointedPartial(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("abcdefgh")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(root, ".remote-part")
	e := Engine{Store: store, ChunkSize: 4}
	if _, err := e.PrepareRemoteUpload("remote-corrupt", int64(len(data)), filepath.Join(root, "movie.bin"), partial, fp); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.AppendRemoteUpload("remote-corrupt", partial, 0, data[:4]); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(partial, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("XXXX"), 0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.PrepareRemoteUpload("remote-corrupt", int64(len(data)), filepath.Join(root, "movie.bin"), partial, fp); !errors.Is(err, ErrCorruptPartial) {
		t.Fatalf("corrupt partial err=%v", err)
	}
}
