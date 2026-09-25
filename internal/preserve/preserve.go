package preserve

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"resumexfer/internal/engine"
	"resumexfer/internal/state"
)

type Manager struct {
	Store *state.Store
	Now   func() time.Time
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Manager) IsTrackedDestination(path string) bool {
	if m.Store == nil || path == "" {
		return false
	}

	clean := filepath.Clean(path)
	target := trackedTarget(clean)

	for _, manifest := range m.Store.Manifests() {
		if manifest.Direction != "phone-to-laptop" || manifest.Cancelled {
			continue
		}
		for _, entry := range manifest.Pending() {
			if filepath.Clean(entry.Destination) == target {
				return true
			}
		}
	}

	return false
}

func trackedTarget(path string) string {
	lower := strings.ToLower(path)
	for _, suffix := range []string{".partial", ".crdownload", ".part", ".tmp"} {
		if strings.HasSuffix(lower, suffix) {
			return path[:len(path)-len(suffix)]
		}
	}
	return path
}

func (m *Manager) Preserve(original string) (string, error) {
	return m.preserveWhen(original, nil)
}

func (m *Manager) PreserveTracked(original string) (string, error) {
	return m.preserveWhen(original, func() bool {
		return m.IsTrackedDestination(original)
	})
}

func (m *Manager) preserveWhen(original string, stillNeeded func() bool) (string, error) {
	if m.Store == nil {
		return "", fmt.Errorf("store required")
	}
	if stillNeeded != nil && !stillNeeded() {
		return "", nil
	}
	info, err := os.Stat(original)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("not a regular file")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("stat metadata unavailable")
	}
	cacheDir := filepath.Join(filepath.Dir(original), ".resumexfer-cache")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return "", err
	}
	seed := fmt.Sprintf("%d:%d:%s", st.Dev, st.Ino, filepath.Base(original))
	sum := sha256.Sum256([]byte(seed))
	id := hex.EncodeToString(sum[:16])
	link := filepath.Join(cacheDir, id+"-"+filepath.Base(original))
	if err := os.Link(original, link); err != nil && !os.IsExist(err) {
		return "", err
	}
	c := state.Candidate{ID: id, Source: original, PartialPath: link, Size: info.Size(), UpdatedAt: m.now()}
	if err := m.Store.PutCandidate(c); err != nil {
		return "", err
	}
	if stillNeeded != nil && !stillNeeded() {
		if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if err := m.Store.DeleteCandidate(id); err != nil {
			return "", err
		}
		return "", nil
	}
	return link, nil
}

func (m *Manager) Cleanup(retention time.Duration) error {
	cutoff := m.now().Add(-retention)

	// Engine partials are always Resumexfer-owned sidecars. Remove stale ones
	// before Store.Prune drops the transfer metadata, otherwise an abandoned
	// .resumexfer-part file could become an untracked disk-space leak.
	for _, tr := range m.Store.Transfers() {
		stamp := tr.UpdatedAt
		if stamp.IsZero() {
			stamp = tr.CreatedAt
		}
		if stamp.IsZero() || !stamp.Before(cutoff) {
			continue
		}
		if tr.Destination != "" {
			partial := filepath.Clean(tr.Destination + engine.PartialSuffix)
			if partial != filepath.Clean(tr.Destination) {
				if err := os.Remove(partial); err != nil && !os.IsNotExist(err) {
					return err
				}
			}
		}
		if err := m.Store.DeleteTransfer(tr.ID); err != nil {
			return err
		}
	}

	for _, c := range m.Store.Candidates() {
		if c.UpdatedAt.IsZero() || !c.UpdatedAt.Before(cutoff) {
			continue
		}
		if isKnownTemp(c.Source) {
			_ = os.Remove(c.Source)
		}
		_ = os.Remove(c.PartialPath)
		if err := m.Store.DeleteCandidate(c.ID); err != nil {
			return err
		}
	}
	return nil
}

func isKnownTemp(path string) bool {
	lower := strings.ToLower(path)
	for _, suffix := range []string{".part", ".partial", ".crdownload", ".tmp"} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}
	return false
}

func (m *Manager) SetCandidateTimeForTest(partialPath string, t time.Time) error {
	for _, c := range m.Store.Candidates() {
		if c.PartialPath == partialPath {
			c.UpdatedAt = t
			return m.Store.PutCandidate(c)
		}
	}
	return fmt.Errorf("candidate not found")
}
