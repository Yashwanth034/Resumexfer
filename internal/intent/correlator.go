package intent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"resumexfer/internal/gvfs"
	"resumexfer/internal/state"
)

var (
	ErrNoSelectionMatch   = errors.New("observed source does not match selection")
	ErrAmbiguousSelection = errors.New("observed source matches multiple selected roots")
)

func Correlate(in state.Intent, mounts []gvfs.Mount, observedSource, observedDestination string) (state.Manifest, error) {
	if in.ID == "" {
		return state.Manifest{}, fmt.Errorf("intent id required")
	}
	if len(in.URIs) == 0 {
		return state.Manifest{}, fmt.Errorf("intent selection required")
	}

	roots := make([]string, 0, len(in.URIs))
	for _, uri := range in.URIs {
		path, err := gvfs.ResolveURI(uri, mounts)
		if err != nil {
			return state.Manifest{}, err
		}
		roots = append(roots, filepath.Clean(path))
	}

	observedSource = filepath.Clean(observedSource)
	observedDestination = filepath.Clean(observedDestination)
	matches := make([]string, 0, 1)
	for _, root := range roots {
		suffix, ok, err := observedSuffix(root, observedSource)
		if err != nil {
			return state.Manifest{}, err
		}
		if ok {
			matches = append(matches, suffix)
		}
	}
	if len(matches) == 0 {
		return state.Manifest{}, ErrNoSelectionMatch
	}
	if len(matches) > 1 {
		return state.Manifest{}, ErrAmbiguousSelection
	}

	destinationRoot, err := inferDestinationRoot(observedDestination, matches[0])
	if err != nil {
		return state.Manifest{}, err
	}

	return buildManifestFromRoots(in, roots, destinationRoot)
}

func BuildManifest(in state.Intent, mounts []gvfs.Mount, destinationRoot string) (state.Manifest, error) {
	if in.ID == "" {
		return state.Manifest{}, fmt.Errorf("intent id required")
	}
	if len(in.URIs) == 0 {
		return state.Manifest{}, fmt.Errorf("intent selection required")
	}
	if !filepath.IsAbs(destinationRoot) {
		return state.Manifest{}, fmt.Errorf("destination root must be absolute")
	}

	roots := make([]string, 0, len(in.URIs))
	for _, uri := range in.URIs {
		path, err := gvfs.ResolveURI(uri, mounts)
		if err != nil {
			return state.Manifest{}, err
		}
		roots = append(roots, filepath.Clean(path))
	}

	return buildManifestFromRoots(in, roots, filepath.Clean(destinationRoot))
}

func buildManifestFromRoots(in state.Intent, roots []string, destinationRoot string) (state.Manifest, error) {
	manifest := state.Manifest{
		ID:              in.ID,
		Direction:       in.Direction,
		DestinationRoot: filepath.Clean(destinationRoot),
		UpdatedAt:       time.Now(),
	}

	seenDestinations := make(map[string]struct{})
	for _, root := range roots {
		entries, err := expandRoot(in.ID, root, destinationRoot)
		if err != nil {
			return state.Manifest{}, err
		}
		for _, entry := range entries {
			if _, exists := seenDestinations[entry.Destination]; exists {
				return state.Manifest{}, fmt.Errorf("destination collision: %s", entry.Destination)
			}
			seenDestinations[entry.Destination] = struct{}{}
			manifest.Entries = append(manifest.Entries, entry)
		}
	}

	return manifest, nil
}

func observedSuffix(root, observed string) (string, bool, error) {
	info, err := os.Stat(root)
	if err != nil {
		return "", false, err
	}
	if !info.IsDir() {
		if filepath.Clean(root) != filepath.Clean(observed) {
			return "", false, nil
		}
		return filepath.Base(root), true, nil
	}

	rel, err := filepath.Rel(root, observed)
	if err != nil {
		return "", false, err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false, nil
	}
	if rel == "." {
		return filepath.Base(root), true, nil
	}
	return filepath.Join(filepath.Base(root), rel), true, nil
}

func inferDestinationRoot(observedDestination, suffix string) (string, error) {
	if !filepath.IsAbs(observedDestination) {
		return "", fmt.Errorf("observed destination must be absolute")
	}
	parts := pathParts(suffix)
	if len(parts) == 0 {
		return "", fmt.Errorf("empty observed suffix")
	}
	root := observedDestination
	for range parts {
		parent := filepath.Dir(root)
		if parent == root {
			return "", fmt.Errorf("destination does not contain selection suffix")
		}
		root = parent
	}
	if filepath.Clean(filepath.Join(root, suffix)) != observedDestination {
		return "", fmt.Errorf("observed destination does not match selection layout")
	}
	return root, nil
}

func pathParts(path string) []string {
	clean := filepath.Clean(path)
	if clean == "." || clean == string(filepath.Separator) {
		return nil
	}
	return strings.FieldsFunc(clean, func(r rune) bool { return r == filepath.Separator })
}

func expandRoot(intentID, root, destinationRoot string) ([]state.ManifestEntry, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("unsupported source type: %s", root)
		}
		destination := filepath.Join(destinationRoot, filepath.Base(root))
		return []state.ManifestEntry{manifestEntry(intentID, root, destination, info.Size())}, nil
	}

	entries := make([]state.ManifestEntry, 0)
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported source type: %s", path)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(destinationRoot, filepath.Base(root), rel)
		entries = append(entries, manifestEntry(intentID, path, destination, info.Size()))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func manifestEntry(intentID, source, destination string, size int64) state.ManifestEntry {
	sum := sha256.Sum256([]byte(intentID + "\x00" + source + "\x00" + destination))
	return state.ManifestEntry{
		ID:          hex.EncodeToString(sum[:]),
		Source:      source,
		Destination: destination,
		Size:        size,
	}
}
