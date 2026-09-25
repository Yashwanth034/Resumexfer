package gvfs

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrMountNotFound  = errors.New("gvfs mount not found")
	ErrUnsafePath     = errors.New("unsafe path")
	ErrUnsupportedURI = errors.New("unsupported URI")
)

type Mount struct {
	Host string
	Root string
}

type Source struct {
	*os.File
	Size int64
}

func MountFromURI(runtimeDir, raw string) (Mount, bool) {
	if runtimeDir == "" || raw == "" {
		return Mount{}, false
	}
	u, err := url.Parse(raw)
	if err != nil || strings.ToLower(u.Scheme) != "mtp" || u.Host == "" {
		return Mount{}, false
	}
	return Mount{
		Host: u.Host,
		Root: filepath.Join(filepath.Clean(runtimeDir), "gvfs", "mtp:host="+url.QueryEscape(u.Host)),
	}, true
}

func MountFromPath(runtimeDir, path string) (Mount, bool) {
	if runtimeDir == "" || path == "" {
		return Mount{}, false
	}

	gvfsDir := filepath.Join(filepath.Clean(runtimeDir), "gvfs")
	cleanPath := filepath.Clean(path)

	rel, err := filepath.Rel(gvfsDir, cleanPath)
	if err != nil ||
		rel == "." ||
		rel == ".." ||
		strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return Mount{}, false
	}

	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "mtp:host=") {
		return Mount{}, false
	}

	host, err := url.QueryUnescape(strings.TrimPrefix(parts[0], "mtp:host="))
	if err != nil || host == "" {
		return Mount{}, false
	}

	return Mount{
		Host: host,
		Root: filepath.Join(gvfsDir, parts[0]),
	}, true
}

func Discover(runtimeDir string) ([]Mount, error) {
	gvfsDir := filepath.Join(runtimeDir, "gvfs")
	entries, err := os.ReadDir(gvfsDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	mounts := make([]Mount, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "mtp:host=") {
			continue
		}
		host, err := url.QueryUnescape(strings.TrimPrefix(entry.Name(), "mtp:host="))
		if err != nil {
			continue
		}
		mounts = append(mounts, Mount{
			Host: host,
			Root: filepath.Join(gvfsDir, entry.Name()),
		})
	}
	return mounts, nil
}

func ResolveURI(raw string, mounts []Mount) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}

	switch u.Scheme {
	case "file":
		return resolveFileURI(u)
	case "mtp":
		return resolveMTPURI(u, mounts)
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedURI, u.Scheme)
	}
}

func resolveFileURI(u *url.URL) (string, error) {
	if u.Host != "" && u.Host != "localhost" {
		return "", fmt.Errorf("%w: remote file host", ErrUnsupportedURI)
	}
	path, err := url.PathUnescape(u.EscapedPath())
	if err != nil {
		return "", err
	}
	if !filepath.IsAbs(path) {
		return "", ErrUnsafePath
	}
	return filepath.Clean(path), nil
}

func resolveMTPURI(u *url.URL, mounts []Mount) (string, error) {
	mount, ok := findMount(u.Host, mounts)
	if !ok {
		return "", ErrMountNotFound
	}

	escaped := strings.TrimPrefix(u.EscapedPath(), "/")
	parts := strings.Split(escaped, "/")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return "", err
		}
		if decoded == "" || decoded == "." {
			continue
		}
		if decoded == ".." || strings.Contains(decoded, "/") || strings.Contains(decoded, `\`) {
			return "", ErrUnsafePath
		}
		clean = append(clean, decoded)
	}

	resolved := filepath.Join(append([]string{mount.Root}, clean...)...)
	rel, err := filepath.Rel(mount.Root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", ErrUnsafePath
	}
	return resolved, nil
}

func findMount(host string, mounts []Mount) (Mount, bool) {
	for _, mount := range mounts {
		if mount.Host == host {
			return mount, true
		}
	}
	return Mount{}, false
}

func OpenSource(path string) (*Source, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, fmt.Errorf("source is not a regular file")
	}
	return &Source{File: file, Size: info.Size()}, nil
}
