package portal

import (
	"path/filepath"
	"strings"
	"testing"
)

func FuzzSplitSessionPath(f *testing.F) {
	for _, seed := range []string{
		"/",
		"/token/",
		"/token/file/0",
		"/s/token/",
		"/s/token/api/status",
		"////",
		"/../token",
		"/token/%00",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, path string) {
		token, rest, ok := splitSessionPath(path)
		if !ok {
			return
		}
		if token == "" || strings.Contains(token, "/") {
			t.Fatalf("accepted invalid token %q from %q", token, path)
		}
		if strings.HasPrefix(rest, "/") {
			t.Fatalf("accepted absolute rest %q from %q", rest, path)
		}
	})
}

func FuzzCleanRelativePath(f *testing.F) {
	for _, seed := range []string{
		"file.txt",
		"folder/file.txt",
		"./folder/file.txt",
		"folder//file.txt",
		"../escape",
		"/absolute",
		"..\\escape",
		"folder\\file.txt",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		got, err := cleanRelativePath(input)
		if err != nil {
			return
		}
		if got == "" || filepath.IsAbs(got) {
			t.Fatalf("accepted unsafe relative path input=%q got=%q", input, got)
		}
		for _, part := range strings.Split(got, "/") {
			if part == "" || part == "." || part == ".." || strings.ContainsRune(part, 0) {
				t.Fatalf("accepted unsafe component input=%q got=%q", input, got)
			}
		}
	})
}

func FuzzPortalRemoteAllowed(f *testing.F) {
	for _, seed := range []string{
		"127.0.0.1:1234",
		"192.168.1.2:8787",
		"169.254.10.2:8787",
		"8.8.8.8:53",
		"bad",
		"[::1]:8787",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, remote string) {
		_ = portalRemoteAllowed(remote)
	})
}
