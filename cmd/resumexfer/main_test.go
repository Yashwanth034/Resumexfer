package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunHelpAndVersion(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), nil, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "resumexfer share") || !strings.Contains(out.String(), "resumexfer receive") {
		t.Fatalf("help output=%q", out.String())
	}

	out.Reset()
	old := version
	version = "test-version"
	defer func() { version = old }()
	if err := run(context.Background(), []string{"version"}, &out); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(out.String()); got != "Resumexfer test-version" {
		t.Fatalf("version output=%q", got)
	}
}

func TestRunRejectsInvalidCommands(t *testing.T) {
	for _, args := range [][]string{
		{"share"},
		{"receive"},
		{"receive", "a", "b"},
		{"unknown"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("args=%v unexpectedly succeeded", args)
		}
	}
}

func TestStandaloneShareStartsAndStopsCleanly(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "hello.txt")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, []string{"share", path}, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "Open this link on the other device") || !strings.Contains(text, "Stopped.") {
		t.Fatalf("share output=%q", text)
	}
}

func TestStandaloneReceiveStartsAndStopsCleanly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()

	var out bytes.Buffer
	if err := run(ctx, []string{"receive", t.TempDir()}, &out); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	if !strings.Contains(text, "Receive") || !strings.Contains(text, "Stopped.") {
		t.Fatalf("receive output=%q", text)
	}
}

func TestIsLoopbackURL(t *testing.T) {
	cases := map[string]bool{
		"http://127.0.0.1:8787/token/":  true,
		"http://localhost:8787/token/":  true,
		"http://192.0.2.10:8787/token/": false,
	}
	for raw, want := range cases {
		if got := isLoopbackURL(raw); got != want {
			t.Fatalf("isLoopbackURL(%q)=%v want=%v", raw, got, want)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	cases := map[int64]string{
		0:           "0 B",
		1024:        "1.0 KiB",
		1024 * 1024: "1.0 MiB",
	}
	for value, want := range cases {
		if got := formatBytes(value); got != want {
			t.Fatalf("formatBytes(%d)=%q want=%q", value, got, want)
		}
	}
}
