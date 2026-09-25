package portal

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"resumexfer/internal/engine"
	"resumexfer/internal/fingerprint"
	"resumexfer/internal/recovery"
	"resumexfer/internal/state"
)

type readerFromProbe struct {
	header          http.Header
	body            bytes.Buffer
	status          int
	readerFromCalls int
	fileBackedCalls int
}

func (w *readerFromProbe) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *readerFromProbe) WriteHeader(status int) {
	w.status = status
}

func (w *readerFromProbe) Write(p []byte) (int, error) {
	return w.body.Write(p)
}

func (w *readerFromProbe) ReadFrom(src io.Reader) (int64, error) {
	w.readerFromCalls++
	if limited, ok := src.(*io.LimitedReader); ok {
		if _, ok := limited.R.(*os.File); ok {
			w.fileBackedCalls++
		}
	}
	return io.Copy(&w.body, src)
}

func testManager(t *testing.T) *Manager {
	t.Helper()
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func TestShareServesRangeAndRejectsChangedSource(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "hello world.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 1 || info.TotalFiles != 1 || info.TotalBytes != 11 {
		t.Fatalf("unexpected info: %#v", info)
	}
	base := info.URLs[0]

	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("hello world.txt")) {
		t.Fatalf("page status=%d body=%q", resp.StatusCode, body)
	}

	req, _ := http.NewRequest(http.MethodGet, base+"file/0", nil)
	req.Header.Set("Range", "bytes=6-10")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent || string(body) != "world" {
		t.Fatalf("range status=%d body=%q", resp.StatusCode, body)
	}
	partialProgress, ok := manager.Snapshot(info.ID)
	if !ok || partialProgress.BytesDone != 5 || partialProgress.DoneFiles != 0 || partialProgress.Complete {
		t.Fatalf("partial progress=%#v ok=%v", partialProgress, ok)
	}

	resp, err = http.Get(base + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	completeProgress, ok := manager.Snapshot(info.ID)
	if !ok || completeProgress.BytesDone != 11 || completeProgress.DoneFiles != 1 || !completeProgress.Complete {
		t.Fatalf("complete progress=%#v ok=%v", completeProgress, ok)
	}

	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(path, []byte("changed source"), 0o600); err != nil {
		t.Fatal(err)
	}
	resp, err = http.Get(base + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("changed source status=%d", resp.StatusCode)
	}
}

func TestTrackingResponseWriterKeepsFileBackedReaderFrom(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sendfile.bin")
	const size = int64(9 << 20)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		file.Close()
		t.Fatal(err)
	}
	defer file.Close()

	probe := &readerFromProbe{}
	var ranges [][2]int64
	tracked := &trackingResponseWriter{
		ResponseWriter: probe,
		record: func(start, end int64) {
			ranges = append(ranges, [2]int64{start, end})
		},
	}
	limited := &io.LimitedReader{R: file, N: size}
	n, err := tracked.ReadFrom(limited)
	if err != nil {
		t.Fatal(err)
	}
	if n != size || limited.N != 0 {
		t.Fatalf("copied=%d remaining=%d want=%d/0", n, limited.N, size)
	}
	if probe.readerFromCalls != 2 || probe.fileBackedCalls != 2 {
		t.Fatalf("readerFrom calls=%d file-backed=%d want=2/2", probe.readerFromCalls, probe.fileBackedCalls)
	}
	if len(ranges) != 2 || ranges[0] != [2]int64{0, 8 << 20} || ranges[1] != [2]int64{8 << 20, 9 << 20} {
		t.Fatalf("progress ranges=%v", ranges)
	}
}

func TestTrackingReadSeekerBatchesProgressCallbacks(t *testing.T) {
	data := bytes.Repeat([]byte{0x7a}, 5<<20)
	var calls int
	var covered int64
	tracked := &trackingReadSeeker{
		ReadSeeker: bytes.NewReader(data),
		record: func(start, end int64) {
			calls++
			covered += end - start
		},
	}
	buf := make([]byte, 32<<10)
	for {
		_, err := tracked.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if covered != int64(len(data)) {
		t.Fatalf("covered bytes=%d want=%d", covered, len(data))
	}
	if calls > 6 {
		t.Fatalf("progress callbacks=%d want <=6 for 5 MiB", calls)
	}
}

func TestSingleFileSharePageUsesDirectDownloadInsteadOfZip(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "movie.mp4")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x42}, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("share page status=%d", resp.StatusCode)
	}
	if !bytes.Contains(body, []byte("id=\"download\"")) ||
		!bytes.Contains(body, []byte(">Download</button>")) ||
		!bytes.Contains(body, []byte("id=\"fileName\">movie.mp4</strong>")) ||
		!bytes.Contains(body, []byte("api/fast/offer")) {
		t.Fatalf("single-file page does not offer the fast single Download action: %q", body)
	}
	if bytes.Contains(body, []byte("Download all as ZIP")) {
		t.Fatalf("single-file page still promotes ZIP: %q", body)
	}

	resp, err = http.Get(info.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("direct file status=%d", resp.StatusCode)
	}
	if disposition := resp.Header.Get("Content-Disposition"); !strings.Contains(disposition, "movie.mp4") {
		t.Fatalf("content disposition=%q", disposition)
	}
}

func TestSendPageUsesPipelinedFastPathAndLargeFileDirectFallback(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "movie.mp4")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x42}, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, marker := range []string{
		"storageChain=Promise.resolve()",
		"queueBlock(parts,index,start,end)",
		"LARGE_DIRECT=1536*1024*1024",
		"window.location.assign(fallback.href)",
		"BLOCK=16*1024*1024",
	} {
		if !bytes.Contains(body, []byte(marker)) {
			t.Fatalf("send page missing fast-path contract %q", marker)
		}
	}
}

func TestReceivePageUsesContinuousStreamingUpload(t *testing.T) {
	manager := testManager(t)
	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, marker := range []string{
		"new XMLHttpRequest()",
		"api/stream/",
		"xhr.upload.onprogress",
		"formatUploadSpeed",
		"api/finish",
		"verify_chunks",
		"checkpointHashes",
	} {
		if !bytes.Contains(body, []byte(marker)) {
			t.Fatalf("receive page missing streaming-upload contract %q", marker)
		}
	}
}

func TestDefaultPortalAddressAndTokenStayCompact(t *testing.T) {
	manager := newManager(Config{})
	if manager.cfg.ListenAddress != defaultPortalListenAddress {
		t.Fatalf("default listen address=%q want=%q", manager.cfg.ListenAddress, defaultPortalListenAddress)
	}
	for i := 0; i < 32; i++ {
		token := randomToken()
		if len(token) != 12 {
			t.Fatalf("token length=%d want=8 token=%q", len(token), token)
		}
		if strings.ContainsAny(token, "/+ =") {
			t.Fatalf("token is not URL-safe: %q", token)
		}
	}
}

func TestCapabilityTokensAreUniqueAndURLSafe(t *testing.T) {
	seen := make(map[string]struct{}, 2048)
	for i := 0; i < 2048; i++ {
		token := randomToken()
		if len(token) != 12 {
			t.Fatalf("token length=%d want 12", len(token))
		}
		if strings.ContainsAny(token, "/+=") {
			t.Fatalf("token is not URL-safe: %q", token)
		}
		if _, exists := seen[token]; exists {
			t.Fatalf("duplicate capability token generated: %q", token)
		}
		seen[token] = struct{}{}
	}
}

func TestPortalRemotePolicyAllowsOnlyLocalNetworkClients(t *testing.T) {
	for _, addr := range []string{
		"127.0.0.1:1234",
		"192.168.1.20:1234",
		"10.42.0.9:1234",
		"172.20.4.5:1234",
		"169.254.25.8:1234",
	} {
		if !portalRemoteAllowed(addr) {
			t.Fatalf("expected local remote address to be allowed: %s", addr)
		}
	}
	for _, addr := range []string{
		"8.8.8.8:53",
		"1.1.1.1:443",
		"203.0.113.10:9999",
		"not-an-address",
	} {
		if portalRemoteAllowed(addr) {
			t.Fatalf("expected non-local remote address to be rejected: %s", addr)
		}
	}
}

func TestPortalSecurityHeadersAndRejectsRebindingHost(t *testing.T) {
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	defer manager.Close()

	path := filepath.Join(t.TempDir(), "tiny.bin")
	if err := os.WriteFile(path, []byte("secure"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 1 {
		t.Fatalf("urls=%v", info.URLs)
	}

	resp, err := http.Get(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("portal status=%d", resp.StatusCode)
	}
	for header, want := range map[string]string{
		"Cache-Control":                     "no-store, max-age=0",
		"Referrer-Policy":                   "no-referrer",
		"X-Content-Type-Options":            "nosniff",
		"X-Frame-Options":                   "DENY",
		"X-Permitted-Cross-Domain-Policies": "none",
	} {
		if got := resp.Header.Get(header); got != want {
			t.Fatalf("%s=%q want=%q", header, got, want)
		}
	}
	if got := resp.Header.Get("Content-Security-Policy"); !strings.Contains(got, "frame-ancestors 'none'") || !strings.Contains(got, "object-src 'none'") {
		t.Fatalf("Content-Security-Policy=%q", got)
	}
	if got := resp.Header.Get("Permissions-Policy"); !strings.Contains(got, "camera=()") || !strings.Contains(got, "microphone=()") {
		t.Fatalf("Permissions-Policy=%q", got)
	}

	u, err := url.Parse(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, info.URLs[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = net.JoinHostPort("attacker.example", u.Port())
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("rebinding host status=%d want=%d", resp.StatusCode, http.StatusMisdirectedRequest)
	}
}

func TestCompactPortalURLAndLegacyPathParsing(t *testing.T) {
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	defer manager.Close()
	path := filepath.Join(t.TempDir(), "tiny.bin")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 1 {
		t.Fatalf("urls=%v", info.URLs)
	}
	u, err := url.Parse(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 1 || len(parts[0]) != 12 {
		t.Fatalf("compact path=%q parts=%v", u.Path, parts)
	}
	if strings.Contains(u.Path, "/s/") {
		t.Fatalf("new URL still contains legacy /s/ prefix: %q", u.Path)
	}

	for _, tc := range []struct {
		path  string
		token string
		rest  string
	}{
		{"/AbCdEf12/", "AbCdEf12", ""},
		{"/AbCdEf12/file/0", "AbCdEf12", "file/0"},
		{"/s/AbCdEf12/", "AbCdEf12", ""},
		{"/s/AbCdEf12/file/0", "AbCdEf12", "file/0"},
	} {
		token, rest, ok := splitSessionPath(tc.path)
		if !ok || token != tc.token || rest != tc.rest {
			t.Fatalf("splitSessionPath(%q)=(%q,%q,%v) want=(%q,%q,true)", tc.path, token, rest, ok, tc.token, tc.rest)
		}
	}
	for _, invalid := range []string{"0//0", "/token//file/0", "/s//token/", "/s/token//api/status"} {
		if token, rest, ok := splitSessionPath(invalid); ok {
			t.Fatalf("splitSessionPath(%q) unexpectedly accepted token=%q rest=%q", invalid, token, rest)
		}
	}
}

func TestConfiguredPortalPortFallsBackWhenBusy(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	manager := New(Config{
		ListenAddress: busy.Addr().String(),
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	defer manager.Close()
	path := filepath.Join(t.TempDir(), "a.bin")
	if err := os.WriteFile(path, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatalf("fallback start share: %v", err)
	}
	u, err := url.Parse(info.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	if u.Port() == "" || u.Port() == strconv.Itoa(busy.Addr().(*net.TCPAddr).Port) {
		t.Fatalf("fallback URL=%q did not select a free port", info.URLs[0])
	}
}

func TestShareDirectoryProvidesDownloadAllZip(t *testing.T) {
	root := t.TempDir()
	folder := filepath.Join(root, "Folder")
	if err := os.MkdirAll(filepath.Join(folder, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "a.txt"), []byte("A"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "sub", "b.txt"), []byte("BB"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(folder, "empty.txt"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{folder})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(info.URLs[0] + "all.zip")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("zip status=%d", resp.StatusCode)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, file := range zr.File {
		if file.Method != zip.Store {
			t.Fatalf("zip entry %q method=%d want store=%d", file.Name, file.Method, zip.Store)
		}
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		content, _ := io.ReadAll(r)
		r.Close()
		got[file.Name] = string(content)
	}
	if got["Folder/a.txt"] != "A" || got["Folder/sub/b.txt"] != "BB" || got["Folder/empty.txt"] != "" {
		t.Fatalf("zip entries=%#v", got)
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || !snapshot.Complete || snapshot.DoneFiles != snapshot.TotalFiles || snapshot.BytesDone != snapshot.TotalBytes {
		t.Fatalf("zip completion snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestZeroByteShareMarksDaemonVisibleSessionComplete(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "empty.bin")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if info.Complete {
		t.Fatalf("fresh zero-byte share must not be complete before it is requested: %#v", info)
	}
	resp, err := http.Get(info.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("zero-byte file status=%d", resp.StatusCode)
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || !snapshot.Complete || snapshot.DoneFiles != 1 || snapshot.TotalFiles != 1 || snapshot.BytesDone != 0 || snapshot.TotalBytes != 0 {
		t.Fatalf("zero-byte completion snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestReceiveUploadResumesFromExistingPartialAndPromotesSafely(t *testing.T) {
	destination := t.TempDir()
	source := []byte("helloworld")
	manager := testManager(t)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	init := func() uploadInitResponse {
		payload := testReceiveInitPayload(t, "movie.bin", "nested/movie.bin", source)
		resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("init status=%d body=%q", resp.StatusCode, body)
		}
		var out uploadInitResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	first := init()
	if first.Offset != 0 || first.UploadID == "" {
		t.Fatalf("first init=%#v", first)
	}
	putChunk(t, base, first.UploadID, 0, []byte("hello"), http.StatusOK)

	second := init()
	if second.UploadID != first.UploadID || second.Offset != 5 || second.Complete {
		t.Fatalf("resume init=%#v", second)
	}
	putChunk(t, base, first.UploadID, 5, []byte("world"), http.StatusOK)

	final := filepath.Join(destination, "nested", "movie.bin")
	content, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "helloworld" {
		t.Fatalf("final content=%q", content)
	}

	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || snapshot.BytesDone != 10 || snapshot.DoneFiles != 1 || snapshot.TotalBytes != 10 {
		t.Fatalf("snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestReceiveUploadRejectsChangedSourceWithSameNameAndSize(t *testing.T) {
	destination := t.TempDir()
	manager := testManager(t)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]
	original := []byte("helloworld")
	changed := []byte("HELLOWORLD")

	postInit := func(data []byte) (*http.Response, uploadInitResponse) {
		payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
		resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		var out uploadInitResponse
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
				resp.Body.Close()
				t.Fatal(err)
			}
		}
		return resp, out
	}

	firstResp, first := postInit(original)
	firstResp.Body.Close()
	if first.UploadID == "" || first.Offset != 0 {
		t.Fatalf("first init=%#v", first)
	}
	putChunk(t, base, first.UploadID, 0, original[:5], http.StatusOK)

	changedResp, _ := postInit(changed)
	body, _ := io.ReadAll(changedResp.Body)
	changedResp.Body.Close()
	if changedResp.StatusCode != http.StatusConflict {
		t.Fatalf("changed source status=%d body=%q", changedResp.StatusCode, body)
	}
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, first.UploadID)
	partialBytes, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(partialBytes, original[:5]) {
		t.Fatalf("partial changed bytes=%q err=%v", partialBytes, err)
	}

	resumeResp, resumed := postInit(original)
	resumeResp.Body.Close()
	if resumeResp.StatusCode != http.StatusOK || resumed.UploadID != first.UploadID || resumed.Offset != 5 {
		t.Fatalf("original resume status=%d init=%#v", resumeResp.StatusCode, resumed)
	}
}

func TestReceiveUploadSurvivesAbruptPortalRestart(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "portal-state.json")
	config := Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	}
	manager, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	t.Cleanup(client.CloseIdleConnections)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	source := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "nested/movie.bin", source)
	resp, err := client.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var first uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&first); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if first.UploadID == "" || first.Offset != 0 {
		t.Fatalf("first init=%#v", first)
	}

	var persisted persistedState
	b, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &persisted); err != nil {
		t.Fatal(err)
	}
	if len(persisted.Sessions) != 1 || len(persisted.Sessions[0].Uploads) != 1 || persisted.Sessions[0].Uploads[0].ID != first.UploadID {
		t.Fatalf("upload metadata was not durably registered before upload: %#v", persisted)
	}

	putChunkWithClient(t, client, info.URLs[0], first.UploadID, 0, []byte("hello"), http.StatusOK)

	manager.mu.Lock()
	server := manager.server
	manager.server = nil
	manager.listener = nil
	manager.closed = true
	manager.mu.Unlock()
	if server != nil {
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	}

	restarted, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	snapshot, ok := restarted.Snapshot(info.ID)
	if !ok || snapshot.BytesDone != 5 || snapshot.TotalBytes != 10 || snapshot.DoneFiles != 0 {
		t.Fatalf("restart snapshot=%#v ok=%v", snapshot, ok)
	}
	if len(snapshot.URLs) == 0 {
		t.Fatal("restart did not restore portal URL")
	}
	changedPayload := testReceiveInitPayload(t, "movie.bin", "nested/movie.bin", []byte("HELLOWORLD"))
	changedResp, err := client.Post(snapshot.URLs[0]+"api/init", "application/json", bytes.NewReader(changedPayload))
	if err != nil {
		t.Fatal(err)
	}
	changedBody, _ := io.ReadAll(changedResp.Body)
	changedResp.Body.Close()
	if changedResp.StatusCode != http.StatusConflict {
		t.Fatalf("changed source after restart status=%d body=%q", changedResp.StatusCode, changedBody)
	}
	resp, err = client.Post(snapshot.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var second uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&second); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if second.UploadID != first.UploadID || second.Offset != 5 || second.Complete {
		t.Fatalf("restart init=%#v first=%#v", second, first)
	}
	putChunkWithClient(t, client, snapshot.URLs[0], second.UploadID, second.Offset, []byte("world"), http.StatusOK)
	content, err := os.ReadFile(filepath.Join(destination, "nested", "movie.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "helloworld" {
		t.Fatalf("final content=%q", content)
	}
}

func TestOpenDropsLegacyReceiveResumeStateWithoutFingerprint(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "portal-state.json")
	config := Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	}
	manager, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:5], http.StatusOK)

	manager.mu.Lock()
	server := manager.server
	manager.server = nil
	manager.listener = nil
	manager.closed = true
	manager.mu.Unlock()
	if server != nil {
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	}

	var persisted map[string]any
	encoded, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		t.Fatal(err)
	}
	sessions := persisted["sessions"].([]any)
	uploads := sessions[0].(map[string]any)["uploads"].([]any)
	delete(uploads[0].(map[string]any), "fingerprint")
	encoded, err = json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	restored, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	if _, ok := restored.Snapshot(info.ID); ok {
		t.Fatal("legacy receive session without source identity was restored")
	}
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	partialBytes, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(partialBytes, data[:5]) {
		t.Fatalf("legacy partial changed bytes=%q err=%v", partialBytes, err)
	}
}

func TestReceiveCreatesZeroByteFiles(t *testing.T) {
	destination := t.TempDir()
	manager := testManager(t)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	payload := testReceiveInitPayload(t, "empty.txt", "empty.txt", nil)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("init status=%d body=%q", resp.StatusCode, body)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		t.Fatal(err)
	}
	if !init.Complete || init.Offset != 0 {
		t.Fatalf("zero-byte init=%#v", init)
	}
	stat, err := os.Stat(filepath.Join(destination, "empty.txt"))
	if err != nil || stat.Size() != 0 {
		t.Fatalf("empty stat=%#v err=%v", stat, err)
	}
}

func TestReceiveRejectsTraversalAndExistingDestination(t *testing.T) {
	destination := t.TempDir()
	if err := os.WriteFile(filepath.Join(destination, "existing.txt"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	for _, tc := range []struct {
		name     string
		relative string
	}{
		{name: "evil.txt", relative: "../evil.txt"},
		{name: "existing.txt", relative: "existing.txt"},
	} {
		payload := testReceiveInitPayload(t, tc.name, tc.relative, []byte("data"))
		resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusConflict {
			t.Fatalf("relative=%s status=%d", tc.relative, resp.StatusCode)
		}
	}
	content, _ := os.ReadFile(filepath.Join(destination, "existing.txt"))
	if string(content) != "keep" {
		t.Fatalf("existing destination changed: %q", content)
	}
}

func TestSharePauseBlocksDownloadUntilResume(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "paused.bin")
	payload := bytes.Repeat([]byte("pause-proof-"), 4096)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := testManager(t)
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.SetPaused(info.ID, true); err != nil {
		t.Fatal(err)
	}
	paused, ok := manager.Snapshot(info.ID)
	if !ok || !paused.Paused {
		t.Fatalf("paused snapshot=%#v ok=%v", paused, ok)
	}

	type result struct {
		body []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := http.Get(info.URLs[0] + "file/0")
		if err != nil {
			done <- result{err: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		if readErr == nil && resp.StatusCode != http.StatusOK {
			readErr = fmt.Errorf("status=%d", resp.StatusCode)
		}
		done <- result{body: body, err: readErr}
	}()

	select {
	case got := <-done:
		t.Fatalf("download completed while paused: err=%v bytes=%d", got.err, len(got.body))
	case <-time.After(120 * time.Millisecond):
	}

	if err := manager.SetPaused(info.ID, false); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if !bytes.Equal(got.body, payload) {
			t.Fatalf("resumed body bytes=%d want=%d", len(got.body), len(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("download did not resume")
	}
	deadline := time.Now().Add(time.Second)
	var resumed SessionInfo
	for time.Now().Before(deadline) {
		resumed, ok = manager.Snapshot(info.ID)
		if ok && !resumed.Paused && resumed.BytesDone == int64(len(payload)) && resumed.DoneFiles == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("resumed snapshot did not converge after completed response: %#v ok=%v", resumed, ok)
}

func TestShareRejectsSameMetadataContentReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "stable.bin")
	original := []byte("hello world")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	originalModTime := info.ModTime()

	manager := testManager(t)
	session, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}

	replacement := []byte("HELLO WORLD")
	if err := os.WriteFile(path, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, originalModTime, originalModTime); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(original)) || !info.ModTime().Equal(originalModTime) {
		t.Fatalf("failed to preserve metadata size=%d modtime=%v want=%d %v", info.Size(), info.ModTime(), len(original), originalModTime)
	}

	resp, err := http.Get(session.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func TestShareZipRejectsSameMetadataContentReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "stable.bin")
	original := []byte("hello world")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	originalModTime := info.ModTime()

	manager := testManager(t)
	session, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("HELLO WORLD"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, originalModTime, originalModTime); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(session.URLs[0] + "all.zip")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func TestRestoreDropsLegacySendSessionWithoutFingerprint(t *testing.T) {
	saved := persistedSession{
		ID:        "legacy-share",
		Token:     "token",
		Mode:      ModeSend,
		CreatedAt: time.Unix(100, 0),
		ExpiresAt: time.Unix(200, 0),
		Entries: []shareEntry{{
			Path:    filepath.Join(t.TempDir(), "legacy.bin"),
			Name:    "legacy.bin",
			ZipName: "legacy.bin",
			Size:    10,
		}},
	}
	restored, keep, err := restoreSession(saved, nil)
	if err != nil {
		t.Fatal(err)
	}
	if keep || restored != nil {
		t.Fatalf("legacy send session restored=%#v keep=%v", restored, keep)
	}
}

func TestOpenRestoresShareFingerprintAfterAbruptRestart(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "stable.bin")
	original := []byte("hello world")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	originalModTime := fileInfo.ModTime()
	statePath := filepath.Join(root, "portal.json")
	config := Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	}
	manager, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}

	manager.mu.Lock()
	server := manager.server
	manager.server = nil
	manager.listener = nil
	manager.closed = true
	manager.mu.Unlock()
	if server != nil {
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(path, []byte("HELLO WORLD"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, originalModTime, originalModTime); err != nil {
		t.Fatal(err)
	}

	restored, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	snapshot, ok := restored.Snapshot(info.ID)
	if !ok || len(snapshot.URLs) != 1 {
		t.Fatalf("restored snapshot=%#v ok=%v", snapshot, ok)
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	resp, err := client.Get(snapshot.URLs[0] + "file/0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
}

func TestReceivePauseBlocksUploadChunkUntilResume(t *testing.T) {
	destination := t.TempDir()
	manager := testManager(t)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]
	payload := testReceiveInitPayload(t, "paused.bin", "paused.bin", []byte("hello"))
	resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if err := manager.SetPaused(info.ID, true); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		u := base + "api/upload/" + url.PathEscape(init.UploadID) + "?offset=0"
		req, _ := http.NewRequest(http.MethodPut, u, strings.NewReader("hello"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			done <- fmt.Errorf("status=%d body=%q", resp.StatusCode, body)
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		t.Fatalf("upload completed while paused: %v", err)
	case <-time.After(120 * time.Millisecond):
	}
	if _, err := os.Stat(filepath.Join(destination, "paused.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination exists while paused: %v", err)
	}

	if err := manager.SetPaused(info.ID, false); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upload did not resume")
	}
	content, err := os.ReadFile(filepath.Join(destination, "paused.bin"))
	if err != nil || string(content) != "hello" {
		t.Fatalf("content=%q err=%v", content, err)
	}
}

func TestReceivePageLearnsUpdatedRoutesAndAllowsCrossOriginAPI(t *testing.T) {
	var routesMu sync.RWMutex
	routes := []HostRoute{{Address: "127.0.0.1", Interface: "lo", Kind: "loopback"}}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostRoutes: func() []HostRoute {
			routesMu.RLock()
			defer routesMu.RUnlock()
			return append([]HostRoute(nil), routes...)
		},
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte(`<input id="files" type="file" multiple>`)) {
		t.Fatal("ordinary receive page lost multi-file selection")
	}
	for _, marker := range []string{
		"routeFetch",
		"refreshRoutes",
		"mergeRoutes(info.urls)",
		"api/status",
		`id="folder" type="file" webkitdirectory multiple`,
		"folderInput",
		"fingerprintFile",
		"prefixSHA256",
		"needs_verification",
		"api/verify/",
		"crypto.subtle",
		"new SHA256",
		`id="pauseTransfer"`,
		`id="cancelTransfer"`,
		"api/pause",
		"api/resume",
		"api/cancel",
	} {
		if !bytes.Contains(body, []byte(marker)) {
			t.Fatalf("receive page missing %q", marker)
		}
	}

	routesMu.Lock()
	routes = []HostRoute{
		{Address: "127.0.0.1", Interface: "lo", Kind: "loopback"},
		{Address: "192.0.2.44", Interface: "enp0s20u1", Kind: "usb-network"},
	}
	routesMu.Unlock()
	resp, err = http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		URLs   []string    `json:"urls"`
		Routes []RouteInfo `json:"routes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(status.URLs) != 2 || !strings.Contains(status.URLs[1], "192.0.2.44") {
		t.Fatalf("updated urls=%#v", status.URLs)
	}
	if len(status.Routes) != 2 || status.Routes[1].Interface != "enp0s20u1" || status.Routes[1].Kind != "usb-network" {
		t.Fatalf("updated routes=%#v", status.Routes)
	}

	baseURL, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	allowedOrigin := "http://" + net.JoinHostPort("192.0.2.44", baseURL.Port())
	req, _ := http.NewRequest(http.MethodOptions, base+"api/status", nil)
	req.Header.Set("Origin", allowedOrigin)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != allowedOrigin {
		t.Fatalf("preflight status=%d allow-origin=%q", resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
	}

	req, _ = http.NewRequest(http.MethodOptions, base+"api/status", nil)
	req.Header.Set("Origin", "http://attacker.example:"+baseURL.Port())
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("untrusted origin status=%d allow-origin=%q", resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestBrowserControlAPIPausesResumesAndCancelsSession(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "movie.bin")
	if err := os.WriteFile(shared, []byte("shared-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartShare([]string{shared})
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	post := func(path string) *http.Response {
		t.Helper()
		resp, err := http.Post(base+path, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := post("api/pause")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause status=%d", resp.StatusCode)
	}
	if snapshot, ok := manager.Snapshot(info.ID); !ok || !snapshot.Paused {
		t.Fatalf("pause snapshot=%#v ok=%v", snapshot, ok)
	}

	resp = post("api/resume")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("resume status=%d", resp.StatusCode)
	}
	if snapshot, ok := manager.Snapshot(info.ID); !ok || snapshot.Paused {
		t.Fatalf("resume snapshot=%#v ok=%v", snapshot, ok)
	}

	resp = post("api/cancel")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status=%d", resp.StatusCode)
	}
	if _, ok := manager.Snapshot(info.ID); ok {
		t.Fatal("cancelled session is still available")
	}

	resp, err = http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("status after cancel=%d want=%d", resp.StatusCode, http.StatusGone)
	}
}

func TestPauseControlDoesNotWaitForActiveUploadPersistenceLock(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "portal-state.json")
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", []byte("hello"))
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	manager.mu.RLock()
	s := manager.sessions[info.ID]
	manager.mu.RUnlock()
	if s == nil {
		t.Fatal("receive session missing")
	}
	u := findUploadByID(s, init.UploadID)
	if u == nil {
		t.Fatal("upload state missing")
	}

	u.mu.Lock()
	start := time.Now()
	if err := manager.SetPaused(info.ID, true); err != nil {
		u.mu.Unlock()
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		u.mu.Unlock()
		t.Fatalf("pause control blocked for %v while upload lock was held", elapsed)
	}
	if snapshot, ok := manager.Snapshot(info.ID); !ok || !snapshot.Paused {
		u.mu.Unlock()
		t.Fatalf("pause snapshot=%#v ok=%v", snapshot, ok)
	}
	u.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, readErr := os.ReadFile(statePath)
		if readErr == nil && bytes.Contains(data, []byte("\"paused\": true")) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("asynchronous pause state was not persisted")
}

func TestCancelControlDoesNotWaitForActiveUploadLock(t *testing.T) {
	root := t.TempDir()
	statePath := filepath.Join(root, "portal-state.json")
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     statePath,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", []byte("hello"))
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	manager.mu.RLock()
	s := manager.sessions[info.ID]
	manager.mu.RUnlock()
	if s == nil {
		t.Fatal("receive session missing")
	}
	u := findUploadByID(s, init.UploadID)
	if u == nil {
		t.Fatal("upload state missing")
	}

	u.mu.Lock()
	start := time.Now()
	if err := manager.CancelSession(info.ID); err != nil {
		u.mu.Unlock()
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		u.mu.Unlock()
		t.Fatalf("cancel control blocked for %v while upload lock was held", elapsed)
	}
	if _, ok := manager.Snapshot(info.ID); ok {
		u.mu.Unlock()
		t.Fatal("cancelled session remained visible")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, statErr := os.Stat(statePath); errors.Is(statErr, os.ErrNotExist) {
			u.mu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	u.mu.Unlock()
	t.Fatal("cancelled session was not removed from persisted state")
}

func TestSendPageRetargetsDownloadsAcrossUpdatedRoutes(t *testing.T) {
	root := t.TempDir()
	shared := filepath.Join(root, "movie.bin")
	if err := os.WriteFile(shared, []byte("shared-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	var routesMu sync.RWMutex
	routes := []HostRoute{{Address: "127.0.0.1", Interface: "lo", Kind: "loopback"}}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostRoutes: func() []HostRoute {
			routesMu.RLock()
			defer routesMu.RUnlock()
			return append([]HostRoute(nil), routes...)
		},
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartShare([]string{shared})
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, marker := range []string{
		`data-path="file/0"`,
		"retargetDownloads",
		"mergeRoutes(info.urls)",
		"api/status",
		"Current path is unavailable",
		`id="pauseTransfer"`,
		`id="cancelTransfer"`,
		"api/pause",
		"api/resume",
		"api/cancel",
	} {
		if !bytes.Contains(body, []byte(marker)) {
			t.Fatalf("send page missing %q", marker)
		}
	}

	routesMu.Lock()
	routes = []HostRoute{
		{Address: "127.0.0.1", Interface: "lo", Kind: "loopback"},
		{Address: "192.0.2.55", Interface: "usb0", Kind: "usb-network"},
	}
	routesMu.Unlock()
	resp, err = http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		URLs []string `json:"urls"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(status.URLs) != 2 || !strings.Contains(status.URLs[1], "192.0.2.55") {
		t.Fatalf("updated urls=%#v", status.URLs)
	}
}

func TestReceiveUploadContinuesAcrossPortalRouteAddresses(t *testing.T) {
	destination := t.TempDir()
	manager := New(Config{
		ListenAddress: "0.0.0.0:0",
		HostRoutes: func() []HostRoute {
			return []HostRoute{
				{Address: "127.0.0.1", Interface: "wifi-test", Kind: "wifi"},
				{Address: "127.0.0.2", Interface: "ethernet-test", Kind: "ethernet"},
			}
		},
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 2 {
		t.Fatalf("urls=%#v", info.URLs)
	}
	data := []byte("route-handoff-upload")
	payload := testReceiveInitPayload(t, "handoff.bin", "handoff.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	cut := int64(7)
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:cut], http.StatusOK)
	putChunk(t, info.URLs[1], init.UploadID, cut, data[cut:], http.StatusOK)

	got, err := os.ReadFile(filepath.Join(destination, "handoff.bin"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("route handoff upload=%q err=%v", got, err)
	}
}

func TestReceiveUploadSerializesSameOffsetAcrossPortalRoutes(t *testing.T) {
	destination := t.TempDir()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "0.0.0.0:0",
		Store:         store,
		HostRoutes: func() []HostRoute {
			return []HostRoute{
				{Address: "127.0.0.1", Interface: "wifi-test", Kind: "wifi"},
				{Address: "127.0.0.2", Interface: "ethernet-test", Kind: "ethernet"},
			}
		},
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 2 {
		t.Fatalf("urls=%#v", info.URLs)
	}
	data := []byte("duplicate-route-write")
	payload := testReceiveInitPayload(t, "duplicate.bin", "duplicate.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	cut := int64(8)
	type result struct {
		status int
		body   map[string]any
		err    error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, base := range info.URLs {
		base := base
		go func() {
			<-start
			req, reqErr := http.NewRequest(http.MethodPut, base+"api/upload/"+url.PathEscape(init.UploadID)+"?offset=0", bytes.NewReader(data[:cut]))
			if reqErr != nil {
				results <- result{err: reqErr}
				return
			}
			response, doErr := http.DefaultClient.Do(req)
			if doErr != nil {
				results <- result{err: doErr}
				return
			}
			defer response.Body.Close()
			body := map[string]any{}
			_ = json.NewDecoder(response.Body).Decode(&body)
			results <- result{status: response.StatusCode, body: body}
		}()
	}
	close(start)

	got := []result{<-results, <-results}
	okCount, conflictCount := 0, 0
	for _, item := range got {
		if item.err != nil {
			t.Fatal(item.err)
		}
		switch item.status {
		case http.StatusOK:
			okCount++
			if offset, _ := item.body["offset"].(float64); int64(offset) != cut {
				t.Fatalf("successful offset=%v, want %d", item.body["offset"], cut)
			}
		case http.StatusConflict:
			conflictCount++
			if offset, _ := item.body["expected_offset"].(float64); int64(offset) != cut {
				t.Fatalf("conflict expected_offset=%v, want %d", item.body["expected_offset"], cut)
			}
		default:
			t.Fatalf("unexpected concurrent upload result: status=%d body=%#v", item.status, item.body)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("concurrent results=%#v", got)
	}

	putChunk(t, info.URLs[1], init.UploadID, cut, data[cut:], http.StatusOK)
	final, err := os.ReadFile(filepath.Join(destination, "duplicate.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(final, data) {
		t.Fatalf("final upload=%q", final)
	}
}

func TestSharedFileRangeContinuesAcrossPortalRouteAddresses(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "handoff.bin")
	data := []byte("route-handoff-download")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "0.0.0.0:0",
		HostRoutes: func() []HostRoute {
			return []HostRoute{
				{Address: "127.0.0.1", Interface: "wifi-test", Kind: "wifi"},
				{Address: "127.0.0.2", Interface: "ethernet-test", Kind: "ethernet"},
			}
		},
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 2 {
		t.Fatalf("urls=%#v", info.URLs)
	}

	readRange := func(base, value string) []byte {
		req, err := http.NewRequest(http.MethodGet, base+"file/0", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Range", value)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("range %q status=%d body=%q", value, resp.StatusCode, body)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}

	cut := 8
	first := readRange(info.URLs[0], fmt.Sprintf("bytes=0-%d", cut-1))
	second := readRange(info.URLs[1], fmt.Sprintf("bytes=%d-%d", cut, len(data)-1))
	if got := append(first, second...); !bytes.Equal(got, data) {
		t.Fatalf("route handoff download=%q", got)
	}
}

func TestManagedReceiveStartsAfterInterruptedLargeRecovery(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "phone-large.bin")
	destination := filepath.Join(root, "laptop-large.bin")
	const size = 80 << 20
	sourceFile, err := os.OpenFile(source, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := sourceFile.Truncate(size); err != nil {
		sourceFile.Close()
		t.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "manifest-large-handoff",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:          "entry-large-handoff",
			Source:      source,
			Destination: destination,
			Size:        size,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	stop := errors.New("interrupt large recovery")
	runner := recovery.Runner{Store: store, Engine: engine.Engine{
		Store: store,
		Progress: func(done, total int64) error {
			if done >= 16<<20 && done < total {
				return stop
			}
			return nil
		},
	}}
	if err := runner.RecoverManifest("manifest-large-handoff"); !errors.Is(err, stop) {
		t.Fatalf("interrupted recovery = %v, want %v", err, stop)
	}

	manifest, _ := store.Manifest("manifest-large-handoff")
	entry := manifest.Entries[0]
	if entry.SourceFingerprint == nil {
		t.Fatal("interrupted recovery did not persist source identity")
	}
	partial := destination + engine.PartialSuffix
	info, err := os.Stat(partial)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= 0 || info.Size() >= size {
		t.Fatalf("interrupted partial size = %d, want between 0 and %d", info.Size(), size)
	}
	if err := store.SetManifestAwaitingReconnect("manifest-large-handoff", true); err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(root, ".resumexfer-cache")
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	staleCandidatePath := filepath.Join(cacheDir, "stale-large-handoff.bin")
	if err := os.WriteFile(staleCandidatePath, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutCandidate(state.Candidate{
		ID:          "stale-large-handoff",
		Source:      destination,
		PartialPath: staleCandidatePath,
		Size:        size,
		UpdatedAt:   time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })

	session, err := manager.StartManifestReceive("manifest-large-handoff", "entry-large-handoff")
	if err != nil {
		t.Fatalf("start manifest receive after interrupted large recovery: %v", err)
	}
	if session.ManifestID != "manifest-large-handoff" || session.EntryID != "entry-large-handoff" {
		t.Fatalf("unexpected managed session: %#v", session)
	}
	if session.BytesDone != info.Size() {
		t.Fatalf("managed continuation offset = %d, want partial size %d", session.BytesDone, info.Size())
	}

	payload, err := json.Marshal(uploadInitRequest{
		Name:        filepath.Base(source),
		Size:        size,
		Fingerprint: entry.SourceFingerprint,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(session.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !init.NeedsVerification || init.Offset != info.Size() {
		t.Fatalf("managed continuation init status=%d response=%#v", resp.StatusCode, init)
	}

	verifyManagedPrefix(t, session.URLs[0], init.UploadID, info.Size(), sha256Hex(make([]byte, int(info.Size()))), http.StatusOK)
	chunk := make([]byte, 8<<20)
	for offset := info.Size(); offset < size; offset += int64(len(chunk)) {
		putChunk(t, session.URLs[0], init.UploadID, offset, chunk, http.StatusOK)
	}

	finalFile, err := os.Open(destination)
	if err != nil {
		t.Fatal(err)
	}
	finalFP, err := fingerprint.ReaderAt(finalFile, size, 64*1024)
	closeErr := finalFile.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if !entry.SourceFingerprint.Compatible(finalFP) {
		t.Fatal("portal continuation final source identity mismatch")
	}
	finalManifest, _ := store.Manifest("manifest-large-handoff")
	if !finalManifest.Entries[0].Complete || finalManifest.Entries[0].BytesDone != size {
		t.Fatalf("portal continuation did not complete manifest: %#v", finalManifest.Entries[0])
	}
	if finalManifest.AwaitingReconnect {
		t.Fatal("portal continuation left manifest awaiting reconnect after completion")
	}
	if candidates := store.Candidates(); len(candidates) != 0 {
		t.Fatalf("portal continuation left stale candidates: %#v", candidates)
	}
	if _, err := os.Stat(staleCandidatePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("portal continuation left stale candidate cache file: %v", err)
	}
}

func TestManagedReceiveContinuesSameManifestFromVerifiedPhysicalPartial(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "movie.bin")
	partial := destination + engine.PartialSuffix
	if err := os.WriteFile(partial, data[:5], 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := state.Manifest{
		ID:        "manifest-1",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:                "entry-1",
			Source:            "mtp://phone/movie.bin",
			Destination:       destination,
			Size:              int64(len(data)),
			BytesDone:         1,
			CreatedByJob:      true,
			SourceFingerprint: &fp,
		}},
	}
	if err := store.PutManifest(manifest); err != nil {
		t.Fatal(err)
	}
	if err := store.PutTransfer(state.Transfer{
		ID:                "entry-1",
		Destination:       destination,
		Size:              int64(len(data)),
		ChunkSize:         8 << 20,
		Chunks:            map[int]state.Chunk{},
		SourceFingerprint: &fp,
		CreatedAt:         time.Now(),
		UpdatedAt:         time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartManifestReceive("manifest-1", "entry-1")
	if err != nil {
		t.Fatal(err)
	}
	if info.BytesDone != 5 || info.ManifestID != "manifest-1" || info.EntryID != "entry-1" {
		t.Fatalf("managed session info=%#v", info)
	}
	reused, err := manager.StartManifestReceive("manifest-1", "entry-1")
	if err != nil {
		t.Fatal(err)
	}
	if reused.ID != info.ID || len(reused.URLs) == 0 || reused.URLs[0] != info.URLs[0] {
		t.Fatalf("repeated managed portal start created a competing session: first=%#v reused=%#v", info, reused)
	}
	base := info.URLs[0]
	resp, err := http.Get(base)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte("Continue transfer")) || !bytes.Contains(body, []byte("verify the existing USB progress")) {
		t.Fatalf("managed receive page missing continuation guidance: %q", body)
	}
	if !bytes.Contains(body, []byte(`<input id="files" type="file">`)) || bytes.Contains(body, []byte(`<input id="files" type="file" multiple>`)) {
		t.Fatal("managed receive page must require a single source file")
	}
	if bytes.Contains(body, []byte("webkitdirectory")) {
		t.Fatal("managed continuation page must not expose folder selection")
	}
	init := postManagedInit(t, base, "movie.bin", data, fp, http.StatusOK)
	if init.Offset != 5 || !init.NeedsVerification || init.UploadID == "" {
		t.Fatalf("managed init=%#v", init)
	}

	putChunk(t, base, init.UploadID, 5, data[5:], http.StatusConflict)
	got, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(got, data[:5]) {
		t.Fatalf("unverified write changed partial: %q err=%v", got, err)
	}

	lease, err := store.AcquireManifestTransport("manifest-1", "usb-recovery:test", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verifyPayload, err := json.Marshal(uploadVerifyRequest{Offset: 5, PrefixSHA256: sha256Hex(data[:5])})
	if err != nil {
		t.Fatal(err)
	}
	verifyResp, err := http.Post(base+"api/verify/"+url.PathEscape(init.UploadID), "application/json", bytes.NewReader(verifyPayload))
	if err != nil {
		t.Fatal(err)
	}
	var busy map[string]any
	if err := json.NewDecoder(verifyResp.Body).Decode(&busy); err != nil {
		verifyResp.Body.Close()
		t.Fatal(err)
	}
	verifyResp.Body.Close()
	if verifyResp.StatusCode != http.StatusConflict || busy["transport_busy"] != true {
		t.Fatalf("busy verification status=%d response=%#v", verifyResp.StatusCode, busy)
	}
	if err := store.ReleaseManifestTransport("manifest-1", lease.Owner, lease.Generation); err != nil {
		t.Fatal(err)
	}

	verifyManagedPrefix(t, base, init.UploadID, 5, sha256Hex(data[:5]), http.StatusOK)
	updated, _ := store.Manifest("manifest-1")
	if updated.Entries[0].BytesDone != 5 {
		t.Fatalf("verified progress=%d want 5", updated.Entries[0].BytesDone)
	}
	putChunk(t, base, init.UploadID, 5, data[5:], http.StatusOK)

	final, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(final, data) {
		t.Fatalf("final data=%q", final)
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial still exists: %v", err)
	}
	updated, _ = store.Manifest("manifest-1")
	if !updated.Entries[0].Complete || updated.Entries[0].BytesDone != int64(len(data)) {
		t.Fatalf("manifest entry=%#v", updated.Entries[0])
	}

	// Managed continuation must become terminal as soon as its one known
	// manifest entry is finalized. The desktop portal must not depend on a
	// second browser-side api/finish request or it can remain stuck at 100%.
	statusResp, err := http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var status map[string]any
	if err := json.NewDecoder(statusResp.Body).Decode(&status); err != nil {
		statusResp.Body.Close()
		t.Fatal(err)
	}
	statusResp.Body.Close()
	if statusResp.StatusCode != http.StatusOK || status["complete"] != true {
		t.Fatalf("managed completion status=%d response=%#v", statusResp.StatusCode, status)
	}
	if got := int64(status["bytes_done"].(float64)); got != int64(len(data)) {
		t.Fatalf("managed completion bytes_done=%d want=%d", got, len(data))
	}

	if updated.TransportOwner != "" {
		t.Fatalf("transport owner still held: %q", updated.TransportOwner)
	}
	if _, ok := store.Transfer("entry-1"); ok {
		t.Fatal("stale engine transfer survived managed completion")
	}
}

func TestManagedReceiveKeepsLeaseWhileSlowChunkIsActive(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("slow-body")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "slow.bin")
	if err := store.PutManifest(state.Manifest{
		ID: "manifest-slow", Direction: "phone-to-laptop", Managed: true,
		Entries: []state.ManifestEntry{{ID: "entry-slow", Destination: destination, Size: int64(len(data)), CreatedByJob: true, SourceFingerprint: &fp}},
	}); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress:           "127.0.0.1:0",
		HostAddresses:           func() []string { return []string{"127.0.0.1"} },
		Store:                   store,
		TransportLeaseTTL:       90 * time.Millisecond,
		TransportLeaseHeartbeat: 20 * time.Millisecond,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartManifestReceive("manifest-slow", "entry-slow")
	if err != nil {
		t.Fatal(err)
	}
	init := postManagedInit(t, info.URLs[0], "slow.bin", data, fp, http.StatusOK)
	verifyManagedPrefix(t, info.URLs[0], init.UploadID, 0, sha256Hex(nil), http.StatusOK)

	reader, writer := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, info.URLs[0]+"api/upload/"+url.PathEscape(init.UploadID)+"?offset=0", reader)
	if err != nil {
		t.Fatal(err)
	}
	responseCh := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			responseCh <- err
			return
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			responseCh <- fmt.Errorf("slow upload status=%d body=%q", resp.StatusCode, body)
			return
		}
		responseCh <- nil
	}()

	deadline := time.Now().Add(time.Second)
	for {
		manifest, _ := store.Manifest("manifest-slow")
		if manifest.TransportOwner != "" && manifest.TransportLeaseUntil.After(time.Now().Add(40*time.Millisecond)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("managed upload did not acquire/refresh its lease")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(220 * time.Millisecond)
	if _, err := store.AcquireManifestTransport("manifest-slow", "usb-recovery:competitor", time.Now(), time.Second); !errors.Is(err, state.ErrManifestTransportBusy) {
		t.Fatalf("competing transport acquired active slow upload lease: %v", err)
	}
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-responseCh; err != nil {
		t.Fatal(err)
	}
	final, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(final, data) {
		t.Fatalf("slow upload final=%q err=%v", final, err)
	}
}

func TestManagedReceiveHandsOffFromWifiToUSBWithoutRestarting(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("wifi-to-usb-handoff")
	source := filepath.Join(root, "phone-source.bin")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "received.bin")
	partial := destination + engine.PartialSuffix
	if err := os.WriteFile(partial, data[:4], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID: "manifest-wifi-usb", Direction: "phone-to-laptop", Managed: true,
		Entries: []state.ManifestEntry{{
			ID: "entry-wifi-usb", Source: source, Destination: destination,
			Size: int64(len(data)), BytesDone: 1, CreatedByJob: true, SourceFingerprint: &fp,
		}},
	}); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress:           "127.0.0.1:0",
		HostAddresses:           func() []string { return []string{"127.0.0.1"} },
		Store:                   store,
		TransportLeaseTTL:       80 * time.Millisecond,
		TransportLeaseHeartbeat: 20 * time.Millisecond,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartManifestReceive("manifest-wifi-usb", "entry-wifi-usb")
	if err != nil {
		t.Fatal(err)
	}
	init := postManagedInit(t, info.URLs[0], "phone-source.bin", data, fp, http.StatusOK)
	if init.Offset != 4 || !init.NeedsVerification {
		t.Fatalf("managed init=%#v", init)
	}
	verifyManagedPrefix(t, info.URLs[0], init.UploadID, 4, sha256Hex(data[:4]), http.StatusOK)
	putChunk(t, info.URLs[0], init.UploadID, 4, data[4:9], http.StatusOK)

	got, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(got, data[:9]) {
		t.Fatalf("wifi partial=%q err=%v", got, err)
	}
	manifest, _ := store.Manifest("manifest-wifi-usb")
	if manifest.Entries[0].BytesDone != 9 || manifest.TransportOwner != "" {
		t.Fatalf("quiescent wifi chunk retained transport ownership: %#v", manifest)
	}

	runner := recovery.Runner{
		Store:                   store,
		Engine:                  engine.Engine{Store: store},
		TransportLeaseTTL:       time.Second,
		TransportLeaseHeartbeat: 20 * time.Millisecond,
	}
	if err := runner.RecoverManifest("manifest-wifi-usb"); err != nil {
		t.Fatal(err)
	}
	final, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(final, data) {
		t.Fatalf("USB handoff final=%q err=%v", final, err)
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial survived USB completion: %v", err)
	}
	manifest, _ = store.Manifest("manifest-wifi-usb")
	if !manifest.Entries[0].Complete || manifest.Entries[0].BytesDone != int64(len(data)) || manifest.TransportOwner != "" {
		t.Fatalf("completed manifest=%#v", manifest)
	}

	staleURL := info.URLs[0] + "api/upload/" + url.PathEscape(init.UploadID) + "?offset=9"
	staleReq, err := http.NewRequest(http.MethodPut, staleURL, bytes.NewReader(data[9:]))
	if err != nil {
		t.Fatal(err)
	}
	staleResp, err := http.DefaultClient.Do(staleReq)
	if err != nil {
		t.Fatal(err)
	}
	var staleResult struct {
		Offset   int64 `json:"offset"`
		Complete bool  `json:"complete"`
	}
	if err := json.NewDecoder(staleResp.Body).Decode(&staleResult); err != nil {
		staleResp.Body.Close()
		t.Fatal(err)
	}
	staleResp.Body.Close()
	if staleResp.StatusCode != http.StatusOK || !staleResult.Complete || staleResult.Offset != int64(len(data)) {
		t.Fatalf("stale browser completion status=%d response=%#v", staleResp.StatusCode, staleResult)
	}
	finalAfterStaleBrowser, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(finalAfterStaleBrowser, data) {
		t.Fatalf("stale browser changed completed file=%q err=%v", finalAfterStaleBrowser, err)
	}
}

func TestManagedReceiveRejectsChangedSourceAndPartialRace(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("abcdefghij")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "movie.bin")
	partial := destination + engine.PartialSuffix
	if err := os.WriteFile(partial, data[:5], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID: "manifest-2", Direction: "phone-to-laptop", Managed: true,
		Entries: []state.ManifestEntry{{ID: "entry-2", Destination: destination, Size: int64(len(data)), BytesDone: 2, CreatedByJob: true, SourceFingerprint: &fp}},
	}); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{ListenAddress: "127.0.0.1:0", HostAddresses: func() []string { return []string{"127.0.0.1"} }, Store: store})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartManifestReceive("manifest-2", "entry-2")
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]

	wrong := append([]byte(nil), data...)
	wrong[len(wrong)-1] ^= 0x7f
	wrongFP, err := fingerprint.ReaderAt(bytes.NewReader(wrong), int64(len(wrong)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	postManagedInit(t, base, "movie.bin", wrong, wrongFP, http.StatusConflict)

	init := postManagedInit(t, base, "movie.bin", data, fp, http.StatusOK)
	if err := os.WriteFile(partial, data[:6], 0o600); err != nil {
		t.Fatal(err)
	}
	verifyManagedPrefix(t, base, init.UploadID, 5, sha256Hex(data[:5]), http.StatusConflict)
	manifest, _ := store.Manifest("manifest-2")
	if manifest.TransportOwner != "" {
		t.Fatalf("lease remained after raced prefix: %q", manifest.TransportOwner)
	}
	got, err := os.ReadFile(partial)
	if err != nil || !bytes.Equal(got, data[:6]) {
		t.Fatalf("raced partial changed: %q err=%v", got, err)
	}

	init = postManagedInit(t, base, "movie.bin", data, fp, http.StatusOK)
	verifyManagedPrefix(t, base, init.UploadID, 6, sha256Hex([]byte("xxxxxx")), http.StatusConflict)
	manifest, _ = store.Manifest("manifest-2")
	if manifest.TransportOwner != "" {
		t.Fatalf("lease remained after hash mismatch: %q", manifest.TransportOwner)
	}
	got, _ = os.ReadFile(partial)
	if !bytes.Equal(got, data[:6]) {
		t.Fatalf("hash mismatch modified partial: %q", got)
	}
}

func postManagedInit(t *testing.T, base, name string, data []byte, fp fingerprint.Fingerprint, wantStatus int) uploadInitResponse {
	return postManagedInitRelative(t, base, name, "", data, fp, wantStatus)
}

func postManagedInitRelative(t *testing.T, base, name, relative string, data []byte, fp fingerprint.Fingerprint, wantStatus int) uploadInitResponse {
	t.Helper()
	payload, err := json.Marshal(uploadInitRequest{Name: name, RelativePath: relative, Size: int64(len(data)), Fingerprint: &fp})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("managed init status=%d want=%d body=%q", resp.StatusCode, wantStatus, body)
	}
	if wantStatus != http.StatusOK {
		return uploadInitResponse{}
	}
	var out uploadInitResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func testReceiveInitPayload(t *testing.T, name, relative string, data []byte) []byte {
	t.Helper()
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(uploadInitRequest{
		Name:         name,
		RelativePath: relative,
		Size:         int64(len(data)),
		Fingerprint:  &fp,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func verifyManagedPrefix(t *testing.T, base, uploadID string, offset int64, prefixHash string, wantStatus int) {
	t.Helper()
	payload, err := json.Marshal(uploadVerifyRequest{Offset: offset, PrefixSHA256: prefixHash})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(base+"api/verify/"+url.PathEscape(uploadID), "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("managed verify status=%d want=%d body=%q", resp.StatusCode, wantStatus, body)
	}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestAdoptReceivePartialFreezesVerifiedWirelessCheckpointForUSB(t *testing.T) {
	root := t.TempDir()
	destinationRoot := filepath.Join(root, "destination")
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer manager.Close()
	info, err := manager.StartReceive(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("wireless-adoption-checkpoint-"), 240000)
	if len(data) <= int(engine.DefaultChunkSize+(1<<20)) {
		t.Fatalf("test data too small: %d", len(data))
	}
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("init status=%d", resp.StatusCode)
	}
	firstWrite := int(engine.DefaultChunkSize + (1 << 20))
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:firstWrite], http.StatusOK)
	destination := filepath.Join(destinationRoot, "movie.bin")
	if !manager.HasReceivePartial(destination, int64(len(data))) {
		t.Fatal("matching wireless partial was not discoverable")
	}
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	adoption, found, err := manager.AdoptReceivePartial(destination, bytes.NewReader(data), int64(len(data)), fp)
	if err != nil {
		t.Fatal(err)
	}
	if !found || adoption.Offset != engine.DefaultChunkSize {
		t.Fatalf("adoption=%#v found=%v", adoption, found)
	}
	candidate, ok := store.Candidate(adoption.CandidateID)
	if !ok {
		t.Fatal("adoption candidate was not persisted")
	}
	if candidate.Destination != destination || candidate.ChunkSize != engine.DefaultChunkSize {
		t.Fatalf("candidate=%#v", candidate)
	}
	stat, err := os.Stat(candidate.PartialPath)
	if err != nil {
		t.Fatal(err)
	}
	if stat.Size() != engine.DefaultChunkSize {
		t.Fatalf("adopted partial size=%d want=%d", stat.Size(), engine.DefaultChunkSize)
	}
	if manager.HasReceivePartial(destination, int64(len(data))) {
		t.Fatal("adopted upload remained writable by wireless session")
	}
	putChunk(t, info.URLs[0], init.UploadID, adoption.Offset, data[adoption.Offset:adoption.Offset+1024], http.StatusNotFound)
}

func TestAdoptReceivePartialRejectsChangedSourceWithoutFreezingWirelessUpload(t *testing.T) {
	root := t.TempDir()
	destinationRoot := filepath.Join(root, "destination")
	if err := os.MkdirAll(destinationRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{ListenAddress: "127.0.0.1:0", HostAddresses: func() []string { return []string{"127.0.0.1"} }, Store: store})
	defer manager.Close()
	info, err := manager.StartReceive(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("a"), int(engine.DefaultChunkSize+1024))
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:engine.DefaultChunkSize], http.StatusOK)
	changed := bytes.Repeat([]byte("b"), len(data))
	changedFP, err := fingerprint.ReaderAt(bytes.NewReader(changed), int64(len(changed)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(destinationRoot, "movie.bin")
	if _, found, err := manager.AdoptReceivePartial(destination, bytes.NewReader(changed), int64(len(changed)), changedFP); err != nil || found {
		t.Fatalf("changed source adoption found=%v err=%v", found, err)
	}
	if !manager.HasReceivePartial(destination, int64(len(data))) {
		t.Fatal("changed-source attempt froze the original wireless upload")
	}
	putChunk(t, info.URLs[0], init.UploadID, engine.DefaultChunkSize, data[engine.DefaultChunkSize:], http.StatusOK)
}

func TestReceiveMultiFileBatchWaitsForExplicitFinish(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	manager, err := Open(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     filepath.Join(root, "portal.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]
	upload := func(name string, data []byte) {
		t.Helper()
		payload := testReceiveInitPayload(t, name, name, data)
		resp, err := http.Post(base+"api/init", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		var init uploadInitResponse
		if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
			resp.Body.Close()
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("init %s status=%d", name, resp.StatusCode)
		}
		putChunk(t, base, init.UploadID, 0, data, http.StatusOK)
	}

	upload("one.txt", []byte("one"))
	resp, err := http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var firstStatus map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&firstStatus); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if firstStatus["complete"] == true {
		t.Fatalf("first file prematurely completed browser batch: %#v", firstStatus)
	}

	upload("two.txt", []byte("two"))
	resp, err = http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var secondStatus map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&secondStatus); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if secondStatus["complete"] == true {
		t.Fatalf("batch completed before api/finish: %#v", secondStatus)
	}

	resp, err = http.Post(base+"api/finish", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("finish status=%d", resp.StatusCode)
	}
	resp, err = http.Get(base + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	var finalStatus map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&finalStatus); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if finalStatus["complete"] != true || int(finalStatus["done_files"].(float64)) != 2 || int(finalStatus["total_files"].(float64)) != 2 {
		t.Fatalf("final batch status=%#v", finalStatus)
	}
	for name, want := range map[string]string{"one.txt": "one", "two.txt": "two"} {
		got, err := os.ReadFile(filepath.Join(destination, name))
		if err != nil || string(got) != want {
			t.Fatalf("%s=%q err=%v", name, got, err)
		}
	}
}

func TestStreamingUploadWithPortalStateDoesNotDeadlock(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	manager, err := Open(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		StatePath:     filepath.Join(root, "portal.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("tiny-fast-file")
	payload := testReceiveInitPayload(t, "tiny.bin", "tiny.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%sapi/stream/%s?offset=0", info.URLs[0], url.PathEscape(init.UploadID)), bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("stream upload blocked behind portal persistence: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status=%d body=%q", resp.StatusCode, body)
	}
	resp, err = client.Get(info.URLs[0] + "api/status")
	if err != nil {
		t.Fatalf("status blocked after completed upload: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status after stream=%d", resp.StatusCode)
	}
}

func TestManagedReceiveUsesBoundedUSBCheckpointProof(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	chunkSize := engine.DefaultChunkSize
	size := chunkSize * 4
	source := bytes.Repeat([]byte("q"), int(size))
	fp, err := fingerprint.ReaderAt(bytes.NewReader(source), size, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "movie.bin")
	partial := destination + engine.PartialSuffix
	durable := chunkSize * 3
	if err := os.WriteFile(partial, source[:int(durable)], 0o600); err != nil {
		t.Fatal(err)
	}
	chunks := map[int]state.Chunk{}
	for i := 0; i < 3; i++ {
		start := int64(i) * chunkSize
		sum := sha256.Sum256(source[int(start):int(start+chunkSize)])
		chunks[i] = state.Chunk{Hash: hex.EncodeToString(sum[:]), Size: chunkSize}
	}
	if err := store.PutTransfer(state.Transfer{
		ID:                "entry",
		Source:            "/phone/movie.bin",
		Destination:       destination,
		Size:              size,
		ChunkSize:         chunkSize,
		Chunks:            chunks,
		SourceFingerprint: &fp,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:                "job",
		Direction:         "phone-to-laptop",
		Managed:           true,
		AwaitingReconnect: true,
		Entries: []state.ManifestEntry{{
			ID:                "entry",
			Source:            "/phone/movie.bin",
			Destination:       destination,
			Size:              size,
			BytesDone:         durable,
			CreatedByJob:      true,
			SourceFingerprint: &fp,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer manager.Close()
	info, err := manager.StartManifestReceive("job", "entry")
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(uploadInitRequest{Name: "movie.bin", RelativePath: "movie.bin", Size: size, Fingerprint: &fp})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || init.Offset != durable || len(init.VerifyChunks) != 3 {
		t.Fatalf("checkpoint init status=%d response=%#v", resp.StatusCode, init)
	}
	hashes := make([]string, 0, len(init.VerifyChunks))
	for _, checkpoint := range init.VerifyChunks {
		start := int(checkpoint.Offset)
		end := start + int(checkpoint.Size)
		sum := sha256.Sum256(source[start:end])
		hashes = append(hashes, hex.EncodeToString(sum[:]))
	}
	verifyBody, err := json.Marshal(uploadVerifyRequest{Offset: durable, ChunkSHA256: hashes})
	if err != nil {
		t.Fatal(err)
	}
	resp, err = http.Post(info.URLs[0]+"api/verify/"+url.PathEscape(init.UploadID), "application/json", bytes.NewReader(verifyBody))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("checkpoint verify status=%d body=%q", resp.StatusCode, body)
	}
}

func TestReceiveStreamingUploadCompletesExactBytesAndCleansCheckpoint(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer manager.Close()

	data := bytes.Repeat([]byte("streaming-phone-upload-0123456789"), 400000)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || init.UploadID == "" || init.Offset != 0 {
		t.Fatalf("init status=%d response=%#v", resp.StatusCode, init)
	}

	req, err := http.NewRequest(
		http.MethodPut,
		fmt.Sprintf("%sapi/stream/%s?offset=0", info.URLs[0], url.PathEscape(init.UploadID)),
		bytes.NewReader(data),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || out["complete"] != true || out["streamed"] != true {
		t.Fatalf("stream status=%d response=%#v", resp.StatusCode, out)
	}
	final, err := os.ReadFile(filepath.Join(destination, "movie.bin"))
	if err != nil || !bytes.Equal(final, data) {
		t.Fatalf("final len=%d err=%v", len(final), err)
	}
	if partials, err := filepath.Glob(filepath.Join(destination, ".movie.bin.resumexfer-part-*")); err != nil || len(partials) != 0 {
		t.Fatalf("partials=%v err=%v", partials, err)
	}
	if _, ok := store.Transfer(portalTransferID(info.ID, init.UploadID)); ok {
		t.Fatal("streamed upload left engine checkpoint state")
	}
	snapshot, ok := manager.Snapshot(info.ID)
	if !ok || snapshot.BytesDone != int64(len(data)) || snapshot.DoneFiles != 1 {
		t.Fatalf("snapshot=%#v ok=%v", snapshot, ok)
	}
}

func TestReceiveUploadWithStoreRestartsFromEngineCheckpoint(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	portalState := filepath.Join(root, "portal.json")
	transferState := filepath.Join(root, "transfers.json")
	store, err := state.Open(transferState)
	if err != nil {
		t.Fatal(err)
	}
	openManager := func(store *state.Store) *Manager {
		manager, err := Open(Config{
			ListenAddress: "127.0.0.1:0",
			HostAddresses: func() []string { return []string{"127.0.0.1"} },
			StatePath:     portalState,
			Store:         store,
		})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()

	data := bytes.Repeat([]byte("resumexfer-engine-checkpoint-"), 190000)
	firstWrite := int(engine.DefaultChunkSize + (1 << 20))
	if len(data) <= firstWrite {
		t.Fatalf("test data too small: %d", len(data))
	}
	manager := openManager(store)
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	base := info.URLs[0]
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := client.Post(base+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || init.Offset != 0 {
		t.Fatalf("init status=%d response=%#v", resp.StatusCode, init)
	}

	putChunkWithClient(t, client, base, init.UploadID, 0, data[:firstWrite], http.StatusOK)
	transferID := portalTransferID(info.ID, init.UploadID)
	transfer, ok := store.Transfer(transferID)
	if !ok || len(transfer.Chunks) != 1 || transfer.Chunks[0].Size != engine.DefaultChunkSize {
		t.Fatalf("engine checkpoint=%#v ok=%v", transfer, ok)
	}
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	if stat, err := os.Stat(partial); err != nil || stat.Size() != int64(firstWrite) {
		t.Fatalf("pre-restart partial=%#v err=%v", stat, err)
	}

	manager.mu.Lock()
	server := manager.server
	manager.server = nil
	manager.listener = nil
	manager.closed = true
	manager.mu.Unlock()
	if server != nil {
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	}

	reopenedStore, err := state.Open(transferState)
	if err != nil {
		t.Fatal(err)
	}
	manager = openManager(reopenedStore)
	defer manager.Close()
	restored, ok := manager.Snapshot(info.ID)
	if !ok || len(restored.URLs) == 0 {
		t.Fatalf("restored session=%#v ok=%v", restored, ok)
	}
	base = restored.URLs[0]
	resp, err = client.Post(base+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var resumed uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&resumed); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resumed.UploadID != init.UploadID || resumed.Offset != engine.DefaultChunkSize {
		t.Fatalf("resume status=%d response=%#v", resp.StatusCode, resumed)
	}
	if stat, err := os.Stat(partial); err != nil || stat.Size() != engine.DefaultChunkSize {
		t.Fatalf("verified partial=%#v err=%v", stat, err)
	}

	putChunkWithClient(t, client, base, resumed.UploadID, resumed.Offset, data[resumed.Offset:], http.StatusOK)
	final, err := os.ReadFile(filepath.Join(destination, "movie.bin"))
	if err != nil || !bytes.Equal(final, data) {
		t.Fatalf("final size=%d err=%v", len(final), err)
	}
	if _, ok := reopenedStore.Transfer(transferID); ok {
		t.Fatal("completed browser upload left engine transfer state")
	}
}

func TestReceiveUploadWithStoreRejectsCorruptEngineCheckpoint(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "destination")
	if err := os.MkdirAll(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "transfers.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	defer manager.Close()
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), int(engine.DefaultChunkSize+1024))
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:engine.DefaultChunkSize], http.StatusOK)
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	file, err := os.OpenFile(partial, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte("CORRUPT"), 0); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	resp, err = http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("corrupt resume status=%d body=%q", resp.StatusCode, body)
	}
	if _, err := os.Stat(filepath.Join(destination, "movie.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt upload produced final destination: %v", err)
	}
}

func TestSessionInfoRoutesPreserveLegacyURLs(t *testing.T) {
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostRoutes: func() []HostRoute {
			return []HostRoute{
				{Address: "192.0.2.10", Interface: "enp3s0", Kind: "ethernet"},
				{Address: "192.0.2.11", Interface: "wlp2s0", Kind: "wifi"},
			}
		},
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Routes) != 2 || len(info.URLs) != 2 {
		t.Fatalf("info=%#v", info)
	}
	for i, route := range info.Routes {
		if info.URLs[i] != route.URL {
			t.Fatalf("urls[%d]=%q route=%#v", i, info.URLs[i], route)
		}
	}
	if info.Routes[0].Kind != "ethernet" || info.Routes[0].Interface != "enp3s0" {
		t.Fatalf("first route=%#v", info.Routes[0])
	}
}

func TestClassifyNetworkInterface(t *testing.T) {
	root := t.TempDir()
	makeInterface := func(name string) string {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}

	wifi := makeInterface("wlp2s0")
	if err := os.Mkdir(filepath.Join(wifi, "wireless"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := classifyNetworkInterface(root, "wlp2s0"); got != "wifi" {
		t.Fatalf("wifi kind=%q", got)
	}

	for _, tc := range []struct {
		name   string
		target string
		want   string
	}{
		{name: "enp3s0", target: filepath.Join(root, "devices", "pci0000:00", "0000:00:1f.6"), want: "ethernet"},
		{name: "enxusb", target: filepath.Join(root, "devices", "pci0000:00", "usb1", "1-2"), want: "usb-network"},
		{name: "thunder0", target: filepath.Join(root, "devices", "pci0000:00", "thunderbolt", "0-1"), want: "thunderbolt-network"},
	} {
		iface := makeInterface(tc.name)
		if err := os.MkdirAll(tc.target, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(tc.target, filepath.Join(iface, "device")); err != nil {
			t.Fatal(err)
		}
		if got := classifyNetworkInterface(root, tc.name); got != tc.want {
			t.Fatalf("%s kind=%q want=%q", tc.name, got, tc.want)
		}
	}

	driverTarget := filepath.Join(root, "drivers", "thunderbolt-net")
	if err := os.MkdirAll(driverTarget, 0o700); err != nil {
		t.Fatal(err)
	}
	driverIface := makeInterface("weird0")
	driverDevice := filepath.Join(root, "devices", "virtual", "weird0")
	if err := os.MkdirAll(driverDevice, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(driverDevice, filepath.Join(driverIface, "device")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(driverTarget, filepath.Join(driverDevice, "driver")); err != nil {
		t.Fatal(err)
	}
	if got := classifyNetworkInterface(root, "weird0"); got != "thunderbolt-network" {
		t.Fatalf("thunderbolt driver kind=%q", got)
	}

	makeInterface("thunderbolt0")
	if got := classifyNetworkInterface(root, "thunderbolt0"); got != "thunderbolt-network" {
		t.Fatalf("virtual thunderbolt kind=%q", got)
	}
	makeInterface("usb0")
	if got := classifyNetworkInterface(root, "usb0"); got != "usb-network" {
		t.Fatalf("virtual usb gadget kind=%q", got)
	}
	makeInterface("usb-test")
	if got := classifyNetworkInterface(root, "usb-test"); got != "network" {
		t.Fatalf("nonstandard usb name kind=%q", got)
	}
	makeInterface("tun0")
	if got := classifyNetworkInterface(root, "tun0"); got != "network" {
		t.Fatalf("virtual kind=%q", got)
	}
}

func TestNetworkLinksFromSysfsReportsConnectedUnconfiguredLinks(t *testing.T) {
	root := t.TempDir()
	makeInterface := func(name, carrier string) string {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if carrier != "" {
			if err := os.WriteFile(filepath.Join(path, "carrier"), []byte(carrier), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return path
	}

	makeInterface("usb0", "1\n")
	makeInterface("thunderbolt0", "0\n")
	makeInterface("docker0", "1\n")
	makeInterface("br-4a9cd71855a3", "1\n")
	makeInterface("veth123456", "1\n")
	wifi := makeInterface("wlp2s0", "1\n")
	if err := os.Mkdir(filepath.Join(wifi, "wireless"), 0o700); err != nil {
		t.Fatal(err)
	}

	links := networkLinksFromSysfs(root, []HostRoute{{Address: "192.0.2.10", Interface: "wlp2s0", Kind: "wifi"}})
	if len(links) != 3 {
		t.Fatalf("links=%#v", links)
	}
	byName := make(map[string]LinkInfo, len(links))
	for _, link := range links {
		byName[link.Interface] = link
	}
	if got := byName["usb0"]; got.Kind != "usb-network" || got.State != "connected" || got.HasIPv4 {
		t.Fatalf("usb0=%#v", got)
	}
	if got := byName["thunderbolt0"]; got.Kind != "thunderbolt-network" || got.State != "disconnected" || got.HasIPv4 {
		t.Fatalf("thunderbolt0=%#v", got)
	}
	if got := byName["wlp2s0"]; got.Kind != "wifi" || got.State != "connected" || !got.HasIPv4 {
		t.Fatalf("wlp2s0=%#v", got)
	}
}

func TestSessionInfoExposesLinkReadiness(t *testing.T) {
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostRoutes: func() []HostRoute {
			return []HostRoute{{Address: "127.0.0.1", Interface: "lo", Kind: "loopback"}}
		},
		HostLinks: func() []LinkInfo {
			return []LinkInfo{{Interface: "usb0", Kind: "usb-network", State: "connected", HasIPv4: false}}
		},
	})
	t.Cleanup(func() { _ = manager.Close() })

	info, err := manager.StartReceive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Links) != 1 || info.Links[0].Interface != "usb0" || info.Links[0].HasIPv4 {
		t.Fatalf("links=%#v", info.Links)
	}

	resp, err := http.Get(info.URLs[0] + "api/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status struct {
		Links []LinkInfo `json:"links"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if len(status.Links) != 1 || status.Links[0].Kind != "usb-network" || status.Links[0].State != "connected" {
		t.Fatalf("status links=%#v", status.Links)
	}
}

func TestCloseReceiveCleansAbandonedPartialAndEngineState(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("init status=%d", resp.StatusCode)
	}
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:5], http.StatusOK)

	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("partial missing before close: %v", err)
	}
	transferID := portalTransferID(info.ID, init.UploadID)
	if _, ok := store.Transfer(transferID); !ok {
		t.Fatal("engine transfer missing before close")
	}

	if err := manager.CloseSession(info.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned partial survived close: %v", err)
	}
	if _, ok := store.Transfer(transferID); ok {
		t.Fatal("abandoned engine transfer survived close")
	}
	if _, err := os.Stat(filepath.Join(destination, "movie.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected final destination after close: %v", err)
	}
}

func TestExpiredReceiveCleansAbandonedPartialAndEngineState(t *testing.T) {
	now := time.Unix(1000, 0)
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		SessionTTL:    time.Minute,
		Now:           func() time.Time { return now },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:5], http.StatusOK)
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	transferID := portalTransferID(info.ID, init.UploadID)

	now = now.Add(2 * time.Minute)
	if _, ok := manager.Snapshot(info.ID); ok {
		t.Fatal("expired receive session still present")
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired partial survived: %v", err)
	}
	if _, ok := store.Transfer(transferID); ok {
		t.Fatal("expired engine transfer survived")
	}
}

func TestOpenCleansExpiredPersistedReceivePartialAndEngineState(t *testing.T) {
	now := time.Unix(1000, 0)
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	portalState := filepath.Join(root, "portal-state.json")
	transferState := filepath.Join(root, "state.json")
	store, err := state.Open(transferState)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		SessionTTL:    time.Minute,
		Now:           func() time.Time { return now },
		StatePath:     portalState,
		Store:         store,
	}
	manager, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	putChunk(t, info.URLs[0], init.UploadID, 0, data[:5], http.StatusOK)
	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	transferID := portalTransferID(info.ID, init.UploadID)
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("partial did not survive daemon shutdown: %v", err)
	}

	now = now.Add(2 * time.Minute)
	reopenedStore, err := state.Open(transferState)
	if err != nil {
		t.Fatal(err)
	}
	config.Store = reopenedStore
	manager, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	if _, ok := manager.Snapshot(info.ID); ok {
		t.Fatal("expired persisted receive session restored")
	}
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired persisted partial survived reopen: %v", err)
	}
	if _, ok := reopenedStore.Transfer(transferID); ok {
		t.Fatal("expired persisted engine transfer survived reopen")
	}
}

func TestCloseManagedReceivePreservesPartialAndReleasesLease(t *testing.T) {
	root := t.TempDir()
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	fp, err := fingerprint.ReaderAt(bytes.NewReader(data), int64(len(data)), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "movie.bin")
	partial := destination + engine.PartialSuffix
	if err := os.WriteFile(partial, data[:5], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := store.PutManifest(state.Manifest{
		ID:        "manifest-cleanup",
		Direction: "phone-to-laptop",
		Managed:   true,
		Entries: []state.ManifestEntry{{
			ID:                "entry-cleanup",
			Destination:       destination,
			Size:              int64(len(data)),
			BytesDone:         5,
			CreatedByJob:      true,
			SourceFingerprint: &fp,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartManifestReceive("manifest-cleanup", "entry-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	manager.mu.RLock()
	session := manager.sessions[info.ID]
	manager.mu.RUnlock()
	if session == nil {
		t.Fatal("managed session missing")
	}
	if err := manager.acquireManagedLease(session); err != nil {
		t.Fatal(err)
	}
	manifest, _ := store.Manifest("manifest-cleanup")
	if manifest.TransportOwner == "" {
		t.Fatal("managed lease was not acquired")
	}

	if err := manager.CloseSession(info.ID); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(partial)
	if err != nil {
		t.Fatalf("managed partial removed on close: %v", err)
	}
	if !bytes.Equal(got, data[:5]) {
		t.Fatalf("managed partial changed: %q", got)
	}
	manifest, _ = store.Manifest("manifest-cleanup")
	if manifest.TransportOwner != "" || !manifest.TransportLeaseUntil.IsZero() {
		t.Fatalf("managed lease survived close: %#v", manifest)
	}
}

type blockingBody struct {
	started chan struct{}
	release chan struct{}
	data    []byte
	once    sync.Once
	done    bool
}

func (b *blockingBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	b.once.Do(func() { close(b.started) })
	<-b.release
	b.done = true
	return copy(p, b.data), nil
}

func TestCloseReceiveWaitsForActiveWriterThenCleansPartial(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "receive")
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := state.Open(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		Store:         store,
	})
	t.Cleanup(func() { _ = manager.Close() })
	info, err := manager.StartReceive(destination)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("helloworld")
	payload := testReceiveInitPayload(t, "movie.bin", "movie.bin", data)
	resp, err := http.Post(info.URLs[0]+"api/init", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	var init uploadInitResponse
	if err := json.NewDecoder(resp.Body).Decode(&init); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()

	manager.mu.RLock()
	session := manager.sessions[info.ID]
	manager.mu.RUnlock()
	if session == nil {
		t.Fatal("receive session missing")
	}
	body := &blockingBody{started: make(chan struct{}), release: make(chan struct{}), data: data[:5]}
	req := httptest.NewRequest(http.MethodPut, "/api/upload/"+url.PathEscape(init.UploadID)+"?offset=0", body)
	recorder := httptest.NewRecorder()
	writeDone := make(chan struct{})
	go func() {
		manager.writeUploadChunk(recorder, req, session, init.UploadID)
		close(writeDone)
	}()
	<-body.started

	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.CloseSession(info.ID) }()
	select {
	case err := <-closeDone:
		t.Fatalf("close returned while writer still active: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(body.release)
	<-writeDone
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}

	partial := partialPath(filepath.Join(destination, "movie.bin"), info.ID, init.UploadID)
	if _, err := os.Stat(partial); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial survived active-writer close: %v", err)
	}
	if _, ok := store.Transfer(portalTransferID(info.ID, init.UploadID)); ok {
		t.Fatal("engine state survived active-writer close")
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("writer status=%d body=%q", recorder.Code, recorder.Body.Bytes())
	}
}

func TestCloseAndExpiryInvalidateCapabilityLink(t *testing.T) {
	now := time.Unix(1000, 0)
	root := t.TempDir()
	path := filepath.Join(root, "x.txt")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
		SessionTTL:    time.Minute,
		Now:           func() time.Time { return now },
	})
	defer manager.Close()
	first, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.CloseSession(first.ID); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(first.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("closed status=%d", resp.StatusCode)
	}

	second, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	resp, err = http.Get(second.URLs[0])
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("expired status=%d", resp.StatusCode)
	}
}

func putChunk(t *testing.T, base, uploadID string, offset int64, payload []byte, wantStatus int) {
	putChunkWithClient(t, http.DefaultClient, base, uploadID, offset, payload, wantStatus)
}

func putChunkWithClient(t *testing.T, client *http.Client, base, uploadID string, offset int64, payload []byte, wantStatus int) {
	t.Helper()
	u := base + "api/upload/" + url.PathEscape(uploadID) + "?offset=" + strconv.FormatInt(offset, 10)
	req, _ := http.NewRequest(http.MethodPut, u, bytes.NewReader(payload))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("chunk status=%d want=%d body=%q", resp.StatusCode, wantStatus, body)
	}
}
