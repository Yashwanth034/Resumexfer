package portal

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func TestFastWebRTCDownloadTransfersExactBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "fast.bin")
	data := bytes.Repeat([]byte("resumexfer-fast-path-"), 65536)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	defer manager.Close()
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.URLs) != 1 {
		t.Fatalf("urls=%v", info.URLs)
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	dc, err := pc.CreateDataChannel("resumexfer-file", nil)
	if err != nil {
		t.Fatal(err)
	}

	var received bytes.Buffer
	complete := make(chan struct{}, 1)
	fail := make(chan error, 1)
	dc.OnOpen(func() {
		msg, _ := json.Marshal(fastControlMessage{Cmd: "start", Index: 0, Offset: 0})
		if err := dc.SendText(string(msg)); err != nil {
			select {
			case fail <- err:
			default:
			}
		}
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			var control fastServerMessage
			if err := json.Unmarshal(msg.Data, &control); err != nil {
				select {
				case fail <- err:
				default:
				}
				return
			}
			switch control.Type {
			case "error":
				select {
				case fail <- io.ErrUnexpectedEOF:
				default:
				}
			case "complete":
				select {
				case complete <- struct{}{}:
				default:
				}
			}
			return
		}
		_, _ = received.Write(msg.Data)
		ack, _ := json.Marshal(fastControlMessage{Cmd: "ack", Offset: int64(received.Len())})
		if err := dc.SendText(string(ack)); err != nil {
			select {
			case fail <- err:
			default:
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		t.Fatal("client ICE gathering timed out")
	}

	body, err := json.Marshal(pc.LocalDescription())
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(info.URLs[0]+"api/fast/offer", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(resp.Body)
		t.Fatalf("offer status=%d body=%s", resp.StatusCode, detail)
	}
	var answer webrtc.SessionDescription
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		t.Fatal(err)
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-fail:
		t.Fatal(err)
	case <-complete:
	case <-time.After(15 * time.Second):
		t.Fatalf("fast transfer timed out after receiving %d/%d bytes", received.Len(), len(data))
	}
	if !bytes.Equal(received.Bytes(), data) {
		t.Fatalf("received bytes mismatch: got=%d want=%d", received.Len(), len(data))
	}

	snapshot, ok := manager.Snapshot(info.ID)
	if !ok {
		t.Fatal("session disappeared")
	}
	if snapshot.BytesDone != int64(len(data)) || snapshot.DoneFiles != 1 {
		t.Fatalf("progress=%d/%d doneFiles=%d", snapshot.BytesDone, len(data), snapshot.DoneFiles)
	}
}

func TestFastWebRTCDownloadResumesFromStoredOffset(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "resume.bin")
	data := bytes.Repeat([]byte("0123456789abcdef"), 131072)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	const offset = int64(384 << 10)

	manager := New(Config{
		ListenAddress: "127.0.0.1:0",
		HostAddresses: func() []string { return []string{"127.0.0.1"} },
	})
	defer manager.Close()
	info, err := manager.StartShare([]string{path})
	if err != nil {
		t.Fatal(err)
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	dc, err := pc.CreateDataChannel("resumexfer-file", nil)
	if err != nil {
		t.Fatal(err)
	}

	var received bytes.Buffer
	complete := make(chan struct{}, 1)
	dc.OnOpen(func() {
		msg, _ := json.Marshal(fastControlMessage{Cmd: "start", Index: 0, Offset: offset})
		_ = dc.SendText(string(msg))
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			var control fastServerMessage
			_ = json.Unmarshal(msg.Data, &control)
			if control.Type == "complete" {
				select {
				case complete <- struct{}{}:
				default:
				}
			}
			return
		}
		_, _ = received.Write(msg.Data)
		ack, _ := json.Marshal(fastControlMessage{Cmd: "ack", Offset: offset + int64(received.Len())})
		_ = dc.SendText(string(ack))
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		t.Fatal("client ICE gathering timed out")
	}
	body, _ := json.Marshal(pc.LocalDescription())
	resp, err := http.Post(info.URLs[0]+"api/fast/offer", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(resp.Body)
		t.Fatalf("offer status=%d body=%s", resp.StatusCode, detail)
	}
	var answer webrtc.SessionDescription
	if err := json.NewDecoder(resp.Body).Decode(&answer); err != nil {
		t.Fatal(err)
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}

	select {
	case <-complete:
	case <-time.After(15 * time.Second):
		t.Fatalf("resume timed out after %d bytes", received.Len())
	}
	if !bytes.Equal(received.Bytes(), data[offset:]) {
		t.Fatalf("resumed suffix mismatch got=%d want=%d", received.Len(), len(data)-int(offset))
	}
}
