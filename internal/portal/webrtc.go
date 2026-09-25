package portal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

const (
	fastChunkSize       = 64 << 10
	fastAckWindow       = 64 << 20
	fastBufferedHigh    = 24 << 20
	fastBufferedLow     = 8 << 20
	fastSignalBodyLimit = 2 << 20
)

type fastControlMessage struct {
	Cmd    string `json:"cmd"`
	Index  int    `json:"index,omitempty"`
	Offset int64  `json:"offset,omitempty"`
}

type fastServerMessage struct {
	Type   string `json:"type"`
	Name   string `json:"name,omitempty"`
	Size   int64  `json:"size,omitempty"`
	Offset int64  `json:"offset,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (s *session) addFastPeer(pc *webrtc.PeerConnection) {
	s.fastMu.Lock()
	if s.fastPeers == nil {
		s.fastPeers = make(map[*webrtc.PeerConnection]struct{})
	}
	s.fastPeers[pc] = struct{}{}
	s.fastMu.Unlock()
}

func (s *session) removeFastPeer(pc *webrtc.PeerConnection) {
	s.fastMu.Lock()
	delete(s.fastPeers, pc)
	s.fastMu.Unlock()
}

func (s *session) closeFastPeers() {
	s.fastMu.Lock()
	peers := make([]*webrtc.PeerConnection, 0, len(s.fastPeers))
	for pc := range s.fastPeers {
		peers = append(peers, pc)
	}
	s.fastPeers = nil
	s.fastMu.Unlock()
	for _, pc := range peers {
		_ = pc.Close()
	}
}

func (m *Manager) serveFastOffer(w http.ResponseWriter, r *http.Request, s *session) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Mode != ModeSend {
		http.Error(w, "fast download is unavailable for this session", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, fastSignalBodyLimit)
	defer r.Body.Close()

	var offer webrtc.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		http.Error(w, "invalid WebRTC offer", http.StatusBadRequest)
		return
	}
	if offer.Type != webrtc.SDPTypeOffer || offer.SDP == "" {
		http.Error(w, "WebRTC offer required", http.StatusBadRequest)
		return
	}

	// No STUN/TURN server is configured intentionally. Resumexfer's wireless
	// portal is a same-LAN feature, so host candidates keep setup local and
	// avoid any internet dependency or relay.
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		http.Error(w, "could not create fast transfer connection", http.StatusInternalServerError)
		return
	}
	s.addFastPeer(pc)

	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			s.removeFastPeer(pc)
			_ = pc.Close()
		})
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			cleanup()
		}
	})
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() != "resumexfer-file" {
			_ = dc.Close()
			return
		}
		m.bindFastDataChannel(s, dc)
	})

	if err := pc.SetRemoteDescription(offer); err != nil {
		cleanup()
		http.Error(w, "could not accept WebRTC offer", http.StatusBadRequest)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		cleanup()
		http.Error(w, "could not create WebRTC answer", http.StatusInternalServerError)
		return
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		cleanup()
		http.Error(w, "could not set WebRTC answer", http.StatusInternalServerError)
		return
	}

	select {
	case <-gatherComplete:
	case <-time.After(5 * time.Second):
		cleanup()
		http.Error(w, "WebRTC candidate gathering timed out", http.StatusGatewayTimeout)
		return
	case <-s.closedCh:
		cleanup()
		http.Error(w, "session closed", http.StatusGone)
		return
	}

	local := pc.LocalDescription()
	if local == nil {
		cleanup()
		http.Error(w, "WebRTC answer unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, local)
}

func (m *Manager) bindFastDataChannel(s *session, dc *webrtc.DataChannel) {
	startCh := make(chan fastControlMessage, 1)
	ackCh := make(chan int64, 32)
	lowCh := make(chan struct{}, 1)
	closedCh := make(chan struct{})
	var closeOnce sync.Once

	closeSignal := func() {
		closeOnce.Do(func() { close(closedCh) })
	}
	dc.SetBufferedAmountLowThreshold(fastBufferedLow)
	dc.OnBufferedAmountLow(func() {
		select {
		case lowCh <- struct{}{}:
		default:
		}
	})
	dc.OnClose(closeSignal)
	dc.OnError(func(error) { closeSignal() })
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if !msg.IsString {
			return
		}
		var control fastControlMessage
		if err := json.Unmarshal(msg.Data, &control); err != nil {
			return
		}
		switch control.Cmd {
		case "start":
			select {
			case startCh <- control:
			default:
			}
		case "ack":
			select {
			case ackCh <- control.Offset:
			default:
				select {
				case <-ackCh:
				default:
				}
				select {
				case ackCh <- control.Offset:
				default:
				}
			}
		}
	})
	dc.OnOpen(func() {
		go func() {
			select {
			case start := <-startCh:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go func() {
					select {
					case <-closedCh:
						cancel()
					case <-s.closedCh:
						cancel()
					case <-ctx.Done():
					}
				}()
				if err := m.streamFastFile(ctx, s, dc, start, ackCh, lowCh); err != nil &&
					!errors.Is(err, context.Canceled) && !errors.Is(err, errPortalSessionClosed) {
					_ = sendFastJSON(dc, fastServerMessage{Type: "error", Error: err.Error()})
				}
			case <-closedCh:
			case <-s.closedCh:
			}
		}()
	})
}

func (m *Manager) streamFastFile(
	ctx context.Context,
	s *session,
	dc *webrtc.DataChannel,
	start fastControlMessage,
	ackCh <-chan int64,
	lowCh <-chan struct{},
) error {
	s.mu.RLock()
	if start.Index < 0 || start.Index >= len(s.Entries) {
		s.mu.RUnlock()
		return fmt.Errorf("shared file index is invalid")
	}
	entry := s.Entries[start.Index]
	s.mu.RUnlock()

	offset := start.Offset
	if offset < 0 || offset > entry.Size {
		return fmt.Errorf("resume offset is invalid")
	}

	file, err := os.Open(entry.Path)
	if err != nil {
		return fmt.Errorf("shared file is unavailable")
	}
	defer file.Close()
	matches, err := shareEntryMatches(file, entry)
	if err != nil || !matches {
		return fmt.Errorf("shared file changed after sharing started")
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if offset > 0 {
		m.recordDownload(s, start.Index, 0, offset)
	}
	if err := sendFastJSON(dc, fastServerMessage{
		Type:   "meta",
		Name:   entry.Name,
		Size:   entry.Size,
		Offset: offset,
	}); err != nil {
		return err
	}

	buf := make([]byte, fastChunkSize)
	sent := offset
	acked := offset

	for sent < entry.Size {
		for sent-acked >= fastAckWindow {
			next, err := waitFastAck(ctx, s, ackCh, acked, sent)
			if err != nil {
				return err
			}
			if next > acked {
				m.recordDownload(s, start.Index, acked, next)
				acked = next
			}
		}
		if err := waitFastBuffer(ctx, s, dc, lowCh); err != nil {
			return err
		}
		if err := s.waitUntilResumed(ctx); err != nil {
			if err.Error() == "portal session closed" {
				return errPortalSessionClosed
			}
			return err
		}

		want := int64(len(buf))
		if remain := entry.Size - sent; remain < want {
			want = remain
		}
		n, readErr := io.ReadFull(file, buf[:int(want)])
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		if err := dc.Send(buf[:n]); err != nil {
			return err
		}
		sent += int64(n)
	}

	if entry.Size == 0 {
		m.recordDownload(s, start.Index, 0, 0)
	}
	for acked < entry.Size {
		next, err := waitFastAck(ctx, s, ackCh, acked, sent)
		if err != nil {
			return err
		}
		if next > acked {
			m.recordDownload(s, start.Index, acked, next)
			acked = next
		}
	}
	if err := sendFastJSON(dc, fastServerMessage{Type: "complete", Size: entry.Size, Offset: entry.Size}); err != nil {
		return err
	}
	return m.markDeliveryComplete(s, start.Index)
}

func waitFastAck(ctx context.Context, s *session, ackCh <-chan int64, acked, sent int64) (int64, error) {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return acked, ctx.Err()
		case <-s.closedCh:
			return acked, errPortalSessionClosed
		case next := <-ackCh:
			if next > sent {
				next = sent
			}
			if next > acked {
				return next, nil
			}
		case <-timer.C:
			return acked, fmt.Errorf("phone stopped acknowledging transfer progress")
		}
	}
}

func waitFastBuffer(ctx context.Context, s *session, dc *webrtc.DataChannel, lowCh <-chan struct{}) error {
	for dc.BufferedAmount() > fastBufferedHigh {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closedCh:
			return errPortalSessionClosed
		case <-lowCh:
		case <-time.After(10 * time.Second):
			if dc.BufferedAmount() > fastBufferedHigh {
				return fmt.Errorf("fast transfer connection stalled")
			}
		}
	}
	return nil
}

func sendFastJSON(dc *webrtc.DataChannel, value fastServerMessage) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return dc.SendText(string(data))
}
