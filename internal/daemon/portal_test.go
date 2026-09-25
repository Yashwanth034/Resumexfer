package daemon

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"resumexfer/internal/fingerprint"
	"resumexfer/internal/portal"
)

type fakePortal struct {
	sharePaths         []string
	receiveDestination string
	manifestID         string
	entryID            string
	closedID           string
	pausedID           string
	pausedValue        bool
	info               portal.SessionInfo
	snapshotOK         bool
	err                error
}

type fakeLinkSetupper struct {
	interfaces []string
	err        error
}

func (f *fakeLinkSetupper) EnableIPv4LinkLocal(iface string) error {
	f.interfaces = append(f.interfaces, iface)
	return f.err
}

func (f *fakePortal) StartShare(paths []string) (portal.SessionInfo, error) {
	f.sharePaths = append([]string(nil), paths...)
	return f.info, f.err
}

func (f *fakePortal) StartManifestShare(manifestID string) (portal.SessionInfo, error) {
	f.manifestID = manifestID
	return f.info, f.err
}

func (f *fakePortal) StartReceive(destination string) (portal.SessionInfo, error) {
	f.receiveDestination = destination
	return f.info, f.err
}

func (f *fakePortal) StartManifestReceive(manifestID, entryID string) (portal.SessionInfo, error) {
	f.manifestID = manifestID
	f.entryID = entryID
	return f.info, f.err
}

func (f *fakePortal) HasReceivePartial(destination string, size int64) bool {
	return false
}

func (f *fakePortal) AdoptReceivePartial(destination string, source io.ReaderAt, size int64, fp fingerprint.Fingerprint) (portal.ReceiveAdoption, bool, error) {
	return portal.ReceiveAdoption{}, false, f.err
}

func (f *fakePortal) Snapshot(id string) (portal.SessionInfo, bool) {
	return f.info, f.snapshotOK
}

func (f *fakePortal) CloseSession(id string) error {
	f.closedID = id
	return f.err
}

func (f *fakePortal) CancelSession(id string) error {
	f.closedID = id
	return f.err
}

func (f *fakePortal) SetPaused(id string, paused bool) error {
	f.pausedID = id
	f.pausedValue = paused
	return f.err
}

func TestHandlePortalShareReceiveStatusAndClose(t *testing.T) {
	want := portal.SessionInfo{ID: "session-1", Mode: portal.ModeSend, URLs: []string{"http://192.0.2.1:1234/s/token/"}, Complete: true}
	fake := &fakePortal{info: want, snapshotOK: true}
	server := New(Config{Portal: fake})

	data, handled, err := server.handlePortal(request{Action: "portal_share", Paths: []string{"/tmp/a", "/tmp/b"}})
	if err != nil || !handled || !reflect.DeepEqual(data, want) {
		t.Fatalf("share data=%#v handled=%v err=%v", data, handled, err)
	}
	if !reflect.DeepEqual(fake.sharePaths, []string{"/tmp/a", "/tmp/b"}) {
		t.Fatalf("share paths=%#v", fake.sharePaths)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_share_manifest", ManifestID: "manifest-send"})
	if err != nil || !handled || !reflect.DeepEqual(data, want) || fake.manifestID != "manifest-send" {
		t.Fatalf("manifest share data=%#v handled=%v manifest=%q err=%v", data, handled, fake.manifestID, err)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_receive", Destination: "/tmp/out"})
	if err != nil || !handled || !reflect.DeepEqual(data, want) || fake.receiveDestination != "/tmp/out" {
		t.Fatalf("receive data=%#v handled=%v dest=%q err=%v", data, handled, fake.receiveDestination, err)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_continue_manifest", ManifestID: "manifest-1", EntryID: "entry-1"})
	if err != nil || !handled || !reflect.DeepEqual(data, want) || fake.manifestID != "manifest-1" || fake.entryID != "entry-1" {
		t.Fatalf("continue data=%#v handled=%v manifest=%q entry=%q err=%v", data, handled, fake.manifestID, fake.entryID, err)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_status", SessionID: "session-1"})
	if err != nil || !handled || !reflect.DeepEqual(data, want) {
		t.Fatalf("status data=%#v handled=%v err=%v", data, handled, err)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_pause", SessionID: "session-1"})
	if err != nil || !handled || data != nil || fake.pausedID != "session-1" || !fake.pausedValue {
		t.Fatalf("pause data=%#v handled=%v id=%q paused=%v err=%v", data, handled, fake.pausedID, fake.pausedValue, err)
	}
	data, handled, err = server.handlePortal(request{Action: "portal_resume", SessionID: "session-1"})
	if err != nil || !handled || data != nil || fake.pausedID != "session-1" || fake.pausedValue {
		t.Fatalf("resume data=%#v handled=%v id=%q paused=%v err=%v", data, handled, fake.pausedID, fake.pausedValue, err)
	}

	data, handled, err = server.handlePortal(request{Action: "portal_cancel", SessionID: "session-1"})
	if err != nil || !handled || data != nil || fake.closedID != "session-1" {
		t.Fatalf("cancel data=%#v handled=%v id=%q err=%v", data, handled, fake.closedID, err)
	}
	fake.closedID = ""
	data, handled, err = server.handlePortal(request{Action: "portal_close", SessionID: "session-1"})
	if err != nil || !handled || data != nil || fake.closedID != "session-1" {
		t.Fatalf("close data=%#v handled=%v id=%q err=%v", data, handled, fake.closedID, err)
	}
}

func TestHandlePortalValidatesRequestsAndPropagatesErrors(t *testing.T) {
	server := New(Config{})
	for _, req := range []request{
		{Action: "portal_share", Paths: []string{"/tmp/a"}},
		{Action: "portal_share_manifest", ManifestID: "m"},
		{Action: "portal_receive", Destination: "/tmp"},
		{Action: "portal_continue_manifest", ManifestID: "m", EntryID: "e"},
		{Action: "portal_status", SessionID: "x"},
		{Action: "portal_pause", SessionID: "x"},
		{Action: "portal_resume", SessionID: "x"},
		{Action: "portal_cancel", SessionID: "x"},
		{Action: "portal_close", SessionID: "x"},
	} {
		_, handled, err := server.handlePortal(req)
		if !handled || err == nil {
			t.Fatalf("request=%#v handled=%v err=%v", req, handled, err)
		}
	}

	fake := &fakePortal{err: errors.New("boom")}
	server = New(Config{Portal: fake})
	_, handled, err := server.handlePortal(request{Action: "portal_share", Paths: []string{"/tmp/a"}})
	if !handled || !errors.Is(err, fake.err) {
		t.Fatalf("handled=%v err=%v", handled, err)
	}

	_, handled, err = server.handlePortal(request{Action: "not-portal"})
	if handled || err != nil {
		t.Fatalf("non-portal handled=%v err=%v", handled, err)
	}
}

func TestHandlePortalPrepareLinkAllowsOnlyLiveUnconfiguredWiredInterface(t *testing.T) {
	setup := &fakeLinkSetupper{}
	fake := &fakePortal{
		snapshotOK: true,
		info: portal.SessionInfo{Links: []portal.LinkInfo{
			{Interface: "usb0", Kind: "usb-network", State: "connected", HasIPv4: false},
			{Interface: "wlp2s0", Kind: "wifi", State: "connected", HasIPv4: false},
			{Interface: "enp3s0", Kind: "ethernet", State: "disconnected", HasIPv4: false},
			{Interface: "enp4s0", Kind: "ethernet", State: "connected", HasIPv4: true},
		}},
	}
	server := New(Config{Portal: fake, LinkSetupper: setup})

	data, handled, err := server.handlePortal(request{Action: "portal_prepare_link", SessionID: "session-1", Interface: "usb0"})
	if err != nil || !handled || data != nil {
		t.Fatalf("data=%#v handled=%v err=%v", data, handled, err)
	}
	if !reflect.DeepEqual(setup.interfaces, []string{"usb0"}) {
		t.Fatalf("setup interfaces=%#v", setup.interfaces)
	}

	for _, iface := range []string{"wlp2s0", "enp3s0", "enp4s0", "unknown0"} {
		before := len(setup.interfaces)
		_, handled, err := server.handlePortal(request{Action: "portal_prepare_link", SessionID: "session-1", Interface: iface})
		if !handled || err == nil {
			t.Fatalf("iface=%q handled=%v err=%v", iface, handled, err)
		}
		if len(setup.interfaces) != before {
			t.Fatalf("unsafe iface=%q invoked setup", iface)
		}
	}
}

func TestHandlePortalPrepareLinkPropagatesSetupFailure(t *testing.T) {
	wantErr := errors.New("nmcli failed")
	setup := &fakeLinkSetupper{err: wantErr}
	fake := &fakePortal{snapshotOK: true, info: portal.SessionInfo{Links: []portal.LinkInfo{{
		Interface: "thunderbolt0", Kind: "thunderbolt-network", State: "connected", HasIPv4: false,
	}}}}
	server := New(Config{Portal: fake, LinkSetupper: setup})
	_, handled, err := server.handlePortal(request{Action: "portal_prepare_link", SessionID: "session-1", Interface: "thunderbolt0"})
	if !handled || !errors.Is(err, wantErr) {
		t.Fatalf("handled=%v err=%v", handled, err)
	}
}
