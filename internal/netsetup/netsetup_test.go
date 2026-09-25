package netsetup

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEnableIPv4LinkLocalUsesTemporaryDeviceModification(t *testing.T) {
	var gotName string
	var gotArgs []string
	manager := Manager{Run: func(name string, args ...string) ([]byte, error) {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return nil, nil
	}}
	if err := manager.EnableIPv4LinkLocal("usb0"); err != nil {
		t.Fatal(err)
	}
	if gotName != "nmcli" {
		t.Fatalf("command=%q", gotName)
	}
	want := []string{"--wait", "10", "device", "modify", "usb0", "ipv4.link-local", "enabled"}
	if !reflect.DeepEqual(gotArgs, want) {
		t.Fatalf("args=%#v want=%#v", gotArgs, want)
	}
}

func TestEnableIPv4LinkLocalRejectsUnsafeInterfaceBeforeCommand(t *testing.T) {
	called := false
	manager := Manager{Run: func(string, ...string) ([]byte, error) {
		called = true
		return nil, nil
	}}
	for _, iface := range []string{"", "-wlan0", "eth0;reboot", "interface-name-is-too-long"} {
		if err := manager.EnableIPv4LinkLocal(iface); err == nil {
			t.Fatalf("iface=%q unexpectedly accepted", iface)
		}
	}
	if called {
		t.Fatal("network command ran for invalid interface")
	}
}

func TestEnableIPv4LinkLocalReturnsBoundedCommandFailure(t *testing.T) {
	manager := Manager{Run: func(string, ...string) ([]byte, error) {
		return []byte("device has no active connection\n"), errors.New("exit status 10")
	}}
	err := manager.EnableIPv4LinkLocal("enp3s0")
	if err == nil || !strings.Contains(err.Error(), "device has no active connection") {
		t.Fatalf("err=%v", err)
	}
}
