package netsetup

import (
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

var interfaceNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,14}$`)

type Runner func(name string, args ...string) ([]byte, error)

type Manager struct {
	Run Runner
}

func (m Manager) EnableIPv4LinkLocal(iface string) error {
	iface = strings.TrimSpace(iface)
	if !interfaceNamePattern.MatchString(iface) {
		return fmt.Errorf("invalid network interface %q", iface)
	}
	run := m.Run
	if run == nil {
		if _, err := exec.LookPath("nmcli"); err != nil {
			return fmt.Errorf("NetworkManager nmcli is unavailable: %w", err)
		}
		run = func(name string, args ...string) ([]byte, error) {
			return exec.Command(name, args...).CombinedOutput()
		}
	}
	output, err := run("nmcli", "--wait", "10", "device", "modify", iface, "ipv4.link-local", "enabled")
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return fmt.Errorf("could not enable temporary IPv4 link-local on %s: %w", iface, err)
		}
		return fmt.Errorf("could not enable temporary IPv4 link-local on %s: %s", iface, message)
	}
	return nil
}
