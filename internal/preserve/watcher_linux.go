//go:build linux

package preserve

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type Watcher struct {
	manager *Manager
	fd      int
	mu      sync.RWMutex
	dirs    map[int]string
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
}

func NewWatcher(manager *Manager) (*Watcher, error) {
	fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if err != nil {
		return nil, err
	}
	w := &Watcher{manager: manager, fd: fd, dirs: map[int]string{}, stop: make(chan struct{}), done: make(chan struct{})}
	go w.loop()
	return w, nil
}

func (w *Watcher) AddDir(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return err
	}
	wd, err := syscall.InotifyAddWatch(w.fd, abs, syscall.IN_CREATE|syscall.IN_MOVED_TO|syscall.IN_CLOSE_WRITE)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.dirs[wd] = abs
	w.mu.Unlock()
	return nil
}

func (w *Watcher) Close() error {
	var err error
	w.once.Do(func() { close(w.stop); <-w.done; err = syscall.Close(w.fd) })
	return err
}

func (w *Watcher) loop() {
	defer close(w.done)
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-w.stop:
			return
		default:
		}
		n, err := syscall.Read(w.fd, buf)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EINTR) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		}
		off := 0
		for off+syscall.SizeofInotifyEvent <= n {
			ev := (*syscall.InotifyEvent)(unsafe.Pointer(&buf[off]))
			nameBytes := buf[off+syscall.SizeofInotifyEvent : off+syscall.SizeofInotifyEvent+int(ev.Len)]
			name := string(bytes.TrimRight(nameBytes, "\x00"))
			off += syscall.SizeofInotifyEvent + int(ev.Len)
			if name == "" || name == ".resumexfer-cache" || strings.HasPrefix(name, ".resumexfer-") || strings.HasSuffix(name, ".resumexfer-part") {
				continue
			}
			w.mu.RLock()
			dir := w.dirs[int(ev.Wd)]
			w.mu.RUnlock()
			if dir == "" {
				continue
			}
			path := filepath.Join(dir, name)
			info, statErr := os.Stat(path)
			if statErr != nil {
				continue
			}
			if info.IsDir() {
				_ = w.AddDir(path)
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			_, _ = w.manager.PreserveTracked(path)
		}
	}
}
