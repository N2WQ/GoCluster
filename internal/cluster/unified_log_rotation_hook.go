package cluster

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// propRotationHook is the bounded capture retained by the file owner. Clearing
// it on shutdown prevents an indefinitely blocked file operation from retaining
// an entire retired runtime through a callback closure.
type propRotationHook struct {
	mu        sync.Mutex
	scheduler *propReportScheduler
	root      *os.Root // Borrowed from the verified propagation logger.
	closed    bool
}

func (h *propRotationHook) setRoot(root *os.Root) {
	h.mu.Lock()
	if !h.closed {
		h.root = root
	}
	h.mu.Unlock()
}

func (h *propRotationHook) reportRoot() *os.Root {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	return h.root
}

func (h *propRotationHook) setScheduler(s *propReportScheduler) {
	h.mu.Lock()
	if !h.closed {
		h.scheduler = s
	}
	h.mu.Unlock()
}

func (h *propRotationHook) onRotate(previous time.Time, previousPath, newPath string) {
	activeName := filepath.Base(newPath)
	if previous.IsZero() || previousPath == "" || strings.EqualFold(filepath.Base(previousPath), activeName) || (runtime.GOOS == "windows" && strings.Contains(activeName, "~")) {
		return
	}
	h.mu.Lock()
	scheduler := h.scheduler
	h.mu.Unlock()
	if scheduler != nil {
		scheduler.Enqueue(propReportJob{Date: previous, LogPath: previousPath})
	}
}

// onRootedRotate acquires a bounded snapshot before rotation cleanup. The
// caller is the propagation file worker, so a stuck open cannot block peers.
func (h *propRotationHook) onRootedRotate(previous time.Time, previousPath, newPath string, root *os.Root) {
	activeName := filepath.Base(newPath)
	if root == nil || previous.IsZero() || previousPath == "" || strings.EqualFold(filepath.Base(previousPath), activeName) || (runtime.GOOS == "windows" && strings.Contains(activeName, "~")) {
		return
	}
	h.mu.Lock()
	scheduler := h.scheduler
	h.mu.Unlock()
	if scheduler == nil {
		return
	}
	lease := reserveReportArchive()
	if lease.generation == 0 {
		return
	}
	owner := &logCloseOwner{}
	file, err := openSafeLogInRootOwned(root, filepath.Base(previousPath), os.O_RDONLY, 0, owner)
	if err != nil {
		if owner.err != nil {
			lease.retainUncertain(owner)
			return
		}
		_ = lease.release(false) //nolint:errcheck // No file was acquired.
		return
	}
	if !lease.set(file) {
		_ = file.Close()
		_ = lease.release(false) //nolint:errcheck // Failed close keeps a reserved owner.
		return
	}
	if !scheduler.Enqueue(propReportJob{Date: previous, LogPath: previousPath, archive: lease}) {
		_ = lease.release(false) //nolint:errcheck // Failed close keeps the bounded owner reserved.
	}
}

func (h *propRotationHook) close() {
	h.mu.Lock()
	h.closed = true
	h.scheduler = nil
	h.root = nil
	h.mu.Unlock()
}
