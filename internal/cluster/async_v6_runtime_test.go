package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/peerdiag"
)

func TestV6RuntimeDoesNotWaitPastDeadlineForDashboard(t *testing.T) {
	v6SurfaceGate.Lock()
	if v6SurfaceGate.occupied {
		v6SurfaceGate.Unlock()
		t.Fatal("dashboard gate unexpectedly occupied")
	}
	v6SurfaceGate.occupied = true
	v6SurfaceGate.Unlock()
	surface := &v6Surface{created: make(chan struct{}), ready: make(chan struct{}), done: make(chan struct{})}
	runtime := &clusterRuntime{surface: surface, v6Logging: &v6LogRuntime{}}
	started := time.Now()
	runtime.closeV6Logging(started.Add(50 * time.Millisecond))
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Errorf("shutdown exceeded deadline: %s", elapsed)
	}
	close(surface.created)
	close(surface.ready)
	select {
	case <-surface.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("dashboard did not retire after its stalled constructor returned")
	}
}

func TestV6LoggingConflictRequestsRuntimeShutdown(t *testing.T) {
	l := &v6LogRuntime{conflicts: make(chan [2]uint8, 1)}
	// A second report must not block a destination worker when one is pending.
	l.reportConflict(streamSystemFile, streamPropagation)
	l.reportConflict(streamPeerConnections, streamPeerOverlong)
	r := &clusterRuntime{v6Logging: l}
	err := r.waitForShutdown()
	if err == nil || !strings.Contains(err.Error(), "system") || !strings.Contains(err.Error(), "propagation") {
		t.Fatalf("shutdown error = %v", err)
	}
}

func TestV6LoggingConflictStopsBeforeNetworkStart(t *testing.T) {
	l := &v6LogRuntime{conflicts: make(chan [2]uint8, 1)}
	l.reportConflict(streamSystemFile, streamPropagation)
	r := &clusterRuntime{v6Logging: l}
	if r.startNetworkServices() || r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "propagation") {
		t.Fatalf("conflict did not stop startup: %v", r.startupErr)
	}
}

func TestV6RuntimeWritesSystemAndPeerFiles(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	l, err := newV6LogRuntime(&output)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: filepath.Join(root, "system"), RetentionDays: 7}
	cfg.PeerConnections = config.EventFileLoggingConfig{Enabled: true, Dir: filepath.Join(root, "peers"), RetentionDays: 7}
	l.startFiles(cfg, nil, true)
	_, _ = l.fanout.Write([]byte("system-v6-message\n"))
	peer := peerdiag.New(true, &l.files, streamPeerConnections, streamPeerOverlong)
	if !peer.Emit(peerdiag.Connection, peerdiag.Fields{Action: "connected"}) {
		t.Fatal("peer record was not admitted")
	}
	if l.files.Emit(streamPeerConnections, time.Now().UTC(), []byte(strings.Repeat("x", 2049))) {
		t.Fatal("oversized peer record was admitted")
	}
	deadline := time.Now().Add(3 * time.Second)
	for peer.Snapshot().CurrentWritten == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := peer.Snapshot(); got.CurrentWritten == 0 || got.Generation == 0 || got.ChargedBytes != 0 {
		t.Fatalf("peer logging status = %+v", got)
	}
	peer.Retire()
	l.close(time.Now().Add(3 * time.Second))
	for path, want := range map[string]string{
		filepath.Join(root, "system", "system.log"): "system-v6-message",
		filepath.Join(root, "peers", "peers.log"):   "event=peer_connection action=connected",
	} {
		data, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(data), want) {
			t.Errorf("read %s = %q, %v", path, data, err)
		}
	}
}

func TestV6RuntimeBlockedConsoleDoesNotBlockHealthyFileOrShutdown(t *testing.T) {
	root := t.TempDir()
	w := &stalledConsoleWriter{entered: make(chan struct{}), release: make(chan struct{})}
	l, err := newV6LogRuntime(w)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: filepath.Join(root, "system"), RetentionDays: 7}
	l.startFiles(cfg, nil, true)
	_, _ = l.fanout.Write([]byte("independent-file-output\n"))
	select {
	case <-w.entered:
	case <-time.After(3 * time.Second):
		close(w.release)
		t.Fatal("console was not entered")
	}
	started := time.Now()
	l.close(started.Add(100 * time.Millisecond))
	if elapsed := time.Since(started); elapsed > time.Second {
		close(w.release)
		t.Fatalf("shutdown took %s", elapsed)
	}
	data, err := os.ReadFile(filepath.Join(root, "system", "system.log"))
	close(w.release)
	if err != nil || !strings.Contains(string(data), "independent-file-output") {
		t.Fatalf("healthy file = %q, %v", data, err)
	}
}

func TestV6QuietFileStillCleansExpiredArchivesAtStartup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, "01-Jan-2000.log")
	if err := os.WriteFile(archive, []byte("expired"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(&bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}, nil, true)
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err = os.Stat(archive)
		if os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			l.close(time.Now().Add(time.Second))
			t.Fatalf("quiet log did not clean archive: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	l.close(time.Now().Add(2 * time.Second))
}

func TestV6PeerStatusTracksRealFileFailureAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(path, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(&bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{RetentionDays: 7}
	cfg.PeerConnections = config.EventFileLoggingConfig{Enabled: true, Dir: path, RetentionDays: 7}
	l.startFiles(cfg, nil, true)
	service := peerdiag.New(true, &l.files, streamPeerConnections, streamPeerOverlong)
	if !service.Emit(peerdiag.Connection, peerdiag.Fields{Action: "connected"}) {
		t.Fatal("record not admitted")
	}
	deadline := time.Now().Add(3 * time.Second)
	for service.Snapshot().Dropped == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := service.Snapshot(); status.Dropped == 0 || status.CurrentWritten != 0 || !status.Degraded {
		l.close(time.Now().Add(time.Second))
		t.Fatalf("failed write status = %+v", status)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if !service.Emit(peerdiag.Connection, peerdiag.Fields{Action: "connected"}) {
		t.Fatal("recovery record not admitted")
	}
	for service.Snapshot().CurrentWritten == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if status := service.Snapshot(); status.CurrentWritten == 0 || status.Degraded {
		l.close(time.Now().Add(time.Second))
		t.Fatalf("recovered write status = %+v", status)
	}
	service.Retire()
	l.close(time.Now().Add(2 * time.Second))
}
