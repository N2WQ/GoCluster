package cluster

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func TestV7PhysicalFolderAliasRejectsBeforeCleanup(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	archive := filepath.Join(dir, "01-Jan-2000.log")
	if err := os.WriteFile(archive, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 1}
	cfg.Propagation.Enabled, cfg.Propagation.Dir = true, alias
	l.startFiles(cfg, nil, true)
	l.fanout.Write([]byte("must not be written\n"))
	select {
	case <-l.conflicts:
	case <-time.After(3 * time.Second):
		l.close(time.Now().Add(time.Second))
		t.Fatal("physical alias was not reported")
	}
	l.close(time.Now().Add(3 * time.Second))
	if data, err := os.ReadFile(archive); err != nil || string(data) != "preserve" {
		t.Fatalf("archive modified before conflict: %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "system.log")); !os.IsNotExist(err) {
		t.Fatalf("active log created before conflict: %v", err)
	}
}

func TestV7PhysicalAliasStopsRuntime(t *testing.T) {
	root := t.TempDir()
	dir, alias := filepath.Join(root, "system"), filepath.Join(root, "alias")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.close(time.Now().Add(3 * time.Second)) }()
	runtime := &clusterRuntime{v6Logging: l}
	result := make(chan error, 1)
	go func() { result <- runtime.waitForShutdown() }()
	cfg := config.LoggingConfig{Enabled: true, Dir: dir}
	cfg.Propagation.Enabled, cfg.Propagation.Dir = true, alias
	l.startFiles(cfg, nil, false)
	select {
	case err := <-result:
		if err == nil || !strings.Contains(err.Error(), "system") || !strings.Contains(err.Error(), "propagation") {
			t.Fatalf("runtime shutdown error = %v", err)
		}
	case <-time.After(3 * time.Second):
		l.reportConflict(streamSystemFile, streamPropagation)
		<-result
		t.Fatal("physical conflict did not stop the runtime")
	}
}

func TestV7ConflictBeforeBatchPreventsCleanup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(dir, "01-Jan-2000.log")
	if err := os.WriteFile(archive, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	l.reportConflict(streamSystemFile, streamPropagation)
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 1}, nil, false)
	l.fanout.Write([]byte("must not write\n"))
	handle := l.files.handles[streamSystemFile]
	deadline := time.Now().Add(3 * time.Second)
	for !handle.Stats().Degraded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !handle.Stats().Degraded {
		l.close(time.Now().Add(time.Second))
		t.Fatal("logging worker did not reject the conflicted batch")
	}
	l.close(time.Now().Add(3 * time.Second))
	if data, err := os.ReadFile(archive); err != nil || string(data) != "keep" {
		t.Fatalf("archive after conflict = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "system.log")); !os.IsNotExist(err) {
		t.Fatalf("active log created after conflict: %v", err)
	}
}

func TestV7LinkedActiveFilesRejectBeforeWriting(t *testing.T) {
	root := t.TempDir()
	system, propagation := filepath.Join(root, "system"), filepath.Join(root, "propagation")
	for _, dir := range []string{system, propagation} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	first, second := filepath.Join(system, "system.log"), filepath.Join(propagation, "propagation.log")
	if err := os.WriteFile(first, []byte("preserve\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(first, second); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: system, RetentionDays: 7}
	cfg.Propagation.Enabled, cfg.Propagation.Dir = true, propagation
	l.startFiles(cfg, nil, true)
	l.fanout.Write([]byte("must not be written\n"))
	select {
	case <-l.conflicts:
	case <-time.After(3 * time.Second):
		l.close(time.Now().Add(time.Second))
		t.Fatal("linked active files were not reported")
	}
	l.close(time.Now().Add(3 * time.Second))
	for _, path := range []string{first, second} {
		data, err := os.ReadFile(path)
		if err != nil || strings.TrimSpace(string(data)) != "preserve" {
			t.Fatalf("linked file changed: %q, %v", data, err)
		}
	}
}

func TestV7BlockedFolderCheckDoesNotBlockShutdownOrRetainSuccessor(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	l.openLogRoot = func(path string) (*os.Root, error) {
		if path == dir {
			close(entered)
			<-release
		}
		return os.OpenRoot(path)
	}
	cfg := config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}
	l.startFiles(cfg, nil, true)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		l.close(time.Now().Add(time.Second))
		t.Fatal("folder check did not start")
	}
	if n, err := l.fanout.Write([]byte("old generation\n")); err != nil || n == 0 {
		close(release)
		t.Fatalf("logging admission blocked or failed: %d, %v", n, err)
	}
	done := l.files.handles[streamSystemFile].Done()
	started := time.Now()
	l.close(started.Add(100 * time.Millisecond))
	if time.Since(started) > time.Second {
		close(release)
		t.Fatal("shutdown waited for the folder check")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("old owner did not retire after the folder check returned")
	}
	next, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	next.startFiles(cfg, nil, true)
	next.fanout.Write([]byte("new generation\n"))
	next.close(time.Now().Add(3 * time.Second))
	data, err := os.ReadFile(filepath.Join(dir, "system.log"))
	if err != nil || !strings.Contains(string(data), "new generation") || strings.Contains(string(data), "old generation") {
		t.Fatalf("successor output = %q, %v", data, err)
	}
}

func TestV7RecoversWhenInvalidActivePathIsFixed(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.MkdirAll(filepath.Join(dir, "system.log"), 0700); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.close(time.Now().Add(3 * time.Second)) }()
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}, nil, false)
	h := l.files.handles[streamSystemFile]
	h.EmitString(time.Now(), 0, "first")
	deadline := time.Now().Add(3 * time.Second)
	for !h.Stats().Degraded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !h.Stats().Degraded {
		t.Fatal("invalid active path was not rejected")
	}
	if err := os.Remove(filepath.Join(dir, "system.log")); err != nil {
		t.Fatal(err)
	}
	h.EmitString(time.Now(), 0, "recovered")
	for h.Stats().Written == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.Stats().Written == 0 {
		t.Fatal("logging did not recover after fixing the active path")
	}
}

func TestV7LateConflictReachesCurrentRuntime(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shared")
	old, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	old.openLogRoot = func(path string) (*os.Root, error) {
		if path == dir {
			close(entered)
			<-release
		}
		return os.OpenRoot(path)
	}
	old.startFiles(config.LoggingConfig{Enabled: true, Dir: dir}, nil, false)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		old.close(time.Now().Add(time.Second))
		t.Fatal("old folder check did not start")
	}
	oldDone := old.files.handles[streamSystemFile].Done()
	old.close(time.Now().Add(50 * time.Millisecond))
	next, err := newV6LogRuntime(io.Discard)
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer func() { next.close(time.Now().Add(3 * time.Second)) }()
	cfg := config.LoggingConfig{}
	cfg.Propagation.Enabled, cfg.Propagation.Dir = true, dir
	next.startFiles(cfg, nil, false)
	handle := next.files.handles[streamPropagation]
	handle.EmitString(time.Now(), 0, "must not write")
	deadline := time.Now().Add(3 * time.Second)
	for !handle.Stats().Degraded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !handle.Stats().Degraded {
		close(release)
		t.Fatal("successor did not finish its folder check")
	}
	close(release)
	select {
	case <-next.conflicts:
	case <-time.After(3 * time.Second):
		t.Fatal("late conflict did not reach the current runtime")
	}
	select {
	case <-oldDone:
	case <-time.After(3 * time.Second):
		t.Fatal("old logging worker did not retire")
	}
	before := handle.Stats().Dropped
	handle.EmitString(time.Now(), 0, "must still not write")
	deadline = time.Now().Add(3 * time.Second)
	for handle.Stats().Dropped == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(dir, "shared.log")); !os.IsNotExist(err) {
		t.Fatalf("successor wrote after a late conflict: %v", err)
	}
}

func TestV7ExistingLogRotatesWhileRuntimeIsOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "system")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)
	active := filepath.Join(dir, "system.log")
	if err := os.WriteFile(active, []byte(yesterday.Format(logTimestampLayout)+" old record\n"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}, nil, false)
	if !l.files.Emit(streamSystemFile, now, []byte("new record")) {
		t.Fatal("record was not admitted")
	}
	l.close(time.Now().Add(3 * time.Second))
	archived, err := os.ReadFile(archiveLogPathForDate(dir, yesterday))
	if err != nil || !strings.Contains(string(archived), "old record") {
		t.Fatalf("archive = %q, %v", archived, err)
	}
	current, err := os.ReadFile(active)
	if err != nil || !strings.Contains(string(current), "new record") {
		t.Fatalf("active = %q, %v", current, err)
	}
}

func TestV7DisabledPeeringDoesNotStartOverlongLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "logs")
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}, nil, false)
	if got := l.files.handles[streamPeerOverlong].Stats().Generation; got != 0 {
		t.Errorf("disabled peer overlong generation = %d", got)
	}
	l.fanout.Write([]byte("system still works\n"))
	l.close(time.Now().Add(3 * time.Second))
	if data, err := os.ReadFile(filepath.Join(dir, "logs.log")); err != nil || !strings.Contains(string(data), "system still works") {
		t.Fatalf("system log = %q, %v", data, err)
	}
}

func TestV7UnsafeSymlinkDoesNotHoldExternalFileOpen(t *testing.T) {
	root := t.TempDir()
	dir, external := filepath.Join(root, "system"), filepath.Join(root, "external")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(external, []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(dir, "system.log")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.close(time.Now().Add(3 * time.Second)) }()
	l.startFiles(config.LoggingConfig{Enabled: true, Dir: dir, RetentionDays: 7}, nil, false)
	h := l.files.handles[streamSystemFile]
	h.EmitString(time.Now(), 0, "discarded")
	deadline := time.Now().Add(3 * time.Second)
	for !h.Stats().Degraded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !h.Stats().Degraded {
		t.Fatal("unsafe file was not rejected")
	}
	if err := os.Rename(external, external+"-renamed"); err != nil {
		t.Fatalf("external target remained open: %v", err)
	}
}

func TestV7RetargetedFolderStopsBeforeWritingToOtherLog(t *testing.T) {
	root := t.TempDir()
	first, second, alias := filepath.Join(root, "first"), filepath.Join(root, "propagation"), filepath.Join(root, "alias")
	for _, dir := range []string{first, second} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(first, alias); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	l, err := newV6LogRuntime(io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { l.close(time.Now().Add(3 * time.Second)) }()
	cfg := config.LoggingConfig{Enabled: true, Dir: alias, RetentionDays: 7}
	cfg.Propagation.Enabled, cfg.Propagation.Dir = true, second
	l.startFiles(cfg, nil, false)
	l.fanout.Write([]byte("first destination\n"))
	firstLog := filepath.Join(first, "alias.log")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(firstLog); err == nil && strings.Contains(string(data), "first destination") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if data, err := os.ReadFile(firstLog); err != nil || !strings.Contains(string(data), "first destination") {
		t.Fatalf("first log not ready: %q, %v", data, err)
	}
	if err := os.Remove(alias); err != nil {
		t.Skipf("cannot retarget directory symlink: %v", err)
	}
	if err := os.Symlink(second, alias); err != nil {
		t.Fatal(err)
	}
	l.fanout.Write([]byte("must not reach second folder\n"))
	select {
	case <-l.conflicts:
	case <-time.After(3 * time.Second):
		t.Fatal("retargeted folder conflict not reported")
	}
	if _, err := os.Stat(filepath.Join(second, "alias.log")); !os.IsNotExist(err) {
		t.Fatalf("wrote through retargeted folder: %v", err)
	}
}
