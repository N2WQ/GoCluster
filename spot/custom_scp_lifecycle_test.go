// File role: Falsifies Custom SCP worker/database retirement using blocked real
// Checkpoint and Verify operations, queued Close, and complete family writes.
package spot

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func TestCustomSCPCleanupOwnerRetirement(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{CleanupInterval: time.Millisecond})
	store.cleanupPassMu.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(store.cleanupPassMu.Unlock) }
	defer release()
	store.StartCleanup()
	store.cleanupMu.Lock()
	quit, ownerDone := store.quit, store.cleanupDone
	store.cleanupMu.Unlock()
	store.StartCleanup()
	store.cleanupMu.Lock()
	if store.quit != quit || store.cleanupDone != ownerDone {
		store.cleanupMu.Unlock()
		t.Fatal("duplicate Start created another owner")
	}
	store.cleanupMu.Unlock()
	awaitCustomSCPCleanupCondition(t, "cleanup entered before pass lock", func() bool {
		if store.dbMu.TryLock() {
			store.dbMu.Unlock()
			return false
		}
		return true
	})
	stop := runCustomSCPCleanupTask(func() error { store.StopCleanup(); return nil })
	awaitCustomSCPCleanupChannel(t, quit, "Stop signaled retiring owner")
	start := runCustomSCPCleanupTask(func() error { store.StartCleanup(); return nil })
	closes := make([]*customSCPCleanupTask, 4)
	for i := range closes {
		closes[i] = runCustomSCPCleanupTask(store.Close)
	}
	requireCustomSCPCleanupTaskPending(t, stop, "Stop returned while callback was blocked")
	requireCustomSCPCleanupTaskPending(t, start, "Start overlapped the retiring owner")
	for _, closeTask := range closes {
		requireCustomSCPCleanupTaskPending(t, closeTask, "Close returned before cleanup joined")
	}
	release()
	awaitCustomSCPCleanupTask(t, stop, nil)
	awaitCustomSCPCleanupTask(t, start, nil)
	for _, closeTask := range closes {
		awaitCustomSCPCleanupTask(t, closeTask, nil)
	}
	awaitCustomSCPCleanupChannel(t, ownerDone, "old owner completed")
	store.StartCleanup()
	store.StopCleanup()
	store.cleanupMu.Lock()
	defer store.cleanupMu.Unlock()
	if !store.closing || store.quit != nil || store.cleanupDone != nil {
		t.Fatal("Close did not retire cleanup ownership terminally")
	}
}

func TestCustomSCPMaintenanceClose(t *testing.T) {
	t.Run("checkpoint_disposal", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "scp")
		dest := filepath.Join(t.TempDir(), "checkpoint")
		gate := newCustomSCPCleanupGate()
		defer gate.unblock()
		fs := &customSCPCleanupCheckpointFS{FS: vfs.Default, dest: dest, gate: gate}
		store := openCustomSCPCleanupStoreWithFS(t, path, fs)
		store.recordObservation("K1CKPT", "40m", "CW", "N0AAA", 101, 0, false, freshCustomSCPTestTime())
		operation := runCustomSCPCleanupTask(func() error { return store.Checkpoint(dest) })
		awaitCustomSCPCleanupChannel(t, gate.entered, "actual checkpoint directory disposal")
		closing := runCustomSCPCleanupTask(store.Close)
		awaitCustomSCPCleanupCondition(t, "Close writer queued during checkpoint disposal", func() bool {
			if store.dbMu.TryRLock() {
				store.dbMu.RUnlock()
				return false
			}
			return true
		})
		requireCustomSCPCleanupTaskPending(t, closing, "Close returned before checkpoint disposal")
		gate.unblock()
		awaitCustomSCPCleanupTask(t, operation, nil)
		awaitCustomSCPCleanupTask(t, closing, nil)
		if count, err := VerifyCustomSCPCheckpoint(t.Context(), dest, time.Second); err != nil || count != 2 {
			t.Fatalf("completed checkpoint not readable: count=%d error=%v", count, err)
		}
	})
	t.Run("verify_cancel_disposes_iterator", func(t *testing.T) {
		store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
		store.recordObservation("K1VERIFY", "40m", "CW", "N0AAA", 101, 0, false, freshCustomSCPTestTime())
		gate := newCustomSCPCleanupGate()
		defer gate.unblock()
		base, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &customSCPCleanupVerifyContext{Context: base, gate: gate}
		operation := runCustomSCPCleanupTask(func() error { _, err := store.Verify(ctx, time.Second); return err })
		awaitCustomSCPCleanupChannel(t, gate.entered, "actual Verify iterator scan")
		closing := runCustomSCPCleanupTask(store.Close)
		awaitCustomSCPCleanupCondition(t, "Close writer queued during Verify scan", func() bool {
			if store.dbMu.TryRLock() {
				store.dbMu.RUnlock()
				return false
			}
			return true
		})
		requireCustomSCPCleanupTaskPending(t, closing, "Close returned before Verify iterator disposal")
		cancel()
		gate.unblock()
		awaitCustomSCPCleanupTask(t, operation, context.Canceled)
		awaitCustomSCPCleanupTask(t, closing, nil)
	})
}

func TestCustomSCPRecordSpotFamilyClose(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	now := freshCustomSCPTestTime()
	sp := NewSpot("KH6/W1AW", "N0AAA", 7020, "CW")
	// Grid omission deliberately exercises the existing unknown-cell value;
	// lifetime ownership must not depend on global H3 mapping initialization.
	sp.Time, sp.Confidence = now, "V"
	store.diskMu.Lock()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(store.diskMu.Unlock) }
	defer release()
	record := runCustomSCPCleanupTask(func() error { store.RecordSpot(sp); return nil })
	awaitCustomSCPCleanupCondition(t, "RecordSpot owns lifetime before mutation", func() bool {
		if store.dbMu.TryLock() {
			store.dbMu.Unlock()
			return false
		}
		return true
	})
	closing := runCustomSCPCleanupTask(store.Close)
	awaitCustomSCPCleanupCondition(t, "Close writer queued before RecordSpot resumes", func() bool {
		if store.dbMu.TryRLock() {
			store.dbMu.RUnlock()
			return false
		}
		return true
	})
	release()
	awaitCustomSCPCleanupTask(t, record, nil)
	awaitCustomSCPCleanupTask(t, closing, nil)
	reopened := reopenCustomSCPCleanupTestStore(t, store)
	for _, call := range []string{"W1AW/KH6", "W1AW"} {
		key := customSCPKey{call: call, band: "40m", bucket: "cw"}
		value := getCustomSCPCleanupRaw(t, reopened, observationKeyString(key, "N0AAA"))
		if len(value) != 10 || int64(binary.BigEndian.Uint64(value[:8])) != now.Unix() || binary.BigEndian.Uint16(value[8:]) != 0 {
			t.Fatalf("complete family observation did not survive Close: %s %x", call, value)
		}
	}
	assertCustomSCPCleanupCoupling(t, reopened)
}

func TestCustomSCPPostCloseCompatibility(t *testing.T) {
	store := newCustomSCPCleanupTestStore(t, CustomSCPOptions{})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store.recordObservation("K1DIRECT", "40m", "CW", "N0AAA", 101, 0, false, freshCustomSCPTestTime())
	sp := NewSpot("K1PUBLIC", "N0BBB", 7020, "CW")
	sp.Time, sp.Confidence, sp.DEGridNorm = freshCustomSCPTestTime(), "V", "FN20"
	store.RecordSpot(sp)
	if stats := store.StatsSnapshot(); stats.ObservationKeys != 2 || stats.ObservationSpotters != 2 || stats.StaticCalls != 2 {
		t.Fatalf("post-Close memory-only writes changed: %+v", stats)
	}
	if err := store.Checkpoint(filepath.Join(t.TempDir(), "late")); err == nil || !strings.Contains(err.Error(), "store not initialized") {
		t.Fatalf("late Checkpoint compatibility: %v", err)
	}
	if _, err := store.Verify(t.Context(), time.Second); err == nil || !strings.Contains(err.Error(), "store not initialized") {
		t.Fatalf("late Verify compatibility: %v", err)
	}
	store.StartCleanup()
	store.cleanupMu.Lock()
	if store.quit != nil || store.cleanupDone != nil {
		store.cleanupMu.Unlock()
		t.Fatal("post-Close observation restarted background cleanup")
	}
	store.cleanupMu.Unlock()
	assertCustomSCPCleanupCoupling(t, store)
	fixture := &CustomSCPStore{opts: sanitizeCustomSCPOptions(CustomSCPOptions{}), entries: make(map[customSCPKey]*customSCPEntry)}
	fixture.recordObservation("K1MEM", "40m", "CW", "N0AAA", 101, 0, false, freshCustomSCPTestTime())
	if fixture.StatsSnapshot().ObservationKeys != 1 {
		t.Fatal("nil-DB in-memory fixtures became no-op writes")
	}
	assertCustomSCPCleanupCoupling(t, fixture)
}

type customSCPCleanupTask struct {
	done chan struct{}
	err  error
}

func runCustomSCPCleanupTask(fn func() error) *customSCPCleanupTask {
	task := &customSCPCleanupTask{done: make(chan struct{})}
	go func() { task.err = fn(); close(task.done) }()
	return task
}

func awaitCustomSCPCleanupChannel(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out: %s", description)
	}
}

func awaitCustomSCPCleanupTask(t *testing.T, task *customSCPCleanupTask, expected error) {
	t.Helper()
	awaitCustomSCPCleanupChannel(t, task.done, "operation completed")
	if !errors.Is(task.err, expected) {
		t.Fatalf("operation result: got %v want %v", task.err, expected)
	}
}

func requireCustomSCPCleanupTaskPending(t *testing.T, task *customSCPCleanupTask, failure string) {
	t.Helper()
	// The caller already observed entry into the blocked real operation. This
	// bounded observation rejects premature completion; it does not schedule it.
	select {
	case <-task.done:
		t.Fatalf("%s: %v", failure, task.err)
	case <-time.After(20 * time.Millisecond):
	}
}

func awaitCustomSCPCleanupCondition(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %s", description)
		}
		runtime.Gosched()
	}
}

type customSCPCleanupGate struct {
	entered, release       chan struct{}
	enterOnce, releaseOnce sync.Once
}

func newCustomSCPCleanupGate() *customSCPCleanupGate {
	return &customSCPCleanupGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *customSCPCleanupGate) block()   { g.enterOnce.Do(func() { close(g.entered) }); <-g.release }
func (g *customSCPCleanupGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

type customSCPCleanupVerifyContext struct {
	context.Context
	gate *customSCPCleanupGate
}

func (c *customSCPCleanupVerifyContext) Done() <-chan struct{} {
	c.gate.block()
	return c.Context.Done()
}

type customSCPCleanupCheckpointFS struct {
	vfs.FS
	dest string
	gate *customSCPCleanupGate
}

func (f *customSCPCleanupCheckpointFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	if filepath.Clean(name) == filepath.Clean(f.dest) {
		return &customSCPCleanupCheckpointDir{File: file, gate: f.gate}, nil
	}
	return file, nil
}

type customSCPCleanupCheckpointDir struct {
	vfs.File
	gate *customSCPCleanupGate
}

func (d *customSCPCleanupCheckpointDir) Close() error { d.gate.block(); return d.File.Close() }

func openCustomSCPCleanupStoreWithFS(t *testing.T, path string, fs vfs.FS) *CustomSCPStore {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := pebble.Open(path, &pebble.Options{FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	store := &CustomSCPStore{
		opts: customSCPCleanupTestOptions(CustomSCPOptions{Path: path}), db: db,
		entries: make(map[customSCPKey]*customSCPEntry), entryExpiry: newCustomSCPEntryExpiryQueue(16),
		static: make(map[string]int64), staticExpiry: newCustomSCPStaticExpiryQueue(16),
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}
