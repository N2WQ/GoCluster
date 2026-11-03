package cluster

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type heldArchiveRunner struct {
	started chan struct{}
	release chan struct{}
	result  chan string
}

func (r *heldArchiveRunner) Run(_ context.Context, job propReportJob) error {
	close(r.started)
	<-r.release
	file, err := job.archive.take()
	if err != nil {
		return err
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	r.result <- string(data)
	if readErr != nil {
		return readErr
	}
	return closeErr
}

func TestRootedRotationReportSurvivesRetentionAndRetarget(t *testing.T) {
	base := t.TempDir()
	first, second, link := filepath.Join(base, "first"), filepath.Join(base, "second"), filepath.Join(base, "propagation")
	for _, dir := range []string{first, second} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(first, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	root, err := os.OpenRoot(link)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	runner := &heldArchiveRunner{started: make(chan struct{}), release: make(chan struct{}), result: make(chan string, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newPropReportScheduler(runner, nil, time.Minute)
	if !scheduler.Start(ctx) {
		cancel()
		t.Fatal("scheduler failed to start")
	}
	var release sync.Once
	defer func() { release.Do(func() { close(runner.release) }); cancel(); scheduler.Wait() }()
	var hook propRotationHook
	hook.setScheduler(scheduler)
	sink, err := newDailyFileSinkInRoot(link, 1, root)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sink.workerHook = true
	sink.SetRotateHook(func(previous time.Time, archive, active string) {
		hook.onRootedRotate(previous, archive, active, root)
	})
	now := time.Now().UTC()
	previous := now.AddDate(0, 0, -1)
	if !sink.writeLine("original propagation", previous) || !sink.writeLine("today", now) {
		t.Fatal("failed to rotate logs")
	}
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("rotation did not schedule report")
	}
	if err := os.WriteFile(filepath.Join(second, logFileNameForDate(previous)), []byte("wrong folder"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		release.Do(func() { close(runner.release) })
		t.Skipf("cannot retarget directory symlink: %v", err)
	}
	if err := os.Symlink(second, link); err != nil {
		release.Do(func() { close(runner.release) })
		t.Fatal(err)
	}
	release.Do(func() { close(runner.release) })
	select {
	case data := <-runner.result:
		if !strings.Contains(data, "original propagation") || strings.Contains(data, "wrong folder") {
			t.Fatalf("report snapshot = %q", data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("report did not read its snapshot")
	}
}

func TestReportArchiveLeaseKeepsOwnershipUntilReaderCloses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lease := reserveReportArchive()
	if lease.generation == 0 || !lease.set(file) {
		t.Fatal("cannot reserve report archive")
	}
	reader, err := lease.take()
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.release(false); err != nil {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(reader); err != nil || string(data) != "data" {
		t.Fatalf("borrowed archive = %q, %v", data, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	// A stale cleanup cannot close a new owner's file after slot reuse.
	next := reserveReportArchive()
	if next.generation == 0 {
		t.Fatal("archive slot was not released")
	}
	defer next.release(false)
	file, err = os.Open(path)
	if err != nil || !next.set(file) {
		t.Fatalf("new archive owner: %v", err)
	}
	if err := lease.release(false); err != nil {
		t.Fatal(err)
	}
	newReader, err := next.take()
	if err != nil {
		t.Fatal(err)
	}
	if err := newReader.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRootedReportPendingArchiveClosesAfterSchedulerCancellation(t *testing.T) {
	runner := &stuckReportRunner{started: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newPropReportScheduler(runner, nil, time.Minute)
	if !scheduler.Start(ctx) {
		cancel()
		t.Fatal("scheduler failed to start")
	}
	var release sync.Once
	defer func() { cancel(); release.Do(func() { close(runner.release) }); scheduler.Wait() }()
	if !scheduler.Enqueue(propReportJob{Date: time.Now()}) {
		t.Fatal("initial report rejected")
	}
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("report did not start")
	}
	path := filepath.Join(t.TempDir(), "archive")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	lease := reserveReportArchive()
	if lease.generation == 0 || !lease.set(file) {
		t.Fatal("archive reservation failed")
	}
	if !scheduler.Enqueue(propReportJob{Date: time.Now().AddDate(0, 0, -1), archive: lease}) {
		_ = lease.release(false)
		t.Fatal("queued archive rejected")
	}
	cancel()
	deadline, finish := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer finish()
	if scheduler.WaitContext(deadline) {
		t.Fatal("stuck report was reported as retired")
	}
	release.Do(func() { close(runner.release) })
	scheduler.Wait()
	if _, err := file.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("queued archive remained open after shutdown: %v", err)
	}
}

func TestRootedDailyReportUsesRegisteredDirectory(t *testing.T) {
	base := t.TempDir()
	first, second, link := filepath.Join(base, "first"), filepath.Join(base, "second"), filepath.Join(base, "propagation")
	for _, dir := range []string{first, second} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(first, link); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	root, err := os.OpenRoot(link)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	date := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	name := logFileNameForDate(date)
	for dir, text := range map[string]string{first: "verified", second: "wrong"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	scheduler := newPropReportScheduler(nil, nil, time.Second)
	if !enqueueDailyPropReportInRoot(scheduler, link, date, root) {
		t.Fatal("daily report was not queued")
	}
	job := <-scheduler.queue
	if err := os.Remove(link); err != nil {
		t.Skipf("cannot retarget directory symlink: %v", err)
	}
	if err := os.Symlink(second, link); err != nil {
		t.Fatal(err)
	}
	file, err := openSafeLogInRoot(job.root, filepath.Base(job.LogPath), os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || string(data) != "verified" {
		t.Fatalf("daily archive = %q, read %v, close %v", data, readErr, closeErr)
	}
}
