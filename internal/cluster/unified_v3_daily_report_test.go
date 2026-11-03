package cluster

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDailyReportSkipsArchiveNamedAsActive(t *testing.T) {
	day := time.Date(2026, time.November, 1, 12, 30, 0, 0, time.UTC)
	scheduler := newPropReportScheduler(nil, nil, time.Minute)
	parent := t.TempDir()
	conflict := filepath.Join(parent, "01-Nov-2026")
	if enqueueDailyPropReport(scheduler, conflict, day) {
		t.Fatal("active file was enqueued as an archive")
	}
	select {
	case job := <-scheduler.queue:
		t.Fatalf("conflicting job queued: %+v", job)
	default:
	}
	if enqueueDailyPropReport(scheduler, filepath.Join(parent, "01-nov-2026"), day) {
		t.Fatal("case-insensitive active-file alias was enqueued")
	}
	if runtime.GOOS == "windows" && enqueueDailyPropReport(scheduler, filepath.Join(parent, "01-NOV~1"), day) {
		t.Fatal("ambiguous short-name path was enqueued")
	}
	ordinary := filepath.Join(parent, "propagation")
	if !enqueueDailyPropReport(scheduler, ordinary, day) {
		t.Fatal("ordinary archive was not queued")
	}
	select {
	case job := <-scheduler.queue:
		if job.LogPath != filepath.Join(ordinary, "01-Nov-2026.log") || !job.Date.Equal(time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("wrong report job: %+v", job)
		}
	default:
		t.Fatal("ordinary report job missing")
	}
}
