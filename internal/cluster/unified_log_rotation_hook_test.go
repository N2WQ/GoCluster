package cluster

import (
	"runtime"
	"testing"
	"time"
)

func TestPropRotationHookDetachesScheduler(t *testing.T) {
	s := newPropReportScheduler(nil, nil, time.Second)
	var h propRotationHook
	h.setScheduler(s)
	date := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.onRotate(date, "archive.log", "active.log")
	select {
	case job := <-s.queue:
		if !job.Date.Equal(date) || job.LogPath != "archive.log" {
			t.Fatalf("unexpected job: %+v", job)
		}
	default:
		t.Fatal("rotation did not enqueue report")
	}
	h.close()
	h.setScheduler(s)
	h.onRotate(date.AddDate(0, 0, 1), "later.log", "active.log")
	if h.scheduler != nil {
		t.Fatal("closed rotation hook retained scheduler")
	}
	select {
	case <-s.queue:
		t.Fatal("closed rotation hook enqueued a report")
	default:
	}
}

func TestPropRotationHookSkipsUnavailableOrActiveArchive(t *testing.T) {
	s := newPropReportScheduler(nil, nil, time.Second)
	var h propRotationHook
	h.setScheduler(s)
	date := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.onRotate(date, "", "active.log")
	h.onRotate(date, "01-Jan-2026.log", "01-jan-2026.log")
	select {
	case job := <-s.queue:
		t.Fatalf("scheduled report for unavailable or live archive: %+v", job)
	default:
	}
}

func TestPropRotationHookSkipsAmbiguousWindowsActiveName(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows short names are platform-specific")
	}
	s := newPropReportScheduler(nil, nil, time.Second)
	var h propRotationHook
	h.setScheduler(s)
	date := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h.onRotate(date, "archive.log", `logs\ACTIVE~1.log`)
	select {
	case job := <-s.queue:
		t.Fatalf("ambiguous active filename was reported: %+v", job)
	default:
	}
}
