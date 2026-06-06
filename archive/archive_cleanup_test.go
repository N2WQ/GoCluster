package archive

import (
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// Purpose: Ensure cleanup deletes expired rows with one timestamp range delete.
// Key aspects: Inserts stale and retained rows, then validates the retention boundary.
// Upstream: go test.
// Downstream: NewWriter, cleanupOnceAt, Recent.
func TestCleanupOnceRangeDeletesExpiredRows(t *testing.T) {
	writer := newCleanupTestWriter(t, 1)
	defer writer.Stop()

	now := time.Now().UTC()
	old := now.Add(-10 * time.Second)

	batch := make([]*spot.Spot, 0, 12)
	for i := 0; i < 5; i++ {
		s := spot.NewSpot("DXFT", "DEFT", 14074.0, "FT8")
		s.Time = old
		batch = append(batch, s)
	}
	for i := 0; i < 5; i++ {
		s := spot.NewSpot("DXCW", "DECW", 14030.0, "CW")
		s.Time = old
		batch = append(batch, s)
	}
	keepFT := spot.NewSpot("DXFTNEW", "DEFTNEW", 14074.0, "FT8")
	keepFT.Time = now
	batch = append(batch, keepFT)
	keepCW := spot.NewSpot("DXCWNEW", "DECWNEW", 14030.0, "CW")
	keepCW.Time = now
	batch = append(batch, keepCW)

	writer.flush(batch)
	writer.cleanupOnceAt(now)

	spots, err := writer.Recent(10)
	if err != nil {
		t.Fatalf("recent failed: %v", err)
	}
	if len(spots) != 2 {
		t.Fatalf("expected 2 retained spots, got %d", len(spots))
	}
	seen := make(map[string]bool, len(spots))
	for _, s := range spots {
		if s != nil {
			seen[s.DXCall] = true
		}
	}
	if !seen["DXFTNEW"] || !seen["DXCWNEW"] {
		t.Fatalf("expected retained DXFTNEW and DXCWNEW, got %+v", seen)
	}
}

// Purpose: Verify the archive cutoff is exclusive for expired rows.
// Key aspects: Deletes rows before cutoff while retaining rows exactly at and after cutoff.
// Upstream: go test.
// Downstream: cleanupOnceAt, spotKeyBytes range end semantics.
func TestCleanupOnceRetainsCutoffBoundary(t *testing.T) {
	const retentionSeconds = 10
	writer := newCleanupTestWriter(t, retentionSeconds)
	defer writer.Stop()

	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-retentionSeconds * time.Second)

	expired := spot.NewSpot("DXOLD", "DEOLD", 14020.0, "CW")
	expired.Time = cutoff.Add(-time.Nanosecond)
	atCutoff := spot.NewSpot("DXCUT", "DECUT", 14021.0, "CW")
	atCutoff.Time = cutoff
	afterCutoff := spot.NewSpot("DXNEW", "DENEW", 14022.0, "CW")
	afterCutoff.Time = cutoff.Add(time.Nanosecond)

	writer.flush([]*spot.Spot{expired, atCutoff, afterCutoff})
	writer.cleanupOnceAt(now)

	seen := recentDXCalls(t, writer)
	if seen["DXOLD"] {
		t.Fatalf("expected DXOLD before cutoff to be deleted, got %+v", seen)
	}
	if !seen["DXCUT"] || !seen["DXNEW"] {
		t.Fatalf("expected cutoff and newer rows to remain, got %+v", seen)
	}
}

// Purpose: Verify repeated range cleanup removes old rows inserted after a prior cleanup.
// Key aspects: Avoids an unsafe in-memory cleanup watermark that would miss late old rows.
// Upstream: go test.
// Downstream: cleanupOnceAt, Pebble range tombstone sequence behavior.
func TestCleanupOnceDeletesLateOldRowsAfterPriorCleanup(t *testing.T) {
	const retentionSeconds = 10
	writer := newCleanupTestWriter(t, retentionSeconds)
	defer writer.Stop()

	now := time.Date(2026, 6, 6, 12, 0, 0, 0, time.UTC)
	old := now.Add(-time.Minute)

	firstOld := spot.NewSpot("DXOLD1", "DEOLD1", 14020.0, "CW")
	firstOld.Time = old
	writer.flush([]*spot.Spot{firstOld})
	writer.cleanupOnceAt(now)
	if seen := recentDXCalls(t, writer); len(seen) != 0 {
		t.Fatalf("expected first cleanup to remove old rows, got %+v", seen)
	}

	lateOld := spot.NewSpot("DXOLD2", "DEOLD2", 14021.0, "CW")
	lateOld.Time = old
	writer.flush([]*spot.Spot{lateOld})
	if seen := recentDXCalls(t, writer); !seen["DXOLD2"] {
		t.Fatalf("expected late old row to be visible before next cleanup, got %+v", seen)
	}

	writer.cleanupOnceAt(now)
	if seen := recentDXCalls(t, writer); len(seen) != 0 {
		t.Fatalf("expected second cleanup to remove late old rows, got %+v", seen)
	}
}

func TestNewWriterDefaultsRetentionSeconds(t *testing.T) {
	writer, err := NewWriter(config.ArchiveConfig{
		DBPath:      filepath.Join(t.TempDir(), "archive.db"),
		Synchronous: "off",
	})
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}
	defer writer.Stop()
	if writer.cfg.RetentionSeconds != config.DefaultArchiveRetentionSeconds {
		t.Fatalf("expected retention default %d, got %d", config.DefaultArchiveRetentionSeconds, writer.cfg.RetentionSeconds)
	}
}

func BenchmarkCleanupOnceRangeDeleteLargeExpired(b *testing.B) {
	const rowsPerIteration = 5000
	now := time.Now().UTC()
	old := now.Add(-time.Hour)

	writer, err := NewWriter(config.ArchiveConfig{
		Enabled:                true,
		DBPath:                 filepath.Join(b.TempDir(), "archive.db"),
		QueueSize:              rowsPerIteration,
		BatchSize:              rowsPerIteration,
		BatchIntervalMS:        1,
		CleanupIntervalSeconds: 60,
		RetentionSeconds:       1,
		Synchronous:            "off",
	})
	if err != nil {
		b.Fatalf("NewWriter() error: %v", err)
	}
	defer writer.Stop()

	b.ReportAllocs()
	b.ReportMetric(rowsPerIteration, "expired_rows/op")
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		batch := make([]*spot.Spot, 0, rowsPerIteration)
		for n := 0; n < cap(batch); n++ {
			s := spot.NewSpot("DXOLD", "DEOLD", 14020.0, "CW")
			s.Time = old
			batch = append(batch, s)
		}
		writer.flush(batch)
		b.StartTimer()
		writer.cleanupOnceAt(now)
		b.StopTimer()
	}
}

func newCleanupTestWriter(t *testing.T, retentionSeconds int) *Writer {
	t.Helper()
	writer, err := NewWriter(config.ArchiveConfig{
		Enabled:                true,
		DBPath:                 filepath.Join(t.TempDir(), "archive.db"),
		QueueSize:              10,
		BatchSize:              10,
		BatchIntervalMS:        1,
		CleanupIntervalSeconds: 60,
		RetentionSeconds:       retentionSeconds,
		Synchronous:            "off",
	})
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}
	return writer
}

func recentDXCalls(t *testing.T, writer *Writer) map[string]bool {
	t.Helper()
	spots, err := writer.Recent(10)
	if err != nil {
		t.Fatalf("recent failed: %v", err)
	}
	seen := make(map[string]bool, len(spots))
	for _, s := range spots {
		if s != nil {
			seen[s.DXCall] = true
		}
	}
	return seen
}
