package archive

import (
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// Purpose: Ensure cleanup deletes old rows even when batch size is small.
// Key aspects: Inserts more stale rows than the batch size and validates retention.
// Upstream: go test.
// Downstream: NewWriter, cleanupOnce, db.QueryRow.
func TestCleanupOnceDeletesInBatches(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "archive.db")
	cfg := config.ArchiveConfig{
		Enabled:                 true,
		DBPath:                  dbPath,
		QueueSize:               10,
		BatchSize:               10,
		BatchIntervalMS:         1,
		CleanupIntervalSeconds:  60,
		CleanupBatchSize:        2,
		CleanupBatchYieldMS:     0,
		RetentionFTSeconds:      1,
		RetentionDefaultSeconds: 1,
		BusyTimeoutMS:           1000,
	}
	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter() error: %v", err)
	}
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
	writer.cleanupOnce()

	var count int
	if err := writer.db.QueryRow(`select count(*) from spots`).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 retained spots, got %d", count)
	}
}
