package recorder

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/spot"

	_ "modernc.org/sqlite"
)

func TestRecorderLimitPerMode(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "spots.db")

	rec, err := NewRecorder(dbPath, 2)
	if err != nil {
		t.Fatalf("NewRecorder failed: %v", err)
	}
	defer rec.Close()

	base := &spot.Spot{
		DXCall:     "K1ABC",
		DECall:     "W1AAA-1-#",
		Frequency:  14074.0,
		Band:       "20m",
		Mode:       "FT8",
		Time:       time.Now().UTC(),
		SourceType: spot.SourcePSKReporter,
	}

	rec.Record(base)
	rec.Record(base)
	rec.Record(base) // should be ignored (limit=2)

	time.Sleep(200 * time.Millisecond)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM spot_records`).Scan(&count); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}
