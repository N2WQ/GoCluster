package recorder

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"dxcluster/spot"

	_ "modernc.org/sqlite"
)

// Recorder persists a limited number of spots per mode into SQLite.
type Recorder struct {
	db            *sql.DB
	perModeLimit  int
	mu            sync.Mutex
	perModeCounts map[string]int
}

// NewRecorder opens (or creates) the SQLite database at path and ensures schema exists.
func NewRecorder(path string, perModeLimit int) (*Recorder, error) {
	if perModeLimit <= 0 {
		return nil, errors.New("recorder: per-mode limit must be > 0")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("recorder: ensure dir: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("recorder: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Recorder{
		db:            db,
		perModeLimit:  perModeLimit,
		perModeCounts: make(map[string]int),
	}, nil
}

func initSchema(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS spot_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT,
    dx_call TEXT,
    de_call TEXT,
    frequency REAL,
    band TEXT,
    report INTEGER,
    observed_at INTEGER,
    comment TEXT,
    source_type TEXT,
    source_node TEXT,
    is_beacon INTEGER,
    dx_continent TEXT,
    dx_country TEXT,
    dx_cq_zone INTEGER,
    dx_itu_zone INTEGER,
    dx_grid TEXT,
    de_continent TEXT,
    de_country TEXT,
    de_cq_zone INTEGER,
    de_itu_zone INTEGER,
    de_grid TEXT
);`
	_, err := db.Exec(schema)
	return err
}

// Close closes the underlying database.
func (r *Recorder) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

// Record inserts the spot if the per-mode limit has not been reached.
func (r *Recorder) Record(s *spot.Spot) {
	if r == nil || r.db == nil || s == nil {
		return
	}
	mode := strings.ToUpper(strings.TrimSpace(s.Mode))
	if mode == "" {
		mode = "UNKNOWN"
	}

	r.mu.Lock()
	count := r.perModeCounts[mode]
	if count >= r.perModeLimit {
		r.mu.Unlock()
		return
	}
	r.perModeCounts[mode] = count + 1
	r.mu.Unlock()

	go r.insert(mode, s)
}

func (r *Recorder) insert(mode string, s *spot.Spot) {
	_, err := r.db.Exec(`
INSERT INTO spot_records (
    mode, dx_call, de_call, frequency, band, report, observed_at, comment,
    source_type, source_node, is_beacon,
    dx_continent, dx_country, dx_cq_zone, dx_itu_zone, dx_grid,
    de_continent, de_country, de_cq_zone, de_itu_zone, de_grid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		mode,
		s.DXCall,
		s.DECall,
		s.Frequency,
		s.Band,
		s.Report,
		s.Time.UTC().Unix(),
		s.Comment,
		string(s.SourceType),
		s.SourceNode,
		boolToInt(s.IsBeacon),
		s.DXMetadata.Continent,
		s.DXMetadata.Country,
		s.DXMetadata.CQZone,
		s.DXMetadata.ITUZone,
		s.DXMetadata.Grid,
		s.DEMetadata.Continent,
		s.DEMetadata.Country,
		s.DEMetadata.CQZone,
		s.DEMetadata.ITUZone,
		s.DEMetadata.Grid,
	)
	if err != nil {
		fmt.Printf("Recorder: failed to insert spot: %v\n", err)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
