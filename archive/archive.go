package archive

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	_ "modernc.org/sqlite"
)

// Writer persists spots to SQLite asynchronously with per-mode retention.
// It is designed to be removable: the hot path never blocks on the writer,
// and backpressure results in dropped archive writes (logged via counters).
type Writer struct {
	cfg       config.ArchiveConfig
	db        *sql.DB
	queue     chan *spot.Spot
	stop      chan struct{}
	dropCount uint64
}

// Purpose: Initialize archive storage and return a writer instance.
// Key aspects: Creates SQLite DB and schema; sets queue size defaults.
// Upstream: main.go archive setup.
// Downstream: ensureSchema, SQLite open/pragmas.
func NewWriter(cfg config.ArchiveConfig) (*Writer, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return nil, fmt.Errorf("archive: mkdir: %w", err)
	}
	db, err := sql.Open("sqlite", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("archive: open db: %w", err)
	}
	if _, err := db.Exec(`pragma journal_mode=WAL; pragma synchronous=NORMAL; pragma busy_timeout=` + fmt.Sprintf("%d", cfg.BusyTimeoutMS)); err != nil {
		return nil, fmt.Errorf("archive: pragmas: %w", err)
	}
	if err := ensureSchema(db); err != nil {
		return nil, err
	}
	qsize := cfg.QueueSize
	if qsize <= 0 {
		qsize = 10000
	}
	return &Writer{
		cfg:   cfg,
		db:    db,
		queue: make(chan *spot.Spot, qsize),
		stop:  make(chan struct{}),
	}, nil
}

// Purpose: Start background loops for inserts and retention cleanup.
// Key aspects: Runs goroutines; writer remains non-blocking for callers.
// Upstream: main.go startup.
// Downstream: insertLoop goroutine, cleanupLoop goroutine.
func (w *Writer) Start() {
	// Goroutine: batched insert loop drains queue without blocking ingest.
	go w.insertLoop()
	// Goroutine: periodic cleanup loop enforces retention windows.
	go w.cleanupLoop()
}

// Purpose: Stop the writer and close the underlying DB.
// Key aspects: Signals loops to exit; closes DB without waiting for full drain.
// Upstream: main.go shutdown.
// Downstream: close(w.stop), db.Close.
func (w *Writer) Stop() {
	close(w.stop)
	_ = w.db.Close()
}

// Purpose: Try to enqueue a spot for archival without blocking.
// Key aspects: Drops silently when the queue is full to protect hot path.
// Upstream: main.go spot ingest/broadcast.
// Downstream: writer queue channel.
func (w *Writer) Enqueue(s *spot.Spot) {
	if w == nil || s == nil {
		return
	}
	select {
	case w.queue <- s:
	default:
		// Drop silently to avoid interfering with the hot path.
	}
}

// Purpose: Batch and insert queued spots into SQLite.
// Key aspects: Uses a size/time batch; flushes on stop signal.
// Upstream: Start goroutine.
// Downstream: flush, time.Timer.
func (w *Writer) insertLoop() {
	batch := make([]*spot.Spot, 0, w.cfg.BatchSize)
	timer := time.NewTimer(time.Duration(w.cfg.BatchIntervalMS) * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-w.stop:
			w.flush(batch)
			return
		case s := <-w.queue:
			batch = append(batch, s)
			if len(batch) >= w.cfg.BatchSize {
				w.flush(batch)
				batch = batch[:0]
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(time.Duration(w.cfg.BatchIntervalMS) * time.Millisecond)
			}
		case <-timer.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
			timer.Reset(time.Duration(w.cfg.BatchIntervalMS) * time.Millisecond)
		}
	}
}

// Purpose: Flush a batch of spots into SQLite in a single transaction.
// Key aspects: Best-effort logging on errors; per-spot inserts within tx.
// Upstream: insertLoop.
// Downstream: sql.Tx, stmt.Exec.
func (w *Writer) flush(batch []*spot.Spot) {
	if len(batch) == 0 {
		return
	}
	tx, err := w.db.Begin()
	if err != nil {
		log.Printf("archive: begin tx: %v", err)
		return
	}
	stmt, err := tx.Prepare(`insert into spots(ts, dx, de, freq, mode, report, has_report, comment, source, source_node, ttl, is_beacon, dx_grid, de_grid, confidence, band) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		log.Printf("archive: prepare: %v", err)
		_ = tx.Rollback()
		return
	}
	now := time.Now().UTC()
	for _, s := range batch {
		if s == nil {
			continue
		}
		if _, err := stmt.Exec(
			s.Time.UTC().Unix(),
			s.DXCall,
			s.DECall,
			s.Frequency,
			s.Mode,
			s.Report,
			boolToInt(s.HasReport),
			s.Comment,
			string(s.SourceType),
			s.SourceNode,
			s.TTL,
			boolToInt(s.IsBeacon),
			s.DXMetadata.Grid,
			s.DEMetadata.Grid,
			s.Confidence,
			s.Band,
		); err != nil {
			log.Printf("archive: insert failed: %v", err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		log.Printf("archive: commit: %v", err)
	}
	_ = now
}

// Purpose: Periodically enforce retention policy by deleting old rows.
// Key aspects: Uses a ticker; exits on stop signal.
// Upstream: Start goroutine.
// Downstream: cleanupOnce, time.Ticker.
func (w *Writer) cleanupLoop() {
	interval := time.Duration(w.cfg.CleanupIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.cleanupOnce()
		}
	}
}

// Purpose: Run one retention cleanup pass.
// Key aspects: Applies separate retention windows for FT vs other modes.
// Upstream: cleanupLoop.
// Downstream: sql.Exec deletes.
func (w *Writer) cleanupOnce() {
	now := time.Now().UTC().Unix()
	cutoffFT := now - int64(w.cfg.RetentionFTSeconds)
	cutoffDefault := now - int64(w.cfg.RetentionDefaultSeconds)

	// FT modes
	if _, err := w.db.Exec(`delete from spots where mode in ('FT8','FT4') and ts < ?`, cutoffFT); err != nil {
		log.Printf("archive: cleanup FT: %v", err)
	}
	// All others
	if _, err := w.db.Exec(`delete from spots where mode not in ('FT8','FT4') and ts < ?`, cutoffDefault); err != nil {
		log.Printf("archive: cleanup default: %v", err)
	}
}

// Purpose: Ensure the archive schema and indexes exist.
// Key aspects: Executes a single multi-statement schema block.
// Upstream: NewWriter.
// Downstream: db.Exec.
func ensureSchema(db *sql.DB) error {
	schema := `
	create table if not exists spots (
		id integer primary key autoincrement,
		ts integer,
		dx text,
		de text,
		freq real,
		mode text,
		report integer,
		has_report integer,
		comment text,
		source text,
		source_node text,
		ttl integer,
		is_beacon integer,
		dx_grid text,
		de_grid text,
		confidence text,
		band text
	);
	create index if not exists idx_spots_ts on spots(ts);
	create index if not exists idx_spots_mode_ts on spots(mode, ts);
	create index if not exists idx_spots_dx_ts on spots(dx, ts);
	create index if not exists idx_spots_de_ts on spots(de, ts);
	`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("archive: schema: %w", err)
	}
	return nil
}

// Purpose: Convert a bool to a SQLite-friendly integer.
// Key aspects: True->1, false->0.
// Upstream: flush, Recent row mapping.
// Downstream: None.
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Purpose: Delete the archive DB file (test helper).
// Key aspects: Validates path to avoid removing empty target.
// Upstream: Tests or maintenance tools.
// Downstream: os.Remove.
func DropDB(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("archive: empty path")
	}
	return os.Remove(path)
}

// Purpose: Return the most recent N archived spots, newest-first.
// Key aspects: Read-only query; reconstructs Spot objects from rows.
// Upstream: Telnet SHOW/DX handlers when archive is enabled.
// Downstream: db.Query, Spot normalization helpers.
func (w *Writer) Recent(limit int) ([]*spot.Spot, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("archive: writer is nil")
	}
	if limit <= 0 {
		return []*spot.Spot{}, nil
	}
	rows, err := w.db.Query(`select ts, dx, de, freq, mode, report, has_report, comment, source, source_node, ttl, is_beacon, dx_grid, de_grid, confidence, band from spots order by ts desc limit ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("archive: query recent: %w", err)
	}
	defer rows.Close()

	results := make([]*spot.Spot, 0, limit)
	for rows.Next() {
		var (
			ts         int64
			dx         string
			de         string
			freq       float64
			mode       string
			report     int
			hasReport  int
			comment    string
			source     string
			sourceNode string
			ttl        int
			isBeacon   int
			dxGrid     string
			deGrid     string
			conf       string
			band       string
		)
		if err := rows.Scan(&ts, &dx, &de, &freq, &mode, &report, &hasReport, &comment, &source, &sourceNode, &ttl, &isBeacon, &dxGrid, &deGrid, &conf, &band); err != nil {
			return nil, fmt.Errorf("archive: scan recent: %w", err)
		}
		s := &spot.Spot{
			DXCall:     dx,
			DECall:     de,
			Frequency:  freq,
			Mode:       mode,
			Report:     report,
			Time:       time.Unix(ts, 0).UTC(),
			Comment:    comment,
			SourceType: spot.SourceType(source),
			SourceNode: sourceNode,
			TTL:        uint8(clampToByte(ttl)),
			IsBeacon:   isBeacon > 0,
			HasReport:  hasReport > 0,
			Confidence: conf,
			Band:       band,
		}
		s.DXMetadata.Grid = dxGrid
		s.DEMetadata.Grid = deGrid
		s.EnsureNormalized()
		s.RefreshBeaconFlag()
		results = append(results, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("archive: iterate recent: %w", err)
	}
	return results, nil
}

// Purpose: Clamp an integer into the 0..255 range.
// Key aspects: Used to rebuild uint8 fields from DB rows.
// Upstream: Recent.
// Downstream: None.
func clampToByte(v int) int {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return v
}
