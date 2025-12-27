// Package gridstore persists known calls and grids to a small SQLite database,
// providing durable lookup and aggregation for grid statistics and TTL purges.
package gridstore

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)
)

const (
	busyTimeoutMS    = 5000
	busyRetryMax     = 3
	busyRetryBackoff = 50 * time.Millisecond
)

// Record represents a single callsign entry with optional grid and known-call flag.
type Record struct {
	Call         string
	IsKnown      bool
	Grid         sql.NullString
	Observations int
	FirstSeen    time.Time
	UpdatedAt    time.Time
	ExpiresAt    *time.Time
}

// Store manages the SQLite database that holds call metadata.
type Store struct {
	db *sql.DB
}

// Purpose: Load all call records from the store.
// Key aspects: Reads full table; returns a slice of Record values.
// Upstream: Administrative tools or diagnostics.
// Downstream: db.Query, row scanning.
func (s *Store) Entries() ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("gridstore: store is not initialized")
	}
	rows, err := s.db.Query(`SELECT call, is_known, grid, observations, first_seen, updated_at, expires_at FROM calls`)
	if err != nil {
		return nil, fmt.Errorf("gridstore: query entries: %w", err)
	}
	defer rows.Close()

	var list []Record
	for rows.Next() {
		var (
			call         string
			isKnown      int
			grid         sql.NullString
			observations int
			firstSeen    int64
			updatedAt    int64
			expiresAt    sql.NullInt64
		)
		if err := rows.Scan(&call, &isKnown, &grid, &observations, &firstSeen, &updatedAt, &expiresAt); err != nil {
			return nil, fmt.Errorf("gridstore: scan entries: %w", err)
		}
		var expires *time.Time
		if expiresAt.Valid {
			t := time.Unix(expiresAt.Int64, 0).UTC()
			expires = &t
		}
		rec := Record{
			Call:         call,
			IsKnown:      isKnown != 0,
			Grid:         grid,
			Observations: observations,
			FirstSeen:    time.Unix(firstSeen, 0).UTC(),
			UpdatedAt:    time.Unix(updatedAt, 0).UTC(),
			ExpiresAt:    expires,
		}
		list = append(list, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gridstore: iterate entries: %w", err)
	}
	return list, nil
}

// Purpose: Open or create the gridstore SQLite database.
// Key aspects: Initializes schema and configures SQLite for low contention.
// Upstream: main.go startup and tools.
// Downstream: initSchema, sql.Open, PRAGMA setup.
func Open(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("gridstore: database path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("gridstore: ensure directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("gridstore: open: %w", err)
	}
	// Limit concurrency to avoid SQLITE_BUSY in the pure-Go driver for this small store.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA busy_timeout = ` + fmt.Sprint(busyTimeoutMS) + `; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("gridstore: set pragmas: %w", err)
	}

	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Purpose: Close the underlying database handle.
// Key aspects: Safe to call on nil store.
// Upstream: main.go shutdown or tests.
// Downstream: db.Close.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Purpose: Delete records older than the cutoff time.
// Key aspects: Uses updated_at timestamps; returns rows removed.
// Upstream: Periodic maintenance in main pipeline.
// Downstream: db.Exec.
func (s *Store) PurgeOlderThan(cutoff time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("gridstore: store is not initialized")
	}
	res, err := s.db.Exec(`DELETE FROM calls WHERE updated_at < ?`, cutoff.UTC().Unix())
	if err != nil {
		return 0, fmt.Errorf("gridstore: purge: %w", err)
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("gridstore: purge rows: %w", err)
	}
	return removed, nil
}

// Purpose: Clear the known-call flag for all entries.
// Key aspects: Bulk update across the calls table.
// Upstream: Admin reset flows or tests.
// Downstream: db.Exec.
func (s *Store) ClearKnownFlags() error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	if _, err := s.db.Exec(`UPDATE calls SET is_known = 0 WHERE is_known != 0`); err != nil {
		return fmt.Errorf("gridstore: clear known flags: %w", err)
	}
	return nil
}

// Purpose: Insert or update a call record atomically.
// Key aspects: Normalizes callsign; uses UPSERT to merge flags and counts.
// Upstream: Spot processing and enrichment.
// Downstream: withBusyRetry, db.Exec.
func (s *Store) Upsert(rec Record) error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	rec.Call = strings.TrimSpace(strings.ToUpper(rec.Call))
	if rec.Call == "" {
		return errors.New("gridstore: call is empty")
	}
	now := time.Now().UTC()
	if rec.UpdatedAt.IsZero() {
		rec.UpdatedAt = now
	}
	if rec.FirstSeen.IsZero() {
		rec.FirstSeen = rec.UpdatedAt
	}
	var expires *int64
	if rec.ExpiresAt != nil {
		ts := rec.ExpiresAt.UTC().Unix()
		expires = &ts
	}

	err := withBusyRetry(func() error {
		_, execErr := s.db.Exec(`
INSERT INTO calls (call, is_known, grid, observations, first_seen, updated_at, expires_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(call) DO UPDATE SET
    is_known = MAX(calls.is_known, excluded.is_known),
    grid = COALESCE(excluded.grid, calls.grid),
    observations = calls.observations + excluded.observations,
    first_seen = MIN(calls.first_seen, excluded.first_seen),
    updated_at = MAX(calls.updated_at, excluded.updated_at),
    expires_at = COALESCE(excluded.expires_at, calls.expires_at)
`, rec.Call, boolToInt(rec.IsKnown), nullString(rec.Grid), rec.Observations, rec.FirstSeen.UTC().Unix(), rec.UpdatedAt.UTC().Unix(), expires)
		if execErr != nil {
			return fmt.Errorf("gridstore: upsert %s: %w", rec.Call, execErr)
		}
		return nil
	})
	return err
}

// Purpose: Insert or update multiple records in a single transaction.
// Key aspects: Reuses a prepared statement; normalizes timestamps per batch.
// Upstream: Batch updates from spot pipelines or tools.
// Downstream: withBusyRetry, sql.Tx.
func (s *Store) UpsertBatch(recs []Record) error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	if len(recs) == 0 {
		return nil
	}
	err := withBusyRetry(func() error {
		tx, beginErr := s.db.Begin()
		if beginErr != nil {
			return fmt.Errorf("gridstore: begin tx: %w", beginErr)
		}
		stmt, prepErr := tx.Prepare(`
INSERT INTO calls (call, is_known, grid, observations, first_seen, updated_at, expires_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(call) DO UPDATE SET
    is_known = MAX(calls.is_known, excluded.is_known),
    grid = COALESCE(excluded.grid, calls.grid),
    observations = calls.observations + excluded.observations,
    first_seen = MIN(calls.first_seen, excluded.first_seen),
    updated_at = MAX(calls.updated_at, excluded.updated_at),
    expires_at = COALESCE(excluded.expires_at, calls.expires_at)
`)
		if prepErr != nil {
			tx.Rollback()
			return fmt.Errorf("gridstore: prepare batch: %w", prepErr)
		}
		now := time.Now().UTC()
		for i := range recs {
			rec := recs[i]
			rec.Call = strings.TrimSpace(strings.ToUpper(rec.Call))
			if rec.Call == "" {
				continue
			}
			if rec.UpdatedAt.IsZero() {
				rec.UpdatedAt = now
			}
			if rec.FirstSeen.IsZero() {
				rec.FirstSeen = rec.UpdatedAt
			}
			var expires *int64
			if rec.ExpiresAt != nil {
				ts := rec.ExpiresAt.UTC().Unix()
				expires = &ts
			}
			if _, execErr := stmt.Exec(rec.Call, boolToInt(rec.IsKnown), nullString(rec.Grid), rec.Observations, rec.FirstSeen.UTC().Unix(), rec.UpdatedAt.UTC().Unix(), expires); execErr != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("gridstore: batch upsert %s: %w", rec.Call, execErr)
			}
		}
		if err := stmt.Close(); err != nil {
			tx.Rollback()
			return fmt.Errorf("gridstore: close stmt: %w", err)
		}
		if err := tx.Commit(); err != nil {
			tx.Rollback()
			return fmt.Errorf("gridstore: commit: %w", err)
		}
		return nil
	})
	return err
}

// Purpose: Fetch a record by callsign.
// Key aspects: Returns (nil, nil) when not found; normalizes callsign.
// Upstream: Spot enrichment queries.
// Downstream: db.QueryRow.
func (s *Store) Get(call string) (*Record, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("gridstore: store is not initialized")
	}
	call = strings.TrimSpace(strings.ToUpper(call))
	if call == "" {
		return nil, errors.New("gridstore: call is empty")
	}
	var (
		isKnown      int
		grid         sql.NullString
		observations int
		firstSeen    int64
		updatedAt    int64
		expiresAt    sql.NullInt64
	)
	err := s.db.QueryRow(`
SELECT is_known, grid, observations, first_seen, updated_at, expires_at
FROM calls
WHERE call = ?
`, call).Scan(&isKnown, &grid, &observations, &firstSeen, &updatedAt, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gridstore: get %s: %w", call, err)
	}
	var expires *time.Time
	if expiresAt.Valid {
		t := time.Unix(expiresAt.Int64, 0).UTC()
		expires = &t
	}
	return &Record{
		Call:         call,
		IsKnown:      isKnown != 0,
		Grid:         grid,
		Observations: observations,
		FirstSeen:    time.Unix(firstSeen, 0).UTC(),
		UpdatedAt:    time.Unix(updatedAt, 0).UTC(),
		ExpiresAt:    expires,
	}, nil
}

// Purpose: Return the total number of stored calls.
// Key aspects: Simple COUNT(*) query.
// Upstream: Metrics/diagnostics.
// Downstream: db.QueryRow.
func (s *Store) Count() (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("gridstore: store is not initialized")
	}
	var count int64
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM calls`).Scan(&count); err != nil {
		return 0, fmt.Errorf("gridstore: count: %w", err)
	}
	return count, nil
}

// Purpose: Create the calls table schema and indexes if missing.
// Key aspects: Executes a multi-statement schema definition.
// Upstream: Open.
// Downstream: db.Exec.
func initSchema(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS calls (
    call TEXT PRIMARY KEY,
    is_known INTEGER NOT NULL DEFAULT 0,
    grid TEXT,
    observations INTEGER NOT NULL DEFAULT 0,
    first_seen INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_calls_updated_at ON calls(updated_at);
CREATE INDEX IF NOT EXISTS idx_calls_grid ON calls(grid);
`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("gridstore: init schema: %w", err)
	}
	return nil
}

// Purpose: Convert a bool to 0/1 for SQLite writes.
// Key aspects: True -> 1, false -> 0.
// Upstream: Upsert, UpsertBatch.
// Downstream: None.
func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// Purpose: Convert sql.NullString to nil or string for binding.
// Key aspects: Preserves NULLs when invalid.
// Upstream: Upsert, UpsertBatch.
// Downstream: None.
func nullString(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}

// Purpose: Retry SQLite operations that fail with SQLITE_BUSY.
// Key aspects: Bounded retry with linear backoff.
// Upstream: Upsert, UpsertBatch.
// Downstream: IsBusyError, time.Sleep.
func withBusyRetry(fn func() error) error {
	var err error
	for attempt := 1; attempt <= busyRetryMax; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}
		if !IsBusyError(err) || attempt == busyRetryMax {
			return err
		}
		time.Sleep(time.Duration(attempt) * busyRetryBackoff)
	}
	return err
}

// Purpose: Detect SQLITE_BUSY / database locked errors.
// Key aspects: Matches normalized error strings from the driver.
// Upstream: withBusyRetry.
// Downstream: strings.Contains.
func IsBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToUpper(err.Error())
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "DATABASE IS LOCKED")
}
