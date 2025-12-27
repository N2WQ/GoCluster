package uls

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"dxcluster/config"

	_ "modernc.org/sqlite"
)

type tableSpec struct {
	Name      string
	FileNames []string
	Schema    string
	Indexes   []string
	Columns   int
}

var tables = []tableSpec{
	{
		Name:      "HD",
		FileNames: []string{"HD.DAT"},
		Columns:   8, // unique_system_identifier, call_sign, license_status, radio_service_code, grant_date, expired_date, cancellation_date, last_action_date
		Schema: `CREATE TABLE IF NOT EXISTS HD (
			unique_system_identifier INTEGER,
			call_sign TEXT,
			license_status TEXT,
			radio_service_code TEXT,
			grant_date TEXT,
			expired_date TEXT,
			cancellation_date TEXT,
			last_action_date TEXT
		);`,
		Indexes: []string{
			"CREATE INDEX IF NOT EXISTS idx_HD_call_sign ON HD (call_sign, license_status);",
			"CREATE INDEX IF NOT EXISTS idx_HD_unique_sys_id ON HD (unique_system_identifier);",
			"CREATE INDEX IF NOT EXISTS idx_HD_license_status ON HD (license_status);",
		},
	},
	{
		Name:      "AM",
		FileNames: []string{"AM.DAT"},
		Columns:   2, // unique_system_identifier, call_sign
		Schema: `CREATE TABLE IF NOT EXISTS AM (
			unique_system_identifier INTEGER,
			call_sign TEXT
		);`,
		Indexes: []string{
			"CREATE INDEX IF NOT EXISTS idx_AM_call_sign ON AM (call_sign);",
			"CREATE INDEX IF NOT EXISTS idx_AM_unique_sys_id ON AM (unique_system_identifier);",
		},
	},
}

// Purpose: Build the FCC ULS SQLite database from extracted data files.
// Key aspects: Loads HD first to capture active IDs, then filters AM by that set;
// uses a temp DB file and atomic rename to avoid partial state.
// Upstream: Refresh in uls/downloader.go after download/extract.
// Downstream: loadTable, buildInsertSQL, resolveFile, parseID, SQLite operations.
func buildDatabase(extractDir, dbPath string, tempDir string) error {
	dir := filepath.Dir(dbPath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("fcc uls: create db directory: %w", err)
		}
	}

	tmpFile, err := os.CreateTemp(dir, "fcc-uls-*.dbtmp")
	if err != nil {
		return fmt.Errorf("fcc uls: create temp db: %w", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()

	defer os.Remove(tmpPath)

	db, err := sql.Open("sqlite", tmpPath+"?_pragma=journal_mode(OFF)&_pragma=synchronous(OFF)")
	if err != nil {
		return fmt.Errorf("fcc uls: open sqlite: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA foreign_keys=OFF;"); err != nil {
		return fmt.Errorf("fcc uls: pragma foreign_keys: %w", err)
	}
	// Direct SQLite temp files to a caller-provided directory to avoid unwritable %TEMP%.
	if strings.TrimSpace(tempDir) != "" {
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return fmt.Errorf("fcc uls: ensure temp dir: %w", err)
		}
		if _, err := db.Exec(fmt.Sprintf("PRAGMA temp_store_directory='%s';", tempDir)); err != nil {
			return fmt.Errorf("fcc uls: set temp_store_directory: %w", err)
		}
	}

	activeIDs := make(map[uint64]struct{})

	// Load HD first to collect the set of active license IDs, then load AM using that set.
	for _, tbl := range tables {
		if err := loadTable(db, extractDir, tbl, activeIDs); err != nil {
			return err
		}
		for _, idx := range tbl.Indexes {
			if _, err := db.Exec(idx); err != nil {
				return fmt.Errorf("fcc uls: create index for %s: %w", tbl.Name, err)
			}
		}
	}

	if err := db.Close(); err != nil {
		return fmt.Errorf("fcc uls: close sqlite: %w", err)
	}

	if err := os.Remove(dbPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("fcc uls: remove old db: %w", err)
	}
	if err := os.Rename(tmpPath, dbPath); err != nil {
		return fmt.Errorf("fcc uls: replace db: %w", err)
	}
	return nil
}

// Purpose: Load a single ULS table from a DAT file into SQLite.
// Key aspects: Uses a transaction and prepared statement; filters inactive IDs for AM.
// Upstream: buildDatabase iterates over tables.
// Downstream: resolveFile, buildInsertSQL, parseID, SQL insert/commit/rollback.
func loadTable(db *sql.DB, extractDir string, spec tableSpec, activeIDs map[uint64]struct{}) error {
	filePath := resolveFile(extractDir, spec.FileNames)
	if filePath == "" {
		return fmt.Errorf("fcc uls: missing data file for %s", spec.Name)
	}

	if _, err := db.Exec(spec.Schema); err != nil {
		return fmt.Errorf("fcc uls: create table %s: %w", spec.Name, err)
	}

	insertSQL := buildInsertSQL(spec.Name, spec.Columns)
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("fcc uls: begin tx for %s: %w", spec.Name, err)
	}
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("fcc uls: prepare insert for %s: %w", spec.Name, err)
	}
	defer stmt.Close()

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("fcc uls: open %s: %w", filePath, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)

	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "|")

		switch spec.Name {
		case "HD":
			// HD field order: unique_system_identifier at index 1, call_sign at 4, license_status at 5,
			// radio_service_code at 6, grant_date at 7, expired_date at 8, cancellation_date at 9, last_action_date at 42
			if len(fields) < 43 {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(fields[5]), "A") {
				continue
			}
			id, err := parseID(fields[1])
			if err != nil {
				continue
			}
			activeIDs[id] = struct{}{}
			args := []any{
				id,
				strings.TrimSpace(fields[4]),
				strings.TrimSpace(fields[5]),
				strings.TrimSpace(fields[6]),
				strings.TrimSpace(fields[7]),
				strings.TrimSpace(fields[8]),
				strings.TrimSpace(fields[9]),
				strings.TrimSpace(fields[42]),
			}
			if _, err := stmt.Exec(args...); err != nil {
				tx.Rollback()
				return fmt.Errorf("fcc uls: insert into %s at row %d: %w", spec.Name, rowCount+1, err)
			}
		case "AM":
			// AM field order: unique_system_identifier at index 1, call_sign at index 4
			if len(fields) < 5 {
				continue
			}
			id, err := parseID(fields[1])
			if err != nil {
				continue
			}
			if _, ok := activeIDs[id]; !ok {
				continue
			}
			call := strings.TrimSpace(fields[4])
			args := []any{id, call}
			if _, err := stmt.Exec(args...); err != nil {
				tx.Rollback()
				return fmt.Errorf("fcc uls: insert into %s at row %d: %w", spec.Name, rowCount+1, err)
			}
		}
		rowCount++
	}
	if err := scanner.Err(); err != nil {
		tx.Rollback()
		return fmt.Errorf("fcc uls: scan %s: %w", spec.Name, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fcc uls: commit %s: %w", spec.Name, err)
	}
	return nil
}

// Purpose: Build an INSERT statement with the requested column count.
// Key aspects: Generates positional placeholders for SQLite.
// Upstream: loadTable.
// Downstream: None.
func buildInsertSQL(table string, columns int) string {
	placeholders := make([]string, columns)
	for i := 0; i < columns; i++ {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO %s VALUES (%s);", table, strings.Join(placeholders, ","))
}

// Purpose: Locate the first existing data file in the extract directory.
// Key aspects: Checks original and lowercase filenames to handle case differences.
// Upstream: loadTable.
// Downstream: os.Stat.
func resolveFile(dir string, candidates []string) string {
	for _, name := range candidates {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
		if _, err := os.Stat(filepath.Join(dir, strings.ToLower(name))); err == nil {
			return filepath.Join(dir, strings.ToLower(name))
		}
	}
	return ""
}

// Purpose: Convenience entry point to force a full ULS refresh.
// Key aspects: Delegates to Refresh with force=true; intended for manual runs.
// Upstream: Manual invocation or tooling (not used in the main pipeline).
// Downstream: Refresh in uls/downloader.go.
func BuildOnce(cfg config.FCCULSConfig) error {
	_, err := Refresh(cfg, true)
	return err
}

// Purpose: Parse a ULS numeric identifier string into uint64.
// Key aspects: Trims whitespace and uses base-10 parsing.
// Upstream: loadTable.
// Downstream: strconv.ParseUint.
func parseID(raw string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
}
