package sqliteutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PreflightResult reports the outcome of a SQLite preflight check.
type PreflightResult struct {
	Healthy         bool   // No issues detected; safe to proceed.
	Quarantined     bool   // The database was renamed to avoid startup stalls.
	QuarantinePath  string // Path of the quarantined database (main file only).
	Elapsed         time.Duration
	CheckpointError error // Nil when checkpoint succeeded.
	CheckError      error // Nil when quick_check succeeded.
}

// Preflight runs a bounded WAL checkpoint + quick_check before the main open path.
// On timeout or error it renames the database (and sidecars) to a timestamped
// quarantine path so startup can continue with a fresh file.
func Preflight(path, role string, timeout time.Duration, logf func(string, ...any)) (PreflightResult, error) {
	if logf == nil {
		logf = log.Printf
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	start := time.Now().UTC()
	res := PreflightResult{}
	existing := collectExisting(path)

	if strings.TrimSpace(path) == "" {
		return res, errors.New("preflight: empty path")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return res, fmt.Errorf("preflight: ensure dir: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return res, fmt.Errorf("preflight: open %s db: %w", role, err)
	}
	defer db.Close()
	// Keep operations serialized and honor busy timeout.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.ExecContext(ctx, fmt.Sprintf("pragma busy_timeout=%d", timeout.Milliseconds())); err != nil {
		return res, fmt.Errorf("preflight: set busy_timeout %s: %w", role, err)
	}

	checkpointErr := runCheckpoint(ctx, db)
	checkErr := quickCheck(ctx, db)
	res.Elapsed = time.Since(start)
	res.CheckpointError = checkpointErr
	res.CheckError = checkErr

	if checkpointErr == nil && checkErr == nil {
		res.Healthy = true
		return res, nil
	}

	// If context timed out, treat as fatal for this file.
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return res, fmt.Errorf("preflight: %s db timed out after %s", role, timeout)
	}

	_ = db.Close()
	quarantinePath, quarantineErr := quarantine(path, existing, logf)
	if quarantineErr != nil {
		return res, fmt.Errorf(
			"preflight: %s db quarantine failed: %w (checkpoint=%s, quick_check=%s)",
			role,
			quarantineErr,
			formatErr(checkpointErr),
			formatErr(checkErr),
		)
	}
	res.Quarantined = true
	res.QuarantinePath = quarantinePath
	if checkpointErr != nil {
		logf("%s db preflight: checkpoint failed (%v); quarantined to %s; elapsed=%s", role, checkpointErr, quarantinePath, res.Elapsed)
	} else {
		logf("%s db preflight: quick_check failed (%v); quarantined to %s; elapsed=%s", role, checkErr, quarantinePath, res.Elapsed)
	}
	return res, nil
}

func runCheckpoint(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "pragma wal_checkpoint(TRUNCATE)")
	return err
}

func quickCheck(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "pragma quick_check")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		if scanErr := rows.Scan(&status); scanErr != nil {
			return scanErr
		}
		if strings.TrimSpace(status) != "ok" {
			return fmt.Errorf("quick_check reported %q", status)
		}
	}
	return rows.Err()
}

func formatErr(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

type fileState struct {
	path string
	have bool
}

func collectExisting(path string) []fileState {
	targets := []string{
		path,
		path + "-wal",
		path + "-shm",
		path + "-journal",
	}
	out := make([]fileState, 0, len(targets))
	for _, t := range targets {
		_, err := os.Stat(t)
		out = append(out, fileState{path: t, have: err == nil})
	}
	return out
}

func quarantine(path string, existing []fileState, logf func(string, ...any)) (string, error) {
	ts := time.Now().UTC().Format("20060102T150405Z")
	quarantinePath := fmt.Sprintf("%s.bad-%s", path, ts)

	if len(existing) == 0 {
		existing = collectExisting(path)
	}

	for _, state := range existing {
		if !state.have {
			continue
		}
		dest := state.path + ".bad-" + ts
		if _, err := os.Stat(state.path); err != nil {
			if os.IsNotExist(err) {
				logf("preflight: expected %s but it was missing during quarantine", state.path)
				continue
			}
			return "", err
		}
		if err := os.Rename(state.path, dest); err != nil {
			return "", err
		}
	}

	// Sidecars can disappear after checkpoint; log if we expected them but they
	// were missing when we attempted quarantine.
	for _, state := range existing {
		if state.have {
			continue
		}
		if strings.HasSuffix(state.path, "-wal") || strings.HasSuffix(state.path, "-shm") || strings.HasSuffix(state.path, "-journal") {
			logf("preflight: expected sidecar %s but it was missing during quarantine", state.path)
		}
	}
	return quarantinePath, nil
}
