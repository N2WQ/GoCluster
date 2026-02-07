package spot

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestDecisionLoggerPersistsDecisionPath(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "callcorr_debug.log")
	ts := time.Date(2026, 2, 7, 15, 30, 0, 0, time.UTC)

	logger, err := NewDecisionLogger(basePath, 32)
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	logger.Enqueue(CorrectionLogEntry{
		Trace: CorrectionTrace{
			Timestamp:    ts,
			Strategy:     "majority",
			DecisionPath: "anchor",
			FrequencyKHz: 7010.0,
			SubjectCall:  "K1A8C",
			WinnerCall:   "K1ABC",
			Mode:         "CW",
			Decision:     "applied",
		},
	})
	if err := logger.Close(); err != nil {
		t.Fatalf("close logger: %v", err)
	}

	dbPath := DecisionLogPath(basePath, ts)
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	var decisionPath, decision string
	if err := db.QueryRow(`SELECT decision_path, decision FROM decisions LIMIT 1`).Scan(&decisionPath, &decision); err != nil {
		t.Fatalf("query row: %v", err)
	}
	if decisionPath != "anchor" {
		t.Fatalf("expected decision_path=anchor, got %q", decisionPath)
	}
	if decision != "applied" {
		t.Fatalf("expected decision=applied, got %q", decision)
	}
}
