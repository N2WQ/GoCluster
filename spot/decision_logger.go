package spot

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/bandmap"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)
)

// CorrectionLogEntry captures a completed decision alongside the supporting votes
// used to reach it. The votes slice is deliberately lightweight (bandmap.SpotEntry)
// to avoid pulling in the full Spot struct and to keep log payloads small.
type CorrectionLogEntry struct {
	Trace CorrectionTrace
	Votes []bandmap.SpotEntry
}

// CorrectionTraceLogger accepts decision entries for asynchronous persistence.
// Implementations MUST drop or buffer without blocking the hot path.
type CorrectionTraceLogger interface {
	Enqueue(entry CorrectionLogEntry)
	Close() error
	Dropped() int64
}

// decisionLogger writes call-correction decisions to a daily-rotated SQLite database
// without blocking the caller. Entries are buffered on a bounded channel; when full,
// records are dropped and the drop count is exposed via Dropped().
type decisionLogger struct {
	basePath string
	queue    chan CorrectionLogEntry

	mu           sync.Mutex
	db           *sql.DB
	currentPath  string
	decisionStmt *sql.Stmt
	voteStmt     *sql.Stmt

	wg        sync.WaitGroup
	closeOnce sync.Once

	dropped  atomic.Int64
	errCount atomic.Int64
}

const (
	defaultDecisionQueue = 8192
	schemaVersionKey     = "schema_version"
	currentSchemaVersion = "1"
)

// NewDecisionLogger builds a non-blocking SQLite-backed logger. The basePath acts
// as a prefix/pattern:
//   - If it is empty, files are written to data/analysis/callcorr_YYYY-MM-DD.db.
//   - If it points to a directory, files are written inside that directory with
//     the same callcorr_YYYY-MM-DD.db pattern.
//   - If it points to a file, the directory is honored and the basename gets a
//     date suffix with a .db extension (e.g., foo.log -> foo_2025-12-04.db).
//
// The caller must invoke Close() on shutdown to flush buffered entries.
func NewDecisionLogger(basePath string, queueSize int) (CorrectionTraceLogger, error) {
	if queueSize <= 0 {
		queueSize = defaultDecisionQueue
	}
	l := &decisionLogger{
		basePath: strings.TrimSpace(basePath),
		queue:    make(chan CorrectionLogEntry, queueSize),
	}
	l.wg.Add(1)
	go l.run()
	return l, nil
}

// Enqueue attempts to buffer the entry without blocking. When the queue is full,
// the entry is dropped and the dropped counter increments.
func (l *decisionLogger) Enqueue(entry CorrectionLogEntry) {
	select {
	case l.queue <- entry:
	default:
		d := l.dropped.Add(1)
		if d == 1 || d%1000 == 0 {
			log.Printf("call-correction logger backpressure: dropped %d entries", d)
		}
	}
}

// Dropped returns how many log entries were discarded due to backpressure.
func (l *decisionLogger) Dropped() int64 {
	return l.dropped.Load()
}

// Close flushes the queue and releases database handles.
func (l *decisionLogger) Close() error {
	var closeErr error
	l.closeOnce.Do(func() {
		close(l.queue)
		l.wg.Wait()
		l.mu.Lock()
		defer l.mu.Unlock()
		closeErr = l.closeDBLocked()
	})
	return closeErr
}

func (l *decisionLogger) run() {
	defer l.wg.Done()
	for entry := range l.queue {
		if err := l.write(entry); err != nil {
			l.reportError(err)
		}
	}
}

func (l *decisionLogger) write(entry CorrectionLogEntry) error {
	ts := entry.Trace.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	_, err := l.ensureDB(ts)
	if err != nil {
		return err
	}

	if l.decisionStmt == nil || l.voteStmt == nil {
		return fmt.Errorf("call-correction logger: prepared statements not initialized")
	}

	res, err := l.decisionStmt.Exec(
		ts.UTC().Unix(),
		entry.Trace.Strategy,
		entry.Trace.FrequencyKHz,
		strings.ToUpper(entry.Trace.SubjectCall),
		strings.ToUpper(entry.Trace.WinnerCall),
		strings.ToUpper(entry.Trace.Mode),
		strings.ToUpper(entry.Trace.Source),
		entry.Trace.TotalReporters,
		entry.Trace.SubjectSupport,
		entry.Trace.WinnerSupport,
		entry.Trace.RunnerUpSupport,
		entry.Trace.SubjectConfidence,
		entry.Trace.WinnerConfidence,
		entry.Trace.Distance,
		entry.Trace.DistanceModel,
		entry.Trace.MaxEditDistance,
		entry.Trace.MinReports,
		entry.Trace.MinAdvantage,
		entry.Trace.MinConfidence,
		entry.Trace.Distance3ExtraReports,
		entry.Trace.Distance3ExtraAdvantage,
		entry.Trace.Distance3ExtraConfidence,
		entry.Trace.FreqGuardMinSeparationKHz,
		entry.Trace.FreqGuardRunnerUpRatio,
		entry.Trace.Decision,
		entry.Trace.Reason,
	)
	if err != nil {
		return fmt.Errorf("call-correction logger: insert decision: %w", err)
	}

	votesJSON, err := encodeVotes(entry.Votes)
	if err != nil {
		return fmt.Errorf("call-correction logger: marshal votes: %w", err)
	}
	if votesJSON == "" {
		return nil
	}
	rowID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("call-correction logger: fetch row id: %w", err)
	}
	if _, err := l.voteStmt.Exec(rowID, votesJSON); err != nil {
		return fmt.Errorf("call-correction logger: insert votes: %w", err)
	}
	return nil
}

func (l *decisionLogger) ensureDB(ts time.Time) (*sql.DB, error) {
	path := l.pathFor(ts)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.db != nil && l.currentPath == path {
		return l.db, nil
	}

	if err := l.closeDBLocked(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("call-correction logger: mkdir %s: %w", filepath.Dir(path), err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("call-correction logger: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("call-correction logger: pragmas: %w", err)
	}
	if err := initDecisionSchema(db); err != nil {
		db.Close()
		return nil, err
	}

	decisionStmt, err := db.Prepare(`
INSERT INTO decisions (
    ts, strategy, freq_khz, subject, winner, mode, source,
    total_reporters, subject_support, winner_support, runner_up_support,
    subject_confidence, winner_confidence, distance, distance_model,
    max_edit_distance, min_reports, min_advantage, min_confidence,
    d3_extra_reports, d3_extra_advantage, d3_extra_confidence,
    freq_guard_min_sep_khz, freq_guard_runner_ratio, decision, reason
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("call-correction logger: prepare decisions: %w", err)
	}
	voteStmt, err := db.Prepare(`INSERT INTO decision_votes(decision_id, votes_json) VALUES (?, ?)`)
	if err != nil {
		decisionStmt.Close()
		db.Close()
		return nil, fmt.Errorf("call-correction logger: prepare votes: %w", err)
	}

	l.db = db
	l.decisionStmt = decisionStmt
	l.voteStmt = voteStmt
	l.currentPath = path
	return l.db, nil
}

func (l *decisionLogger) closeDBLocked() error {
	var firstErr error
	if l.decisionStmt != nil {
		if err := l.decisionStmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		l.decisionStmt = nil
	}
	if l.voteStmt != nil {
		if err := l.voteStmt.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		l.voteStmt = nil
	}
	if l.db != nil {
		if err := l.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		l.db = nil
	}
	l.currentPath = ""
	return firstErr
}

func (l *decisionLogger) pathFor(ts time.Time) string {
	return DecisionLogPath(l.basePath, ts)
}

func encodeVotes(votes []bandmap.SpotEntry) (string, error) {
	if len(votes) == 0 {
		return "", nil
	}
	type voteRecord struct {
		Call    string `json:"call,omitempty"`
		Spotter string `json:"spotter,omitempty"`
		Mode    string `json:"mode,omitempty"`
		FreqHz  uint32 `json:"freq_hz,omitempty"`
		Time    int64  `json:"time,omitempty"`
		SNR     int    `json:"snr,omitempty"`
	}
	out := make([]voteRecord, 0, len(votes))
	for _, v := range votes {
		out = append(out, voteRecord{
			Call:    strings.ToUpper(strings.TrimSpace(v.Call)),
			Spotter: strings.ToUpper(strings.TrimSpace(v.Spotter)),
			Mode:    strings.ToUpper(strings.TrimSpace(v.Mode)),
			FreqHz:  v.FreqHz,
			Time:    v.Time,
			SNR:     v.SNR,
		})
	}
	data, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func initDecisionSchema(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("call-correction logger: db is nil")
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    strategy TEXT,
    freq_khz REAL,
    subject TEXT,
    winner TEXT,
    mode TEXT,
    source TEXT,
    total_reporters INTEGER,
    subject_support INTEGER,
    winner_support INTEGER,
    runner_up_support INTEGER,
    subject_confidence INTEGER,
    winner_confidence INTEGER,
    distance INTEGER,
    distance_model TEXT,
    max_edit_distance INTEGER,
    min_reports INTEGER,
    min_advantage INTEGER,
    min_confidence INTEGER,
    d3_extra_reports INTEGER,
    d3_extra_advantage INTEGER,
    d3_extra_confidence INTEGER,
    freq_guard_min_sep_khz REAL,
    freq_guard_runner_ratio REAL,
    decision TEXT,
    reason TEXT
);
CREATE TABLE IF NOT EXISTS decision_votes (
    decision_id INTEGER PRIMARY KEY,
    votes_json TEXT,
    FOREIGN KEY(decision_id) REFERENCES decisions(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);
CREATE INDEX IF NOT EXISTS idx_decisions_ts ON decisions(ts);
CREATE INDEX IF NOT EXISTS idx_decisions_subject ON decisions(subject);
CREATE INDEX IF NOT EXISTS idx_decisions_winner ON decisions(winner);
CREATE INDEX IF NOT EXISTS idx_decisions_decision ON decisions(decision);
`); err != nil {
		return fmt.Errorf("call-correction logger: init schema: %w", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)`, schemaVersionKey, currentSchemaVersion); err != nil {
		return fmt.Errorf("call-correction logger: write metadata: %w", err)
	}
	return nil
}

// DecisionLogPath resolves the SQLite file path for a given base path and timestamp.
// The base path behaves like NewDecisionLogger:
//   - blank -> data/analysis/callcorr_YYYY-MM-DD.db
//   - directory -> <dir>/callcorr_YYYY-MM-DD.db
//   - file -> <dir>/<basename>_YYYY-MM-DD.db (extension coerced to .db)
func DecisionLogPath(basePath string, ts time.Time) string {
	date := ts.Format("2006-01-02")
	basePath = strings.TrimSpace(basePath)
	if basePath == "" {
		return filepath.Join("data", "analysis", fmt.Sprintf("callcorr_%s.db", date))
	}

	clean := filepath.Clean(basePath)
	if info, err := os.Stat(clean); err == nil && info.IsDir() {
		return filepath.Join(clean, fmt.Sprintf("callcorr_%s.db", date))
	}

	dir := filepath.Dir(clean)
	base := strings.TrimSuffix(filepath.Base(clean), filepath.Ext(clean))
	ext := filepath.Ext(clean)
	if ext == "" || strings.EqualFold(ext, ".log") || strings.EqualFold(ext, ".txt") {
		ext = ".db"
	}
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "callcorr"
	}

	return filepath.Join(dir, fmt.Sprintf("%s_%s%s", base, date, ext))
}

func (l *decisionLogger) reportError(err error) {
	if err == nil {
		return
	}
	n := l.errCount.Add(1)
	if n == 1 || n%100 == 0 {
		log.Printf("call-correction logger error (%d): %v", n, err)
	}
}
