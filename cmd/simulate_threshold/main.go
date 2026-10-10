// Program simulate_threshold compares hypothetical scalar gates with recorded
// outcomes. It does not replay other gates or predict correction accuracy.
// See docs/historical-analysis.md for legacy input and claim boundaries.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"dxcluster/strutil"

	_ "modernc.org/sqlite"
)

type decisionRecord struct {
	id                                                                        int64
	subject, winner                                                           string
	distance, winnerConfidence, winnerSupport, subjectSupport, totalReporters int
	minReports, minAdvantage, minConfidence                                   int
	decision                                                                  sql.NullString
	reason                                                                    string
}

type thresholdSettings struct{ reports, advantage, confidence int }

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Historical decisions SQLite database")
	newMinConf := flag.Int("min-confidence", 55, "Hypothetical minimum logged score")
	newMinReports := flag.Int("min-reports", 3, "Hypothetical minimum winner reports")
	newMinAdvantage := flag.Int("min-advantage", 1, "Hypothetical minimum support advantage")
	flag.Parse()
	settings := thresholdSettings{*newMinReports, *newMinAdvantage, *newMinConf}
	if err := runThreshold(*dbPath, settings, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// No report is published until the complete historical selection has loaded.
// The database is retired before main chooses status, including stdout failure.
func runThreshold(dbPath string, settings thresholdSettings, out io.Writer) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open threshold database: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	decisions, err := loadAllDecisions(db)
	if err != nil {
		return err
	}
	if err := reportThreshold(out, dbPath, decisions, settings); err != nil {
		return fmt.Errorf("write threshold comparison: %w", err)
	}
	return nil
}

func loadAllDecisions(db *sql.DB) (decisions []decisionRecord, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT id, subject, winner, distance,
		       winner_confidence, winner_support, subject_support, total_reporters,
		       min_reports, min_advantage, min_confidence, decision, COALESCE(reason, '')
		FROM decisions WHERE distance > 0 AND distance <= 3
		ORDER BY distance, winner_confidence DESC, id,
		         subject COLLATE BINARY, winner COLLATE BINARY,
		         winner_support, subject_support, total_reporters,
		         min_reports, min_advantage, min_confidence,
		         decision COLLATE BINARY, COALESCE(reason, '') COLLATE BINARY
	`)
	if err != nil {
		return nil, fmt.Errorf("query threshold decisions: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			decisions = nil
			err = errors.Join(err, fmt.Errorf("close threshold rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var d decisionRecord
		if err := rows.Scan(&d.id, &d.subject, &d.winner, &d.distance,
			&d.winnerConfidence, &d.winnerSupport, &d.subjectSupport, &d.totalReporters,
			&d.minReports, &d.minAdvantage, &d.minConfidence, &d.decision, &d.reason); err != nil {
			return nil, fmt.Errorf("scan threshold decision: %w", err)
		}
		d.subject = strutil.NormalizeUpper(d.subject)
		d.winner = strutil.NormalizeUpper(d.winner)
		decisions = append(decisions, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate threshold decisions: %w", err)
	}
	return decisions, nil
}

func classifyDecision(decision sql.NullString) string {
	if decision.Valid && strings.EqualFold(decision.String, "applied") {
		return "applied"
	}
	if decision.Valid && strings.EqualFold(decision.String, "rejected") {
		return "rejected"
	}
	return "unknown"
}

func checkThresholds(d *decisionRecord, minReports, minAdvantage, minConfidence int) bool {
	if d == nil {
		return false
	}
	extraAdvantage, extraScore := 0, 0
	if d.distance == 3 {
		extraAdvantage, extraScore = 1, 5
	}
	// A mathematical threshold above MaxInt is impossible for a native-int
	// input. Preserve that result instead of overflowing a positive extra.
	maxInt := int(^uint(0) >> 1)
	if minAdvantage > maxInt-extraAdvantage || minConfidence > maxInt-extraScore {
		return false
	}
	return d.winnerSupport >= minReports &&
		d.winnerSupport-d.subjectSupport >= minAdvantage+extraAdvantage &&
		d.winnerConfidence >= minConfidence+extraScore
}
