// Program analyze_distance3 describes historical distance-3 decisions and
// hypothetical scalar gates. Logged scores do not establish correction accuracy.
// See docs/historical-analysis.md for the observation and scenario boundaries.
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

	_ "modernc.org/sqlite"
)

type distance3Case struct {
	subject, winner                                                 string
	winnerSupport, subjectSupport, totalReporters, winnerConfidence int
	decision                                                        sql.NullString
	reason                                                          string
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Historical decisions SQLite database")
	flag.Parse()
	if err := run(*dbPath, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run owns the database until reporting finishes. A failed load publishes no
// partial counts; output failure cannot turn successful reads into CLI success.
func run(dbPath string, out io.Writer) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open distance-3 database: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	cases, err := loadDistance3Cases(db)
	if err != nil {
		return err
	}
	if err := reportDistance3(out, dbPath, cases); err != nil {
		return fmt.Errorf("write distance-3 analysis: %w", err)
	}
	return nil
}

func loadDistance3Cases(db *sql.DB) (cases []distance3Case, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT subject, winner, winner_support, subject_support, total_reporters,
		       winner_confidence, decision, COALESCE(reason, '')
		FROM decisions WHERE distance = 3
		ORDER BY winner_confidence DESC, subject COLLATE BINARY, winner COLLATE BINARY,
		         decision COLLATE BINARY, winner_support, subject_support,
		         total_reporters, COALESCE(reason, '') COLLATE BINARY
	`)
	if err != nil {
		return nil, fmt.Errorf("query distance-3 decisions: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			cases = nil
			err = errors.Join(err, fmt.Errorf("close distance-3 rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var c distance3Case
		if err := rows.Scan(&c.subject, &c.winner, &c.winnerSupport, &c.subjectSupport,
			&c.totalReporters, &c.winnerConfidence, &c.decision, &c.reason); err != nil {
			return nil, fmt.Errorf("scan distance-3 decision: %w", err)
		}
		cases = append(cases, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate distance-3 decisions: %w", err)
	}
	return cases, nil
}

func classifyDistance3(decision sql.NullString) string {
	if decision.Valid && strings.EqualFold(decision.String, "applied") {
		return "applied"
	}
	if decision.Valid && strings.EqualFold(decision.String, "rejected") {
		return "rejected"
	}
	return "unknown"
}
