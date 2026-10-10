// Program inspect_decisions reports historical outcome counts and samples.
// Samples are descriptive observations, not correction-accuracy measurements.
// See docs/historical-analysis.md for selection and provenance boundaries.
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

type outcomeCount struct {
	value sql.NullString
	count int64
}

type decisionSample struct {
	subject, winner                                                     string
	distance, score, support, reporters, reports, advantage, confidence int
	decision, reason                                                    sql.NullString
}

type inspection struct {
	total, applied, rejected, unknown                  int64
	outcomes                                           []outcomeCount
	appliedSamples, distance1Samples, highScoreSamples []decisionSample
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Historical decisions SQLite database")
	flag.Parse()
	if err := run(*dbPath, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// Database and row ownership end before status selection. All four existing
// selections must load before publication; they do not form one SQL snapshot.
func run(dbPath string, out io.Writer) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open decision-inspection database: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	data, err := loadInspection(db)
	if err != nil {
		return err
	}
	if err := reportInspection(out, dbPath, data); err != nil {
		return fmt.Errorf("write decision inspection: %w", err)
	}
	return nil
}

func outcomeKind(value sql.NullString) string {
	if value.Valid && strings.EqualFold(value.String, "applied") {
		return "applied"
	}
	if value.Valid && strings.EqualFold(value.String, "rejected") {
		return "rejected"
	}
	return "unknown"
}

func loadInspection(db *sql.DB) (*inspection, error) {
	outcomes, err := loadOutcomeBreakdown(db)
	if err != nil {
		return nil, err
	}
	data := &inspection{outcomes: outcomes}
	for _, row := range outcomes {
		data.total += row.count
		switch outcomeKind(row.value) {
		case "applied":
			data.applied += row.count
		case "rejected":
			data.rejected += row.count
		default:
			data.unknown += row.count
		}
	}
	data.appliedSamples, err = loadSamples(db, appliedQuery, "applied samples", scanAppliedSample)
	if err != nil {
		return nil, err
	}
	data.distance1Samples, err = loadSamples(db, distance1Query, "distance-1 rejection samples", scanDistance1Sample)
	if err != nil {
		return nil, err
	}
	data.highScoreSamples, err = loadSamples(db, highScoreQuery, "high-score rejection samples", scanHighScoreSample)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Binary grouping preserves unknown spellings even when a legacy decision
// column uses NOCASE. Raw breakdown cardinality is bounded by distinct input.
func loadOutcomeBreakdown(db *sql.DB) (counts []outcomeCount, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT decision, COUNT(*) AS count FROM decisions
		GROUP BY decision COLLATE BINARY
		ORDER BY count DESC, decision COLLATE BINARY
	`)
	if err != nil {
		return nil, fmt.Errorf("query outcome breakdown: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			counts = nil
			err = errors.Join(err, fmt.Errorf("close outcome breakdown rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var row outcomeCount
		if err := rows.Scan(&row.value, &row.count); err != nil {
			return nil, fmt.Errorf("scan outcome breakdown: %w", err)
		}
		counts = append(counts, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outcome breakdown: %w", err)
	}
	return counts, nil
}
