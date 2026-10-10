// Program analyze1c summarizes historical outcomes and applied logged scores
// by distance. It establishes no correlation, calibration or reliability.
// See docs/historical-analysis.md for population and provenance boundaries.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

type distanceStats struct {
	distance, totalDecisions, appliedCount, rejectedCount, unknownCount int
	confidences                                                         []int
	rejectionReasons                                                    map[string]int
}

type distanceAnalysis struct {
	totalDecisions, appliedCount, rejectedCount, unknownCount int
	byDistance                                                map[int]*distanceStats
	unknownOutcomes                                           map[string]int
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Historical decisions SQLite database")
	flag.Parse()
	if err := run(*dbPath, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// Retire the database before main selects status. Failed loads never reach the
// writer; an output failure can still leave a prefix in the caller's stream.
func run(dbPath string, out io.Writer) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open distance-summary database: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	analysis, err := loadDistanceAnalysis(db)
	if err != nil {
		return err
	}
	if err := reportDistanceAnalysis(out, dbPath, analysis); err != nil {
		return fmt.Errorf("write distance summary: %w", err)
	}
	return nil
}

// Preserve the four-column legacy projection and every distance. Invocation
// maps have at most one key per input row; only applied scores retain a slice.
func loadDistanceAnalysis(db *sql.DB) (analysis *distanceAnalysis, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT distance, decision, winner_confidence, reason
		FROM decisions ORDER BY distance, decision
	`)
	if err != nil {
		return nil, fmt.Errorf("query distance summaries: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			analysis = nil
			err = errors.Join(err, fmt.Errorf("close distance-summary rows: %w", closeErr))
		}
	}()
	analysis = &distanceAnalysis{byDistance: make(map[int]*distanceStats), unknownOutcomes: make(map[string]int)}
	for rows.Next() {
		var distance, score int
		var decision, reason sql.NullString
		if err := rows.Scan(&distance, &decision, &score, &reason); err != nil {
			return nil, fmt.Errorf("scan distance-summary row: %w", err)
		}
		stats := analysis.byDistance[distance]
		if stats == nil {
			stats = &distanceStats{distance: distance, rejectionReasons: make(map[string]int)}
			analysis.byDistance[distance] = stats
		}
		analysis.totalDecisions++
		stats.totalDecisions++
		switch {
		case decision.Valid && strings.EqualFold(decision.String, "applied"):
			analysis.appliedCount++
			stats.appliedCount++
			stats.confidences = append(stats.confidences, score)
		case decision.Valid && strings.EqualFold(decision.String, "rejected"):
			analysis.rejectedCount++
			stats.rejectedCount++
			label := "UNKNOWN"
			if reason.Valid && reason.String != "" {
				label = reason.String
			}
			stats.rejectionReasons[label]++
		default:
			analysis.unknownCount++
			stats.unknownCount++
			label := "<NULL>"
			if decision.Valid {
				label = fmt.Sprintf("%q", decision.String)
			}
			analysis.unknownOutcomes[label]++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate distance summaries: %w", err)
	}
	return analysis, nil
}

// Sum exactly before division and final presentation conversion. Converting
// native endpoints first would lose the -0.5 mean/median of {MinInt, MaxInt}.
// The caller owns scores, so median sorting adds no second retained score list.
func confidenceSummary(scores []int) (mean, median float64, available bool) {
	if len(scores) == 0 {
		return 0, 0, false
	}
	var sum, value big.Int
	for _, score := range scores {
		value.SetInt64(int64(score))
		sum.Add(&sum, &value)
	}
	var average big.Rat
	average.SetFrac(&sum, big.NewInt(int64(len(scores))))
	mean, _ = average.Float64()
	sort.Ints(scores)
	mid := len(scores) / 2
	if len(scores)%2 == 1 {
		median = float64(scores[mid])
	} else {
		sum.SetInt64(int64(scores[mid-1]))
		value.SetInt64(int64(scores[mid]))
		sum.Add(&sum, &value)
		average.SetFrac(&sum, big.NewInt(2))
		median, _ = average.Float64()
	}
	return mean, median, true
}
