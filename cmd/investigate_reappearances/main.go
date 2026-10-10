// Program investigate_reappearances describes capped historical observations.
// First-frequency separation does not establish accuracy or simultaneous stations.
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
	"time"

	"dxcluster/strutil"

	_ "modernc.org/sqlite"
)

type reappearanceCase struct {
	correctionID                                     int64
	correctionTime                                   time.Time
	subject, winner                                  string
	freqKHz                                          float64
	distance, confidence                             int
	subjectFreqKHz, winnerFreqKHz, freqSeparationKHz float64
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Historical decisions SQLite database")
	lookAheadHours := flag.Int("lookahead", 24, "Hours to look ahead")
	flag.Parse()
	if err := run(*dbPath, *lookAheadHours, os.Stdout); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// Rows and database retire before main selects status. Failed loads publish no
// partial cases; separate statements do not promise one database snapshot.
func run(dbPath string, lookAheadHours int, out io.Writer) (err error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open reappearance database: %w", err)
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	cases, err := analyzeBothReappearances(db, lookAheadHours)
	if err != nil {
		return err
	}
	if err := reportReappearances(out, dbPath, lookAheadHours, cases); err != nil {
		return fmt.Errorf("write reappearance observations: %w", err)
	}
	return nil
}

func analyzeBothReappearances(db *sql.DB, lookAheadHours int) (cases []reappearanceCase, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT id, ts, subject, winner, freq_khz, distance, winner_confidence
		FROM decisions WHERE LOWER(decision) COLLATE BINARY = 'applied'
		ORDER BY ts, id, subject COLLATE BINARY, winner COLLATE BINARY,
		         freq_khz, distance, winner_confidence
	`)
	if err != nil {
		return nil, fmt.Errorf("query applied corrections: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			cases = nil
			err = errors.Join(err, fmt.Errorf("close applied-correction rows: %w", closeErr))
		}
	}()
	for rows.Next() {
		var c reappearanceCase
		var ts int64
		if err := rows.Scan(&c.correctionID, &ts, &c.subject, &c.winner,
			&c.freqKHz, &c.distance, &c.confidence); err != nil {
			return nil, fmt.Errorf("scan applied corrections: %w", err)
		}
		c.correctionTime = time.Unix(ts, 0).UTC()
		c.subject = strutil.NormalizeUpper(c.subject)
		c.winner = strutil.NormalizeUpper(c.winner)
		// Preserve the command's existing lookahead conversion and (start,end].
		end := c.correctionTime.Add(time.Duration(lookAheadHours) * time.Hour).Unix()
		first, err := observeFirstAppearances(db, ts, end, c.subject, c.winner)
		if err != nil {
			return nil, fmt.Errorf("correction %d follow-up: %w", c.correctionID, err)
		}
		if first.subjectSeen && first.winnerSeen {
			c.subjectFreqKHz, c.winnerFreqKHz = first.subjectFreq, first.winnerFreq
			c.freqSeparationKHz = c.subjectFreqKHz - c.winnerFreqKHz
			if c.freqSeparationKHz < 0 {
				c.freqSeparationKHz = -c.freqSeparationKHz
			}
			cases = append(cases, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied corrections: %w", err)
	}
	return cases, nil
}
