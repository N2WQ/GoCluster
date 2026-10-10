package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Known ASCII outcome spellings use case-only equality before LIMIT; binary
// comparison also defeats inherited NOCASE collations. Each query preserves
// its own projection, so unused numeric fields acquire no Scan requirement.
const appliedQuery = `
	SELECT subject, winner, distance, winner_confidence, total_reporters, decision
	FROM decisions WHERE LOWER(decision) COLLATE BINARY = 'applied'
	ORDER BY subject COLLATE BINARY, winner COLLATE BINARY, distance,
	         winner_confidence, total_reporters, decision COLLATE BINARY
	LIMIT 10
`

const distance1Query = `
	SELECT subject, winner, distance, winner_confidence, winner_support,
	       total_reporters, min_reports, min_advantage, min_confidence, decision, reason
	FROM decisions WHERE distance = 1 AND LOWER(decision) COLLATE BINARY = 'rejected'
	ORDER BY winner_support DESC, subject COLLATE BINARY, winner COLLATE BINARY,
	         distance, winner_confidence, total_reporters, min_reports, min_advantage,
	         min_confidence, decision COLLATE BINARY, reason COLLATE BINARY
	LIMIT 10
`

const highScoreQuery = `
	SELECT subject, winner, distance, winner_confidence, winner_support,
	       total_reporters, decision, reason
	FROM decisions WHERE winner_confidence > 70 AND LOWER(decision) COLLATE BINARY = 'rejected'
	ORDER BY winner_confidence DESC, subject COLLATE BINARY, winner COLLATE BINARY,
	         distance, winner_support, total_reporters,
	         decision COLLATE BINARY, reason COLLATE BINARY
	LIMIT 10
`

// The fixed selectors share only row traversal and retirement. Each returned
// slice contains at most ten samples and lives for this command invocation.
func loadSamples(db *sql.DB, query, stage string, scan func(*sql.Rows, *decisionSample) error) (samples []decisionSample, err error) {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", stage, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			samples = nil
			err = errors.Join(err, fmt.Errorf("close %s rows: %w", stage, closeErr))
		}
	}()
	for rows.Next() {
		var sample decisionSample
		if err := scan(rows, &sample); err != nil {
			return nil, fmt.Errorf("scan %s: %w", stage, err)
		}
		samples = append(samples, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", stage, err)
	}
	return samples, nil
}

func scanAppliedSample(rows *sql.Rows, s *decisionSample) error {
	return rows.Scan(&s.subject, &s.winner, &s.distance, &s.score, &s.reporters, &s.decision)
}

func scanDistance1Sample(rows *sql.Rows, s *decisionSample) error {
	return rows.Scan(&s.subject, &s.winner, &s.distance, &s.score, &s.support,
		&s.reporters, &s.reports, &s.advantage, &s.confidence, &s.decision, &s.reason)
}

func scanHighScoreSample(rows *sql.Rows, s *decisionSample) error {
	return rows.Scan(&s.subject, &s.winner, &s.distance, &s.score, &s.support,
		&s.reporters, &s.decision, &s.reason)
}
