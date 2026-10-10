package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"dxcluster/strutil"
)

type firstAppearances struct {
	subjectSeen, winnerSeen bool
	subjectFreq, winnerFreq float64
}

// One capped cursor belongs to this helper and retires before the next seed.
// The cap combines both identities across all outcomes. Do not return early:
// later selected Scan/Next/Close failures must still invalidate the load.
func observeFirstAppearances(db *sql.DB, start, end int64, subject, winner string) (first *firstAppearances, err error) {
	rows, err := db.QueryContext(context.Background(), `
		SELECT subject, freq_khz FROM decisions
		WHERE ts > ? AND ts <= ?
		  AND (UPPER(subject) COLLATE BINARY = ? OR UPPER(subject) COLLATE BINARY = ?)
		ORDER BY ts, id, subject COLLATE BINARY, freq_khz
		LIMIT 100
	`, start, end, subject, winner)
	if err != nil {
		return nil, fmt.Errorf("query follow-up observations: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			first = nil
			err = errors.Join(err, fmt.Errorf("close follow-up rows: %w", closeErr))
		}
	}()
	first = &firstAppearances{}
	for rows.Next() {
		var observed string
		var freq float64
		if err := rows.Scan(&observed, &freq); err != nil {
			return nil, fmt.Errorf("scan follow-up observations: %w", err)
		}
		observed = strutil.NormalizeUpper(observed)
		if observed == subject && !first.subjectSeen {
			first.subjectSeen, first.subjectFreq = true, freq
		}
		if observed == winner && !first.winnerSeen {
			first.winnerSeen, first.winnerFreq = true, freq
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate follow-up observations: %w", err)
	}
	return first, nil
}
