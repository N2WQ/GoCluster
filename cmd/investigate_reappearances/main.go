// Program investigate_reappearances examines cases where both the corrected winner
// and the original subject appear in subsequent spots, to understand if this indicates
// errors or legitimate competing signals.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type reappearanceCase struct {
	correctionID       int64
	correctionTime     time.Time
	subject            string
	winner             string
	freqKHz            float64
	distance           int
	confidence         int
	subjectReappeared  bool
	winnerReappeared   bool
	bothReappeared     bool
	subjectFreqKHz     float64
	winnerFreqKHz      float64
	freqSeparationKHz  float64
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Path to decision log database")
	lookAheadHours := flag.Int("lookahead", 24, "Hours to look ahead")
	flag.Parse()

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  REAPPEARANCE PATTERN INVESTIGATION\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Find cases where both subject and winner reappear
	cases, err := analyzeBothReappearances(db, *lookAheadHours)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d cases where both subject and winner reappeared\n\n", len(cases))

	if len(cases) == 0 {
		fmt.Println("No dual-reappearance cases to analyze.")
		return
	}

	// Categorize by frequency separation
	closeFreq := 0
	separatedFreq := 0

	for _, c := range cases {
		if c.freqSeparationKHz < 0.5 {
			closeFreq++
		} else {
			separatedFreq++
		}
	}

	fmt.Printf("FREQUENCY SEPARATION ANALYSIS:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	fmt.Printf("  Same frequency (<0.5 kHz):      %d (%.1f%%)\n", closeFreq,
		float64(closeFreq)/float64(len(cases))*100.0)
	fmt.Printf("  Different frequency (≥0.5 kHz): %d (%.1f%%)\n", separatedFreq,
		float64(separatedFreq)/float64(len(cases))*100.0)
	fmt.Printf("\n")

	// Show sample cases
	fmt.Printf("SAMPLE DUAL-REAPPEARANCE CASES:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

	sampleCount := 20
	if len(cases) < sampleCount {
		sampleCount = len(cases)
	}

	for i := 0; i < sampleCount; i++ {
		c := cases[i]
		fmt.Printf("\n%s → %s (dist=%d, conf=%d%%)\n", c.subject, c.winner, c.distance, c.confidence)
		fmt.Printf("  Original freq:  %.1f kHz\n", c.freqKHz)
		if c.subjectFreqKHz > 0 {
			fmt.Printf("  Subject reappeared at: %.1f kHz\n", c.subjectFreqKHz)
		}
		if c.winnerFreqKHz > 0 {
			fmt.Printf("  Winner reappeared at:  %.1f kHz\n", c.winnerFreqKHz)
		}
		if c.freqSeparationKHz > 0 {
			fmt.Printf("  Frequency separation:  %.2f kHz\n", c.freqSeparationKHz)
		}
	}

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  INTERPRETATION\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	if separatedFreq > closeFreq {
		fmt.Printf("✓ LIKELY LEGITIMATE: Most dual-reappearances are at different frequencies.\n")
		fmt.Printf("  This suggests two real stations with similar callsigns operating\n")
		fmt.Printf("  simultaneously (e.g., K1ABC and K1AB, or W3RJ and W3AJ).\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: This is NORMAL behavior. No action needed.\n")
	} else {
		fmt.Printf("⚠ POTENTIAL ISSUE: Many dual-reappearances are at the same frequency.\n")
		fmt.Printf("  This could indicate:\n")
		fmt.Printf("    1. Suffix stripping issues (K1ABC/P → K1ABC)\n")
		fmt.Printf("    2. Weak consensus corrections being overridden\n")
		fmt.Printf("    3. Oscillating corrections (A→B, then B→A)\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: Review specific cases to identify patterns.\n")
	}

	fmt.Printf("\n")
}

func analyzeBothReappearances(db *sql.DB, lookAheadHours int) ([]reappearanceCase, error) {
	// Get all applied corrections
	rows, err := db.Query(`
		SELECT
			id, ts, subject, winner, freq_khz, distance, winner_confidence
		FROM decisions
		WHERE decision = 'applied'
		ORDER BY ts
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cases []reappearanceCase
	for rows.Next() {
		var c reappearanceCase
		var ts int64
		if err := rows.Scan(&c.correctionID, &ts, &c.subject, &c.winner,
			&c.freqKHz, &c.distance, &c.confidence); err != nil {
			return nil, err
		}
		c.correctionTime = time.Unix(ts, 0)
		c.subject = strings.ToUpper(strings.TrimSpace(c.subject))
		c.winner = strings.ToUpper(strings.TrimSpace(c.winner))

		// Check for subsequent appearances
		endTime := c.correctionTime.Add(time.Duration(lookAheadHours) * time.Hour).Unix()

		// Check if both appear in later decisions
		checkQuery := `
			SELECT subject, freq_khz
			FROM decisions
			WHERE ts > ? AND ts <= ?
			  AND (UPPER(subject) = ? OR UPPER(subject) = ?)
			ORDER BY ts
			LIMIT 100
		`

		subRows, err := db.Query(checkQuery, ts, endTime, c.subject, c.winner)
		if err != nil {
			continue
		}

		subjectSeen := false
		winnerSeen := false
		var subjectFreqs, winnerFreqs []float64

		for subRows.Next() {
			var subject string
			var freq float64
			if err := subRows.Scan(&subject, &freq); err != nil {
				continue
			}
			subject = strings.ToUpper(strings.TrimSpace(subject))

			if subject == c.subject {
				subjectSeen = true
				subjectFreqs = append(subjectFreqs, freq)
			}
			if subject == c.winner {
				winnerSeen = true
				winnerFreqs = append(winnerFreqs, freq)
			}
		}
		subRows.Close()

		if subjectSeen && winnerSeen {
			c.bothReappeared = true
			c.subjectReappeared = true
			c.winnerReappeared = true

			// Calculate frequency separation
			if len(subjectFreqs) > 0 && len(winnerFreqs) > 0 {
				c.subjectFreqKHz = subjectFreqs[0]
				c.winnerFreqKHz = winnerFreqs[0]
				sep := c.subjectFreqKHz - c.winnerFreqKHz
				if sep < 0 {
					sep = -sep
				}
				c.freqSeparationKHz = sep
			}

			cases = append(cases, c)
		}
	}

	return cases, rows.Err()
}
