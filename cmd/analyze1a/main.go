// Program analyze1a performs Method 1A: Temporal Stability analysis
// on call correction decisions to validate correction quality.
//
// Principle: If a correction is valid, the corrected callsign should appear
// naturally (uncorrected) in subsequent spots. High stability = likely correct.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	_ "modernc.org/sqlite"
)

type correctionRecord struct {
	id              int64
	timestamp       time.Time
	subject         string
	winner          string
	freqKHz         float64
	distance        int
	winnerSupport   int
	totalReporters  int
	confidence      int
	mode            string
}

type stabilityStats struct {
	totalCorrections      int
	naturalAppearances    int
	subjectReappearances  int
	noSubsequentSpots     int
	stabilityRatio        float64

	byDistance map[int]*distanceStability
}

type distanceStability struct {
	distance              int
	corrections           int
	naturalAppearances    int
	subjectReappearances  int
	stabilityRatio        float64
}

func main() {
	decisionDB := flag.String("decisions", "data/logs/callcorr_debug_modified_2025-12-04.db", "Path to decision log database")
	spotDB := flag.String("spots", "", "Path to spot database (if available)")
	lookAheadHours := flag.Int("lookahead", 24, "Hours to look ahead for validation")
	flag.Parse()

	if err := run(*decisionDB, *spotDB, *lookAheadHours); err != nil {
		log.Fatal(err)
	}
}

func run(decisionDBPath, spotDBPath string, lookAheadHours int) error {
	db, err := sql.Open("sqlite", decisionDBPath)
	if err != nil {
		return fmt.Errorf("open decision database: %w", err)
	}
	defer db.Close()

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  METHOD 1A: TEMPORAL STABILITY ANALYSIS\n")
	fmt.Printf("  Decision Database: %s\n", decisionDBPath)
	fmt.Printf("  Look-ahead window: %d hours\n", lookAheadHours)
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Load all applied corrections
	corrections, err := loadAppliedCorrections(db)
	if err != nil {
		return fmt.Errorf("load corrections: %w", err)
	}

	fmt.Printf("Loaded %d applied corrections from database\n", len(corrections))
	if len(corrections) == 0 {
		fmt.Println("\n⚠ No applied corrections found. Nothing to analyze.")
		return nil
	}

	// For now, we'll do a simplified analysis using the decision votes
	// In a production system, you'd query a separate spot database
	stats, err := analyzeStabilityFromVotes(db, corrections, lookAheadHours)
	if err != nil {
		return fmt.Errorf("analyze stability: %w", err)
	}

	printResults(stats)
	return nil
}

func loadAppliedCorrections(db *sql.DB) ([]correctionRecord, error) {
	rows, err := db.Query(`
		SELECT
			id,
			ts,
			subject,
			winner,
			freq_khz,
			distance,
			winner_support,
			total_reporters,
			winner_confidence,
			mode
		FROM decisions
		WHERE decision = 'applied'
		ORDER BY ts
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var corrections []correctionRecord
	for rows.Next() {
		var c correctionRecord
		var ts int64
		if err := rows.Scan(&c.id, &ts, &c.subject, &c.winner, &c.freqKHz,
			&c.distance, &c.winnerSupport, &c.totalReporters, &c.confidence, &c.mode); err != nil {
			return nil, err
		}
		c.timestamp = time.Unix(ts, 0)
		c.subject = strings.ToUpper(strings.TrimSpace(c.subject))
		c.winner = strings.ToUpper(strings.TrimSpace(c.winner))
		corrections = append(corrections, c)
	}

	return corrections, rows.Err()
}

func analyzeStabilityFromVotes(db *sql.DB, corrections []correctionRecord, lookAheadHours int) (*stabilityStats, error) {
	stats := &stabilityStats{
		totalCorrections: len(corrections),
		byDistance:       make(map[int]*distanceStability),
	}

	fmt.Printf("\nAnalyzing temporal stability...\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

	// For each correction, check if we can find subsequent evidence
	for i, corr := range corrections {
		if i > 0 && i%100 == 0 {
			fmt.Printf("  Processed %d/%d corrections...\n", i, len(corrections))
		}

		// Get distance stats bucket
		ds, exists := stats.byDistance[corr.distance]
		if !exists {
			ds = &distanceStability{distance: corr.distance}
			stats.byDistance[corr.distance] = ds
		}
		ds.corrections++

		// Look for subsequent spots with the winner callsign
		// We check if the winner appears in votes for later decisions
		hasNaturalAppearance, hasSubjectReappearance := checkSubsequentAppearances(
			db, corr, lookAheadHours,
		)

		if hasNaturalAppearance {
			stats.naturalAppearances++
			ds.naturalAppearances++
		}
		if hasSubjectReappearance {
			stats.subjectReappearances++
			ds.subjectReappearances++
		}
		if !hasNaturalAppearance && !hasSubjectReappearance {
			stats.noSubsequentSpots++
		}
	}

	// Calculate stability ratios
	if stats.totalCorrections > 0 {
		stats.stabilityRatio = float64(stats.naturalAppearances) / float64(stats.totalCorrections) * 100.0
	}

	for _, ds := range stats.byDistance {
		if ds.corrections > 0 {
			ds.stabilityRatio = float64(ds.naturalAppearances) / float64(ds.corrections) * 100.0
		}
	}

	return stats, nil
}

func checkSubsequentAppearances(db *sql.DB, corr correctionRecord, lookAheadHours int) (naturalWinner, naturalSubject bool) {
	// Query subsequent decisions within the lookahead window
	// Check if winner or subject appear in vote data

	endTime := corr.timestamp.Add(time.Duration(lookAheadHours) * time.Hour).Unix()

	// This is a simplified heuristic:
	// We check if later decisions have the winner as the subject (meaning it was seen naturally)
	// or if they have the winner in high-confidence votes

	query := `
		SELECT subject, winner, winner_support, total_reporters
		FROM decisions
		WHERE ts > ? AND ts <= ?
		  AND (UPPER(subject) = ? OR UPPER(winner) = ?)
		LIMIT 50
	`

	rows, err := db.Query(query, corr.timestamp.Unix(), endTime, corr.winner, corr.winner)
	if err != nil {
		return false, false
	}
	defer rows.Close()

	for rows.Next() {
		var subject, winner string
		var winnerSupport, totalReporters int
		if err := rows.Scan(&subject, &winner, &winnerSupport, &totalReporters); err != nil {
			continue
		}

		subject = strings.ToUpper(strings.TrimSpace(subject))
		winner = strings.ToUpper(strings.TrimSpace(winner))

		// If the corrected winner appears as a subject in a later decision,
		// that's strong evidence it's the correct callsign
		if subject == corr.winner {
			naturalWinner = true
		}

		// If the original subject reappears as subject, that's evidence the
		// correction might have been wrong
		if subject == corr.subject {
			naturalSubject = true
		}
	}

	return naturalWinner, naturalSubject
}

func printResults(stats *stabilityStats) {
	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  RESULTS\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	fmt.Printf("OVERALL STATISTICS:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	fmt.Printf("  Total corrections analyzed:     %d\n", stats.totalCorrections)
	fmt.Printf("  Winner appeared naturally:      %d (%.1f%%)\n",
		stats.naturalAppearances,
		float64(stats.naturalAppearances)/float64(stats.totalCorrections)*100.0)
	fmt.Printf("  Subject reappeared:             %d (%.1f%%)\n",
		stats.subjectReappearances,
		float64(stats.subjectReappearances)/float64(stats.totalCorrections)*100.0)
	fmt.Printf("  No subsequent spots:            %d (%.1f%%)\n",
		stats.noSubsequentSpots,
		float64(stats.noSubsequentSpots)/float64(stats.totalCorrections)*100.0)
	fmt.Printf("\n")
	fmt.Printf("  TEMPORAL STABILITY RATIO:       %.1f%%\n", stats.stabilityRatio)
	fmt.Printf("\n")

	// Print by distance
	fmt.Printf("STABILITY BY EDIT DISTANCE:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

	distances := make([]int, 0, len(stats.byDistance))
	for d := range stats.byDistance {
		distances = append(distances, d)
	}
	sort.Ints(distances)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Distance\tCorrections\tNatural\tStability\tSubject Reappeared")
	fmt.Fprintln(w, "────────\t───────────\t───────\t─────────\t──────────────────")

	for _, d := range distances {
		ds := stats.byDistance[d]
		fmt.Fprintf(w, "%d\t%d\t%d\t%.1f%%\t%d\n",
			ds.distance,
			ds.corrections,
			ds.naturalAppearances,
			ds.stabilityRatio,
			ds.subjectReappearances,
		)
	}
	w.Flush()

	// Analysis
	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  ANALYSIS & INTERPRETATION\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	if stats.stabilityRatio >= 80.0 {
		fmt.Printf("✓ EXCELLENT STABILITY (%.1f%% ≥ 80%%)\n", stats.stabilityRatio)
		fmt.Printf("  Your corrections are highly reliable. The corrected callsigns appear\n")
		fmt.Printf("  naturally in subsequent spots, confirming they are correct.\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: You can SAFELY RELAX thresholds to increase recall.\n")
		fmt.Printf("  Consider reducing min_consensus_reports from 3 to 2.\n")
	} else if stats.stabilityRatio >= 60.0 {
		fmt.Printf("✓ GOOD STABILITY (%.1f%% ≥ 60%%)\n", stats.stabilityRatio)
		fmt.Printf("  Most corrections are validated by subsequent observations.\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: Current thresholds are reasonable. Small adjustments OK.\n")
	} else if stats.stabilityRatio >= 40.0 {
		fmt.Printf("⚠ MODERATE STABILITY (%.1f%% ≥ 40%%)\n", stats.stabilityRatio)
		fmt.Printf("  Some corrections may be questionable.\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: Review low-stability corrections before relaxing thresholds.\n")
	} else {
		fmt.Printf("⚠ LOW STABILITY (%.1f%% < 40%%)\n", stats.stabilityRatio)
		fmt.Printf("  Many corrections are not validated by subsequent spots.\n")
		fmt.Printf("\n")
		fmt.Printf("  Recommendation: DO NOT RELAX thresholds. Consider tightening them.\n")
	}

	fmt.Printf("\n")

	// Subject reappearance warning
	if stats.subjectReappearances > 0 {
		reappearanceRate := float64(stats.subjectReappearances) / float64(stats.totalCorrections) * 100.0
		if reappearanceRate > 10.0 {
			fmt.Printf("⚠ WARNING: High subject reappearance rate (%.1f%%)\n", reappearanceRate)
			fmt.Printf("  The original (corrected-from) callsigns are reappearing frequently.\n")
			fmt.Printf("  This could indicate:\n")
			fmt.Printf("    1. Two different stations with similar callsigns (normal)\n")
			fmt.Printf("    2. Over-aggressive corrections (concerning)\n")
			fmt.Printf("    3. Callsign suffixes being stripped incorrectly\n")
			fmt.Printf("\n")
		}
	}

	// Distance-specific analysis
	if len(stats.byDistance) > 1 {
		fmt.Printf("DISTANCE-SPECIFIC OBSERVATIONS:\n")
		for _, d := range distances {
			ds := stats.byDistance[d]
			if ds.corrections < 5 {
				continue // Skip low-sample distances
			}

			if ds.stabilityRatio < stats.stabilityRatio-10.0 {
				fmt.Printf("  ⚠ Distance-%d stability (%.1f%%) is significantly lower than average\n",
					ds.distance, ds.stabilityRatio)
			} else if ds.stabilityRatio > stats.stabilityRatio+10.0 {
				fmt.Printf("  ✓ Distance-%d stability (%.1f%%) is higher than average\n",
					ds.distance, ds.stabilityRatio)
			}
		}
		fmt.Printf("\n")
	}

	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	fmt.Printf("NOTE: This analysis is based on decision log data only.\n")
	fmt.Printf("For more accurate validation, cross-reference with:\n")
	fmt.Printf("  • Raw spot database (check if winner appears uncorrected)\n")
	fmt.Printf("  • FCC ULS / MASTER.SCP databases (known callsign validation)\n")
	fmt.Printf("  • CTY database (geographic consistency checks)\n")
	fmt.Printf("\n")
}
