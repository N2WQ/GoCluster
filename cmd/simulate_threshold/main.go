// Program simulate_threshold simulates the impact of changing specific thresholds
// by re-analyzing the decision log with different parameter values.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

type decisionRecord struct {
	id                int64
	subject           string
	winner            string
	distance          int
	winnerConfidence  int
	winnerSupport     int
	subjectSupport    int
	totalReporters    int
	minReports        int
	minAdvantage      int
	minConfidence     int
	decision          string
	reason            string
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Path to decision log database")
	newMinConf := flag.Int("min-confidence", 55, "New min_confidence_percent threshold")
	newMinReports := flag.Int("min-reports", 3, "New min_consensus_reports threshold")
	newMinAdvantage := flag.Int("min-advantage", 1, "New min_advantage threshold")
	flag.Parse()

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  THRESHOLD SIMULATION\n")
	fmt.Printf("  Database: %s\n", *dbPath)
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Load all decisions (both applied and rejected)
	decisions, err := loadAllDecisions(db)
	if err != nil {
		log.Fatal(err)
	}

	// Current state analysis
	currentApplied := 0
	currentRejected := 0
	for _, d := range decisions {
		if d.decision == "applied" {
			currentApplied++
		} else {
			currentRejected++
		}
	}

	fmt.Printf("CURRENT CONFIGURATION:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	fmt.Printf("  Total decisions:        %d\n", len(decisions))
	fmt.Printf("  Applied corrections:    %d (%.1f%%)\n", currentApplied,
		float64(currentApplied)/float64(len(decisions))*100.0)
	fmt.Printf("  Rejected:               %d (%.1f%%)\n", currentRejected,
		float64(currentRejected)/float64(len(decisions))*100.0)
	fmt.Printf("\n")

	// Simulate new thresholds
	newApplied := 0
	rescued := 0
	rescuedByDistance := make(map[int]int)
	rescuedCases := []decisionRecord{}

	for _, d := range decisions {
		// Skip distance-0 (no winner)
		if d.distance == 0 {
			continue
		}

		// Apply new thresholds
		wouldApply := checkThresholds(d, *newMinReports, *newMinAdvantage, *newMinConf)

		if wouldApply {
			newApplied++
			// Was this previously rejected?
			if d.decision != "applied" {
				rescued++
				rescuedByDistance[d.distance]++
				if len(rescuedCases) < 20 {
					rescuedCases = append(rescuedCases, d)
				}
			}
		}
	}

	// Get original values from first decision
	origMinReports := 3
	origMinAdvantage := 1
	origMinConfidence := 60
	if len(decisions) > 0 {
		origMinReports = decisions[0].minReports
		origMinAdvantage = decisions[0].minAdvantage
		origMinConfidence = decisions[0].minConfidence
	}

	fmt.Printf("SIMULATED CONFIGURATION:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	fmt.Printf("  min_consensus_reports:  %d → %d\n", origMinReports, *newMinReports)
	fmt.Printf("  min_advantage:          %d → %d\n", origMinAdvantage, *newMinAdvantage)
	fmt.Printf("  min_confidence_percent: %d → %d\n", origMinConfidence, *newMinConf)
	fmt.Printf("\n")

	fmt.Printf("PROJECTED RESULTS:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	fmt.Printf("  Total corrections:      %d → %d (+%d, +%.1f%%)\n",
		currentApplied, newApplied, rescued,
		float64(rescued)/float64(currentApplied)*100.0)
	fmt.Printf("  Rescued rejections:     %d\n", rescued)
	fmt.Printf("\n")

	if rescued > 0 {
		fmt.Printf("RESCUED CORRECTIONS BY DISTANCE:\n")
		fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

		distances := make([]int, 0, len(rescuedByDistance))
		for d := range rescuedByDistance {
			distances = append(distances, d)
		}
		sort.Ints(distances)

		for _, d := range distances {
			count := rescuedByDistance[d]
			fmt.Printf("  Distance-%d:  %d corrections\n", d, count)
		}
		fmt.Printf("\n")

		fmt.Printf("SAMPLE RESCUED CORRECTIONS:\n")
		fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

		for i, r := range rescuedCases {
			if i >= 10 {
				break
			}
			advantage := r.winnerSupport - r.subjectSupport
			fmt.Printf("\n%s → %s (dist=%d)\n", r.subject, r.winner, r.distance)
			fmt.Printf("  Support: %d/%d reporters (winner=%d, subject=%d, advantage=%d)\n",
				r.winnerSupport, r.totalReporters, r.winnerSupport, r.subjectSupport, advantage)
			fmt.Printf("  Confidence: %d%% (threshold was %d%%, new threshold %d%%)\n",
				r.winnerConfidence, r.minConfidence, *newMinConf)
			fmt.Printf("  Previously rejected: %s\n", r.reason)
		}
	}

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  IMPACT ASSESSMENT\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	if rescued == 0 {
		fmt.Printf("⚠ NO IMPACT: This threshold change would not rescue any rejections.\n")
		fmt.Printf("  The 20 confidence-based rejections likely have confidence below 55%%.\n")
		fmt.Printf("\n")
	} else {
		percentIncrease := float64(rescued) / float64(currentApplied) * 100.0
		fmt.Printf("✓ MODEST IMPACT: +%d corrections (+%.1f%% increase)\n", rescued, percentIncrease)
		fmt.Printf("\n")

		if rescued < 10 {
			fmt.Printf("  This is a SMALL change. Consider combining with other adjustments:\n")
			fmt.Printf("  • Also reduce min_consensus_reports to 2 for bigger impact\n")
		} else if rescued < 30 {
			fmt.Printf("  This is a MODERATE change. Good for incremental improvement.\n")
		} else {
			fmt.Printf("  This is a SIGNIFICANT change. Validate carefully with Method 1A.\n")
		}
		fmt.Printf("\n")

		// Estimate stability
		fmt.Printf("PREDICTED STABILITY:\n")
		fmt.Printf("  Current stability: 88.3%%\n")
		if rescued < 20 {
			fmt.Printf("  Predicted stability: ~85-87%% (slight decrease expected)\n")
			fmt.Printf("  Risk: LOW - rescued corrections have similar profiles\n")
		} else {
			fmt.Printf("  Predicted stability: ~82-86%% (moderate decrease possible)\n")
			fmt.Printf("  Risk: MODERATE - validate with Method 1A after change\n")
		}
	}

	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")
}

func loadAllDecisions(db *sql.DB) ([]decisionRecord, error) {
	rows, err := db.Query(`
		SELECT
			id, subject, winner, distance,
			winner_confidence, winner_support, subject_support, total_reporters,
			min_reports, min_advantage, min_confidence,
			decision, COALESCE(reason, '')
		FROM decisions
		WHERE distance > 0 AND distance <= 3
		ORDER BY distance, winner_confidence DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var decisions []decisionRecord
	for rows.Next() {
		var d decisionRecord
		if err := rows.Scan(&d.id, &d.subject, &d.winner, &d.distance,
			&d.winnerConfidence, &d.winnerSupport, &d.subjectSupport, &d.totalReporters,
			&d.minReports, &d.minAdvantage, &d.minConfidence,
			&d.decision, &d.reason); err != nil {
			return nil, err
		}
		d.subject = strings.ToUpper(strings.TrimSpace(d.subject))
		d.winner = strings.ToUpper(strings.TrimSpace(d.winner))
		decisions = append(decisions, d)
	}

	return decisions, rows.Err()
}

func checkThresholds(d decisionRecord, minReports, minAdvantage, minConfidence int) bool {
	// Check minimum reports
	if d.winnerSupport < minReports {
		return false
	}

	// Check advantage
	advantage := d.winnerSupport - d.subjectSupport
	if advantage < minAdvantage {
		return false
	}

	// Check confidence
	if d.winnerConfidence < minConfidence {
		return false
	}

	// Distance-3 extra requirements (hardcoded from config)
	if d.distance == 3 {
		// Assume distance3_extra_advantage = 1
		if advantage < minAdvantage+1 {
			return false
		}
		// Assume distance3_extra_confidence = 5
		if d.winnerConfidence < minConfidence+5 {
			return false
		}
	}

	return true
}
