// Program analyze1c performs Method 1C: Distance-Confidence Correlation analysis
// on call correction decision logs to validate threshold calibration.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"text/tabwriter"

	_ "modernc.org/sqlite"
)

type distanceStats struct {
	distance         int
	totalDecisions   int
	appliedCount     int
	rejectedCount    int
	meanConfidence   float64
	medianConfidence float64
	confidences      []int
	rejectionReasons map[string]int
}

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Path to decision log database")
	flag.Parse()

	if err := run(*dbPath); err != nil {
		log.Fatal(err)
	}
}

func run(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Query all decisions
	rows, err := db.Query(`
		SELECT
			distance,
			decision,
			winner_confidence,
			reason
		FROM decisions
		ORDER BY distance, decision
	`)
	if err != nil {
		return fmt.Errorf("query decisions: %w", err)
	}
	defer rows.Close()

	// Group by distance
	statsByDistance := make(map[int]*distanceStats)

	var totalDecisions int
	for rows.Next() {
		var distance int
		var decision string
		var winnerConfidence int
		var reason sql.NullString

		if err := rows.Scan(&distance, &decision, &winnerConfidence, &reason); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		totalDecisions++

		stats, exists := statsByDistance[distance]
		if !exists {
			stats = &distanceStats{
				distance:         distance,
				confidences:      []int{},
				rejectionReasons: make(map[string]int),
			}
			statsByDistance[distance] = stats
		}

		stats.totalDecisions++

		if decision == "applied" {
			stats.appliedCount++
			stats.confidences = append(stats.confidences, winnerConfidence)
		} else {
			stats.rejectedCount++
			reasonStr := "UNKNOWN"
			if reason.Valid && reason.String != "" {
				reasonStr = reason.String
			}
			stats.rejectionReasons[reasonStr]++
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	// Calculate statistics
	for _, stats := range statsByDistance {
		if len(stats.confidences) > 0 {
			// Mean
			sum := 0
			for _, c := range stats.confidences {
				sum += c
			}
			stats.meanConfidence = float64(sum) / float64(len(stats.confidences))

			// Median
			sorted := make([]int, len(stats.confidences))
			copy(sorted, stats.confidences)
			sort.Ints(sorted)
			mid := len(sorted) / 2
			if len(sorted)%2 == 0 {
				stats.medianConfidence = float64(sorted[mid-1]+sorted[mid]) / 2.0
			} else {
				stats.medianConfidence = float64(sorted[mid])
			}
		}
	}

	// Print results
	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  METHOD 1C: DISTANCE-CONFIDENCE CORRELATION ANALYSIS\n")
	fmt.Printf("  Database: %s\n", dbPath)
	fmt.Printf("  Total Decisions Analyzed: %d\n", totalDecisions)
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Summary table
	fmt.Printf("SUMMARY BY EDIT DISTANCE:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Distance\tTotal\tApplied\tRejected\tApply Rate\tMean Conf\tMedian Conf")
	fmt.Fprintln(w, "────────\t─────\t───────\t────────\t──────────\t─────────\t───────────")

	// Sort by distance
	distances := make([]int, 0, len(statsByDistance))
	for d := range statsByDistance {
		distances = append(distances, d)
	}
	sort.Ints(distances)

	for _, d := range distances {
		stats := statsByDistance[d]
		applyRate := float64(stats.appliedCount) / float64(stats.totalDecisions) * 100.0

		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%.1f%%\t%.1f%%\t%.1f%%\n",
			stats.distance,
			stats.totalDecisions,
			stats.appliedCount,
			stats.rejectedCount,
			applyRate,
			stats.meanConfidence,
			stats.medianConfidence,
		)
	}
	w.Flush()

	fmt.Printf("\n")
	fmt.Printf("REJECTION REASONS BY DISTANCE:\n")
	fmt.Printf("─────────────────────────────────────────────────────────────────────────────\n")

	for _, d := range distances {
		stats := statsByDistance[d]
		if stats.rejectedCount == 0 {
			continue
		}

		fmt.Printf("\nDistance %d (Rejected: %d)\n", stats.distance, stats.rejectedCount)

		// Sort reasons by count
		type reasonCount struct {
			reason string
			count  int
		}
		reasons := make([]reasonCount, 0, len(stats.rejectionReasons))
		for r, c := range stats.rejectionReasons {
			reasons = append(reasons, reasonCount{r, c})
		}
		sort.Slice(reasons, func(i, j int) bool {
			return reasons[i].count > reasons[j].count
		})

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		for _, rc := range reasons {
			pct := float64(rc.count) / float64(stats.rejectedCount) * 100.0
			fmt.Fprintf(w, "  %s\t%d\t(%.1f%%)\n", rc.reason, rc.count, pct)
		}
		w.Flush()
	}

	// Analysis and recommendations
	fmt.Printf("\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  ANALYSIS & RECOMMENDATIONS\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	// Check distance-1 vs distance-3 comparison
	if stats1, ok1 := statsByDistance[1]; ok1 {
		if stats3, ok3 := statsByDistance[3]; ok3 {
			if len(stats1.confidences) > 0 && len(stats3.confidences) > 0 {
				confDelta := stats1.meanConfidence - stats3.meanConfidence
				applyRate1 := float64(stats1.appliedCount) / float64(stats1.totalDecisions) * 100.0
				applyRate3 := float64(stats3.appliedCount) / float64(stats3.totalDecisions) * 100.0
				applyRateDelta := applyRate1 - applyRate3

				fmt.Printf("Distance-1 vs Distance-3 Comparison:\n")
				fmt.Printf("  • Distance-1 mean confidence: %.1f%%\n", stats1.meanConfidence)
				fmt.Printf("  • Distance-3 mean confidence: %.1f%%\n", stats3.meanConfidence)
				fmt.Printf("  • Confidence delta: %.1f%% (dist-1 higher)\n", confDelta)
				fmt.Printf("  • Distance-1 apply rate: %.1f%%\n", applyRate1)
				fmt.Printf("  • Distance-3 apply rate: %.1f%%\n", applyRate3)
				fmt.Printf("  • Apply rate delta: %.1f%% (dist-1 higher)\n", applyRateDelta)
				fmt.Printf("\n")

				if confDelta < 10.0 {
					fmt.Printf("✓ WELL-CALIBRATED: Distance-3 confidence only %.1f%% lower than distance-1.\n", confDelta)
					fmt.Printf("  This suggests distance-3 corrections that ARE applied are equally reliable.\n")
					fmt.Printf("\n")
				}

				if applyRateDelta > 30.0 {
					fmt.Printf("⚠ CONSERVATIVE: Distance-3 apply rate is %.1f%% lower than distance-1.\n", applyRateDelta)
					fmt.Printf("  You may be rejecting many valid distance-3 corrections.\n")
					fmt.Printf("  Recommendation: Review distance3_extra_* settings in data/config/pipeline.yaml\n")
					fmt.Printf("\n")

					// Check what's blocking distance-3
					if len(stats3.rejectionReasons) > 0 {
						fmt.Printf("  Top distance-3 rejection reasons:\n")
						type reasonCount struct {
							reason string
							count  int
						}
						reasons := make([]reasonCount, 0, len(stats3.rejectionReasons))
						for r, c := range stats3.rejectionReasons {
							reasons = append(reasons, reasonCount{r, c})
						}
						sort.Slice(reasons, func(i, j int) bool {
							return reasons[i].count > reasons[j].count
						})
						for i, rc := range reasons {
							if i >= 3 {
								break
							}
							pct := float64(rc.count) / float64(stats3.rejectedCount) * 100.0
							fmt.Printf("    %d. %s (%.1f%%)\n", i+1, rc.reason, pct)
						}
						fmt.Printf("\n")
					}
				}
			}
		}
	}

	// Overall assessment
	totalApplied := 0
	totalRejected := 0
	for _, stats := range statsByDistance {
		totalApplied += stats.appliedCount
		totalRejected += stats.rejectedCount
	}

	overallApplyRate := float64(totalApplied) / float64(totalDecisions) * 100.0
	fmt.Printf("Overall Statistics:\n")
	fmt.Printf("  • Total decisions: %d\n", totalDecisions)
	fmt.Printf("  • Applied: %d (%.1f%%)\n", totalApplied, overallApplyRate)
	fmt.Printf("  • Rejected: %d (%.1f%%)\n", totalRejected, 100.0-overallApplyRate)
	fmt.Printf("\n")

	if overallApplyRate < 30.0 {
		fmt.Printf("⚠ LOW CORRECTION RATE: Only %.1f%% of decisions resulted in corrections.\n", overallApplyRate)
		fmt.Printf("  This suggests very conservative thresholds. Consider:\n")
		fmt.Printf("  • Reducing min_consensus_reports\n")
		fmt.Printf("  • Reducing min_advantage\n")
		fmt.Printf("  • Reducing min_confidence_percent\n")
		fmt.Printf("\n")
	} else if overallApplyRate > 60.0 {
		fmt.Printf("⚠ HIGH CORRECTION RATE: %.1f%% of decisions resulted in corrections.\n", overallApplyRate)
		fmt.Printf("  This suggests aggressive thresholds. Validate with Method 1A (temporal stability).\n")
		fmt.Printf("\n")
	} else {
		fmt.Printf("✓ BALANCED: %.1f%% correction rate suggests well-calibrated thresholds.\n", overallApplyRate)
		fmt.Printf("\n")
	}

	fmt.Printf("═══════════════════════════════════════════════════════════════════════════\n")
	fmt.Printf("\n")

	return nil
}
