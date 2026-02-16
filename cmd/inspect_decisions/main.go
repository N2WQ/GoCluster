// Program inspect_decisions examines specific decision records from the call correction log
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := flag.String("db", "data/logs/callcorr_debug_modified_2025-12-04.db", "Path to decision log database")
	flag.Parse()

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		log.Printf("open db failed: %v", err)
		return
	}
	defer db.Close()
	ctx := context.Background()

	// Count by decision type
	fmt.Println("\n=== DECISION TYPE BREAKDOWN ===")
	rows, err := db.QueryContext(ctx, `
		SELECT decision, COUNT(*) as count
		FROM decisions
		GROUP BY decision
		ORDER BY count DESC
	`)
	if err != nil {
		log.Printf("query decision breakdown failed: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var decision string
		var count int
		if err := rows.Scan(&decision, &count); err != nil {
			log.Printf("scan decision breakdown failed: %v", err)
			return
		}
		fmt.Printf("%s: %d\n", decision, count)
	}

	// Sample APPLIED decisions (if any)
	fmt.Println("\n=== SAMPLE APPLIED DECISIONS ===")
	rows2, err := db.QueryContext(ctx, `
		SELECT subject, winner, distance, winner_confidence, total_reporters, decision
		FROM decisions
		WHERE decision = 'APPLIED'
		LIMIT 10
	`)
	if err != nil {
		log.Printf("query applied decisions failed: %v", err)
		return
	}
	defer rows2.Close()

	count := 0
	for rows2.Next() {
		var subject, winner, decision string
		var distance, confidence, reporters int
		if err := rows2.Scan(&subject, &winner, &distance, &confidence, &reporters, &decision); err != nil {
			log.Printf("scan applied decision failed: %v", err)
			return
		}
		count++
		fmt.Printf("%s → %s (dist=%d, conf=%d%%, reporters=%d)\n",
			subject, winner, distance, confidence, reporters)
	}
	if count == 0 {
		fmt.Println("(No APPLIED decisions found)")
	}

	// Sample distance-1 rejections
	fmt.Println("\n=== SAMPLE DISTANCE-1 REJECTIONS ===")
	rows3, err := db.QueryContext(ctx, `
		SELECT subject, winner, distance, winner_confidence, winner_support,
		       total_reporters, min_reports, min_advantage, min_confidence,
		       decision, reason
		FROM decisions
		WHERE distance = 1 AND decision != 'APPLIED'
		ORDER BY winner_support DESC
		LIMIT 10
	`)
	if err != nil {
		log.Printf("query distance-1 rejections failed: %v", err)
		return
	}
	defer rows3.Close()

	for rows3.Next() {
		var subject, winner, decision string
		var reason sql.NullString
		var distance, confidence, winnerSupport, totalReporters, minReports, minAdvantage, minConfidence int
		if err := rows3.Scan(&subject, &winner, &distance, &confidence, &winnerSupport,
			&totalReporters, &minReports, &minAdvantage, &minConfidence, &decision, &reason); err != nil {
			log.Printf("scan distance-1 rejection failed: %v", err)
			return
		}

		reasonStr := "UNKNOWN"
		if reason.Valid {
			reasonStr = reason.String
		}

		fmt.Printf("\n%s → %s\n", subject, winner)
		fmt.Printf("  Winner support: %d/%d reporters (conf=%d%%)\n", winnerSupport, totalReporters, confidence)
		fmt.Printf("  Thresholds: min_reports=%d, min_advantage=%d, min_confidence=%d%%\n",
			minReports, minAdvantage, minConfidence)
		fmt.Printf("  Decision: %s (reason: %s)\n", decision, reasonStr)
	}

	// Check if there are any decisions with high confidence that were rejected
	fmt.Println("\n=== HIGH CONFIDENCE REJECTIONS (>70%) ===")
	rows4, err := db.QueryContext(ctx, `
		SELECT subject, winner, distance, winner_confidence, winner_support,
		       total_reporters, decision, reason
		FROM decisions
		WHERE winner_confidence > 70 AND decision != 'APPLIED'
		ORDER BY winner_confidence DESC
		LIMIT 10
	`)
	if err != nil {
		log.Printf("query high-confidence rejections failed: %v", err)
		return
	}
	defer rows4.Close()

	count = 0
	for rows4.Next() {
		var subject, winner, decision string
		var reason sql.NullString
		var distance, confidence, winnerSupport, totalReporters int
		if err := rows4.Scan(&subject, &winner, &distance, &confidence, &winnerSupport,
			&totalReporters, &decision, &reason); err != nil {
			log.Printf("scan high-confidence rejection failed: %v", err)
			return
		}
		count++

		reasonStr := "UNKNOWN"
		if reason.Valid {
			reasonStr = reason.String
		}

		fmt.Printf("%s → %s (dist=%d, conf=%d%%, support=%d/%d, reason=%s)\n",
			subject, winner, distance, confidence, winnerSupport, totalReporters, reasonStr)
	}
	if count == 0 {
		fmt.Println("(No high-confidence rejections found)")
	}

	fmt.Println()
}
