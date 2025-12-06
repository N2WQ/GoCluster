package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type bustedRow struct {
	Bad    string
	Corr   string
	Freq   float64
	Band   string
	Status string
}

type bandStat struct {
	Total       int
	Matched     int
	Absent      int
	PresentMiss int
}

type reasonRow struct {
	Reason string
	Count  int64
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func bandFor(freq float64) string {
	switch {
	case freq < 1800:
		return "LF"
	case freq < 2000:
		return "160m"
	case freq < 4000:
		return "80m"
	case freq < 8000:
		return "40m"
	case freq < 11000:
		return "30m"
	case freq < 15000:
		return "20m"
	case freq < 19000:
		return "17m"
	case freq < 22000:
		return "15m"
	case freq < 25000:
		return "12m"
	case freq < 30000:
		return "10m"
	default:
		return "6m+"
	}
}

func parseBusted(path string, year int, minTs, maxTs int64) (int, []bustedRow, map[string]struct{}) {
	file, err := os.Open(path)
	must(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	totalLines := 0
	rows := make([]bustedRow, 0, 4096)
	unique := make(map[string]struct{})

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		totalLines++
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 6 {
			continue
		}

		freq, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			continue
		}

		// Determine correction tokens (may span multiple fields until SNR/reporter/mode).
		corrStart := 4
		corrEnd := len(parts) - 2 // before reporter and mode
		if corrEnd <= corrStart {
			continue
		}

		// Try to detect SNR numeric position as a boundary.
		snrIdx := len(parts) - 3
		if snrIdx >= corrStart {
			if _, err := strconv.Atoi(parts[snrIdx]); err == nil {
				corrEnd = snrIdx
			}
		}

		if corrEnd <= corrStart {
			continue
		}

		corr := strings.TrimSpace(strings.Join(parts[corrStart:corrEnd], " "))
		if corr == "" {
			continue
		}

		// Parse timestamp to filter to DB coverage window.
		tsStr := fmt.Sprintf("%s-%d %s", parts[0], year, parts[1])
		ts, err := time.Parse("02-Jan-2006 1504Z", tsStr)
		if err != nil {
			continue
		}
		epoch := ts.Unix()
		if epoch < minTs || epoch > maxTs {
			continue
		}

		row := bustedRow{
			Bad:  strings.ToUpper(parts[2]),
			Corr: strings.ToUpper(corr),
			Freq: freq,
			Band: bandFor(freq),
		}
		key := row.Bad + "|" + row.Corr
		unique[key] = struct{}{}
		rows = append(rows, row)
	}
	must(scanner.Err())
	return totalLines, rows, unique
}

func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) * 100 / float64(total)
}

func label(s string) string {
	if s == "" {
		return "<none>"
	}
	return s
}

func main() {
	dateFlag := flag.String("date", "", "Analysis date YYYY-MM-DD (defaults to yesterday)")
	dbFlag := flag.String("db", "", "Path to callcorr_debug_modified_YYYY-MM-DD.db")
	bustedFlag := flag.String("busted", "", "Path to Busted-DD-MMM-YYYY.txt")
	csvFlag := flag.String("csv", "", "Path to RBN CSV (info only)")
	reportFlag := flag.String("report", "", "Output report path")
	flag.Parse()

	analysisDate := time.Now().AddDate(0, 0, -1)
	if *dateFlag != "" {
		d, err := time.Parse("2006-01-02", *dateFlag)
		must(err)
		analysisDate = d
	}

	dayStr := analysisDate.Format("2006-01-02")
	dayCompact := analysisDate.Format("20060102")
	bustedStr := analysisDate.Format("02-Jan-2006")

	dbPath := *dbFlag
	if dbPath == "" {
		dbPath = filepath.Join("data", "logs", fmt.Sprintf("callcorr_debug_modified_%s.db", dayStr))
	}
	bustedPath := *bustedFlag
	if bustedPath == "" {
		bustedPath = filepath.Join("data", "logs", fmt.Sprintf("Busted-%s.txt", bustedStr))
	}
	csvPath := *csvFlag
	if csvPath == "" {
		csvPath = filepath.Join("data", "logs", fmt.Sprintf("%s.csv", dayCompact))
	}
	reportPath := *reportFlag
	if reportPath == "" {
		reportPath = filepath.Join("data", "reports", fmt.Sprintf("analysis-%s.txt", dayStr))
	}

	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro")
	must(err)
	defer db.Close()

	var minTs, maxTs, decisionCount int64
	must(db.QueryRow("select min(ts), max(ts), count(*) from decisions").Scan(&minTs, &maxTs, &decisionCount))

	var appliedCount, uniqueApplied int64
	must(db.QueryRow("select count(*) from decisions where decision='applied'").Scan(&appliedCount))
	must(db.QueryRow("select count(*) from (select distinct subject, winner from decisions where decision='applied')").Scan(&uniqueApplied))

	appliedSet := make(map[string]struct{})
	rows, err := db.Query("select distinct upper(trim(subject)) || '|' || upper(trim(winner)) from decisions where decision='applied'")
	must(err)
	for rows.Next() {
		var key string
		must(rows.Scan(&key))
		appliedSet[key] = struct{}{}
	}
	must(rows.Err())
	rows.Close()

	subjectSet := make(map[string]struct{})
	subjRows, err := db.Query("select distinct upper(trim(subject)) from decisions")
	must(err)
	for subjRows.Next() {
		var s string
		must(subjRows.Scan(&s))
		subjectSet[s] = struct{}{}
	}
	must(subjRows.Err())
	subjRows.Close()

	totalBustedLines, bustedRows, bustedUnique := parseBusted(bustedPath, analysisDate.Year(), minTs, maxTs)

	matchedPairs := 0
	for key := range bustedUnique {
		if _, ok := appliedSet[key]; ok {
			matchedPairs++
		}
	}

	bandStats := make(map[string]*bandStat)
	matchedRows := 0
	absentRows := 0
	presentMissRows := 0

	for i := range bustedRows {
		row := &bustedRows[i]
		bs := bandStats[row.Band]
		if bs == nil {
			bs = &bandStat{}
			bandStats[row.Band] = bs
		}
		bs.Total++

		key := row.Bad + "|" + row.Corr
		if _, ok := appliedSet[key]; ok {
			row.Status = "matched"
			bs.Matched++
			matchedRows++
		} else if _, ok := subjectSet[row.Bad]; !ok {
			row.Status = "absent"
			bs.Absent++
			absentRows++
		} else {
			row.Status = "present_miss"
			bs.PresentMiss++
			presentMissRows++
		}
	}

	rejectReasons := make([]reasonRow, 0)
	rejRows, err := db.Query("select coalesce(reason,''), count(*) from decisions where decision='rejected' group by coalesce(reason,'')")
	must(err)
	for rejRows.Next() {
		var r string
		var c int64
		must(rejRows.Scan(&r, &c))
		rejectReasons = append(rejectReasons, reasonRow{Reason: r, Count: c})
	}
	must(rejRows.Err())
	rejRows.Close()
	sort.Slice(rejectReasons, func(i, j int) bool { return rejectReasons[i].Count > rejectReasons[j].Count })

	d3Reasons := make([]reasonRow, 0)
	d3Rows, err := db.Query("select coalesce(reason,''), count(*) from decisions where decision='rejected' and distance=3 group by coalesce(reason,'')")
	must(err)
	for d3Rows.Next() {
		var r string
		var c int64
		must(d3Rows.Scan(&r, &c))
		d3Reasons = append(d3Reasons, reasonRow{Reason: r, Count: c})
	}
	must(d3Rows.Err())
	d3Rows.Close()
	sort.Slice(d3Reasons, func(i, j int) bool { return d3Reasons[i].Count > d3Reasons[j].Count })

	var totalConfRejects, nearConfD12, nearConfD3 int64
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence'").Scan(&totalConfRejects))
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence' and distance<=2 and winner_confidence between 55 and 59").Scan(&nearConfD12))
	must(db.QueryRow("select count(*) from decisions where decision='rejected' and reason='confidence' and distance=3 and winner_confidence between 60 and 64").Scan(&nearConfD3))

	report := make([]string, 0, 128)
	report = append(report, fmt.Sprintf("Daily Call Correction Analysis - %s", dayStr))
	report = append(report, fmt.Sprintf("Database: %s", dbPath))
	report = append(report, fmt.Sprintf("Reference: %s", bustedPath))
	report = append(report, fmt.Sprintf("RBN CSV:   %s", csvPath))
	report = append(report, "")
	report = append(report, fmt.Sprintf("Coverage window: %s to %s UTC (decisions: %d)",
		time.Unix(minTs, 0).UTC().Format(time.RFC3339),
		time.Unix(maxTs, 0).UTC().Format(time.RFC3339),
		decisionCount,
	))
	report = append(report, fmt.Sprintf("Applied corrections: %d (unique pairs: %d)", appliedCount, uniqueApplied))
	report = append(report, fmt.Sprintf("Reference lines (all): %d", totalBustedLines))
	report = append(report, fmt.Sprintf("Reference pairs in DB window: %d", len(bustedUnique)))
	report = append(report, fmt.Sprintf("Matched pairs: %d", matchedPairs))

	pairRecall := 0.0
	if len(bustedUnique) > 0 {
		pairRecall = float64(matchedPairs) * 100 / float64(len(bustedUnique))
	}
	report = append(report, fmt.Sprintf("Pair-level recall: %.1f%%", pairRecall))
	report = append(report, "")

	report = append(report, "Band recall (reference rows within DB window):")
	report = append(report, "Band  Total Matched Recall Absent PresentMiss")
	bands := make([]string, 0, len(bandStats))
	for b := range bandStats {
		bands = append(bands, b)
	}
	sort.Strings(bands)
	for _, band := range bands {
		bs := bandStats[band]
		rec := 0.0
		if bs.Total > 0 {
			rec = float64(bs.Matched) * 100 / float64(bs.Total)
		}
		report = append(report, fmt.Sprintf("%-4s %5d %7d %6.1f%% %6d %11d",
			band, bs.Total, bs.Matched, rec, bs.Absent, bs.PresentMiss))
	}
	report = append(report, "")

	totalRef := len(bustedRows)
	report = append(report, "Miss classification (rows within window):")
	report = append(report, fmt.Sprintf("  Total reference rows: %d", totalRef))
	report = append(report, fmt.Sprintf("  Matched: %d (%.1f%%)", matchedRows, percent(matchedRows, totalRef)))
	report = append(report, fmt.Sprintf("  Absent subjects: %d (%.1f%%)", absentRows, percent(absentRows, totalRef)))
	report = append(report, fmt.Sprintf("  Present but missed: %d (%.1f%%)", presentMissRows, percent(presentMissRows, totalRef)))
	report = append(report, "")

	report = append(report, "Reject reasons:")
	for _, r := range rejectReasons {
		report = append(report, fmt.Sprintf("  %-15s %7d", label(r.Reason), r.Count))
	}
	report = append(report, "")

	report = append(report, "Distance=3 reject reasons:")
	for _, r := range d3Reasons {
		report = append(report, fmt.Sprintf("  %-15s %7d", label(r.Reason), r.Count))
	}
	report = append(report, "")

	report = append(report, "Confidence gating headroom:")
	report = append(report, fmt.Sprintf("  Total confidence rejects: %d", totalConfRejects))
	report = append(report, fmt.Sprintf("  Dist<=2 with 55-59%% confidence: %d", nearConfD12))
	report = append(report, fmt.Sprintf("  Dist=3 with 60-64%% confidence: %d", nearConfD3))

	must(os.MkdirAll(filepath.Dir(reportPath), 0o755))
	f, err := os.Create(reportPath)
	must(err)
	defer f.Close()

	writer := bufio.NewWriter(f)
	for _, line := range report {
		_, err := writer.WriteString(line + "\n")
		must(err)
	}
	must(writer.Flush())

	for _, line := range report {
		fmt.Println(line)
	}
}
