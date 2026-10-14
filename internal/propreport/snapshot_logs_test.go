package propreport

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotLogMetadata(t *testing.T) {
	tests := []struct {
		name, fields                      string
		modern, valid, available, hasAsOf bool
		asOf                              int64
	}{
		{"legacy", "20m f=1 c=0", false, true, false, false, 0},
		{"epoch_zero", snapshotTestMetadata(0, true, true), true, true, true, true, 0},
		{"dated_unavailable", snapshotTestMetadata(-1, false, true), true, true, false, true, -1},
		{"initial_unavailable", snapshotTestMetadata(0, false, false), true, true, false, false, 0},
		{"minimum", snapshotTestMetadata(math.MinInt64, true, true), true, true, true, true, math.MinInt64},
		{"maximum", snapshotTestMetadata(math.MaxInt64, true, true), true, true, true, true, math.MaxInt64},
		{"partial", "available=true", true, false, false, false, 0},
		{"bare", "has_asof", true, false, false, false, 0},
		{"bad_delimiter", "available:true", true, false, false, false, 0},
		{"bad_key_case", "AVAILABLE=true HAS_ASOF=true ASOF_UNIX=0", true, false, false, false, 0},
		{"bad_bool", "available=1 has_asof=true asof_unix=0", true, false, false, false, 0},
		{"bad_asof", "available=true has_asof=true asof_unix=NaN", true, false, false, false, 0},
		{"overflow", "available=true has_asof=true asof_unix=9223372036854775808", true, false, false, false, 0},
		{"negative_overflow", "available=true has_asof=true asof_unix=-9223372036854775809", true, false, false, false, 0},
		{"duplicate_available", snapshotTestMetadata(0, true, true) + " available=false", true, false, false, false, 0},
		{"duplicate_has_asof", snapshotTestMetadata(0, true, true) + " has_asof=true", true, false, false, false, 0},
		{"duplicate_asof", snapshotTestMetadata(0, true, true) + " asof_unix=0", true, false, false, false, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseSnapshotLogMetadata("Path buckets (5m): " + tc.fields)
			want := snapshotLogMetadata{modern: tc.modern, valid: tc.valid, available: tc.available, hasAsOf: tc.hasAsOf, asOfUnix: tc.asOf}
			if got != want {
				t.Fatalf("metadata=%+v, want %+v", got, want)
			}
		})
	}
}

func TestModernSnapshotNumericPayload(t *testing.T) {
	tests := []struct {
		name, payload string
		want          bool
	}{
		{"known_zero", snapshotTestWeights("20m", 0, 0, 0), true},
		{"normal", "20m t=4 <1=1 1-2=1 2-3=0 3-5=0 5-10=0 >=10=2", true},
		{"commas", "20m t=1,200 <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=1,200", true},
		{"missing_bin", "20m t=0 <1=0 1-2=0 2-3=0 3-5=0 5-10=0", false},
		{"partial_value", "20m t=0 <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=0x", false},
		{"negative", "20m t=0 <1=-1 1-2=0 2-3=0 3-5=0 5-10=0 >=10=1", false},
		{"bad_commas", "20m t=1,,2 <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=12", false},
		{"duplicate_field", snapshotTestWeights("20m", 0, 0, 0) + " >=10=0", false},
		{"duplicate_band", snapshotTestWeights("20m", 0, 0, 0) + " " + snapshotTestWeights("20m", 1, 0, 1), false},
		{"wrong_total", snapshotTestWeights("20m", 2, 0, 1), false},
		{"overflow", "20m t=9223372036854775808 <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=0", false},
		{"sum_overflow", fmt.Sprintf("20m t=%d <1=%d 1-2=1 2-3=0 3-5=0 5-10=0 >=10=0", math.MaxInt, math.MaxInt), false},
		{"max_int", snapshotTestWeights("20m", math.MaxInt, 0, math.MaxInt), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			weights := parseModernSnapshotWeights("Path weight dist (5m): " + snapshotTestMetadata(0, true, true) + " " + tc.payload)
			if _, present := weights["20m"]; present != tc.want {
				t.Fatalf("payload presence=%v, want %v: %+v", present, tc.want, weights)
			}
		})
	}
	for _, payload := range []string{"20m f=9223372036854775808 c=0", "20m f=-1 c=0", "20m f=1 c=0x", "20m f=1", "20m f=1 c=0 c=0"} {
		if counts := parseSnapshotCounts("Path buckets (5m): "+payload, true); len(counts) != 0 {
			t.Fatalf("malformed count payload became a sample: %q %+v", payload, counts)
		}
	}
}

func TestSnapshotPairsAvailabilityAndIdentity(t *testing.T) {
	const emission = "1970/01/01 13:00:00"
	valid := snapshotTestMetadata(0, true, true)
	zeroWeights := snapshotTestWeights("20m", 0, 0, 0)
	tests := []struct {
		name, countMetadata, weightMetadata, countPayload, weightPayload, weightEmission string
		want                                                                             bool
	}{
		{"known_zero", valid, valid, "20m f=0 c=0", zeroWeights, emission, true},
		{"unknown_missing_band", valid, valid, "20m f=0 c=0", snapshotTestWeights("40m", 0, 0, 0), emission, false},
		{"dated_unavailable", snapshotTestMetadata(0, false, true), valid, "20m f=9 c=1", zeroWeights, emission, false},
		{"initial_unavailable", snapshotTestMetadata(0, false, false), valid, "20m f=0 c=0", zeroWeights, emission, false},
		{"weight_unavailable", valid, snapshotTestMetadata(0, false, false), "20m f=9 c=1", zeroWeights, emission, false},
		{"no_asof", snapshotTestMetadata(0, true, false), valid, "20m f=9 c=1", zeroWeights, emission, false},
		{"mismatched_reference", valid, snapshotTestMetadata(1, true, true), "20m f=9 c=1", zeroWeights, emission, false},
		{"distinct_emissions", valid, valid, "20m f=9 c=1", zeroWeights, "1970/01/01 13:05:00", false},
		{"missing_bin", valid, valid, "20m f=9 c=1", "20m t=0 <1=0", emission, false},
		{"partial_metadata", "available=true", valid, "20m f=9 c=1", zeroWeights, emission, false},
		{"legacy_counts_modern_weights", "", valid, "20m f=9 c=1", zeroWeights, emission, false},
		{"modern_counts_legacy_weights", valid, "", "20m f=9 c=1", zeroWeights, emission, false},
		{"legacy_pair", "", "", "20m f=0 c=0", zeroWeights, emission, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := []string{
				snapshotTestLine(emission, "Path buckets (5m):", tc.countMetadata, tc.countPayload),
				snapshotTestLine(tc.weightEmission, "Path weight dist (5m):", tc.weightMetadata, tc.weightPayload),
			}
			rows, _ := collectPathSnapshotSamples(entries, time.Unix(0, 0))
			if (len(rows) != 0) != tc.want {
				t.Fatalf("usable rows=%+v, want presence %v", rows, tc.want)
			}
			if tc.want {
				hour := 0
				if tc.name == "legacy_pair" {
					hour = 13
				}
				if row := rows["20m"][hour]; len(row) != 1 || row[0].FMed != 0 || row[0].Ge10Med != 0 || row[0].Lt1Med != 0 {
					t.Fatalf("known zero must remain quantitative at its actual clock: %+v", rows)
				}
			}
		})
	}
}

func TestSnapshotPairsMissingBandAndLegacyAssumptions(t *testing.T) {
	metadata := snapshotTestMetadata(0, true, true)
	entries := []string{
		snapshotTestLine("1970/01/01 12:00:00", "Path buckets (5m):", metadata, "20m f=9 c=0 40m f=7 c=1"),
		snapshotTestLine("1970/01/01 12:00:00", "Path weight dist (5m):", metadata, snapshotTestWeights("20m", 9, 0, 9)),
	}
	rows, _ := collectPathSnapshotSamples(entries, time.Unix(0, 0))
	if len(rows) != 1 || len(rows["20m"][0]) != 1 || rows["20m"][0][0].Ge10Med != 9 {
		t.Fatalf("missing modern band must be absent while complete band remains usable: %+v", rows)
	}
	legacy, _ := collectPathSnapshotSamples([]string{"1970/01/01 12:00:00 Path buckets (5m): 40m f=7 c=1"}, time.Unix(0, 0))
	if values := legacy["40m"][12]; len(values) != 1 || values[0].FMed != 7 || values[0].Ge10Med != 0 {
		t.Fatalf("legacy omitted-weight assumption changed: %+v", legacy)
	}
}

func TestSnapshotPairsChronologyAndDedup(t *testing.T) {
	date := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	sample := date.Add(12*time.Hour + 59*time.Minute).Unix()
	var entries []string
	for i, count := range []int{1, 99, 7} {
		emission := fmt.Sprintf("2026/10/13 13:%02d:00", i*5)
		payload := snapshotTestWeights("20m", count, 0, count)
		if i == 2 {
			payload = "20m t=7 <1=0" // Last malformed pair must not replace usable history.
		}
		entries = append(entries,
			snapshotTestLine(emission, "Path buckets (5m):", snapshotTestMetadata(sample, true, true), fmt.Sprintf("20m f=%d c=0", count)),
			snapshotTestLine(emission, "Path weight dist (5m):", snapshotTestMetadata(sample, true, true), payload))
	}
	rows, _ := collectPathSnapshotSamples(entries, date)
	if values := rows["20m"][12]; len(values) != 1 || values[0].FMed != 99 || values[0].Ge10Med != 99 {
		t.Fatalf("last complete usable pair must dedup by sampling reference/hour: %+v", rows)
	}
	if len(rows["20m"][13]) != 0 {
		t.Fatalf("emission hour must not relabel modern samples: %+v", rows)
	}
	for _, wrongDate := range []int64{date.Add(-time.Second).Unix(), date.Add(24 * time.Hour).Unix()} {
		metadata := snapshotTestMetadata(wrongDate, true, true)
		rows, _ := collectPathSnapshotSamples([]string{
			snapshotTestLine("2026/10/13 00:00:00", "Path buckets (5m):", metadata, "20m f=9 c=1"),
			snapshotTestLine("2026/10/13 00:00:00", "Path weight dist (5m):", metadata, snapshotTestWeights("20m", 9, 0, 9)),
		}, date)
		if len(rows) != 0 {
			t.Fatalf("cross-date sampling reference %d was attributed to selected date: %+v", wrongDate, rows)
		}
	}
}

func TestSnapshotVarianceHistoricalOrderAndDedup(t *testing.T) {
	entries := []string{
		snapshotTestLine("1970/01/01 12:00:00", "Path ge10 variance (5m):", snapshotTestMetadata(3600, true, true), "20m min=2 med=2 p75=2 max=2 deg=0"),
		snapshotTestLine("1970/01/01 12:05:00", "Path ge10 variance (5m):", snapshotTestMetadata(0, true, true), "20m min=1 med=1 p75=1 max=1 deg=0"),
		snapshotTestLine("1970/01/01 12:10:00", "Path ge10 variance (5m):", snapshotTestMetadata(0, true, true), "20m min=3 med=3 p75=3 max=3 deg=0"),
		snapshotTestLine("1970/01/01 12:15:00", "Path ge10 variance (5m):", snapshotTestMetadata(0, false, true), "20m min=99 med=99 p75=99 max=99 deg=0"),
		snapshotTestLine("1970/01/01 12:20:00", "Path ge10 variance (5m):", snapshotTestMetadata(86400, true, true), "20m min=99 med=99 p75=99 max=99 deg=0"),
	}
	_, variance := collectPathSnapshotSamples(entries, time.Unix(0, 0))
	if len(variance) != 2 || !reflect.DeepEqual(variance[0]["20m"], []ge10Variance{{Min: 3, Med: 3, P75: 3, Max: 3}}) ||
		!reflect.DeepEqual(variance[1]["20m"], []ge10Variance{{Min: 2, Med: 2, P75: 2, Max: 2}}) {
		t.Fatalf("historical references must survive reversed entry order and dedup only identical AsOf: %+v", variance)
	}
}

func TestSnapshotVarianceAvailabilityAndPayload(t *testing.T) {
	const emission = "1970/01/01 12:00:00"
	for _, payload := range []string{
		"20m min=0 med=0 p75=0 deg=1",
		"20m min=-1 med=0 p75=0 max=0 deg=1",
		"20m min=0 med=0 p75=0 max=9223372036854775808 deg=1",
		"20m min=2 med=1 p75=2 max=3 deg=0",
		"20m min=0 med=0 p75=0 max=0 deg=2",
	} {
		_, variance := collectPathSnapshotSamples([]string{
			snapshotTestLine(emission, "Path ge10 variance (5m):", snapshotTestMetadata(0, true, true), payload),
		}, time.Unix(0, 0))
		if len(variance) != 0 {
			t.Fatalf("malformed modern variance became quantitative: %q %+v", payload, variance)
		}
	}
	for _, metadata := range []string{"available=true", snapshotTestMetadata(0, true, true) + " has_asof=true", snapshotTestMetadata(0, true, false)} {
		_, variance := collectPathSnapshotSamples([]string{
			snapshotTestLine(emission, "Path ge10 variance (5m):", metadata, "20m min=0 med=0 p75=0 max=0 deg=1"),
		}, time.Unix(0, 0))
		if len(variance) != 0 {
			t.Fatalf("invalid variance metadata became a legacy zero: %q %+v", metadata, variance)
		}
	}
}

func TestSnapshotMedianNonnegativeIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		values []int
		want   int
	}{
		{"empty", nil, 0},
		{"ordinary_even", []int{1, 2}, 2},
		{"ordinary_odd", []int{9, 1, 2}, 2},
		{"maximum_equal", []int{math.MaxInt, math.MaxInt}, math.MaxInt},
		{"maximum_odd_population", []int{math.MaxInt - 1, math.MaxInt, math.MaxInt - 2}, math.MaxInt - 1},
		{"maximum_even_gap", []int{math.MaxInt - 2, math.MaxInt}, math.MaxInt - 1},
		{"maximum_odd_gap", []int{math.MaxInt - 1, math.MaxInt}, math.MaxInt},
		{"full_positive_range", []int{0, math.MaxInt}, math.MaxInt/2 + 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := median(tc.values); got != tc.want {
				t.Fatalf("median(%v)=%d, want %d", tc.values, got, tc.want)
			}
		})
	}
}

func TestGenerateSnapshotAvailabilityAndSampleHour(t *testing.T) {
	date := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	metadata := snapshotTestMetadata(date.Add(12*time.Hour+59*time.Minute).Unix(), true, true)
	entries := []string{
		snapshotTestLine("2026/10/13 13:00:00", "Path buckets (5m):", metadata, "20m f=0 c=0 40m f=99 c=1"),
		snapshotTestLine("2026/10/13 13:00:00", "Path weight dist (5m):", metadata, snapshotTestWeights("20m", 0, 0, 0)),
		snapshotTestLine("2026/10/13 13:00:00", "Path ge10 variance (5m):", metadata, "20m min=0 med=0 p75=0 max=0 deg=1"),
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "propagation.log")
	if err := os.WriteFile(logPath, []byte(strings.Join(entries, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := Generate(context.Background(), Options{
		Date: date, LogPath: logPath, JSONOut: filepath.Join(dir, "report.json"), ReportOut: filepath.Join(dir, "report.md"),
		ConfigDir: filepath.Join("..", "..", "data", "config"), NoLLM: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Summary.Bands) != 1 || result.Summary.Bands[0].Band != "20m" || len(result.Summary.Bands[0].Hours) != 1 {
		t.Fatalf("Generate fabricated absent-band evidence: %+v", result.Summary.Bands)
	}
	hour := result.Summary.Bands[0].Hours[0]
	if hour.Hour != "12:00" || hour.FMed != 0 || hour.Ge10Med != 0 || hour.Lt1Med != 0 || !hour.Ge10Degenerate || !hour.Ge10VarianceAvailable {
		t.Fatalf("Generate lost valid zero/sample-hour metadata: %+v", hour)
	}
}

func TestGenerateSnapshotVarianceAvailability(t *testing.T) {
	date := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	metadata := snapshotTestMetadata(date.Add(12*time.Hour).Unix(), true, true)
	tests := []struct {
		name, varianceMetadata, variancePayload string
		legacy, wantAvailable, wantDegenerate   bool
		ge10                                    int
	}{
		{"missing", "", "", false, false, false, 9},
		{"unavailable", snapshotTestMetadata(date.Add(12*time.Hour).Unix(), false, true), "20m min=0 med=0 p75=0 max=0 deg=1", false, false, false, 9},
		{"malformed_metadata", "available=true", "20m min=0 med=0 p75=0 max=0 deg=1", false, false, false, 9},
		{"malformed_payload", metadata, "20m min=0 med=0 p75=0 deg=1", false, false, false, 9},
		{"known_zero", metadata, "20m min=0 med=0 p75=0 max=0 deg=1", false, true, true, 0},
		{"known_positive", metadata, "20m min=9 med=9 p75=9 max=9 deg=0", false, true, false, 9},
		{"legacy_assumed_zero", "", "", true, true, false, 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rowMetadata := metadata
			if tc.legacy {
				rowMetadata = ""
			}
			entries := []string{
				snapshotTestLine("2026/10/13 12:05:00", "Path buckets (5m):", rowMetadata, "20m f=9 c=0"),
				snapshotTestLine("2026/10/13 12:05:00", "Path weight dist (5m):", rowMetadata, snapshotTestWeights("20m", 9, 9-tc.ge10, tc.ge10)),
			}
			if tc.variancePayload != "" {
				entries = append(entries, snapshotTestLine("2026/10/13 12:05:00", "Path ge10 variance (5m):", tc.varianceMetadata, tc.variancePayload))
			}
			dir := t.TempDir()
			logPath := filepath.Join(dir, "propagation.log")
			if err := os.WriteFile(logPath, []byte(strings.Join(entries, "\n")+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			result, err := Generate(context.Background(), Options{
				Date: date, LogPath: logPath, JSONOut: filepath.Join(dir, "report.json"), ReportOut: filepath.Join(dir, "report.md"),
				ConfigDir: filepath.Join("..", "..", "data", "config"), NoLLM: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Summary.Bands) != 1 || len(result.Summary.Bands[0].Hours) != 1 {
				t.Fatalf("missing connected quantitative result: %+v", result.Summary.Bands)
			}
			hour := result.Summary.Bands[0].Hours[0]
			if hour.Ge10Med != tc.ge10 || hour.Ge10VarianceAvailable != tc.wantAvailable || hour.Ge10Degenerate != tc.wantDegenerate {
				t.Fatalf("unknown variance was confused with known zero: %+v", hour)
			}
			jsonBytes, err := os.ReadFile(result.JSONPath)
			if err != nil {
				t.Fatal(err)
			}
			var disk struct {
				Bands []struct {
					Hours []struct {
						Available *bool `json:"ge10_variance_available"`
						Ge10      int   `json:"ge10_med"`
					} `json:"hours"`
				} `json:"bands"`
			}
			if err := json.Unmarshal(jsonBytes, &disk); err != nil {
				t.Fatal(err)
			}
			if len(disk.Bands) != 1 || len(disk.Bands[0].Hours) != 1 || disk.Bands[0].Hours[0].Available == nil ||
				*disk.Bands[0].Hours[0].Available != tc.wantAvailable || disk.Bands[0].Hours[0].Ge10 != tc.ge10 {
				t.Fatalf("JSON omitted or changed explicit variance availability: %s", jsonBytes)
			}
			reportBytes, err := os.ReadFile(result.ReportPath)
			if err != nil {
				t.Fatal(err)
			}
			report := string(reportBytes)
			if !tc.wantAvailable {
				if !strings.Contains(report, "No ge10 variance data available.") || strings.Contains(report, "ge10 rarely reaches strong levels") || strings.Contains(report, "informative across the day") {
					t.Fatalf("report interpreted unavailable variance as a measured zero: %s", report)
				}
			} else if tc.wantDegenerate || tc.legacy {
				if !strings.Contains(report, "Degenerate ge10 buckets") {
					t.Fatalf("known zero or legacy assumption lost existing degeneracy behavior: %s", report)
				}
			} else if strings.Contains(report, "ge10 rarely reaches strong levels") {
				t.Fatalf("known positive variance was labeled degenerate: %s", report)
			}
		})
	}
}

func TestSnapshotDegeneracyAvailableHoursOnly(t *testing.T) {
	bands := []bandSummary{{Band: "20m", Hours: []hourStat{
		{Ge10VarianceAvailable: true, Ge10Max: 9},
		{Ge10Max: 0},
		{Ge10Max: 0},
	}}}
	message := degeneracySummary(bands)
	if strings.Contains(message, "ge10 rarely reaches strong levels") || !strings.Contains(message, "unavailable for 2 band-hours") {
		t.Fatalf("unknown hours contaminated the available-hour calculation: %q", message)
	}
	bands[0].Hours[0] = hourStat{Ge10VarianceAvailable: true, Ge10Degenerate: true, Ge10Max: 0}
	if message = degeneracySummary(bands); !strings.Contains(message, "Degenerate ge10 buckets") {
		t.Fatalf("known-zero available hour must still be degenerate: %q", message)
	}
	bands[0].Hours[0] = hourStat{}
	if message = degeneracySummary(bands); message != "No ge10 variance data available." {
		t.Fatalf("all unavailable hours must not claim informative variance: %q", message)
	}
}

func FuzzSnapshotMetadataCannotDowngrade(f *testing.F) {
	for _, seed := range []string{"true", "", "false", "9223372036854775808", "-1", "0", "20m f=1 c=0", "available=true has_asof=true asof_unix=0"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			return
		}
		// Keep arbitrary field values from injecting additional metadata tokens.
		value := strings.Map(func(r rune) rune {
			if r == '=' || r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f' {
				return '_'
			}
			return r
		}, input)
		for _, fields := range []string{"available=" + value, snapshotTestMetadata(0, true, true) + " available=" + value} {
			metadata := parseSnapshotLogMetadata(fields)
			if !metadata.modern || metadata.valid {
				t.Fatalf("partial/duplicate fields downgraded or became valid: %q %+v", fields, metadata)
			}
			rows, _ := collectPathSnapshotSamples([]string{
				snapshotTestLine("1970/01/01 12:00:00", "Path buckets (5m):", fields, "20m f=7 c=0"),
				snapshotTestLine("1970/01/01 12:00:00", "Path weight dist (5m):", "", snapshotTestWeights("20m", 7, 0, 7)),
			}, time.Unix(0, 0))
			if len(rows) != 0 {
				t.Fatalf("unusable modern fields produced legacy quantitative evidence: %q %+v", fields, rows)
			}
		}
	})
}

func snapshotTestMetadata(asOf int64, available, hasAsOf bool) string {
	return fmt.Sprintf("available=%t has_asof=%t asof_unix=%d", available, hasAsOf, asOf)
}

func snapshotTestWeights(band string, total, lt1, ge10 int) string {
	return fmt.Sprintf("%s t=%d <1=%d 1-2=0 2-3=0 3-5=0 5-10=0 >=10=%d", band, total, lt1, ge10)
}

func snapshotTestLine(emission, prefix, metadata, payload string) string {
	return strings.Join([]string{emission, prefix, metadata, payload}, " ")
}
