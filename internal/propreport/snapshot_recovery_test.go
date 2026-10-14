package propreport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRepeatedBandsPreserveCompletePairs(t *testing.T) {
	metadata := snapshotTestMetadata(0, true, true)
	tests := []struct {
		name, counts, weights, countMetadata, weightMetadata, weightEmission string
		want20, want40                                                       int
	}{
		{"truncated_weight", "20m f=99 c=0 40m f=9 c=0", "20m t=99 <1=0 " + snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:00:00", 7, 9},
		{"missing_band", "40m f=9 c=0", snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:00:00", 7, 9},
		{"truncated_count", "20m f=99 40m f=9 c=0", snapshotTestWeights("20m", 99, 0, 99) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:00:00", 7, 9},
		{"unavailable", "20m f=99 c=0 40m f=9 c=0", snapshotTestWeights("20m", 99, 0, 99) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, snapshotTestMetadata(0, false, true), "1970/01/01 12:00:00", 7, 3},
		{"distinct_prefixes", "20m f=99 c=0 40m f=9 c=0", snapshotTestWeights("20m", 99, 0, 99) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:01:00", 7, 3},
		{"distinct_references", "20m f=99 c=0 40m f=9 c=0", snapshotTestWeights("20m", 99, 0, 99) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, snapshotTestMetadata(1, true, true), "1970/01/01 12:00:00", 7, 3},
		{"known_zero", "20m f=0 c=0 40m f=9 c=0", snapshotTestWeights("20m", 0, 0, 0) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:00:00", 0, 9},
		{"complete_replacement", "20m f=11 c=0 40m f=9 c=0", snapshotTestWeights("20m", 11, 0, 11) + " " + snapshotTestWeights("40m", 9, 0, 9), metadata, metadata, "1970/01/01 12:00:00", 11, 9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := snapshotRecoveryBase(metadata)
			// The later encountered emission has an earlier header timestamp.
			entries = append(entries,
				snapshotTestLine("1970/01/01 12:00:00", "Path buckets (5m):", tc.countMetadata, tc.counts),
				snapshotTestLine(tc.weightEmission, "Path weight dist (5m):", tc.weightMetadata, tc.weights))
			rows, _ := collectPathSnapshotSamples(entries, time.Unix(0, 0))
			assertRecoveryPair(t, rows, "20m", tc.want20)
			assertRecoveryPair(t, rows, "40m", tc.want40)
		})
	}
	t.Run("separate_asof_samples", func(t *testing.T) {
		entries := snapshotRecoveryBase(metadata)
		entries = append(entries,
			snapshotTestLine("1970/01/01 12:00:00", "Path buckets (5m):", snapshotTestMetadata(1, true, true), "20m f=11 c=0"),
			snapshotTestLine("1970/01/01 12:00:00", "Path weight dist (5m):", snapshotTestMetadata(1, true, true), snapshotTestWeights("20m", 11, 0, 11)))
		rows, _ := collectPathSnapshotSamples(entries, time.Unix(0, 0))
		if values := rows["20m"][0]; len(values) != 2 || values[0].FMed != 7 || values[0].Ge10Med != 7 || values[1].FMed != 11 || values[1].Ge10Med != 11 {
			t.Fatalf("different references must remain distinct ordered samples: %+v", rows)
		}
	})
}

func TestSnapshotRepeatedVariancePreservesValidBands(t *testing.T) {
	metadata := snapshotTestMetadata(0, true, true)
	tests := []struct {
		name, payload string
		want20        ge10Variance
	}{
		{"truncated", "20m min=99 med=99", ge10Variance{Min: 7, Med: 7, P75: 7, Max: 7}},
		{"missing", "", ge10Variance{Min: 7, Med: 7, P75: 7, Max: 7}},
		{"positive_degenerate", "20m min=9 med=9 p75=9 max=9 deg=1", ge10Variance{Min: 7, Med: 7, P75: 7, Max: 7}},
		{"zero_nondegenerate", "20m min=0 med=0 p75=0 max=0 deg=0", ge10Variance{Min: 7, Med: 7, P75: 7, Max: 7}},
		{"known_zero", "20m min=0 med=0 p75=0 max=0 deg=1", ge10Variance{Deg: true}},
		{"complete_replacement", "20m min=11 med=11 p75=11 max=11 deg=0", ge10Variance{Min: 11, Med: 11, P75: 11, Max: 11}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := []string{
				snapshotTestLine("1970/01/01 13:00:00", "Path ge10 variance (5m):", metadata, "20m min=7 med=7 p75=7 max=7 deg=0 40m min=3 med=3 p75=3 max=3 deg=0"),
				snapshotTestLine("1970/01/01 12:00:00", "Path ge10 variance (5m):", metadata, tc.payload+" 40m min=9 med=9 p75=9 max=9 deg=0"),
			}
			_, variance := collectPathSnapshotSamples(entries, time.Unix(0, 0))
			if !reflect.DeepEqual(variance[0]["20m"], []ge10Variance{tc.want20}) || !reflect.DeepEqual(variance[0]["40m"], []ge10Variance{{Min: 9, Med: 9, P75: 9, Max: 9}}) {
				t.Fatalf("repeated variance must replace only valid bands in encounter order: %+v", variance)
			}
		})
	}
	for _, payload := range []string{"20m min=9 med=9 p75=9 max=9 deg=1", "20m min=0 med=0 p75=0 max=0 deg=0"} {
		_, variance := collectPathSnapshotSamples([]string{snapshotTestLine("1970/01/01 12:00:00", "Path ge10 variance (5m):", metadata, payload)}, time.Unix(0, 0))
		if len(variance) != 0 {
			t.Fatalf("contradiction without prior valid variance must remain unknown: %q %+v", payload, variance)
		}
	}
}

func TestGenerateSnapshotRecovery(t *testing.T) {
	date := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	metadata := snapshotTestMetadata(date.Add(12*time.Hour).Unix(), true, true)
	tests := []struct {
		name, payload                    string
		prior, multiBand, available, deg bool
		wantMax                          int
	}{
		{"partial_multiband", "20m min=99 med=99", true, true, true, false, 7},
		{"positive_flag_with_prior", "20m min=9 med=9 p75=9 max=9 deg=1", true, false, true, false, 7},
		{"zero_flag_with_prior", "20m min=0 med=0 p75=0 max=0 deg=0", true, false, true, false, 7},
		{"positive_flag_unknown", "20m min=9 med=9 p75=9 max=9 deg=1", false, false, false, false, 0},
		{"zero_flag_unknown", "20m min=0 med=0 p75=0 max=0 deg=0", false, false, false, false, 0},
		{"known_zero", "20m min=0 med=0 p75=0 max=0 deg=1", true, false, true, true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counts, weights, variance := "20m f=7 c=0", snapshotTestWeights("20m", 7, 0, 7), "20m min=7 med=7 p75=7 max=7 deg=0"
			if tc.multiBand {
				counts += " 40m f=3 c=0"
				weights += " " + snapshotTestWeights("40m", 3, 0, 3)
				variance += " 40m min=3 med=3 p75=3 max=3 deg=0"
			}
			entries := []string{
				snapshotTestLine("2026/10/13 13:00:00", "Path buckets (5m):", metadata, counts),
				snapshotTestLine("2026/10/13 13:00:00", "Path weight dist (5m):", metadata, weights),
			}
			if tc.prior {
				entries = append(entries, snapshotTestLine("2026/10/13 13:00:00", "Path ge10 variance (5m):", metadata, variance))
			}
			payload := tc.payload
			if tc.multiBand {
				entries = append(entries,
					snapshotTestLine("2026/10/13 12:05:00", "Path buckets (5m):", metadata, "20m f=99 c=0 40m f=9 c=0"),
					snapshotTestLine("2026/10/13 12:05:00", "Path weight dist (5m):", metadata, "20m t=99 <1=0 "+snapshotTestWeights("40m", 9, 0, 9)))
				payload += " 40m min=9 med=9 p75=9 max=9 deg=0"
			}
			entries = append(entries, snapshotTestLine("2026/10/13 12:05:00", "Path ge10 variance (5m):", metadata, payload))
			result, disk, report := generateRecoveryReport(t, date, entries)
			wantBands := 1
			if tc.multiBand {
				wantBands = 2
			}
			if len(result.Summary.Bands) != wantBands || len(disk.Bands) != wantBands {
				t.Fatalf("recovery lost a band: summary=%+v JSON=%+v", result.Summary.Bands, disk.Bands)
			}
			if _, present := disk.Bands["20m"]; !present {
				t.Fatalf("JSON must retain the literal 20m band: %+v", disk)
			}
			if _, present := disk.Bands["40m"]; present != tc.multiBand {
				t.Fatalf("JSON must retain exactly the expected band identities: %+v", disk)
			}
			for _, band := range result.Summary.Bands {
				want, wantMax, available, deg := 7, tc.wantMax, tc.available, tc.deg
				if band.Band == "40m" {
					want, wantMax, available, deg = 9, 9, true, false
				}
				if len(band.Hours) != 1 {
					t.Fatalf("repeated reference became extra observations: %+v", band)
				}
				hour := band.Hours[0]
				if hour.Hour != "12:00" || hour.FMed != want || hour.Ge10Med != want || hour.Ge10Max != wantMax || hour.Ge10VarianceAvailable != available || hour.Ge10Degenerate != deg {
					t.Fatalf("connected report changed recovery/availability: %+v", band)
				}
				jsonBand, present := disk.Bands[band.Band]
				if !present || len(jsonBand) != 1 || jsonBand[0].Available == nil || *jsonBand[0].Available != available || jsonBand[0].Hour != "12:00" || jsonBand[0].F != want || jsonBand[0].Ge10 != want || jsonBand[0].Max != wantMax || jsonBand[0].Deg != deg {
					t.Fatalf("JSON omitted or changed band evidence: %+v", disk)
				}
				if !strings.Contains(report, fmt.Sprintf("f_med ~%d–%d, ge10_med ~%d–%d", want, want, want, want)) {
					t.Fatalf("text omitted recovered quantitative evidence: %s", report)
				}
			}
			if !tc.available && !strings.Contains(report, "No ge10 variance data available.") || tc.deg && !strings.Contains(report, "Degenerate ge10 buckets") || !tc.deg && strings.Contains(report, "Degenerate ge10 buckets") {
				t.Fatalf("text confused unavailable, measured zero or positive variance: %s", report)
			}
		})
	}
}

// Fuzz complete-pair recovery with deliberately invalid band values. Hex encoding
// bounds the input and prevents fuzz data from injecting metadata or new bands;
// the oracle is the literal prior 20m pair and the complete new 40m pair.
func FuzzSnapshotBandRecovery(f *testing.F) {
	for mode := uint8(0); mode < 6; mode++ {
		f.Add(mode, uint8(9), []byte("truncated"))
	}
	f.Fuzz(func(t *testing.T, mode, count uint8, data []byte) {
		if len(data) > 256 {
			return
		}
		metadata := snapshotTestMetadata(0, true, true)
		entries := snapshotRecoveryBase(metadata)
		invalid := "x" + hex.EncodeToString(data)
		counts := fmt.Sprintf("20m f=99 c=0 40m f=%d c=0", count)
		weights := "20m t=" + invalid + " <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=99"
		variance := "20m min=99 med=99 p75=99 max=99 deg=1"
		want20 := 7
		switch mode % 6 {
		case 1:
			counts = "20m f=" + invalid + fmt.Sprintf(" c=0 40m f=%d c=0", count)
			weights = snapshotTestWeights("20m", 99, 0, 99)
		case 2:
			weights = "20m t=99 <1=0"
			variance = "20m min=0 med=0 p75=0 max=0 deg=0"
		case 3:
			counts = fmt.Sprintf("40m f=%d c=0", count)
			weights, variance = "", ""
		case 4:
			want20 = int(count)
			counts = fmt.Sprintf("20m f=%d c=0 40m f=%d c=0", count, count)
			weights = snapshotTestWeights("20m", int(count), 0, int(count))
			variance = snapshotRecoveryVariance("20m", int(count))
		case 5:
			want20 = 0
			counts = fmt.Sprintf("20m f=0 c=0 40m f=%d c=0", count)
			weights = snapshotTestWeights("20m", 0, 0, 0)
			variance = snapshotRecoveryVariance("20m", 0)
		}
		entries = append(entries,
			snapshotTestLine("1970/01/01 13:00:00", "Path ge10 variance (5m):", metadata, snapshotRecoveryVariance("20m", 7)+" "+snapshotRecoveryVariance("40m", 3)),
			snapshotTestLine("1970/01/01 12:00:00", "Path buckets (5m):", metadata, counts),
			snapshotTestLine("1970/01/01 12:00:00", "Path weight dist (5m):", metadata, weights+" "+snapshotTestWeights("40m", int(count), 0, int(count))),
			snapshotTestLine("1970/01/01 12:00:00", "Path ge10 variance (5m):", metadata, variance+" "+snapshotRecoveryVariance("40m", int(count))))
		rows, variances := collectPathSnapshotSamples(entries, time.Unix(0, 0))
		assertRecoveryPair(t, rows, "20m", want20)
		assertRecoveryPair(t, rows, "40m", int(count))
		for band, want := range map[string]int{"20m": want20, "40m": int(count)} {
			values := variances[0][band]
			if len(values) != 1 || values[0] != (ge10Variance{Min: want, Med: want, P75: want, Max: want, Deg: want == 0}) {
				t.Fatalf("variance recovery disagrees with literal expectation: %s %+v want %d", band, values, want)
			}
		}
	})
}

func snapshotRecoveryBase(metadata string) []string {
	return []string{
		snapshotTestLine("1970/01/01 13:00:00", "Path buckets (5m):", metadata, "20m f=7 c=0 40m f=3 c=0"),
		snapshotTestLine("1970/01/01 13:00:00", "Path weight dist (5m):", metadata, snapshotTestWeights("20m", 7, 0, 7)+" "+snapshotTestWeights("40m", 3, 0, 3)),
	}
}

func snapshotRecoveryVariance(band string, value int) string {
	deg := 0
	if value == 0 {
		deg = 1
	}
	return fmt.Sprintf("%s min=%d med=%d p75=%d max=%d deg=%d", band, value, value, value, value, deg)
}

func assertRecoveryPair(t *testing.T, rows map[string]map[int][]hourStat, band string, want int) {
	t.Helper()
	if values := rows[band][0]; len(values) != 1 || values[0].FMed != want || values[0].Ge10Med != want || values[0].Lt1Med != 0 {
		t.Fatalf("%s must retain one complete pair at the sample hour: %+v, want %d", band, rows, want)
	}
}

type recoveryJSONHour struct {
	Hour      string `json:"hour"`
	F         int    `json:"f_med"`
	Ge10      int    `json:"ge10_med"`
	Max       int    `json:"ge10_max"`
	Deg       bool   `json:"ge10_degenerate"`
	Available *bool  `json:"ge10_variance_available"`
}

type recoveryJSON struct {
	Bands map[string][]recoveryJSONHour
}

func generateRecoveryReport(t *testing.T, date time.Time, entries []string) (Result, recoveryJSON, string) {
	t.Helper()
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
	jsonBytes, err := os.ReadFile(result.JSONPath)
	if err != nil {
		t.Fatal(err)
	}
	var disk struct {
		Bands []struct {
			Band  string             `json:"band"`
			Hours []recoveryJSONHour `json:"hours"`
		} `json:"bands"`
	}
	if err := json.Unmarshal(jsonBytes, &disk); err != nil {
		t.Fatal(err)
	}
	parsed := recoveryJSON{Bands: make(map[string][]recoveryJSONHour)}
	for _, band := range disk.Bands {
		if _, duplicate := parsed.Bands[band.Band]; duplicate {
			t.Fatalf("JSON duplicated band %s", band.Band)
		}
		parsed.Bands[band.Band] = band.Hours
	}
	report, err := os.ReadFile(result.ReportPath)
	if err != nil {
		t.Fatal(err)
	}
	return result, parsed, string(report)
}
