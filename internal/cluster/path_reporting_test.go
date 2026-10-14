package cluster

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/propreport"
	"dxcluster/pathreliability"
)

func reportingHistogram(at int64, ge10 int) pathreliability.WeightHistogram {
	return pathreliability.WeightHistogram{Available: true, AsOfUnix: at,
		Edges: []float64{1, 2, 3, 5, 10},
		Bands: []pathreliability.BandWeightHistogram{{Band: "20m", Total: ge10, Bins: []int{0, 0, 0, 0, 0, ge10}}}}
}

func TestPathReportingUnavailableAndEpoch(t *testing.T) {
	initial := pathreliability.PredictorStatsSnapshot{}
	if got := pathBucketLogLine(initial); got != "Path buckets (5m): available=false has_asof=false asof_unix=0" {
		t.Fatalf("initial unavailable fabricated values: %s", got)
	}
	cached := pathreliability.PredictorStatsSnapshot{HasSnapshot: true, AsOfUnix: 0,
		Stats: pathreliability.PredictorStats{CombinedFine: 7, CombinedCoarse: 3},
		Bands: []pathreliability.BandBucketStats{{Band: "20m", Fine: 7, Coarse: 3}}}
	if got := pathBucketLogLine(cached); !strings.Contains(got, "available=false has_asof=true asof_unix=0 20m f=7 c=3") {
		t.Fatalf("cached values lost date/unavailability: %s", got)
	}
	lines := strings.Join(formatPathSnapshotLines(nil, cached), "\n")
	if !strings.Contains(lines, "7 (L2) / 3 (L1) as of 1970-01-01T00:00:00Z (unavailable; cached)") || !strings.Contains(lines, "20m[-]: 7 / 3") {
		t.Fatalf("overview mixed publication or relabeled cache: %s", lines)
	}
	if got := strings.Join(formatPathSnapshotLines(nil, initial), "\n"); got != "[yellow]H3 path pairs[-]: n/a (unavailable)" {
		t.Fatalf("initial unknown became zero: %s", got)
	}
	cached.Available = true
	if got := pathBucketLogLine(cached); !strings.Contains(got, "available=true has_asof=true asof_unix=0") {
		t.Fatalf("valid epoch became missing: %s", got)
	}
	if got := pathWeightLogLine(reportingHistogram(0, 0)); !strings.Contains(got, "available=true has_asof=true asof_unix=0 20m t=0 <1=0 1-2=0 2-3=0 3-5=0 5-10=0 >=10=0") {
		t.Fatalf("known empty epoch band lost: %s", got)
	}
	if got := pathWeightLogLine(pathreliability.WeightHistogram{AsOfUnix: 20}); got != "Path weight dist (5m): available=false has_asof=false asof_unix=20" {
		t.Fatalf("failed histogram fabricated data: %s", got)
	}
}

func TestPathReportingLiveWindowChronology(t *testing.T) {
	var window pathWeightWindow
	if window.record(pathreliability.WeightHistogram{}) || window.hasLastAsOf {
		t.Fatal("unavailable sample changed cursor")
	}
	if !window.record(reportingHistogram(0, 7)) || !window.hasLastAsOf || window.lastAsOf != 0 {
		t.Fatal("valid epoch sample was not recorded")
	}
	for _, at := range []int64{0, -1} {
		if window.record(reportingHistogram(at, 0)) {
			t.Fatal("repeated/older time added a sample")
		}
	}
	bad := reportingHistogram(10, 0)
	bad.Bands[0].Bins = nil
	if window.record(bad) || window.lastAsOf != 0 {
		t.Fatal("missing bins fabricated zero or advanced cursor")
	}
	if !window.record(reportingHistogram(300, 0)) || len(window.samples["20m"]) != 2 {
		t.Fatal("known zero was not a valid sample")
	}
	min, med, p75, max, ok := window.stats("20m")
	if !ok || min != 0 || med != 4 || p75 != 7 || max != 7 {
		t.Fatalf("incorrect two-sample window: %d %d %d %d %v", min, med, p75, max, ok)
	}
	if !window.record(reportingHistogram(3601, 2)) || len(window.samples["20m"]) != 2 || window.samples["20m"][0].ts.Unix() != 300 {
		t.Fatal("expiry did not use actual sampling time")
	}
	if !window.record(reportingHistogram(7202, 1)) || len(window.samples["20m"]) != 1 {
		t.Fatal("old window retained beyond hour")
	}
	line := pathVarianceLogLine(reportingHistogram(7202, 1), &window)
	if line != "Path ge10 variance (5m): available=true has_asof=true asof_unix=7202 20m min=1 med=1 p75=1 max=1 deg=0" {
		t.Fatalf("variance date/value mismatch: %s", line)
	}
	window.samples["20m"] = []pathGe10Sample{{val: math.MaxInt}, {val: math.MaxInt}}
	_, median, _, _, _ := window.stats("20m")
	if median != math.MaxInt {
		t.Fatalf("finite counts overflowed median: %d", median)
	}
}

// Exercise the actual publisher grammar through the real offline report. The
// emission date/hour deliberately differs from the safe sample reference.
func TestPathReportingFormatterToReport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		asOf        time.Time
		emitted     time.Time
		countsFail  bool
		weightsFail bool
		mismatch    bool
		empty       bool
		wantRows    bool
	}{
		{name: "real_sample_old_hour", asOf: time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC), emitted: time.Date(2026, 4, 22, 20, 0, 0, 0, time.UTC), wantRows: true},
		{name: "cached_counts_unavailable", asOf: time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC), countsFail: true},
		{name: "weights_unavailable", asOf: time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC), weightsFail: true},
		{name: "references_mismatch", asOf: time.Date(2026, 4, 22, 10, 0, 0, 0, time.UTC), mismatch: true},
		{name: "valid_zero_epoch", asOf: time.Unix(0, 0).UTC(), empty: true, wantRows: true},
		{name: "cross_midnight", asOf: time.Date(2026, 4, 22, 23, 59, 0, 0, time.UTC), emitted: time.Date(2026, 4, 23, 0, 5, 0, 0, time.UTC), wantRows: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := pathreliability.NewPredictor(pathreliability.DefaultConfig(), []string{"20m", "40m"})
			if !tc.empty {
				p.UpdateWithReceiverHash(pathreliability.BucketCombined, 1, 2, 3, 4, "20m", -5, .5,
					pathreliability.ObservationTimes{ObservedAt: tc.asOf, ArrivedAt: tc.asOf}, false, 1)
			}
			p.RefreshStatsSnapshot(tc.asOf)
			snapshot, histogram := p.StatsSnapshot(), p.WeightHistogramByBand(tc.asOf)
			if !snapshot.Available || !histogram.Available {
				t.Fatal("fixture lacks actual coherent publications")
			}
			snapshot.Available = !tc.countsFail
			histogram.Available = !tc.weightsFail
			if tc.mismatch {
				histogram.AsOfUnix++
			}
			emission := tc.emitted
			if emission.IsZero() {
				emission = tc.asOf
			}
			prefix := emission.Format("2006/01/02 15:04:05")
			content := fmt.Sprintf("%s %s\n%s %s\n", prefix, pathBucketLogLine(snapshot), prefix, pathWeightLogLine(histogram))
			var window pathWeightWindow
			if window.record(histogram) {
				content += fmt.Sprintf("%s %s\n", prefix, pathVarianceLogLine(histogram, &window))
			}
			dir := t.TempDir()
			logPath := filepath.Join(dir, "prop.log")
			if err := os.WriteFile(logPath, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			result, err := propreport.Generate(context.Background(), propreport.Options{
				Date: tc.asOf, LogPath: logPath, JSONOut: filepath.Join(dir, "prop.json"), ReportOut: filepath.Join(dir, "prop.md"),
				ConfigDir: filepath.Join("..", "..", "data", "config"), NoLLM: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantRows {
				if len(result.Summary.Bands) != 0 {
					t.Fatalf("unknown/mismatched sample became quantitative: %+v", result.Summary.Bands)
				}
				return
			}
			if len(result.Summary.Bands) != 2 {
				t.Fatalf("explicit known zero band missing: %+v", result.Summary.Bands)
			}
			for _, band := range result.Summary.Bands {
				wantFine := 0
				if band.Band == "20m" && !tc.empty {
					wantFine = 1
				}
				// Weight diagnostics contain both stored fine and coarse buckets;
				// one report creates two sub-unit buckets, not one union bucket.
				wantLt1 := 2 * wantFine
				if len(band.Hours) != 1 || band.Hours[0].Hour != tc.asOf.Format("15:00") || band.Hours[0].FMed != wantFine || band.Hours[0].Lt1Med != wantLt1 || band.Hours[0].Ge10Med != 0 {
					t.Fatalf("sampling time or known values lost: %+v", band)
				}
			}
		})
	}
}
