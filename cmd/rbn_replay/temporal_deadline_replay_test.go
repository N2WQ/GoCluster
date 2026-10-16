// File role: Proves replay services held decisions on scheduled event time,
// independently of unrelated traffic that neither changes target evidence nor
// reaches capacity. Literal receiver fixtures are software oracles, not RF truth.
package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
)

func TestReplayTemporalDeadlineTrafficIndependence(t *testing.T) {
	day := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	for _, traffic := range []string{"continuous", "one_minute_gap", "ten_minute_gap", "skipped_class", "non_correction_mode", "no_traffic"} {
		t.Run(traffic, func(t *testing.T) {
			rows := replayDeadlineReports(day)
			class, mode := "CQ", "CW"
			switch traffic {
			case "skipped_class":
				class = "DX"
			case "non_correction_mode":
				mode = "FT8"
			}
			if traffic == "continuous" || traffic == "skipped_class" || traffic == "non_correction_mode" {
				for second := 6; second <= 20; second++ {
					rows = append(rows, replayDeadlineCSVRow(day.Add(time.Duration(second)*time.Second), "N1AAA", "K2XYZ", 14080, class, mode))
				}
			}
			if traffic != "no_traffic" {
				gap := 10 * time.Minute
				if traffic == "one_minute_gap" {
					gap = time.Minute
				}
				rows = append(rows, replayDeadlineCSVRow(day.Add(gap), "N1AAA", "K2XYZ", 14080, class, mode))
			}
			r := replayDeadlineCSVRunner(t, day, rows, replayDeadlineConfig())
			if err := r.replayDay(); err != nil {
				t.Fatal(err)
			}
			// Every variant must meet the literal oracle, not merely agree with
			// another variant that could be equally late or equally wrong.
			replayDeadlineAssertCorrection(t, r, day.Add(19*time.Second))
			metrics := r.resolver.MetricsSnapshot()
			if metrics.DropQueueFull != 0 || metrics.DropMaxKeys != 0 || metrics.DropMaxCandidates != 0 || metrics.DropMaxReporters != 0 || metrics.CapLossDiscards != 0 ||
				r.abMetrics.Temporal.FallbackResolver != 0 || r.abMetrics.Temporal.AbstainLowMargin != 0 || r.abMetrics.Temporal.OverflowBypass != 0 {
				t.Fatalf("traffic fixture reached loss/overflow or fallback: resolver=%+v temporal=%+v", metrics, r.abMetrics.Temporal)
			}
			replayDeadlineAssertRetired(t, r)
			replayDeadlineAssertSamples(t, r)
		})
	}
}

func TestReplayTemporalDeadlineBoundaries(t *testing.T) {
	for _, after := range []time.Duration{0, time.Second} {
		t.Run(after.String(), func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			r, held := replayDeadlineHeldRunner(t, at, replayDeadlineConfig())
			due := at.Add(15 * time.Second)
			replayDeadlineSkippedRow(t, r, due.Add(-time.Nanosecond))
			if len(r.temporalPending) != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 || held.DXCall != "K1ABD" {
				t.Fatal("held observation released before its deadline")
			}
			replayDeadlineSkippedRow(t, r, due.Add(after))
			if held.DXCall != "K1ABC" || held.Confidence != "C" {
				t.Fatalf("deadline output=%s/%s want K1ABC/C", held.DXCall, held.Confidence)
			}
			replayDeadlineAssertCorrection(t, r, due)
			replayDeadlineAssertRetired(t, r)
		})
	}
}

func TestReplayTemporalDeadlineEqualTimeOrder(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 45, 0, time.UTC)
	cfg := replayDeadlineConfig()
	cfg.TemporalDecoder.MaxPending = 1
	r, held := replayDeadlineHeldRunner(t, at, cfg)
	due := at.Add(15 * time.Second)
	before := formatCorrectionDecisionSummary(r.tracker)
	row := rbnHistoryRow{Time: due, FreqKHz: 14020, Band: "20m", DXCall: "K1ABC", Spotter: "N0NEW", Mode: "CW", SpotClass: rbnfeed.SpotClassCQ}
	if err := r.processCSVRow(row, true); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(r.runbookSamplesFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.samples) != 1 || !r.samples[0].TS.Equal(due) || !strings.Contains(string(log), due.Format("2006/01/02 15:04:05")+" "+before+"\n") ||
		before == formatCorrectionDecisionSummary(r.tracker) {
		t.Fatal("same-time sample did not capture counters before temporal release")
	}
	if held.DXCall != "K1ABC" || r.abMetrics.Temporal.Pending != 2 || len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 ||
		r.abMetrics.Output.ConfidenceCounts.Samples != 1 || !r.temporalQueue[0].due.Equal(due.Add(15*time.Second)) {
		t.Fatalf("incoming row did not acquire the slot freed by release: held=%s temporal=%+v pending=%d queue=%+v", held.DXCall, r.abMetrics.Temporal, len(r.temporalPending), r.temporalQueue)
	}
	replayDeadlineAssertCorrection(t, r, due)
	replayDeadlineSkippedRow(t, r, due.Add(15*time.Second))
	replayDeadlineAssertRetired(t, r)
}

func TestReplayTemporalDeadlineDeferred(t *testing.T) {
	for _, traffic := range []string{"single_gap", "row_between_deadlines"} {
		t.Run(traffic, func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			cfg := replayDeadlineConfig()
			cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 5
			cfg.TemporalDecoder.MinMarginScore, cfg.TemporalDecoder.OverflowAction = 50000, "abstain"
			r, _ := replayDeadlineHeldRunner(t, at, cfg)
			if traffic == "row_between_deadlines" {
				replayDeadlineSkippedRow(t, r, at.Add(2*time.Second))
				if len(r.temporalQueue) != 1 || !r.temporalQueue[0].due.Equal(at.Add(5*time.Second)) || r.abMetrics.Output.ConfidenceCounts.Samples != 0 {
					t.Fatal("first deadline was not deferred to max wait")
				}
			}
			replayDeadlineSkippedRow(t, r, at.Add(10*time.Minute))
			latency := r.abMetrics.Temporal.CommitLatencyMS
			if latency.Samples != 1 || latency.LE5000 != 1 || latency.GT5000 != 0 || r.abMetrics.Temporal.AbstainLowMargin != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 1 {
				t.Fatalf("deferred decision was not terminal at 5s: temporal=%+v output=%+v", r.abMetrics.Temporal, r.abMetrics.Output)
			}
			replayDeadlineAssertRetired(t, r)
			r.drainTemporal(at.Add(10*time.Minute), true)
			if r.abMetrics.Temporal.CommitLatencyMS.Samples != 1 || r.abMetrics.Output.ConfidenceCounts.Samples != 1 {
				t.Fatal("repeated drain duplicated deferred output")
			}
		})
	}
}

func TestReplayTemporalDeadlineNearMidnight(t *testing.T) {
	day := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	at := day.Add(24*time.Hour - 10*time.Second)
	r := replayDeadlineCSVRunner(t, day, replayDeadlineReports(at), replayDeadlineConfig())
	if err := r.replayDay(); err != nil {
		t.Fatal(err)
	}
	replayDeadlineAssertCorrection(t, r, at.Add(19*time.Second))
	replayDeadlineAssertRetired(t, r)
	replayDeadlineAssertSamples(t, r)
}

func TestReplayTemporalDeadlineFutureEOF(t *testing.T) {
	day := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	cfg := replayDeadlineConfig()
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 73*60*60, 74*60*60
	r := replayDeadlineCSVRunner(t, day, replayDeadlineReports(day), cfg)
	r.recentBandStore = spot.NewRecentBandStore(time.Hour)
	if err := r.replayDay(); err != nil {
		t.Fatal(err)
	}
	if r.abMetrics.Temporal.Pending == 0 || r.abMetrics.Temporal.CommitLatencyMS.Samples != r.abMetrics.Temporal.Pending || len(r.stabilityCollector.corrections) != 0 ||
		!r.nextLegacyCleanup.Equal(r.dayEnd.Add(24*time.Hour+10*time.Minute)) {
		t.Fatalf("force changed EOF clock or failed terminal accounting: temporal=%+v cleanup=%v", r.abMetrics.Temporal, r.nextLegacyCleanup)
	}
	replayDeadlineAssertRetired(t, r)
	replayDeadlineAssertSamples(t, r)
	before := r.abMetrics.Output.ConfidenceCounts.Samples
	r.drainTemporal(r.dayEnd.Add(24*time.Hour), true)
	if r.abMetrics.Output.ConfidenceCounts.Samples != before {
		t.Fatal("EOF force duplicated an already retired output")
	}
}

func TestReplayTemporalDeadlineNoHolds(t *testing.T) {
	day := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	cfg := replayDeadlineConfig()
	cfg.TemporalDecoder.Enabled = false
	r := replayDeadlineCSVRunner(t, day, nil, cfg)
	if err := r.replayDay(); err != nil {
		t.Fatal(err)
	}
	replayDeadlineAssertSamples(t, r)
	if len(r.temporalQueue) != 0 || len(r.temporalPending) != 0 || r.abMetrics.Output.ConfidenceCounts.Samples != 0 {
		t.Fatal("empty disabled replay invented held output")
	}
}

func replayDeadlineConfig() config.CallCorrectionConfig {
	cfg := replayTemporalFreshConfig()
	cfg.RecencySeconds = 300
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 15, 15
	cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.MaxEventsPerKey = 64, 64
	return cfg
}

func replayDeadlineCSVRow(at time.Time, receiver, call string, freq float64, class, mode string) string {
	return fmt.Sprintf("%s,%.1f,20m,%s,10,%s,%s,%s", receiver, freq, call, at.Format("2006-01-02 15:04:05"), class, mode)
}

func replayDeadlineReports(at time.Time) []string {
	rows := make([]string, 0, 6)
	for i, receiver := range []string{"N0AAA", "N0BBB", "N0CCC", "N0EEE"} {
		rows = append(rows, replayDeadlineCSVRow(at.Add(time.Duration(i)*time.Second), receiver, "K1ABC", 14020, "CQ", "CW"))
	}
	rows = append(rows, replayDeadlineCSVRow(at.Add(4*time.Second), "N0DDD", "K1ABD", 14020, "CQ", "CW"))
	// This common row publishes all target reports before traffic diverges.
	// Other traffic stays 60 kHz away and uses a different receiver/callsign.
	return append(rows, replayDeadlineCSVRow(at.Add(5*time.Second), "N1AAA", "K2XYZ", 14080, "CQ", "CW"))
}

func replayDeadlineCSVRunner(t *testing.T, day time.Time, rows []string, cfg config.CallCorrectionConfig) *replayRunner {
	t.Helper()
	r := newReplayRunner(replayRequest{})
	r.runbookSamplesFile = replayDeadlineSampleFile(t)
	t.Cleanup(r.close)
	r.cfg = &config.Config{CallCorrection: cfg}
	r.dayStart, r.dayEnd = day, day.Add(24*time.Hour)
	r.stabilityCollector = newReplayStabilityCollector(replayStabilityConfig{})
	if err := r.buildReplayRuntime(); err != nil {
		t.Fatal(err)
	}
	if err := r.driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	r.csvParser = newTestRBNHistoryCSV(t, "callsign,freq,band,dx,db,date,mode,tx_mode\n"+strings.Join(rows, "\n"))
	return r
}

func replayDeadlineHeldRunner(t *testing.T, at time.Time, cfg config.CallCorrectionConfig) (*replayRunner, *spot.Spot) {
	t.Helper()
	r, key := replayTemporalFreshRunner(t, cfg, at, 5*time.Minute)
	r.runbookSamplesFile = replayDeadlineSampleFile(t)
	t.Cleanup(r.close)
	r.dayStart, r.dayEnd = at.Truncate(24*time.Hour), at.Truncate(24*time.Hour).Add(24*time.Hour)
	r.nextSampleAt = at.Truncate(time.Minute).Add(time.Minute)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	return r, s
}

func replayDeadlineSampleFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "samples-")
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func replayDeadlineSkippedRow(t *testing.T, r *replayRunner, at time.Time) {
	t.Helper()
	row := rbnHistoryRow{Time: at, FreqKHz: 14080, Band: "20m", DXCall: "K2XYZ", Spotter: "N1AAA", Mode: "CW", SpotClass: rbnfeed.SpotClassDX}
	if err := r.processCSVRow(row, true); err != nil {
		t.Fatal(err)
	}
}

func replayDeadlineAssertCorrection(t *testing.T, r *replayRunner, due time.Time) {
	t.Helper()
	found := 0
	for _, correction := range r.stabilityCollector.corrections {
		if correction.Freq != 14020 {
			continue
		}
		found++
		if correction.Winner != "K1ABC" || correction.Ts != due.Unix() {
			t.Errorf("target correction=%+v want K1ABC at %s", correction, due)
		}
	}
	if found != 1 {
		t.Errorf("target corrections=%d want exactly one K1ABC at %s", found, due)
	}
}

func replayDeadlineAssertRetired(t *testing.T, r *replayRunner) {
	t.Helper()
	if len(r.temporalPending) != 0 || len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 {
		t.Errorf("held ownership remains: map=%d heap=%d decoder=%d", len(r.temporalPending), len(r.temporalQueue), r.temporalDecoder.PendingCount())
	}
	for _, item := range r.temporalQueue[:cap(r.temporalQueue)] {
		if item != nil {
			t.Fatal("retired heap backing storage retains an observation")
		}
	}
}

func replayDeadlineAssertSamples(t *testing.T, r *replayRunner) {
	t.Helper()
	if len(r.samples) != 1441 {
		t.Fatalf("samples=%d want 1441", len(r.samples))
	}
	for i, sample := range r.samples {
		if !sample.TS.Equal(r.dayStart.Add(time.Duration(i) * time.Minute)) {
			t.Fatalf("sample %d has timestamp %v", i, sample.TS)
		}
	}
}
