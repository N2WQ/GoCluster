package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
)

func TestPathCapDiagnosticRenderedAndRecorded(t *testing.T) {
	at := time.Date(2026, time.June, 18, 4, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		reports     int
		wantLimited bool
		wantTag     string
	}{
		{"aged_uncapped", 48, false, "n48/u24|w12|a"},
		{"cap_reduced", 54, true, "n54/u27/c24/rx6|w12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := recentConsumerConfig(t, pathreliability.ReceiverContributionShadow)
			s, c, sp := recentConsumerFixture(t, cfg, at.Add(180*time.Second), "20m", "IO91", false)
			recentConsumerReports(t, s, c, sp, tc.reports, 6, 1, -10, at, at, false)
			c.filter = passRuleTestFilter(t, "MODE=FT8 AND PATH=HIGH")
			c.setDiagMode(diagModePath)

			env := deliverPathPredictionReuseSpot(t, s, c, sp)
			if env.pathPrediction == nil {
				t.Fatal("PATH filter did not carry the prediction")
			}
			got := env.pathPrediction.result
			if got.CapLimited != tc.wantLimited || got.CappedCount != 24 || got.CappedWeight != 12 ||
				got.CapWouldBlock || got.InsufficientReason != pathreliability.InsufficientNone ||
				got.Source != pathreliability.SourceCombined || got.Class != "HIGH" ||
				got.Glyph != cfg.GlyphSymbols.High || !got.HasP50 || got.P50DB != -9.5 {
				t.Fatalf("cap diagnostic or observed prediction changed: %+v", got)
			}
			line := strings.TrimRight(s.formatSpotEnvelopeForClient(c, env), "\r\n")
			if len(line) != 78 || !strings.Contains(line, tc.wantTag) {
				t.Fatalf("rendered 78-column diagnostic %q, want tag %q", line, tc.wantTag)
			}
			stats := s.PathPredictionStatsSnapshot()
			wantCount := uint64(0)
			if tc.wantLimited {
				wantCount = 1
			}
			if stats.Total != 1 || stats.Combined != 1 || stats.CapLimited != wantCount || stats.CapWouldBlock != 0 {
				t.Fatalf("counter disagrees with the rendered diagnostic: %+v", stats)
			}
		})
	}
}

func TestPathCapDiagnosticWeightRounding(t *testing.T) {
	result := pathreliability.Result{
		Source: pathreliability.SourceCombined, Weight: 10.5, CappedWeight: 10.5 - 5e-10,
		ObservationCount: 24, CappedCount: 24, ReceiverCount: 3, ReceiverRequired: 3,
		ObservedRecentSupport: pathreliability.ReportSupport{RawUsable: 24, Admitted: 24, AdmittedAvailable: true},
	}
	if got := diagPathTag(pathPrediction{result: result}, true); got != "n24/u24|w11|a0" {
		t.Fatalf("clear cap flag must display the rounded active weight, got %q", got)
	}
	result.CapLimited = true
	if got := diagPathTag(pathPrediction{result: result}, true); got != "n24/u24/c24/rx3|w10|a0" {
		t.Fatalf("set cap flag must display the rounded capped weight, got %q", got)
	}
}
