// File role: Follow actual manual human parsing into existing consumer paths.
// Parser tables own syntax boundaries; this witness detects lost metadata or
// source-specific bypasses in formatting, rules, archive fanout and path learning.
package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/commands"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHumanCommentConsumerPropagation(t *testing.T) {
	requirePathReportH3Mappings(t)
	for _, tc := range []struct {
		comment, remaining string
		report             int
		present            bool
	}{
		{"CW UP 2 QRT 1800Z", "UP 2 QRT 1800Z", 0, false},
		{"FT8 -14 QRT 1800Z", "QRT 1800Z", -14, true},
		{"FT4 +00 QRT 1800Z", "QRT 1800Z", 0, true},
		{"FT8 +31 QRT 1800Z", "+31 QRT 1800Z", 0, false},
		{"FT8 +31 dB QRT 1800Z", "QRT 1800Z", 31, true},
		{"FT8 UP 2 -12 dB QRT 1800Z", "UP 2 QRT 1800Z", -12, true},
		{"FT8 S9 +10 dB QRT 1800Z", "S9 +10 dB QRT 1800Z", 0, false},
		{"FT8 59+20dB QRT 1800Z", "59+20dB QRT 1800Z", 0, false},
	} {
		t.Run(tc.comment, func(t *testing.T) {
			input := make(chan *spot.Spot, 1)
			processor := commands.NewProcessor(nil, nil, input, nil, nil, nil)
			before := time.Now().UTC()
			if text := processor.ProcessCommandForClient("DX 14074 K1ABC "+tc.comment, "W1AW", "", nil, "go"); text != "Spot queued.\n" {
				t.Fatalf("manual human fixture refused: %q", text)
			}
			var s *spot.Spot
			select {
			case s = <-input:
			default:
				t.Fatal("manual human fixture did not enqueue")
			}
			if !s.IsHuman || s.HasReport != tc.present || s.Report != tc.report || s.Comment != tc.remaining || s.Time.Before(before) || s.Time.After(time.Now().UTC()) {
				t.Fatalf("incorrect source metadata before consumers: %+v", s)
			}
			// Literal dB text can remain with no report. Assert metadata and
			// the first literal word; the existing wire width can truncate
			// later words, whose complete stored text is checked separately.
			line := s.FormatDXCluster()
			payload := s.Mode
			if tc.present {
				payload += fmt.Sprintf(" %+d dB", tc.report)
			}
			payload += " " + strings.Fields(tc.remaining)[0]
			if !strings.Contains(line, payload) {
				t.Fatalf("display disagrees with parsed report: %q", line)
			}
			rules, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RuleReject, Expr: "SNR<10"}})
			if err != nil {
				t.Fatal(err)
			}
			if rules.Matches(s, filter.EvaluationContext{}) != (!tc.present || tc.report >= 10) {
				t.Fatal("SNR filter used invented, missing or different report")
			}
			comments, err := filter.PrepareRuleSet([]filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: `COMMENT="1800Z"`}})
			if err != nil || !comments.Matches(s, filter.EvaluationContext{}) {
				t.Fatalf("comment time became unfilterable: %v", err)
			}
			s.DEMetadata.Grid, s.DXMetadata.Grid = "FN20", "EM12"
			s.InvalidateMetadataCache()
			s.EnsureNormalized()
			originalTime := s.Time
			predictor := pathTimestampPredictor()
			pipeline, writer, server := pathTimestampDeliveryPipeline(t, predictor, newPathReportMetrics())
			ctx := outputSpotContext{spot: s}
			if !pipeline.prepareFanoutSpot(&ctx) {
				t.Fatal("human fixture unexpectedly failed fanout preparation")
			}
			wantCount := uint32(0)
			if tc.present {
				wantCount = 1
			}
			if got := pathTimestampPrediction(predictor, s, time.Now().UTC()); got.RawCount != wantCount {
				t.Fatalf("human report presence did not reach existing learning: count=%d want=%d", got.RawCount, wantCount)
			}
			pipeline.emitSpot(&ctx, outputDeliveryPlan{telnetDeliverNow: true, allowFast: true, allowMed: true, allowSlow: true})
			for _, got := range []*spot.Spot{readArchiveQueuedSpot(t, writer), readTelnetBroadcastSpot(t, server), pipeline.buf.GetRecent(1)[0]} {
				if got.HasReport != tc.present || got.Report != tc.report || got.Comment != tc.remaining || got.Time != originalTime {
					t.Fatalf("consumer fanout changed human source fields: %+v", got)
				}
			}
		})
	}
}
