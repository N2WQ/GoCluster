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
			// The display must not turn absent human reports into dB labels.
			line := s.FormatDXCluster()
			if strings.Contains(line, " dB") != tc.present || tc.present && !strings.Contains(line, fmt.Sprintf("%s %+d dB", s.Mode, tc.report)) {
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
