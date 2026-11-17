// Literal arithmetic comes from the separate, non-steered v2 model review.
// These are capped heuristic scores, not probabilities or RF-truth labels.
package spot

import (
	"testing"
	"time"
)

func ccv2BayesSettings(store RecentSupportStore) CorrectionSettings {
	return CorrectionSettings{
		MinConsensusReports: 2, MinAdvantage: 1, MinConfidencePercent: 45, MaxEditDistance: 2,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		FamilyPolicy:    CorrectionFamilyPolicy{Configured: true},
		RecentBandStore: store, RecentBandRecordMinUniqueSpotters: 4,
		ResolverRecentPlus1MinUniqueWinner: 4, ResolverRecentPlus1MaxDistance: 1,
		ResolverRecentPlus1RequireSubjectWeaker: true,
		BayesBonusPolicy: CorrectionBayesBonusPolicy{Configured: true, Enabled: true,
			WeightDistance1Milli: 350, WeightDistance2Milli: 200,
			WeightedSmoothingMilli: 1000, RecentSmoothing: 2,
			ObsLogCapMilli: 350, PriorLogMinMilli: -200, PriorLogMaxMilli: 600,
			ReportThresholdDistance1Milli: 450, ReportThresholdDistance2Milli: 450,
			AdvantageThresholdDistance1Milli: 150, AdvantageThresholdDistance2Milli: 150,
			AdvantageMinWeightedDeltaDistance1Milli: 200, AdvantageMinWeightedDeltaDistance2Milli: 300,
			AdvantageExtraConfidenceDistance1: 3, AdvantageExtraConfidenceDistance2: 5,
			RequireCandidateValidated: true, RequireSubjectUnvalidatedDistance2: true},
	}
}

func TestCCV2BayesIndependentArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		winner, subject, rw, rs      int
		wantDistance1, wantDistance2 int
	}{
		{"no_evidence", 1000, 1000, 0, 0, 0, 0},
		{"positive_caps", 3000, 1000, 4, 0, 560, 470},
		{"partial_subject_two", 3000, 1000, 4, 2, 492, 431},
		{"partial_subject_three", 3000, 1000, 4, 3, 414, 386},
		{"six_receiver_tie", 6000, 5800, 4, 0, 239, 149},
		{"nine_receiver_tie", 9000, 8700, 4, 0, 240, 150},
		{"negative_caps", 1000, 3000, 0, 4, -420, -390},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for distance, subject := range []string{"K1ABD", "K1AXD"} {
				store, _ := newResolverHistoryProbe(t, "legacy", map[string][]int{"K1ABC": {tc.rw}, subject: {tc.rs}})
				cfg := ccv2BayesSettings(store)
				got := EvaluateResolverPrimaryGates(subject, "K1ABC", "20m", "CW", 2, 3, 75,
					tc.subject, tc.winner, cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
				want := []int{tc.wantDistance1, tc.wantDistance2}[distance]
				if got.Distance != distance+1 || got.BayesScoreMilli != want || got.BayesWinnerRecent != tc.rw || got.BayesSubjectRecent != tc.rs {
					t.Fatalf("independent score vector distance%d: %+v want score%d history%d:%d", distance+1, got, want, tc.rw, tc.rs)
				}
			}
		})
	}
}

func TestCCV2BayesReportBoundaryAndNoStacking(t *testing.T) {
	for _, threshold := range []int{469, 470, 471} {
		store, _ := newResolverHistoryProbe(t, "legacy", map[string][]int{"K1ABC": {4}, "K1AXD": {0}})
		cfg := ccv2BayesSettings(store)
		cfg.MinConsensusReports = 4
		cfg.BayesBonusPolicy.ReportThresholdDistance2Milli = threshold
		got := EvaluateResolverPrimaryGates("K1AXD", "K1ABC", "20m", "CW", 1, 3, 75, 1000, 3000,
			cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
		if got.BayesScoreMilli != 470 || got.BayesReportBonusApplied != (threshold <= 470) || got.Allow != (threshold <= 470) {
			t.Fatalf("inclusive report threshold%d drifted: %+v", threshold, got)
		}
	}
	store, _ := newResolverHistoryProbe(t, "legacy", map[string][]int{"K1ABC": {4}, "K1ABD": {0}})
	cfg := ccv2BayesSettings(store)
	cfg.MinConsensusReports, cfg.ResolverRecentPlus1Enabled = 4, true
	got := EvaluateResolverPrimaryGates("K1ABD", "K1ABC", "20m", "CW", 1, 3, 75, 1000, 3000,
		cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
	if !got.Allow || !got.RecentPlus1Applied || got.BayesReportBonusConsidered || got.BayesReportBonusApplied || got.EffectiveSupport != 4 {
		t.Fatalf("recent+1 and Bayes stacked report votes: %+v", got)
	}
}

func TestCCV2BayesAdvantageIndependentOfReport(t *testing.T) {
	for _, tc := range []struct{ minimum, threshold int }{{2, 149}, {2, 150}, {2, 151}, {10, 150}} {
		store, _ := newResolverHistoryProbe(t, "legacy", map[string][]int{"K1ABC": {4}, "K1AXD": {0}})
		cfg := ccv2BayesSettings(store)
		cfg.MinConsensusReports = tc.minimum
		cfg.BayesBonusPolicy.AdvantageThresholdDistance1Milli, cfg.BayesBonusPolicy.AdvantageThresholdDistance2Milli = tc.threshold, tc.threshold
		got := EvaluateResolverPrimaryGates("K1AXD", "K1ABC", "20m", "CW", 9, 9, 61, 8700, 9000,
			cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
		awarded := tc.threshold <= 150
		wantAdvantageSupport := 9
		if awarded {
			wantAdvantageSupport++
		}
		if got.BayesScoreMilli != 150 || !got.BayesAdvantageConsidered || got.BayesAdvantageApplied != awarded || got.BayesReportBonusApplied ||
			got.EffectiveSupport != 9 || got.EffectiveAdvantageSupport != wantAdvantageSupport || got.Allow != (tc.minimum == 2 && awarded) {
			t.Fatalf("independent inclusive advantage boundary minimum%d threshold%d: %+v", tc.minimum, tc.threshold, got)
		}
		if !awarded && got.BayesAdvantageReject != "score_below_threshold" {
			t.Fatalf("below-floor full-report tie was not rejected by score: %+v", got)
		}
		if tc.minimum == 10 && (got.Reason != "min_reports" || got.BayesReportBonusReject != "score_below_threshold") {
			t.Fatalf("advantage bypassed failed report requirement: %+v", got)
		}
		cfg.BayesBonusPolicy.Enabled = false
		off := EvaluateResolverPrimaryGates("K1AXD", "K1ABC", "20m", "CW", 9, 9, 61, 8700, 9000,
			cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
		if off.Allow || off.EffectiveAdvantageSupport != 9 {
			t.Fatalf("off control already admitted the raw tie: %+v", off)
		}
	}
}

func TestCCV2BayesSeparatedAdvantagePreservesQualifications(t *testing.T) {
	for _, tc := range []struct {
		name, reject string
		change       func(*CorrectionSettings, *ResolverPrimaryGateOptions, *int, *int, *int, *int)
	}{
		{"contested", "contested_edit_neighbor", func(_ *CorrectionSettings, o *ResolverPrimaryGateOptions, _, _, _, _ *int) {
			o.RecentPlus1DisallowReason = "contested_edit_neighbor"
		}},
		{"winner_recent", "winner_recent_insufficient", func(_ *CorrectionSettings, _ *ResolverPrimaryGateOptions, rw, _, _, _ *int) { *rw = 3 }},
		{"subject_not_weaker", "subject_not_weaker", func(_ *CorrectionSettings, _ *ResolverPrimaryGateOptions, _, rs, _, _ *int) { *rs = 4 }},
		{"candidate_validation", "candidate_unvalidated", func(c *CorrectionSettings, _ *ResolverPrimaryGateOptions, _, _, _, _ *int) {
			c.RecentBandRecordMinUniqueSpotters = 5
		}},
		{"subject_validated", "subject_validated", func(_ *CorrectionSettings, _ *ResolverPrimaryGateOptions, rw, rs, _, _ *int) { *rw, *rs = 5, 4 }},
		{"weighted_delta", "weighted_delta_insufficient", func(_ *CorrectionSettings, _ *ResolverPrimaryGateOptions, _, _, sw, _ *int) { *sw = 8701 }},
		{"confidence", "confidence_insufficient", func(_ *CorrectionSettings, _ *ResolverPrimaryGateOptions, _, _, _, confidence *int) { *confidence = 49 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rw, rs, sw, confidence := 4, 0, 8700, 61
			cfg := ccv2BayesSettings(nil)
			// Low score thresholds isolate each unchanged non-score qualification.
			cfg.BayesBonusPolicy.AdvantageThresholdDistance1Milli, cfg.BayesBonusPolicy.AdvantageThresholdDistance2Milli = 1, 1
			options := ResolverPrimaryGateOptions{}
			tc.change(&cfg, &options, &rw, &rs, &sw, &confidence)
			store, _ := newResolverHistoryProbe(t, "legacy", map[string][]int{"K1ABC": {rw}, "K1AXD": {rs}})
			cfg.RecentBandStore = store
			got := EvaluateResolverPrimaryGates("K1AXD", "K1ABC", "20m", "CW", 9, 9, confidence, sw, 9000,
				cfg, time.Unix(1700000000, 0), options)
			if got.Allow || !got.BayesAdvantageConsidered || got.BayesAdvantageApplied || got.BayesAdvantageReject != tc.reject {
				t.Fatalf("separated rail bypassed %s: %+v", tc.name, got)
			}
		})
	}
}

func TestCCV2BayesMaskedCellDoesNotQueryHistory(t *testing.T) {
	for _, mask := range []map[string][]int{{}, {"CW": {}}, {"CW": {1}}, {"RTTY": {2}}} {
		store, probe := newResolverHistoryProbe(t, "legacy", nil)
		cfg := ccv2BayesSettings(store)
		cfg.BayesBonusPolicy.EnabledDistancesByMode = mask
		got := EvaluateResolverPrimaryGates("K1AXD", "K1ABC", "20m", "CW", 9, 9, 61, 8700, 9000,
			cfg, time.Unix(1700000000, 0), ResolverPrimaryGateOptions{})
		if got.Allow || got.BayesAdvantageConsidered || got.BayesReportBonusConsidered || got.BayesScoreMilli != 0 || len(probe.queries) != 0 {
			t.Fatalf("inactive cell consumed Bayes history or affected its ordinary gate: mask%v %+v", mask, got)
		}
	}
}
