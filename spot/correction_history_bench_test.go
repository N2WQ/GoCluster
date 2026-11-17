// Baseline-compatible benchmarks: this file uses only the existing gate/store
// APIs and existing observation-fixture helper, never the new history memo.
// Copy it alone into the original-production worktree for paired measurements.
package spot

import (
	"fmt"
	"testing"
	"time"
)

var resolverHistoryBenchmarkResult ResolverPrimaryGateResult

func resolverHistoryBenchmarkSettings(store RecentSupportStore, custom bool) CorrectionSettings {
	return CorrectionSettings{
		MinConsensusReports: 3, MinAdvantage: 1, MinConfidencePercent: 45, MaxEditDistance: 2,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		FamilyPolicy: CorrectionFamilyPolicy{Configured: true, TruncationEnabled: true,
			TruncationMaxLengthDelta: 2, TruncationMinShorterLength: 3, TruncationAllowPrefix: true},
		TruncationAdvantagePolicy: CorrectionTruncationAdvantagePolicy{Configured: true, Enabled: true,
			MinAdvantage: 1, RequireCandidateValidated: true, RequireSubjectUnvalidated: true},
		RecentBandStore: store, RecentBandRecordMinUniqueSpotters: 2,
		RecentBandCustomSCPEnabled: custom, RecentBandCustomSCPRequirements: CustomSCPRequirements{MinScore: 5, MinH3Cells: 1},
		ResolverRecentPlus1Enabled: true, ResolverRecentPlus1MinUniqueWinner: 2,
		ResolverRecentPlus1RequireSubjectWeaker: true, ResolverRecentPlus1MaxDistance: 2, ResolverRecentPlus1AllowTruncation: true,
		BayesBonusPolicy: CorrectionBayesBonusPolicy{Configured: true, Enabled: true,
			WeightDistance1Milli: 350, WeightDistance2Milli: 200, WeightedSmoothingMilli: 1000, RecentSmoothing: 2,
			ObsLogCapMilli: 350, PriorLogMinMilli: -200, PriorLogMaxMilli: 600,
			ReportThresholdDistance1Milli: 450, ReportThresholdDistance2Milli: 650,
			AdvantageThresholdDistance1Milli: 700, AdvantageThresholdDistance2Milli: 950,
			AdvantageMinWeightedDeltaDistance1Milli: 200, AdvantageMinWeightedDeltaDistance2Milli: 300,
			AdvantageExtraConfidenceDistance1: 3, AdvantageExtraConfidenceDistance2: 5,
			RequireCandidateValidated: true, RequireSubjectUnvalidatedDistance2: true},
	}
}

func BenchmarkResolverPrimaryHistory(b *testing.B) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	for _, backend := range []string{"none", "legacy", "custom"} {
		for _, shape := range []struct {
			name, subject, winner         string
			subjectAliases, winnerAliases []string
		}{
			{"bare", "K1ABC", "K1ABCD", []string{"K1ABC"}, []string{"K1ABCD"}},
			{"slash", "KH6 / K1ABC", "KH6 / K1ABCD", []string{"KH6 / K1ABC", "K1ABC/KH6", "K1ABC"}, []string{"KH6 / K1ABCD", "K1ABCD/KH6", "K1ABCD"}},
			{"shared_base", "K1ABC", "KH6/K1ABC", []string{"K1ABC"}, []string{"KH6/K1ABC", "K1ABC/KH6"}},
		} {
			b.Run(backend+"/"+shape.name, func(b *testing.B) {
				var rows []b13FamilyRow
				for _, alias := range shape.subjectAliases {
					rows = append(rows, b13FamilyRow{alias, "N0S0", now.Add(-time.Minute), 101})
				}
				for _, alias := range shape.winnerAliases {
					for i := 0; i < 4; i++ {
						rows = append(rows, b13FamilyRow{alias, fmt.Sprintf("N0W%d", i), now.Add(-time.Minute), uint16(101 + i%2)})
					}
				}
				var store RecentSupportStore
				switch backend {
				case "legacy":
					recent := NewRecentBandStore(time.Hour)
					for _, row := range rows {
						recent.RecordAt(row.call, "20m", "CW", row.receiver, row.seen, now)
					}
					store = recent
				case "custom":
					store = b13FamilyStore(b, rows, 8)
				}
				settings := resolverHistoryBenchmarkSettings(store, backend == "custom")
				if backend == "none" {
					settings.MinConsensusReports = 2
					settings.FamilyPolicy.TruncationEnabled, settings.ResolverRecentPlus1Enabled, settings.BayesBonusPolicy.Enabled = false, false, false
				}
				for warmup := 0; warmup < 8; warmup++ {
					got := EvaluateResolverPrimaryGates(shape.subject, shape.winner, "20m", "CW", 0, 2, 100, 0, 2000, settings, now, ResolverPrimaryGateOptions{})
					if !got.Allow || backend != "none" && (got.RecentPlus1Winner != 4 || got.RecentPlus1Subject != 1) {
						b.Fatalf("baseline-compatible fixture missed intended gate/history values: %+v", got)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					resolverHistoryBenchmarkResult = EvaluateResolverPrimaryGates(shape.subject, shape.winner, "20m", "CW", 0, 2, 100, 0, 2000, settings, now, ResolverPrimaryGateOptions{})
				}
			})
		}
	}
}
