// These changing-store fixtures deliberately disagree after the first lookup.
// Literal counts prove one gate's capture, alias maxima and next-call freshness;
// they do not imply an atomic snapshot across different aliases or stores.
package spot

import (
	"fmt"
	"testing"
	"time"
)

type resolverHistoryProbeQuery struct {
	call, band, mode string
	now              time.Time
	requirements     CustomSCPRequirements
	custom           bool
}

type resolverHistoryProbe struct {
	RecentSupportStore // Unused lifecycle/stat methods must never be called by a gate.
	t                  *testing.T
	values             map[string][]int
	seen               map[string]int
	queries            []resolverHistoryProbeQuery
	strictZero         bool
}

func (p *resolverHistoryProbe) HasRecentSupport(string, string, string, int, time.Time) bool {
	p.t.Fatal("gate replaced a partial count with a Boolean receiver qualification")
	return false
}

func (p *resolverHistoryProbe) RecentSupportCount(call, band, mode string, now time.Time) int {
	return p.read(resolverHistoryProbeQuery{call: call, band: band, mode: mode, now: now})
}

func (p *resolverHistoryProbe) read(query resolverHistoryProbeQuery) int {
	p.t.Helper()
	values, known := p.values[query.call]
	if !known || len(values) == 0 {
		p.t.Fatalf("unexpected historical alias %q", query.call)
	}
	index := p.seen[query.call]
	p.seen[query.call]++
	p.queries = append(p.queries, query)
	if p.strictZero && query.custom && query.requirements.MinScore >= 10 {
		return 0
	}
	return values[min(index, len(values)-1)]
}

type resolverHistoryCustomProbe struct{ *resolverHistoryProbe }

func (p *resolverHistoryCustomProbe) RecentSupportCountWithRequirements(call, band, mode string, requirements CustomSCPRequirements, now time.Time) int {
	return p.read(resolverHistoryProbeQuery{call: call, band: band, mode: mode, now: now, requirements: requirements, custom: true})
}

func newResolverHistoryProbe(t *testing.T, backend string, values map[string][]int) (RecentSupportStore, *resolverHistoryProbe) {
	t.Helper()
	probe := &resolverHistoryProbe{t: t, values: values, seen: make(map[string]int)}
	if backend == "custom" || backend == "custom_disabled" {
		return &resolverHistoryCustomProbe{probe}, probe
	}
	return probe, probe
}

func resolverHistoryTestSettings(store RecentSupportStore, backend string) CorrectionSettings {
	return CorrectionSettings{
		MinConsensusReports: 3, MinAdvantage: 1, MinConfidencePercent: 45, MaxEditDistance: 2,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		FamilyPolicy: CorrectionFamilyPolicy{Configured: true, TruncationEnabled: true,
			TruncationMaxLengthDelta: 2, TruncationMinShorterLength: 3, TruncationAllowPrefix: true},
		TruncationAdvantagePolicy: CorrectionTruncationAdvantagePolicy{Configured: true, Enabled: true,
			MinAdvantage: 1, RequireCandidateValidated: true, RequireSubjectUnvalidated: true},
		RecentBandStore: store, RecentBandRecordMinUniqueSpotters: 2,
		RecentBandCustomSCPEnabled:      backend == "custom" || backend == "custom_unavailable",
		RecentBandCustomSCPRequirements: CustomSCPRequirements{MinScore: 5, MinH3Cells: 2},
		ResolverRecentPlus1Enabled:      true, ResolverRecentPlus1MinUniqueWinner: 2,
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

func assertResolverHistoryQueryContext(t *testing.T, queries []resolverHistoryProbeQuery, backend, band, mode string, now time.Time, requirements CustomSCPRequirements) {
	t.Helper()
	for _, query := range queries {
		if query.band != band || query.mode != mode || !query.now.Equal(now) || query.custom != (backend == "custom") {
			t.Fatalf("gate historical context/dispatch changed: %+v", query)
		}
		if query.custom && query.requirements != requirements {
			t.Fatalf("consumer requirements changed within capture: %+v want %+v", query, requirements)
		}
	}
}

func TestResolverGateHistorySharedBranchesAndFreshRetry(t *testing.T) {
	for _, backend := range []string{"legacy", "custom", "custom_disabled", "custom_unavailable"} {
		t.Run(backend, func(t *testing.T) {
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			store, probe := newResolverHistoryProbe(t, backend, map[string][]int{"K1ABCD": {4, 0}, "K1ABC": {1, 9}})
			settings := resolverHistoryTestSettings(store, backend)
			first := EvaluateResolverPrimaryGates("K1ABC", "K1ABCD", "20m", "CW", 0, 2, 100, 0, 2000, settings, now, ResolverPrimaryGateOptions{})
			if !first.Allow || !first.RecentPlus1Applied || first.BayesReportBonusConsidered ||
				first.RecentPlus1Winner != 4 || first.RecentPlus1Subject != 1 || first.BayesWinnerRecent != 4 || first.BayesSubjectRecent != 1 {
				t.Fatalf("validation/recent+1/Bayes did not share literal first counts4:1, or stacked report rails: %+v", first)
			}
			if len(probe.queries) != 2 || probe.seen["K1ABCD"] != 1 || probe.seen["K1ABC"] != 1 {
				t.Fatalf("repeated gate branches requeried the backend: %+v", probe.queries)
			}
			assertResolverHistoryQueryContext(t, probe.queries, backend, "20m", "CW", now, settings.RecentBandCustomSCPRequirements)
			retryAt := now.Add(time.Second)
			retry := EvaluateResolverPrimaryGates("K1ABC", "K1ABCD", "20m", "CW", 0, 2, 100, 0, 2000, settings, retryAt, ResolverPrimaryGateOptions{})
			if retry.Allow || retry.RecentPlus1Winner != 0 || retry.RecentPlus1Subject != 9 || retry.BayesWinnerRecent != 0 || retry.BayesSubjectRecent != 9 || len(probe.queries) != 4 {
				t.Fatalf("next gate inherited prior capture instead of fresh literal counts0:9: %+v queries=%+v", retry, probe.queries)
			}
			assertResolverHistoryQueryContext(t, probe.queries[2:], backend, "20m", "CW", retryAt, settings.RecentBandCustomSCPRequirements)
		})
	}
}

func TestResolverGateHistoryBayesValidationUsesCapturedCounts(t *testing.T) {
	for _, backend := range []string{"legacy", "custom", "custom_disabled", "custom_unavailable"} {
		t.Run(backend, func(t *testing.T) {
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			store, probe := newResolverHistoryProbe(t, backend, map[string][]int{"K1ABB": {4, 0}, "K1AAA": {1, 9}})
			settings := resolverHistoryTestSettings(store, backend)
			settings.FamilyPolicy.TruncationEnabled, settings.ResolverRecentPlus1Enabled = false, false
			settings.BayesBonusPolicy.ReportThresholdDistance1Milli, settings.BayesBonusPolicy.ReportThresholdDistance2Milli = 380, 390
			settings.BayesBonusPolicy.AdvantageThresholdDistance1Milli, settings.BayesBonusPolicy.AdvantageThresholdDistance2Milli = 380, 390
			got := EvaluateResolverPrimaryGates("K1AAA", "K1ABB", "40m", "RTTY", 2, 2, 70, 1500, 2300, settings, now, ResolverPrimaryGateOptions{})
			// round(1000*ln(3300/2500))=278; capped history prior600*0.2=120.
			if !got.Allow || got.Distance != 2 || !got.BayesReportBonusApplied || !got.BayesAdvantageApplied ||
				got.BayesScoreMilli != 398 || got.BayesWinnerRecent != 4 || got.BayesSubjectRecent != 1 || len(probe.queries) != 2 {
				t.Fatalf("Bayes validation reread or erased eligible partial subject count1: %+v queries=%+v", got, probe.queries)
			}
			assertResolverHistoryQueryContext(t, probe.queries, backend, "40m", "RTTY", now, settings.RecentBandCustomSCPRequirements)
		})
	}
}

func TestResolverGateHistoryEightAliasesAndMaxima(t *testing.T) {
	for _, backend := range []string{"legacy", "custom"} {
		t.Run(backend, func(t *testing.T) {
			values := map[string][]int{
				"F/K1ABC/P": {1, 99}, "K1ABC/F/P": {4, 99}, "K1ABC": {2, 99}, "P/F/K1ABC": {0, 99},
				"EA/K2DEF/M": {1, 99}, "K2DEF/EA/M": {0, 99}, "K2DEF": {1, 99}, "M/EA/K2DEF": {0, 99},
			}
			store, probe := newResolverHistoryProbe(t, backend, values)
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			settings := resolverHistoryTestSettings(store, backend)
			history := newResolverGateHistory(settings, "20m", "CW", now)
			winner := correctionCallIdentity{Raw: "F/K1ABC/P", VoteKey: "K1ABC/F/P", BaseKey: "K1ABC"}
			subject := correctionCallIdentity{Raw: "EA/K2DEF/M", VoteKey: "K2DEF/EA/M", BaseKey: "K2DEF"}
			for repeat := 0; repeat < 3; repeat++ {
				if got := history.support(winner, " p/f/k1abc "); got != 4 || !history.validated(winner, "P/F/K1ABC", 2) {
					t.Fatalf("winner aliases were pooled or requeried: %d", got)
				}
				if got := history.support(subject, " m/ea/k2def "); got != 1 || history.validated(subject, "M/EA/K2DEF", 0) {
					t.Fatalf("partial count1 was pooled, erased or the default minimum2 changed: %d", got)
				}
			}
			if history.used != 8 || len(probe.queries) != 8 {
				t.Fatalf("eight aliases did not saturate bounded capture exactly once: used=%d queries=%+v", history.used, probe.queries)
			}
			for call, count := range probe.seen {
				if count != 1 {
					t.Fatalf("alias %s read%d times", call, count)
				}
			}
			assertResolverHistoryQueryContext(t, probe.queries, backend, "20m", "CW", now, settings.RecentBandCustomSCPRequirements)
			fresh := newResolverGateHistory(settings, "20m", "CW", now.Add(time.Second))
			if got := fresh.support(winner, "P/F/K1ABC"); got != 99 {
				t.Fatalf("new decision did not capture changed alias maxima: %d", got)
			}
		})
	}
}

func TestResolverGateHistorySharedBaseAndRequirements(t *testing.T) {
	for _, backend := range []string{"legacy", "custom"} {
		t.Run(backend, func(t *testing.T) {
			store, probe := newResolverHistoryProbe(t, backend, map[string][]int{"F/K1ABC": {4}, "K1ABC/F": {4}, "K1ABC": {1, 9}})
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			settings := resolverHistoryTestSettings(store, backend)
			history := newResolverGateHistory(settings, "20m", "CW", now)
			winner := normalizeCorrectionCallIdentity("F/K1ABC")
			subject := normalizeCorrectionCallIdentity("K1ABC")
			if history.support(winner, "F/K1ABC") != 4 || history.support(subject, "K1ABC") != 1 || history.used != 3 || len(probe.queries) != 3 {
				t.Fatalf("shared base did not reuse its first partial count: %+v", probe.queries)
			}
		})
	}
	t.Run("fixed_context_then_fresh_requirements", func(t *testing.T) {
		store, probe := newResolverHistoryProbe(t, "custom", map[string][]int{"K1ABC": {4}})
		probe.strictZero = true
		now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
		settings := resolverHistoryTestSettings(store, "custom")
		history := newResolverGateHistory(settings, "20m", "CW", now)
		loose := settings.RecentBandCustomSCPRequirements
		settings.RecentBandCustomSCPRequirements = CustomSCPRequirements{MinScore: 10, MinH3Cells: 3}
		call := normalizeCorrectionCallIdentity("K1ABC")
		if got := history.support(call, "K1ABC"); got != 4 {
			t.Fatalf("caller settings mutation changed an existing capture's context: %d", got)
		}
		assertResolverHistoryQueryContext(t, probe.queries, "custom", "20m", "CW", now, loose)
		fresh := newResolverGateHistory(settings, "40m", "RTTY", now.Add(time.Second))
		if fresh.support(call, "K1ABC") != 0 || fresh.support(call, "K1ABC") != 0 || len(probe.queries) != 2 {
			t.Fatalf("new requirements or zero-result capture reused another decision: %+v", probe.queries)
		}
		assertResolverHistoryQueryContext(t, probe.queries[1:], "custom", "40m", "RTTY", now.Add(time.Second), settings.RecentBandCustomSCPRequirements)
	})
	t.Run("nil_and_empty", func(t *testing.T) {
		history := newResolverGateHistory(CorrectionSettings{}, "20m", "CW", time.Time{})
		if history.support(normalizeCorrectionCallIdentity("K1ABC"), "K1ABC") != 0 || history.used != 0 {
			t.Fatal("nil store invented or retained historical support")
		}
		store, probe := newResolverHistoryProbe(t, "legacy", nil)
		history = newResolverGateHistory(CorrectionSettings{RecentBandStore: store}, "20m", "CW", time.Time{})
		if history.support(correctionCallIdentity{}, " ") != 0 || len(probe.queries) != 0 || history.used != 0 {
			t.Fatal("empty alias queried or consumed a slot")
		}
	})
}

func TestResolverGateHistoryConcreteBackendsPreservePartialCounts(t *testing.T) {
	for _, backend := range []string{"legacy", "custom"} {
		t.Run(backend, func(t *testing.T) {
			now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
			var store RecentSupportStore
			if backend == "legacy" {
				recent := NewRecentBandStore(time.Hour)
				for i := 0; i < 4; i++ {
					recent.RecordAt("K1ABCD", "20m", "CW", fmt.Sprintf("N0W%d", i), now.Add(-time.Minute), now)
				}
				recent.RecordAt("K1ABC", "20m", "CW", "N0S0", now.Add(-time.Minute), now)
				store = recent
			} else {
				rows := []b13FamilyRow{{"K1ABC", "N0S0", now.Add(-time.Minute), 101}}
				for i := 0; i < 4; i++ {
					rows = append(rows, b13FamilyRow{"K1ABCD", fmt.Sprintf("N0W%d", i), now.Add(-time.Minute), uint16(101 + i%2)})
				}
				store = b13FamilyStore(t, rows, 8)
			}
			settings := resolverHistoryTestSettings(store, backend)
			settings.RecentBandCustomSCPRequirements.MinH3Cells = 1 // A one-receiver partial subject remains eligible.
			got := EvaluateResolverPrimaryGates("K1ABC", "K1ABCD", "20m", "CW", 0, 2, 100, 0, 2000, settings, now, ResolverPrimaryGateOptions{})
			if !got.Allow || got.RecentPlus1Winner != 4 || got.RecentPlus1Subject != 1 || got.BayesWinnerRecent != 4 || got.BayesSubjectRecent != 1 {
				t.Fatalf("concrete %s backend changed partial counts or alias capture: %+v", backend, got)
			}
		})
	}
}
