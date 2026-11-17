//go:build !qualification

// These are constructed reachability witnesses, not archive accuracy evidence.
// Actual admitted rows, Custom SCP history and license checks reach real output.
package cluster

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func ccv2BayesDeliveredHistory(t *testing.T, candidate string, at time.Time) *spot.CustomSCPStore {
	t.Helper()
	if a, b := pathreliability.EncodeCoarseCell("FN31AA"), pathreliability.EncodeCoarseCell("JO22AA"); a == pathreliability.InvalidCell || b == pathreliability.InvalidCell || a == b {
		t.Fatal("history fixture requires two actual distinct coarse cells")
	}
	store := customSCPFreshnessOpen(t, spot.CustomSCPOptions{Path: filepath.Join(t.TempDir(), "scp"),
		HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 16, MaxSpottersPerKey: 64,
		CoreMinScore: 5, CoreMinH3Cells: 2})
	for n := 0; n < 4; n++ {
		s := newDeliveryTestSpot(candidate, fmt.Sprintf("N0HA%c", 'A'+rune(n)), at.Add(-time.Hour))
		s.Confidence, s.Report, s.HasReport = "V", 20, true
		s.DEGridNorm = []string{"FN31AA", "JO22AA"}[n%2]
		store.RecordSpot(s)
	}
	if count := store.RecentSupportCountWithRequirements(spot.CorrectionVoteKey(candidate), "20m", "CW", spot.CustomSCPRequirements{MinScore: 5, MinH3Cells: 2}, at); count != 4 {
		t.Fatalf("real V-only history admission supplied%d receivers, need4", count)
	}
	return store
}

func TestCCV2BayesActualConsumerDelivery(t *testing.T) {
	loaded := replacementLicenseConfig(t)
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatal(err)
	}
	temporalDeliveryDirectory(t)
	for _, rail := range []string{"distance2_report", "distance2_advantage", "distance1_slash_advantage", "distance1_bare_contested"} {
		for _, policy := range []struct {
			name               string
			enabled, emptyMask bool
		}{{"off", false, false}, {"on", true, false}, {"empty_mask", true, true}} {
			for _, path := range []string{"immediate", "temporal", "stabilizer_retry"} {
				t.Run(fmt.Sprintf("%s/%s/%s", rail, policy.name, path), func(t *testing.T) {
					replacementLicenseFixture(t, true, "K1AXD", "K1AAA", "K1AAE")
					cfg := loaded
					cfg.BayesBonus.Enabled = policy.enabled
					cfg.BayesBonus.ReportThresholdDistance2Milli = 450
					cfg.BayesBonus.AdvantageThresholdDistance1Milli, cfg.BayesBonus.AdvantageThresholdDistance2Milli = 150, 150
					cfg.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli, cfg.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli = 200, 300
					cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": {2}}
					subject, candidate := "K1AXD", "K1ABC"
					n, shared, wantScore, wantPercent, distance := 9, 3, 150, 59, 2
					if rail == "distance2_report" {
						// Four is the shipped busy-state report floor. The explicit
						// floor isolates its gate from activity-state scheduling.
						cfg.MinConsensusReports = 4
						n, shared, wantScore, wantPercent = 3, 0, 470, 25
					}
					if rail == "distance1_slash_advantage" || rail == "distance1_bare_contested" {
						subject, candidate = "LZ/K1AAE", "LZ/K1AAA"
						n, shared, wantScore, wantPercent, distance = 6, 2, 239, 59, 1
						cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": {1}}
						cfg.BayesBonus.AdvantageThresholdDistance1Milli, cfg.BayesBonus.AdvantageThresholdDistance2Milli = 239, 239
					}
					if rail == "distance1_bare_contested" {
						subject, candidate = "K1AAE", "K1AAA"
					}
					if policy.emptyMask {
						cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{}
					}
					wantApply := policy.enabled && !policy.emptyMask && rail != "distance1_bare_contested"
					if got := spot.CallDistance(subject, candidate, "CW", cfg.DistanceModelCW, cfg.DistanceModelRTTY); got != distance {
						t.Fatalf("fixture changed configured Morse distance: got%d need%d", got, distance)
					}
					at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
					history := ccv2BayesDeliveredHistory(t, candidate, at)
					weights := make(spot.SpotterReliability)
					for receiver := 0; receiver < 2*n-shared; receiver++ {
						weights[fmt.Sprintf("N0RA%c", 'A'+rune(receiver))] = 1
						if receiver >= n && rail != "distance2_report" {
							weights[fmt.Sprintf("N0RA%c", 'A'+rune(receiver))] = .95
						}
					}
					r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 64, MaxActiveKeys: 4, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64,
						HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, MaxEditDistance: cfg.MaxEditDistance,
						DistanceModelCW: cfg.DistanceModelCW, DistanceModelRTTY: cfg.DistanceModelRTTY,
						FreqGuardMinSeparationKHz: cfg.FreqGuardMinSeparationKHz, FreqGuardRunnerUpRatio: cfg.FreqGuardRunnerUpRatio,
						SpotterReliability: weights, MinSpotterReliability: .95})
					d, err := spot.NewSignalResolverDriver(r)
					if err != nil {
						t.Fatal(err)
					}
					key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
					rows := 0
					for group, call := range []string{candidate, subject} {
						count := n
						if group == 1 && rail == "distance2_report" {
							count = 1
						}
						for receiver := 0; receiver < count; receiver++ {
							index := receiver
							if group == 1 && receiver >= shared {
								index = n + receiver - shared
							}
							if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: fmt.Sprintf("N0RA%c", 'A'+rune(index)),
								ObservedAt: at, FrequencyKHz: 14020, RecencyWindow: 5 * time.Minute, Report: 20, HasReport: true}) {
								t.Fatal("admitted witness refused")
							}
							rows++
						}
					}
					if d.Drain() != rows {
						t.Fatal("missing witness rows")
					}
					d.Sweep(at.Add(time.Millisecond))
					selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, subject, at.Add(time.Millisecond))
					if !selection.SnapshotOK || selection.Snapshot.Winner != candidate || selection.Snapshot.EvidenceLoss {
						t.Fatalf("not an admitted settled proposal: %+v", selection)
					}
					settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{Cfg: cfg, MinReports: cfg.MinConsensusReports, RecentBandStore: history})
					options := spot.ResolverPrimaryGateOptions{}
					if rail == "distance1_bare_contested" {
						if !spot.ResolverSnapshotHasComparableEditNeighbor(selection.Snapshot, candidate, "CW", cfg.DistanceModelCW, cfg.DistanceModelRTTY) {
							t.Fatal("bare tie lost its actual contested-neighbor witness")
						}
						options.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
					}
					gate := spot.EvaluateResolverPrimaryGates(subject, candidate, "20m", "CW", correctionflow.ResolverSupportForCall(selection.Snapshot, subject), n,
						correctionflow.ResolverWinnerConfidence(selection.Snapshot), correctionflow.ResolverWeightedSupportForCall(selection.Snapshot, subject), n*1000, settings, at, options)
					if gate.Allow != wantApply || policy.enabled && !policy.emptyMask && gate.BayesScoreMilli != wantScore || gate.BayesReportBonusApplied != (wantApply && rail == "distance2_report") || gate.BayesAdvantageApplied != (wantApply && rail != "distance2_report") {
						t.Fatalf("conditional witness gate differs: %+v", gate)
					}
					f := newAmbiguityDeliveryFixture(t, r, cfg)
					f.pipeline.recentBandStore, f.pipeline.customSCPStore = history, history
					db := loadIngestCTY(t)
					f.pipeline.ctyLookup = func() *cty.CTYDatabase { return db }
					s := newDeliveryTestSpot(subject, "N0AAA", at)
					s.SourceType = spot.SourceManual
					switch path {
					case "immediate":
						f.pipeline.processSpotBody(s, nil)
					case "stabilizer_retry":
						f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s), resolverKey: key, hasResolverKey: true, evidenceEnqueued: true})
					case "temporal":
						temporalCfg := cfg
						temporalCfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
						f.pipeline.correctionCfg = temporalCfg
						controller := newRuntimeTemporalController(temporalCfg)
						f.pipeline.temporal = controller
						temporalDeliveryObserve(t, controller, at, key, s, selection)
						releases := controller.Drain(time.Now().UTC(), false)
						if len(releases) != 1 {
							t.Fatal("missing temporal proposal")
						}
						f.pipeline.processSpotBody(releases[0].pending.spot, &releases[0])
					}
					want := correctionEvidenceDeliveryCase{subject: subject, wantCall: subject, wantGlyph: "V", wantPercent: wantPercent}
					if rail == "distance2_report" {
						want.wantGlyph = "P"
					}
					if wantApply {
						want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = candidate, "C", 61, 1
						if rail == "distance2_report" {
							want.wantPercent = 75
						}
					}
					f.check(t, want, at)
				})
			}
		}
	}
}
