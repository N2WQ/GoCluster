package main

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/testsupport"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

// This is an admitted replay reachability witness, not archive qualification or
// RF-truth evidence. Literal raw/weighted populations and real V-only history
// distinguish the Bayes report rail from ordinary recent+1 and advantage rails.
func TestCCV2BayesActualReplayReachability(t *testing.T) {
	loaded, err := config.Load(filepath.Join(testsupport.RepoRoot(t), "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pathreliability.InitH3MappingsFromDir(testsupport.H3TableDir(t)); err != nil {
		t.Fatal(err)
	}
	first, second := pathreliability.EncodeCoarseCell("FN31AA"), pathreliability.EncodeCoarseCell("JO22AA")
	if first == pathreliability.InvalidCell || second == pathreliability.InvalidCell || first == second {
		t.Fatal("history fixture requires two distinct real coarse cells")
	}
	t.Chdir(t.TempDir())
	const candidate, subject = "K1ABC", "K1AXD"
	for _, policy := range []struct {
		name               string
		enabled, emptyMask bool
	}{{"master_off", false, false}, {"master_on", true, false}, {"master_on_empty", true, true}} {
		for _, path := range []string{"immediate", "temporal"} {
			t.Run(policy.name+"/"+path, func(t *testing.T) {
				replayLicenseGlobals(t, replayLicenseDB(t, candidate, subject))
				cfg := loaded.CallCorrection
				// Isolate the shipped busy-state report floor from activity-state
				// scheduling. The short temporal fixture changes only this test's
				// hold scope/budget; ordinary correction/history/license rails stay.
				cfg.MinConsensusReports = 4
				shortHold := replayTemporalFreshConfig().TemporalDecoder
				cfg.TemporalDecoder.Scope = shortHold.Scope
				cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = shortHold.LagSeconds, shortHold.MaxWaitSeconds
				cfg.BayesBonus.Enabled = policy.enabled
				cfg.BayesBonus.ReportThresholdDistance2Milli = 450
				cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": {2}}
				if policy.emptyMask {
					cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{}
				}
				if distance := spot.CallDistance(subject, candidate, "CW", cfg.DistanceModelCW, cfg.DistanceModelRTTY); distance != 2 {
					t.Fatalf("fixture no longer has configured Morse distance 2: %d", distance)
				}
				r, key := ccv2CanonicalReplayOwner(t, cfg)
				r.outDir = t.TempDir()
				t.Cleanup(r.close)
				if err := r.openRecentBandStore(); err != nil {
					t.Fatal(err)
				}
				history, ok := r.recentBandStore.(*spot.CustomSCPStore)
				if !ok {
					t.Fatal("replay did not construct its configured CustomSCP owner")
				}
				at := time.Now().UTC().Truncate(time.Second)
				for n := 0; n < 4; n++ {
					s := replayTemporalFreshSpot(candidate, at.Add(-time.Hour))
					s.DECall = fmt.Sprintf("N0HA%c", 'A'+rune(n))
					s.DECallNorm = s.DECall
					s.Confidence, s.HasReport, s.Report = "V", true, 20
					s.DEGridNorm = []string{"FN31AA", "JO22AA"}[n%2]
					history.RecordSpot(s)
				}
				requirements := spot.CustomSCPRequirements{MinScore: cfg.CustomSCP.ResolverMinScore, MinH3Cells: cfg.CustomSCP.ResolverMinUniqueH3Cells}
				if count := history.RecentSupportCountWithRequirements(spot.CorrectionVoteKey(candidate), "20m", "CW", requirements, at); count != 4 {
					t.Fatalf("real canonical V-only history supplied %d receivers, need 4", count)
				}
				if count := history.RecentSupportCountWithRequirements(spot.CorrectionVoteKey(subject), "20m", "CW", requirements, at); count != 0 {
					t.Fatalf("subject unexpectedly had historical support: %d", count)
				}
				for n, call := range []string{candidate, candidate, candidate, subject} {
					if !r.resolver.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: fmt.Sprintf("N0RA%c", 'A'+rune(n)),
						ObservedAt: at, FrequencyKHz: 14020, RecencyWindow: 5 * time.Minute, Report: 20, HasReport: true}) {
						t.Fatal("resolver refused a reachability witness")
					}
				}
				if r.driver.Drain() != 4 {
					t.Fatal("missing actual resolver rows")
				}
				r.driver.Sweep(at.Add(time.Millisecond))
				selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, cfg, subject, at.Add(time.Millisecond))
				if !selection.SnapshotOK || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss || selection.Snapshot.Winner != candidate ||
					selection.Snapshot.TotalReporters != 4 || correctionflow.ResolverSupportForCall(selection.Snapshot, candidate) != 3 ||
					correctionflow.ResolverSupportForCall(selection.Snapshot, subject) != 1 ||
					correctionflow.ResolverWeightedSupportForCall(selection.Snapshot, candidate) != 3000 ||
					correctionflow.ResolverWeightedSupportForCall(selection.Snapshot, subject) != 1000 || correctionflow.ResolverWinnerConfidence(selection.Snapshot) != 75 {
					t.Fatalf("replay fixture lost its published 3:1/75%% population: %+v", selection)
				}
				wantCall, wantGlyph, wantMutations := subject, "P", uint64(0)
				if policy.enabled && !policy.emptyMask {
					wantCall, wantGlyph, wantMutations = candidate, "C", 1
				}
				s := replayTemporalFreshSpot(subject, at)
				s.DECall, s.DECallNorm = "N0RAD", "N0RAD"
				evidence := spot.ResolverEvidence{Key: key}
				if path == "temporal" {
					if !r.maybeHoldTemporal(s, evidence, true, at) || len(r.temporalPending) != 1 {
						t.Fatal("actual temporal owner did not hold the published proposal")
					}
					r.drainTemporal(at.Add(2*time.Second), false)
					if len(r.temporalPending) != 0 || r.abMetrics.Temporal.Committed != wantMutations || r.abMetrics.Temporal.HandoffAttempts != 1 {
						t.Fatalf("temporal proposal was not consumed through the output gate: %+v", r.abMetrics.Temporal)
					}
				} else {
					out := maybeApplyResolverCorrectionReplay(s, r.resolver, evidence, true, cfg, r.ctyDB, r.tracker, nil, r.recentBandStore, at)
					if !out.GateEvaluated || out.Applied != (wantMutations == 1) || out.Gate.Distance != 2 || out.Gate.MinReports != 4 ||
						out.Gate.LengthBonus != 0 || out.Gate.RecentPlus1Applied || out.Gate.BayesAdvantageApplied {
						t.Fatalf("actual replay did not isolate the report rail: %+v", out)
					}
					// ln(4000/2000) caps at 350; ln((4+2)/(0+2)) caps
					// at 600. Fixed distance-2 weight 200 contributes 120: 470.
					if wantMutations == 1 && (out.Gate.BayesScoreMilli != 470 || out.Gate.BayesWinnerRecent != 4 || out.Gate.BayesSubjectRecent != 0 || out.Gate.EffectiveSupport != 4) {
						t.Fatalf("literal score/history witness changed: %+v", out.Gate)
					}
					if wantMutations == 0 && out.Gate.Reason != "min_reports" {
						t.Fatalf("negative control missed the report gate: %+v", out.Gate)
					}
					r.processOutcome(s, out, at)
				}
				metrics := r.abMetrics.Resolver
				if s.DXCallNorm != wantCall || s.Confidence != wantGlyph || r.tracker.CallCorrections() != wantMutations || uint64(len(r.stabilityCollector.corrections)) != wantMutations ||
					r.abMetrics.Output.ConfidenceCounts.Samples != 1 || metrics.BayesReportConsidered != wantMutations || metrics.BayesReportApplied != wantMutations ||
					metrics.BayesAdvantageConsidered != 0 || metrics.RecentPlus1Applied != 0 || metrics.RecentPlus1RejectDistance != 1 {
					t.Fatalf("actual replay mask/rail accounting diverged: call=%s glyph=%s mutations=%d metrics=%+v", s.DXCallNorm, s.Confidence, r.tracker.CallCorrections(), metrics)
				}
			})
		}
	}
}
