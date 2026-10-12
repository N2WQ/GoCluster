//go:build !qualification

// File role: Proves evaluation events name the evaluated identity and its own
// support, while actual local delivery and original peer forwarding stay distinct.
package cluster

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func TestB32DecisionDeliveredWeightedOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, input, call, glyph, event string
		percent                         int
		available, reject               bool
		corrections                     uint64
	}{
		{"corrected", "K1ABD", "K1ABC", "C", "Call decision: phase=evaluation outcome=corrected reason=resolver_applied input=K1ABD call=K1ABC proposed=K1ABC at=14020.0kHz share=64% basis=weighted recognition=separate suppressed=false", 64, true, false, 1},
		{"cty_rejected", "K1ABD", "K1ABD", "B", "Call decision: phase=evaluation outcome=rejected reason=resolver_cty_miss input=K1ABD call=K1ABD proposed=K1ABC at=14020.0kHz share=35% basis=weighted recognition=separate suppressed=false", 35, true, true, 0},
		{"zero_weight_retained", "K1ABD", "K1ABD", "B", "Call decision: phase=evaluation outcome=rejected reason=resolver_cty_miss input=K1ABD call=K1ABD proposed=K1ABC at=14020.0kHz share=0% basis=weighted recognition=separate suppressed=false", 0, true, true, 0},
		{"unsupported_retained", "K1ABE", "K1ABE", "B", "Call decision: phase=evaluation outcome=rejected reason=resolver_cty_miss input=K1ABE call=K1ABE proposed=K1ABC at=14020.0kHz share=--% basis=unavailable recognition=separate suppressed=false", 0, false, true, 0},
		{"healthy_unchanged_silent", "K1ABC", "K1ABC", "V", "", 64, true, false, 0},
	} {
		for _, destination := range []string{"ui", "log_fallback"} {
			t.Run(tc.name+"/"+destination, func(t *testing.T) {
				temporalDeliveryDirectory(t)
				at := time.Now().UTC().Truncate(time.Second)
				weights := spot.SpotterReliability{"N0AAA": .900, "N0BBB": .950, "N0DDD": 1.000}
				totalWeight := 2850
				if tc.name == "zero_weight_retained" {
					weights["N0DDD"], totalWeight = 0, 1850
				}
				r, _, key := confidenceAttributionPopulation(t, at.Add(-3*time.Second), "K1ABC", weights)
				original := tc.input
				if original == "K1ABC" {
					original = "K1ABD" // The healthy control still has literal A1/B2 evidence.
				}
				confidenceAttributionRequirePopulation(t, r, key, confidenceAttributionCase{
					name: tc.name, original: original, winner: "K1ABC", winnerWeight: 1850, totalWeight: totalWeight,
					missingOriginal: tc.input == "K1ABE", wantPercent: tc.percent,
				})
				f := correctionLossFixture(t, r, correctionLossConfig())
				if tc.reject {
					b32RejectCTY(t, f.pipeline)
				}
				events := b32CaptureDecisions(t, f.pipeline, destination)
				f.pipeline.processSpot(b32OriginalSpot(tc.input, at), nil)
				b32AssertDelivered(t, f, tc.input, tc.call, tc.glyph, tc.percent, tc.available, at)
				b32AssertDecision(t, events(), tc.event)
				if f.pipeline.tracker.CallCorrections() != tc.corrections || f.pipeline.tracker.TemporalCommitted() != 0 || f.pipeline.tracker.TemporalPathSwitches() != 0 {
					t.Errorf("evaluation diagnostics changed accepted-only accounting: corrections=%d commits=%d switches=%d",
						f.pipeline.tracker.CallCorrections(), f.pipeline.tracker.TemporalCommitted(), f.pipeline.tracker.TemporalPathSwitches())
				}
			})
		}
	}
}

func TestB32DecisionDeliveredLossRecognitionSeparate(t *testing.T) {
	for _, recognition := range []string{"none", "static_only", "history_only"} {
		for _, destination := range []string{"ui", "log_fallback"} {
			t.Run(recognition+"/"+destination, func(t *testing.T) {
				temporalDeliveryDirectory(t)
				at := time.Now().UTC().Truncate(time.Second)
				observed := at.Add(-3 * time.Second)
				r, d, key := correctionLossResolver(t, 2, 8)
				correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA")
				correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0BBB", "N0CCC")
				correctionLossAdmit(t, r, d, key, "K1ABE", 14020, observed, 2*time.Minute, "N0DDD")
				correctionLossEvictions(t, r, 1, 0)
				snapshot := correctionLossPopulation(t, r, key, 3, map[string]int{"K1ABC": 0, "K1ABD": 2, "K1ABE": 1})
				if !snapshot.EvidenceLoss {
					t.Fatal("actual candidate eviction must protect the current population")
				}
				cfg := correctionLossConfig()
				glyph := "?"
				var store *spot.CustomSCPStore
				if recognition != "none" {
					store = b32RecognitionStore(t, recognition, at)
					cfg.CustomSCP = config.CallCorrectionCustomSCPConfig{
						Enabled: true, ResolverMinScore: 3, ResolverMinUniqueSpotters: 2, ResolverMinUniqueH3Cells: 2,
						SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 3, SFloorMinUniqueH3CellsExact: 2,
						SFloorMinUniqueSpottersFamily: 4, SFloorMinUniqueH3CellsFamily: 2,
					}
					glyph = "S"
				}
				f := correctionLossFixture(t, r, cfg)
				if store != nil {
					f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
				}
				events := b32CaptureDecisions(t, f.pipeline, destination)
				f.pipeline.processSpot(b32OriginalSpot("K1ABC", at), nil)
				b32AssertDelivered(t, f, "K1ABC", "K1ABC", glyph, 0, false, at)
				b32AssertDecision(t, events(), "Call decision: phase=evaluation outcome=retained reason=resolver_evidence_loss input=K1ABC call=K1ABC proposed=-- at=14020.0kHz share=--% basis=unavailable_loss recognition=separate suppressed=false")
				if f.pipeline.tracker.CallCorrections() != 0 || f.pipeline.tracker.CorrectionDecisionReasons()["resolver_evidence_loss"] != 1 {
					t.Errorf("loss evaluation must retain identity once: reasons=%v corrections=%d", f.pipeline.tracker.CorrectionDecisionReasons(), f.pipeline.tracker.CallCorrections())
				}
			})
		}
	}
}

func TestB32DecisionSuppressionIsNotDelivery(t *testing.T) {
	for _, destination := range []string{"ui", "log_fallback"} {
		t.Run(destination, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			r, _, key := confidenceAttributionPopulation(t, at.Add(-3*time.Second), "K1ABC",
				spot.SpotterReliability{"N0AAA": .900, "N0BBB": .950, "N0DDD": 1.000})
			confidenceAttributionRequirePopulation(t, r, key, confidenceAttributionCase{
				name: "suppression", original: "K1ABD", winner: "K1ABC", winnerWeight: 1850, totalWeight: 2850, wantPercent: 35,
			})
			cfg := correctionLossConfig()
			cfg.InvalidAction = "suppress"
			f := correctionLossFixture(t, r, cfg)
			b32RejectCTY(t, f.pipeline)
			events := b32CaptureDecisions(t, f.pipeline, destination)
			f.pipeline.processSpot(b32OriginalSpot("K1ABD", at), nil)
			// Actual successful FIFO delivery on both existing TCP sessions and the
			// peer queue proves the suppressed input did not reach any destination.
			b32AssertSuppressionBarrier(t, f, at)
			b32AssertDecision(t, events(), "Call decision: phase=evaluation outcome=rejected reason=resolver_cty_miss input=K1ABD call=K1ABD proposed=K1ABC at=14020.0kHz share=35% basis=weighted recognition=separate suppressed=true")
			if f.pipeline.tracker.CallCorrections() != 0 || f.pipeline.tracker.CorrectionDecisionReasons()["resolver_cty_miss"] != 1 || f.pipeline.tracker.TemporalCommitted() != 0 {
				t.Error("suppression event must not become an accepted correction")
			}
		})
	}
}

func TestB32DecisionReceiverOnlyConsumerBasis(t *testing.T) {
	for _, destination := range []string{"ui", "log_fallback"} {
		t.Run(destination, func(t *testing.T) {
			at := time.Now().UTC().Truncate(time.Second)
			p := &outputPipeline{tracker: stats.NewTracker()}
			events := b32CaptureDecisions(t, p, destination)
			cfg := correctionLossConfig()
			cfg.MinConsensusReports = 3
			selection := correctionflow.ResolverPrimarySelection{SnapshotOK: true, Snapshot: spot.ResolverSnapshot{
				SupportBasis: spot.ResolverSupportBasisReceivers,
				Key:          spot.NewResolverSignalKey(14020, "20m", "CW", 500), State: spot.ResolverStateConfident,
				Winner: "K1ABC", WinnerSupport: 2, RunnerUp: "K1ABD", RunnerSupport: 1, TotalReporters: 3,
				CandidateRanks: []spot.ResolverCandidateSupport{{Call: "K1ABC", Support: 2}, {Call: "K1ABD", Support: 1}},
			}}
			s := b32OriginalSpot("K1ABD", at)
			// This explicitly unweighted consumer uses receiver coverage; zero
			// live reliability weights do not authorize that alternate basis.
			if maybeApplyResolverCorrectionWithSelectionOverride(s, nil, spot.ResolverEvidence{}, false, cfg,
				nil, nil, p.tracker, p.dash, nil, nil, nil, nil, nil, nil, &selection, at, nil) {
				t.Error("a min-report rejection unexpectedly suppressed its retained input")
			}
			if s.DXCallNorm != "K1ABD" || !s.ConfidencePercentOK || s.ConfidencePercent != 33 || p.tracker.CallCorrections() != 0 || p.tracker.CorrectionDecisionReasons()["resolver_gate_min_reports"] != 1 {
				t.Errorf("literal receiver-only A1/B2 consumer gate changed: spot=%+v reasons=%v", s, p.tracker.CorrectionDecisionReasons())
			}
			b32AssertDecision(t, events(), "Call decision: phase=evaluation outcome=rejected reason=resolver_gate_min_reports input=K1ABD call=K1ABD proposed=K1ABC at=14020.0kHz share=33% basis=receivers recognition=separate suppressed=false")
		})
	}
}
