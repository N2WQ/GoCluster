//go:build !qualification

// File role: Reaches live C through real resolver publications and the existing
// TCP/archive consumer fixture. Constructed witnesses establish wiring, not RF accuracy.
package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func selectiveLiveConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{Enabled: true, SelectiveRescueEnabled: true,
		RecencySeconds: 120, FrequencyToleranceHz: 1000, MaxEditDistance: 3,
		MinConsensusReports: 2, MinAdvantage: 1, MinConfidencePercent: 45,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain", InvalidAction: "broadcast"}
}

// Shared receivers prove the distant component's exclusion. The ordinary
// publication still contains that stronger winner and its dissent denominator.
func selectiveLivePopulation(t *testing.T, cfg config.CallCorrectionConfig, candidate, far string, reports int,
	at time.Time) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r, driver, key := selectiveLiveResolver(t, cfg)
	selectiveLiveSeed(t, r, driver, key, candidate, far, reports, at)
	snapshot, ok := r.LookupEligible(key, time.Now().UTC())
	if !ok || snapshot.Winner != far || snapshot.State != spot.ResolverStateConfident || snapshot.EvidenceLoss || snapshot.TotalReporters != reports+1 {
		t.Fatalf("actual settled ordinary stronger-winner precondition failed: %+v", snapshot)
	}
	return r, driver, key
}

func selectiveLiveResolver(t *testing.T, cfg config.CallCorrectionConfig) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 64, MaxActiveKeys: 8,
		MaxCandidatesPerKey: 16, MaxReportersPerCand: 8, HysteresisWindows: 1,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, InactiveTTL: 5 * time.Minute,
		MaxEditDistance: cfg.MaxEditDistance, DistanceModelCW: cfg.DistanceModelCW, DistanceModelRTTY: cfg.DistanceModelRTTY})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
	return r, driver, key
}

func selectiveLiveSeed(t *testing.T, r *spot.SignalResolver, driver *spot.SignalResolverDriver,
	key spot.ResolverSignalKey, candidate, far string, reports int, at time.Time) {
	t.Helper()
	for group, call := range []string{candidate, far} {
		for receiver := 0; receiver < reports+group; receiver++ {
			if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call,
				Spotter: []string{"N0AAA", "N0BBB", "N0CCC", "N0DDD", "N0EEE", "N0FFF"}[receiver], FrequencyKHz: 14020 + float64(group)*.3,
				RecencyWindow: 2 * time.Minute, Report: 12, HasReport: true}) {
				t.Fatal("literal bounded receiver population refused")
			}
		}
	}
	if got := driver.Drain(); got != 2*reports+1 {
		t.Fatalf("driver consumed %d rows", got)
	}
	driver.Sweep(at.Add(time.Millisecond))
}

func selectiveLiveSpot(subject string, at time.Time) *spot.Spot {
	s := newDeliveryTestSpot(subject, "N0AAA", at)
	s.SourceType = spot.SourceManual
	return s
}

func selectiveLivePrecondition(t *testing.T, p *outputPipeline, key spot.ResolverSignalKey, subject, candidate string) {
	t.Helper()
	now := time.Now().UTC()
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(p.signalResolver, key, p.correctionCfg, subject, now)
	assessment := correctionflow.AssessSelectiveFrequencyProposals(key, subject, 14020, selection,
		correctionflow.ReadFrequencySources(p.signalResolver, key, p.correctionCfg, now), p.correctionCfg,
		correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3})
	if len(assessment.Proposals) != 1 || assessment.Proposals[0].Identity != candidate || assessment.ExcludedComponents != 1 {
		t.Fatalf("real retained evidence must offer one useful C rescue: %+v", assessment)
	}
	gate := p.evaluateSelectiveRescueGate(selectiveLiveSpot(subject, now), subject, assessment.Proposals[0], now)
	if !gate.Allow {
		t.Fatalf("C positive hidden behind a gate: %+v", gate)
	}
}

func TestSelectiveRescueLiveImmediateAndDisabled(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled_%t", enabled), func(t *testing.T) {
			cfg := selectiveLiveConfig()
			cfg.SelectiveRescueEnabled = enabled
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			r, _, key := selectiveLivePopulation(t, cfg, "K1ABC", "DL9ZZZ", 2, at)
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			f.pipeline.recentBandStore = nil
			selectiveLivePrecondition(t, f.pipeline, key, "K1ABD", "K1ABC")
			ordinarySpot := selectiveLiveSpot("K1ABD", at)
			evidence, _ := buildResolverEvidenceSnapshot(ordinarySpot, cfg, nil, time.Now().UTC())
			ordinary := applyResolverCorrection(ordinarySpot, r, evidence, true, cfg, nil, nil, nil, nil, nil, nil, nil, time.Time{}, nil)
			if ordinary.terminal() || ordinarySpot.DXCallNorm != "K1ABD" || ordinary.gate.Reason != "max_edit_distance" {
				t.Fatalf("ordinary failure control must reach its actual distance gate: %+v", ordinary)
			}
			s := selectiveLiveSpot("K1ABD", at)
			f.pipeline.processSpot(s, nil)
			want := correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "?"}
			if enabled {
				want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "K1ABC", "C", 66, 1
			}
			if s.SourceType == spot.SourceRBN {
				t.Fatal("positive must prove live non-RBN CW eligibility")
			}
			if enabled {
				f.check(t, want, at)
			} else {
				temporalDeliveryCheckWithoutPercent(t, f, want, at, true)
			}
		})
	}
}

func TestSelectiveRescueTerminalLicenseWithoutB(t *testing.T) {
	temporalDeliveryDirectory(t)
	replacementLicenseFixture(t, false, "K1ABA")
	db := loadIngestCTY(t)
	cfg := selectiveLiveConfig()
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	r, _, key := selectiveLivePopulation(t, cfg, "K1ABA", "K1ABD", 2, at)
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	f.pipeline.ctyLookup, f.pipeline.recentBandStore = func() *cty.CTYDatabase { return db }, nil
	selectiveLivePrecondition(t, f.pipeline, key, "K1ABC", "K1ABA")
	s := selectiveLiveSpot("K1ABC", at)
	evidence, _ := buildResolverEvidenceSnapshot(s, cfg, nil, time.Now().UTC())
	ordinary := applyResolverCorrection(s, r, evidence, true, cfg, db, nil, nil, nil, nil, nil, nil, time.Time{}, nil)
	if !ordinary.replacementRejected || ordinary.applied || ordinary.suppress || s.Confidence == "B" {
		t.Fatalf("real missing-license rejection must be terminal without B: %+v glyph=%s", ordinary, s.Confidence)
	}
	f.pipeline.processSpot(selectiveLiveSpot("K1ABC", at), nil)
	want := correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "?"}
	temporalDeliveryCheckWithoutPercent(t, f, want, at, true)
	if f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionLicenseMissing] != 1 {
		t.Fatal("consumer did not execute exactly one ordinary license rejection")
	}
}

func TestSelectiveRescueOrdinaryMutationTerminal(t *testing.T) {
	temporalDeliveryDirectory(t)
	replacementLicenseFixture(t, true, "K1ABA")
	db := loadIngestCTY(t)
	cfg := selectiveLiveConfig()
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	r, _, _ := selectiveLivePopulation(t, cfg, "K1ABA", "K1ABD", 2, at)
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	f.pipeline.ctyLookup, f.pipeline.recentBandStore = func() *cty.CTYDatabase { return db }, nil
	s := selectiveLiveSpot("K1ABC", at)
	f.pipeline.processSpot(s, nil)
	if s.DXCallNorm != "K1ABD" || s.Confidence != "C" || s.ConfidencePercent != 100 || f.pipeline.tracker.CallCorrections() != 1 ||
		f.pipeline.tracker.CorrectionDecisionAppliedReasons()["resolver_selective_applied"] != 0 {
		t.Fatalf("ordinary success must not seek the lower-frequency rescue: %+v", s)
	}
}
