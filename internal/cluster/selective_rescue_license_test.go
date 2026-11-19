//go:build !qualification

package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestSelectiveRescuePrecedesUnchangedSubjectFinalLicense(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled_%t", enabled), func(t *testing.T) {
			replacementLicenseFixture(t, false) // K1ABD missing; K1ABC is licensed.
			db := loadIngestCTY(t)
			cfg := selectiveLiveConfig()
			cfg.SelectiveRescueEnabled = enabled
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			r, _, key := selectiveLivePopulation(t, cfg, "K1ABC", "DL9ZZZ", 2, at)
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			p := f.pipeline
			p.ctyLookup, p.recentBandStore = func() *cty.CTYDatabase { return db }, nil
			selectiveLivePrecondition(t, p, key, "K1ABD", "K1ABC")
			rejections := 0
			p.unlicensedReporter = func(_, role, call, _, _, _ string, _ float64) {
				if role != "DX" || call != "K1ABD" {
					t.Errorf("unexpected final license rejection: role=%s call=%s", role, call)
				}
				rejections++
			}
			p.processSpot(selectiveLiveSpot("K1ABD", at), nil)
			if enabled {
				if rejections != 0 {
					t.Fatal("unchanged-subject licensing ran before legal C rescue")
				}
				f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABC", wantGlyph: "C", wantPercent: 66, wantCorrections: 1}, at)
			} else if rejections != 1 || f.ring.GetCount() != 0 || p.tracker.CallCorrections() != 0 {
				t.Fatal("off control must reach the unchanged subject's final license rejection")
			}
		})
	}
}

func TestSelectiveRescueReplacementLicenseFailureIsTerminal(t *testing.T) {
	temporalDeliveryDirectory(t)
	replacementLicenseFixture(t, true) // K1ABA is missing; K1ABC and original K1ABD are licensed.
	db := loadIngestCTY(t)
	cfg := selectiveLiveConfig()
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	r, driver, key := selectiveLivePopulation(t, cfg, "K1ABA", "DL9ZZZ", 3, at)
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: receiver,
			FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute, Report: 12, HasReport: true}) {
			t.Fatal("legal second nominee was not admitted")
		}
	}
	if driver.Drain() != 2 {
		t.Fatal("legal nominee owner barrier failed")
	}
	driver.Sweep(at.Add(time.Second))
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	p.ctyLookup, p.recentBandStore = func() *cty.CTYDatabase { return db }, nil
	now := time.Now().UTC()
	ordinary := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABD", now)
	assessment := correctionflow.AssessSelectiveFrequencyProposals(key, "K1ABD", 14020, ordinary,
		correctionflow.ReadFrequencySources(r, key, cfg, now), cfg,
		correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3})
	if len(assessment.Proposals) != 2 || assessment.Proposals[0].Call != "K1ABA" || assessment.Proposals[1].Call != "K1ABC" {
		t.Fatalf("real geometry order must offer missing first, legal second: %+v", assessment)
	}
	s := selectiveLiveSpot("K1ABD", at)
	for _, proposal := range assessment.Proposals {
		if gate := p.evaluateSelectiveRescueGate(s, "K1ABD", proposal, now); !gate.Allow {
			t.Fatalf("both alternatives must reach final validation, not an earlier gate: %+v", gate)
		}
	}
	if !replacementLicenseRejected(s, "K1ABA", db, nil) || replacementLicenseRejected(s, "K1ABC", db, nil) || applyLicenseGate(s, db, nil, nil) {
		t.Fatal("literal license authority must reject only the first C nominee")
	}
	p.processSpot(s, nil)
	if p.tracker.CorrectionDecisionReasons()[resolverDecisionLicenseMissing] != 1 || p.tracker.CorrectionDecisionAppliedReasons()["resolver_selective_applied"] != 0 {
		t.Fatal("one rejected C replacement must be terminal before the legal second nominee")
	}
	temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "?"}, at, true)
}
