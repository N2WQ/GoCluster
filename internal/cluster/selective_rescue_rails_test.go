//go:build !qualification

package cluster

import (
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestSelectiveRescueStabilizerRetryRemainsOrdinaryOnly(t *testing.T) {
	temporalDeliveryDirectory(t)
	replacementLicenseFixture(t, false, "K1ABA")
	db := loadIngestCTY(t)
	cfg := selectiveLiveConfig()
	cfg.StabilizerEnabled, cfg.StabilizerMaxChecks, cfg.StabilizerTimeoutAction = true, 1, stabilizerTimeoutRelease
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	r, driver, key := selectiveLivePopulation(t, cfg, "K1ABA", "K1ABD", 2, at)
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	p.ctyLookup, p.recentBandStore = func() *cty.CTYDatabase { return db }, nil
	selectiveLivePrecondition(t, p, key, "K1ABC", "K1ABA")
	s := selectiveLiveSpot("K1ABC", at)
	original := captureOriginalPeerPublication(s)
	evidence, _ := buildResolverEvidenceSnapshot(s, cfg, nil, time.Now().UTC())
	outcome := applyResolverCorrection(s, r, evidence, true, cfg, db, nil, p.tracker, nil, nil, nil, nil, time.Time{}, nil)
	if !outcome.replacementRejected || outcome.applied || outcome.suppress || s.Confidence != "?" || p.tracker.CallCorrections() != 0 {
		t.Fatalf("actual prior license rejection must retain an ordinary glyph without B: %+v spot=%+v", outcome, s)
	}
	// Transfer the actually rejected request to the existing bounded scheduler.
	// The route must stay ordinary-only even after fresh C becomes reachable.
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 1)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	if !p.telnetStabilizer.enqueueWithTemporalResult(s, 0, "license_rejected_fixture", key, true, true, nil, original, s.Confidence, stabilizerConfidenceUnknown) {
		t.Fatal("actual scheduler refused its free request slot")
	}
	prior, _ := r.Lookup(key)
	// Healthy-key retirement uses observation expiry. The separate process-idle
	// deadline protects loss-affected cells only and cannot retire this witness.
	driver.Sweep(prior.EvaluatedAt.Add(6 * time.Minute))
	if _, ok := r.Lookup(key); ok {
		t.Fatal("old native population must retire before the new-generation retry witness")
	}
	selectiveLiveSeed(t, r, driver, key, "K1ABA", "DL9ZZZ", 2, time.Now().UTC())
	selectiveLivePrecondition(t, p, key, "K1ABC", "K1ABA")
	release := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	if release.peerOriginal != original || release.spot != s {
		t.Fatal("scheduler lost the rejected request's original publication")
	}
	p.handleStabilizerRelease(release)
	if p.telnetStabilizer.Pending() != 0 || p.tracker.CorrectionDecisionAppliedReasons()["resolver_selective_applied"] != 0 ||
		p.tracker.CorrectionDecisionReasons()[resolverDecisionLicenseMissing] != 1 {
		t.Fatal("retry acquired C authority or retained its scheduler reservation")
	}
	temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "?"}, at, true)
}

func TestSelectiveRescueDistanceThreeKeepsFullRails(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, rail := range []string{"allowed", "reports", "advantage", "confidence"} {
		t.Run(rail, func(t *testing.T) {
			cfg := selectiveLiveConfig()
			cfg.DistanceModelCW = "morse"
			cfg.Distance3ExtraReports, cfg.Distance3ExtraAdvantage, cfg.Distance3ExtraConfidence = 1, 1, 5
			if spot.CallDistance("DF1XC", "DF1PU", "CW", cfg.DistanceModelCW, cfg.DistanceModelRTTY) != 3 {
				t.Fatal("literal live fixture must require configured distance three")
			}
			switch rail {
			case "reports":
				cfg.Distance3ExtraReports = 2
			case "advantage":
				cfg.Distance3ExtraAdvantage = 3
			case "confidence":
				cfg.Distance3ExtraConfidence = 56
			}
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			r, _, key := selectiveLivePopulation(t, cfg, "DF1PU", "DL9ZZZ", 3, at)
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			f.pipeline.recentBandStore = nil
			now := time.Now().UTC()
			selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "DF1XC", now)
			assessment := correctionflow.AssessSelectiveFrequencyProposals(key, "DF1XC", 14020, selection,
				correctionflow.ReadFrequencySources(r, key, cfg, now), cfg,
				correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3})
			if len(assessment.Proposals) != 1 || assessment.Proposals[0].Identity != "DF1PU" || assessment.Proposals[0].Support != 3 {
				t.Fatalf("real C nomination must survive independently of its later rails: %+v", assessment)
			}
			gate := f.pipeline.evaluateSelectiveRescueGate(selectiveLiveSpot("DF1XC", at), "DF1XC", assessment.Proposals[0], now)
			if gate.Allow != (rail == "allowed") || gate.Distance != 3 || gate.MinReports != 2+cfg.Distance3ExtraReports ||
				gate.MinAdvantage != 1+cfg.Distance3ExtraAdvantage || gate.MinConfidence != 45+cfg.Distance3ExtraConfidence {
				t.Fatalf("C must use each configured distance-three rail: %+v", gate)
			}
			f.pipeline.processSpot(selectiveLiveSpot("DF1XC", at), nil)
			want := correctionEvidenceDeliveryCase{subject: "DF1XC", wantCall: "DF1XC", wantGlyph: "?"}
			if rail == "allowed" {
				want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "DF1PU", "C", 75, 1
				f.check(t, want, at)
			} else {
				temporalDeliveryCheckWithoutPercent(t, f, want, at, true)
			}
		})
	}
}

func TestSelectiveRescueBayesOnlyComparableNeighborGate(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	requirePathReportH3Mappings(t)
	at := time.Now().UTC()
	cfg.ResolverRecentPlus1Enabled, cfg.BayesBonus.Enabled = false, true
	cfg.MinConsensusReports, cfg.MinAdvantage, cfg.MinConfidencePercent = 4, 1, 45
	cfg.BayesBonus.EnabledDistancesByMode = map[string][]int{"CW": {2}}
	cfg.BayesBonus.ReportThresholdDistance2Milli = 450
	p := &outputPipeline{correctionCfg: cfg, recentBandStore: ccv2BayesDeliveredHistory(t, "K1ABC", at)}
	// Literal scalars isolate the consumer's gate-option forwarding. The native
	// history has its own positive control; C nomination is tested separately.
	proposal := correctionflow.FrequencyProposal{Call: "K1ABC", Identity: "K1ABC", Support: 3, WeightedSupportMilli: 3000,
		SubjectSupport: 1, SubjectWeightedSupportMilli: 1000, ConfidencePercent: 75}
	s := selectiveLiveSpot("K1AXD", at)
	allowed := p.evaluateSelectiveRescueGate(s, "K1AXD", proposal, at)
	if !allowed.Allow || allowed.Distance != 2 || !allowed.BayesReportBonusApplied || allowed.BayesWinnerRecent != 4 || cfg.ResolverRecentPlus1Enabled {
		t.Fatalf("uncontested Bayes-only positive control must be reachable: %+v", allowed)
	}
	proposal.ComparableEditNeighbor = true
	denied := p.evaluateSelectiveRescueGate(s, "K1AXD", proposal, at)
	if denied.Allow || !denied.BayesReportBonusConsidered || denied.BayesReportBonusApplied || denied.BayesReportBonusReject != resolverRecentPlus1DisallowEditNeighborGate {
		t.Fatalf("Bayes-only mode must receive the same contested-neighbor restriction: %+v", denied)
	}
}
