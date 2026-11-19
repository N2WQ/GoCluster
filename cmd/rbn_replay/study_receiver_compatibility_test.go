package main

import (
	"encoding/json"
	"testing"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// A synthetic, validated count table makes the truth ending C more compatible
// with the 1/3 C, 2/3 E receiver budget than the truth ending E. Their common
// prefix cancels: (log(40+a)+2*log(60+a))/3 > log(50+a), a=1/38.
// This is a consumer witness, not a fitted replacement for the frozen asset.
func compatibilityConsumerModel(t *testing.T) *correctionflow.OrdinalModel {
	t.Helper()
	var a correctionflow.OrdinalAsset
	if err := json.Unmarshal(ordinalWireFixture(t), &a); err != nil {
		t.Fatal(err)
	}
	a.Sources[0].AcceptedPairs, a.Sources[0].FittedCWPairs = 1200, 1200
	for i := range a.Cells {
		c := &a.Cells[i]
		*c = correctionflow.OrdinalEventCell{Pairs: 200, ExactPairs: 100, Matches: 3590, Substitutions: 110,
			SourceCharacters: 3700, SubjectCharacters: 3700, GapStops: 3900}
		for symbol := range c.Opportunities {
			c.Opportunities[symbol], c.Events[symbol][symbol] = 100, 100
		}
		c.Events[2][2], c.Events[2][4] = 40, 60
		c.Events[4][2], c.Events[4][4] = 50, 50
	}
	m, err := correctionflow.NewOrdinalModel(&a)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func compatibilityConsumerOwner(t *testing.T, licensed ...string) (*replayRunner, spot.ResolverSignalKey) {
	t.Helper()
	r, key := methodApplyOwner(t, licensed...)
	r.cfg.CallCorrection.MaxEditDistance = 3
	r.cfg.CallCorrection.ResolverRecentPlus1Enabled, r.cfg.CallCorrection.BayesBonus.Enabled = false, false
	r.method.prepared.Plan.SchemaVersion = 2
	r.method.prepared.Candidate = methodCandidatesV2()[1]
	r.method.prepared.Model = compatibilityConsumerModel(t)
	methodApplyRows(t, r, key, "K1ABC", 2, 14020, methodApplyAt, 2*time.Minute)
	methodApplyRows(t, r, key, "K1ABE", 3, 14020, methodApplyAt, 2*time.Minute)
	methodApplyRows(t, r, key, "DL9ZZZ", 6, 14020.2, methodApplyAt, 2*time.Minute)
	methodApplyPublish(t, r, 11, methodApplyAt)
	return r, key
}

func compatibilityConsumerPrecondition(t *testing.T, r *replayRunner, key spot.ResolverSignalKey) correctionflow.ReceiverCompatibilityAssessment {
	t.Helper()
	now := methodApplyAt.Add(time.Millisecond)
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, r.cfg.CallCorrection, "K1ABD", now)
	b := correctionflow.AssessReceiverCompatibilityProposals(key, "K1ABD", 14020, selection,
		correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now), r.cfg.CallCorrection,
		correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3}, r.method.prepared.Model)
	if !b.Available || !b.SelectedOK || b.Selected.Call != "K1ABC" || len(b.Proposals) != 2 || b.Proposals[0].Call != "K1ABE" || len(b.Hypotheses) != 3 || b.ScoreCalls != 6 || b.Denominator != 3000 {
		t.Fatalf("literal model must select the lower-support C identity against E and KEEP: %+v", b)
	}
	return b
}

func TestCCV2BSelectedGateFailureTerminal(t *testing.T) {
	r, key := compatibilityConsumerOwner(t, "K1ABD", "K1ABC", "K1ABE")
	r.cfg.CallCorrection.MinConsensusReports = 3
	b := compatibilityConsumerPrecondition(t, r, key)
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	if gate := r.evaluateMethodGate(s, "K1ABD", b.Proposals[0], methodApplyAt); !gate.Allow {
		t.Fatalf("lower model hypothesis must have a reachable ordinary gate: %+v", gate)
	}
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	receipts := methodApplyReceipts(t, r)
	if out.Applied || s.DXCallNorm != "K1ABD" || len(receipts) != 1 || len(receipts[0].Trials) != 1 || receipts[0].Trials[0].Proposal.Call != "K1ABC" || receipts[0].Trials[0].Gate.Reason != "min_reports" || receipts[0].Compatibility == nil || !receipts[0].Compatibility.SelectedOK {
		t.Fatalf("B must try the selected identity once and never fall through after its gate refusal: %+v receipts=%+v", out, receipts)
	}
}

func TestCCV2BSelectedReplacementFailureTerminal(t *testing.T) {
	r, key := compatibilityConsumerOwner(t, "K1ABD", "K1ABE")
	r.cfg.CallCorrection.MinConsensusReports = 2
	compatibilityConsumerPrecondition(t, r, key)
	if !rejectReplayDXLicense(r.ctyDB, "K1ABC", false, methodApplyAt) || rejectReplayDXLicense(r.ctyDB, "K1ABE", false, methodApplyAt) {
		t.Fatal("first model target must be unlicensed while the lower hypothesis is licensed")
	}
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	receipts := methodApplyReceipts(t, r)
	if out.Applied || !out.replacementRejected || s.DXCallNorm != "K1ABD" || len(receipts) != 1 || len(receipts[0].Trials) != 1 || !receipts[0].Trials[0].Gate.Allow || receipts[0].Trials[0].Proposal.Call != "K1ABC" {
		t.Fatalf("selected B target validation failure retried another hypothesis: %+v receipts=%+v", out, receipts)
	}
}

func TestCCV2BTemporalWinnerIntersection(t *testing.T) {
	for _, permitted := range []string{"K1ABC", "K1ABE"} {
		r, key := compatibilityConsumerOwner(t, "K1ABD", "K1ABC", "K1ABE")
		r.cfg.CallCorrection.MinConsensusReports = 2
		compatibilityConsumerPrecondition(t, r, key)
		now := methodApplyAt.Add(time.Millisecond)
		s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
		evidence := methodApplyEvidence(key)
		ordinary := maybeApplyResolverCorrectionReplay(s, r.resolver, evidence, true, r.cfg.CallCorrection, r.ctyDB,
			r.tracker, r.adaptiveMinReports, r.recentBandStore, now)
		if ordinary.Applied || ordinary.Suppress || ordinary.replacementRejected {
			t.Fatal("ordinary control must leave B reachable")
		}
		saved := correctionflow.FrequencySourceLifecycles(correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now))
		out := r.applyMethodRescue(s, evidence, true, now, ordinary, permitted, &saved)
		receipts := methodApplyReceipts(t, r)
		if len(receipts) != 1 || receipts[0].Compatibility == nil || !receipts[0].Compatibility.SelectedOK || receipts[0].Compatibility.Selected.Call != "K1ABC" || len(receipts[0].Compatibility.Hypotheses) != 3 {
			t.Fatal("temporal identity pruned B's hypotheses before full-population scoring")
		}
		if permitted == "K1ABC" {
			if !out.Applied || s.DXCallNorm != "K1ABC" || len(receipts[0].Trials) != 1 || s.ConfidencePercent != 33 || !s.ConfidencePercentOK {
				t.Fatalf("permitted unique winner must apply with its ordinary share: %+v", out)
			}
		} else if out.Applied || s.DXCallNorm != "K1ABD" || len(receipts[0].Trials) != 0 || out.methodReason != "compatibility_temporal_identity" {
			t.Fatalf("permitted second hypothesis was promoted over the actual best: %+v", out)
		}
	}
}

func TestCCV2BUnavailableDoesNotFallbackToC(t *testing.T) {
	r, key := compatibilityConsumerOwner(t, "K1ABD", "K1ABC", "K1ABE")
	r.cfg.CallCorrection.MinConsensusReports = 3
	compatibilityConsumerPrecondition(t, r, key)
	r.method.prepared.Model = nil
	s := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	out := r.applyResolverOutcome(s, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond))
	receipts := methodApplyReceipts(t, r)
	if out.Applied || s.DXCallNorm != "K1ABD" || len(receipts) != 1 || receipts[0].Compatibility == nil || receipts[0].Compatibility.Available || len(receipts[0].Trials) != 0 {
		t.Fatal("unavailable B gained a neutral score or a C fallback")
	}
	r.method.prepared.Candidate = methodCandidatesV2()[0]
	control := replayTemporalFreshSpot("K1ABD", methodApplyAt)
	if got := r.applyResolverOutcome(control, methodApplyEvidence(key), true, methodApplyAt.Add(time.Millisecond)); !got.Applied || control.DXCallNorm != "K1ABE" {
		t.Fatalf("reachable C fallback control was not useful: %+v", got)
	}
}
