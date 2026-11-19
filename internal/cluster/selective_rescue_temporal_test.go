//go:build !qualification

package cluster

import (
	"fmt"
	"math"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func selectiveTemporalConfig() config.CallCorrectionConfig {
	cfg := selectiveLiveConfig()
	cfg.MinConfidencePercent = 70 // Ordinary 2/3 fails; associated C 2/2 passes.
	cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
	return cfg
}

// The real initial pipeline holds a native K1ABC winner before DL9ZZZ arrives.
// Saved generation remains current while the actual release rereads new rows.
func selectiveTemporalHold(t *testing.T, cfg config.CallCorrectionConfig) (ambiguityDeliveryFixture,
	*spot.SignalResolverDriver, spot.ResolverSignalKey, *spot.Spot, time.Time) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	r, driver, key := selectiveLiveResolver(t, cfg)
	for _, receiver := range []string{"N0AAA", "N0BBB"} {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: receiver,
			FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("initial settled winner was not admitted")
		}
	}
	if driver.Drain() != 2 {
		t.Fatal("initial reports not consumed")
	}
	driver.Sweep(at.Add(time.Millisecond))
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	p.recentBandStore, p.temporal = nil, newRuntimeTemporalController(cfg)
	s := selectiveLiveSpot("K1ABD", at)
	p.processSpot(s, nil)
	if len(p.temporal.pending) != 1 || len(p.temporal.queue) != 1 || f.ring.GetCount() != 0 || p.tracker.CallCorrections() != 0 {
		t.Fatal("real initial pipeline did not hold the ordinary success candidate")
	}
	for _, pending := range p.temporal.pending {
		want := correctionflow.FrequencySourceLifecycles(correctionflow.ReadFrequencySources(r, key, cfg, pending.evidence.ObservedAt))
		if pending.frequencyLifecycles != want || want.ReadMask == 0 || pending.selection.Snapshot.ReceiverEvidence() != nil {
			t.Fatal("pending must copy fixed lifecycle masks and compact receiver views at Observe")
		}
	}
	if driver.Drain() != 1 {
		t.Fatal("held incoming evidence must remain normal asynchronous owner input")
	}
	selectiveLiveSeed(t, r, driver, key, "K1ABC", "DL9ZZZ", 2, time.Now().UTC())
	selectiveLivePrecondition(t, p, key, "K1ABD", "K1ABC")
	return f, driver, key, s, at
}

func selectiveTemporalRelease(t *testing.T, f ambiguityDeliveryFixture, at time.Time, action correctionflow.TemporalDecisionAction) runtimeTemporalRelease {
	t.Helper()
	releases := f.pipeline.temporal.Drain(at, true)
	if len(releases) != 1 || releases[0].decision.Action != action || len(f.pipeline.temporal.pending) != 0 || len(f.pipeline.temporal.queue) != 0 {
		t.Fatalf("actual terminal release did not retire its bounded pending owner: %+v", releases)
	}
	return releases[0]
}

func TestSelectiveRescueTemporalAdmittedApply(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := selectiveTemporalConfig()
	f, _, key, s, at := selectiveTemporalHold(t, cfg)
	release := selectiveTemporalRelease(t, f, time.Now().UTC().Add(4*time.Second), correctionflow.TemporalDecisionActionApply)
	selection, admitted := correctionflow.SelectTemporalReleaseSnapshotWithAuthority(f.pipeline.signalResolver, key, cfg, "K1ABD", release.decision, release.releaseAt)
	if !admitted || release.decision.Winner != "K1ABC" || correctionflow.ResolverWinnerConfidence(selection.Snapshot) != 66 {
		t.Fatal("actual decoder/release guard must admit the existing 2/3 target")
	}
	f.pipeline.processSpot(s, &release)
	f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABC", wantGlyph: "C", wantPercent: 66, wantCorrections: 1}, at)
	if f.pipeline.tracker.TemporalCommitted() != 1 || f.pipeline.temporal.decoder.PendingCount() != 0 {
		t.Fatal("one real mutation must confirm once without retaining pending state")
	}
}

func TestSelectiveRescueTemporalFallbackAndForbiddenActions(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, action := range []string{"fallback_resolver", "abstain", "bypass"} {
		t.Run(action, func(t *testing.T) {
			cfg := selectiveTemporalConfig()
			cfg.TemporalDecoder.MinScore, cfg.TemporalDecoder.OverflowAction = math.MaxInt, action
			f, _, _, s, at := selectiveTemporalHold(t, cfg)
			wantAction := correctionflow.TemporalDecisionActionFallbackResolver
			if action == "abstain" {
				wantAction = correctionflow.TemporalDecisionActionAbstain
			} else if action == "bypass" {
				wantAction = correctionflow.TemporalDecisionActionBypass
			}
			release := selectiveTemporalRelease(t, f, time.Now().UTC().Add(4*time.Second), wantAction)
			f.pipeline.processSpot(s, &release)
			want := correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "P", wantPercent: 33}
			if action == "fallback_resolver" {
				want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "K1ABC", "C", 66, 1
			}
			f.check(t, want, at)
		})
	}
}

func TestSelectiveRescueOverflowNeverAcquiresImmediateAuthority(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, action := range []string{"fallback_resolver", "abstain", "bypass"} {
		t.Run(action, func(t *testing.T) {
			cfg := selectiveTemporalConfig()
			cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.OverflowAction = 1, action
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			r, _, key := selectiveLivePopulation(t, cfg, "K1ABC", "DL9ZZZ", 2, at)
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			p := f.pipeline
			p.recentBandStore, p.temporal = nil, newRuntimeTemporalController(cfg)
			selectiveLivePrecondition(t, p, key, "K1ABD", "K1ABC")
			p.processSpot(selectiveLiveSpot("K1ABD", at), nil)
			if len(p.temporal.pending) != 1 {
				t.Fatal("actual first hold must exhaust the one-request cap")
			}
			incomingAt := at.Add(time.Second)
			p.processSpot(selectiveLiveSpot("K1ABD", incomingAt), nil)
			if p.tracker.TemporalFallbackResolver()+p.tracker.TemporalOverflowBypass()+p.tracker.TemporalAbstainLowMargin() != 1 || len(p.temporal.pending) != 1 {
				t.Fatal("second actual input must execute one overflow branch without consuming the hold")
			}
			temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "?"}, incomingAt, true)
			p.temporal.Drain(time.Now().UTC().Add(time.Hour), true) // Cleanup, not another output invocation.
		})
	}
}

func TestSelectiveRescueReleaseClockCannotReviveExpiredRows(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := selectiveTemporalConfig()
	cfg.TemporalDecoder.MinScore = math.MaxInt
	f, _, _, s, at := selectiveTemporalHold(t, cfg)
	release := selectiveTemporalRelease(t, f, time.Now().UTC().Add(3*time.Minute), correctionflow.TemporalDecisionActionFallbackResolver)
	f.pipeline.processSpot(s, &release)
	temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "?"}, at, true)
}

func TestSelectiveRescueTemporalSameGenerationRereadsRows(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := selectiveTemporalConfig()
	f, driver, key, s, at := selectiveTemporalHold(t, cfg)
	var saved correctionflow.FrequencyLifecycleVector
	for _, pending := range f.pipeline.temporal.pending {
		saved = pending.frequencyLifecycles
	}
	for _, receiver := range []string{"N0DDD", "N0EEE"} {
		if !f.pipeline.signalResolver.Enqueue(spot.ResolverEvidence{ObservedAt: time.Now().UTC(), Key: key, DXCall: "JA5XYZ", Spotter: receiver,
			FrequencyKHz: 14020, RecencyWindow: 2 * time.Minute, Report: 12, HasReport: true}) {
			t.Fatal("new credible rival refused")
		}
	}
	if driver.Drain() != 2 {
		t.Fatal("rival owner barrier")
	}
	prior, _ := f.pipeline.signalResolver.Lookup(key)
	driver.Sweep(prior.EvaluatedAt.Add(time.Second))
	currentRows, ok := f.pipeline.signalResolver.LookupEligible(key, time.Now().UTC().Add(2*time.Second))
	if !ok || correctionflow.ResolverSupportForCall(currentRows, "JA5XYZ") != 2 || currentRows.TotalReporters != 5 {
		t.Fatalf("owner must publish both new positive rival receivers before release: %+v", currentRows)
	}
	if current := correctionflow.FrequencySourceLifecycles(correctionflow.ReadFrequencySources(f.pipeline.signalResolver, key, cfg, time.Now().UTC())); current != saved {
		t.Fatal("fresh-rival case must retain the exact lifecycle vector")
	}
	release := selectiveTemporalRelease(t, f, time.Now().UTC().Add(4*time.Second), correctionflow.TemporalDecisionActionApply)
	_, admitted := correctionflow.SelectTemporalReleaseSnapshotWithAuthority(f.pipeline.signalResolver, key, cfg, "K1ABD", release.decision, release.releaseAt)
	if !admitted {
		t.Fatal("fresh-rival regression must reach C after the existing release guard")
	}
	current := correctionflow.AssessSelectiveFrequencyProposals(key, "K1ABD", 14020, correctionflow.ResolverPrimarySelection{Snapshot: currentRows, SnapshotOK: true},
		correctionflow.ReadFrequencySources(f.pipeline.signalResolver, key, cfg, release.releaseAt), cfg,
		correctionflow.FrequencyProposalOptions{GapHz: 150, MaxWitnessSeparation: time.Minute, MaxDistance: 3})
	if len(current.Proposals) != 0 || len(current.Declines) == 0 {
		t.Fatalf("same-generation new rows must actually trigger C's rival refusal: %+v", current)
	}
	f.pipeline.processSpot(s, &release)
	f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "P", wantPercent: 20}, at)
}

func TestSelectiveRescueTemporalWitnessCopyAndRefusal(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, fault := range []string{"missing", "generation"} {
		t.Run(fmt.Sprintf("saved_%s", fault), func(t *testing.T) {
			cfg := selectiveTemporalConfig()
			f, _, _, s, at := selectiveTemporalHold(t, cfg)
			release := selectiveTemporalRelease(t, f, time.Now().UTC().Add(4*time.Second), correctionflow.TemporalDecisionActionApply)
			if fault == "missing" {
				release.pending.frequencyLifecycles = correctionflow.FrequencyLifecycleVector{}
			} else {
				release.pending.frequencyLifecycles.Generations[2]++
			}
			f.pipeline.processSpot(s, &release)
			f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "P", wantPercent: 33}, at)
		})
	}
}
