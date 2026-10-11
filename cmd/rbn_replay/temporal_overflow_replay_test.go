package main

import (
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/spot"
)

func TestReplayTemporalOverflowUsesFreshOutputAccounting(t *testing.T) {
	for _, action := range []string{"fallback_resolver", "bypass", "abstain"} {
		t.Run(action, func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			cfg := replayTemporalFreshConfig()
			cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.OverflowAction = 1, action
			r, key := replayTemporalFreshRunner(t, cfg, at, time.Second)
			first := replayTemporalFreshSpot("K1ABD", at)
			replayTemporalFreshHold(t, r, key, first, at, "K1ABC")
			fresh := at.Add(10 * time.Second)
			replayTemporalFreshRetire(t, r, key, fresh, "K1ABD")
			replayTemporalFreshSeed(t, r, key, fresh, 2*time.Minute, "K1ABD", "K1ABC", 1)
			next := replayTemporalFreshSpot("K1ABC", fresh)
			replayTemporalFreshOrdinaryControl(t, r, key, next, fresh, "K1ABD")
			if !r.maybeHoldTemporal(next, spot.ResolverEvidence{Key: key}, true, fresh) {
				t.Fatal("overflow did not complete through the temporal output owner")
			}
			wantCall, wantGlyph, wantAccepted, wantHistory := "K1ABD", "C", uint64(1), 1
			if action == "abstain" {
				wantCall, wantGlyph, wantAccepted, wantHistory = "K1ABC", "P", 0, 0
			}
			if next.DXCallNorm != wantCall || next.Confidence != wantGlyph ||
				r.abMetrics.Output.ConfidenceCounts.Samples != 1 || r.abMetrics.Temporal.HandoffAttempts != 1 ||
				r.abMetrics.Temporal.Proposed != 0 || r.abMetrics.Temporal.Committed != wantAccepted ||
				r.tracker.CallCorrections() != wantAccepted || r.abMetrics.Temporal.ConfirmedHistoryKeys != wantHistory ||
				len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 || r.temporalDecoder.PendingCount() != 1 {
				t.Errorf("overflow output/ownership: call=%s glyph=%s temporal=%+v output=%+v pending=%d queue=%d decoder=%d",
					next.DXCallNorm, next.Confidence, r.abMetrics.Temporal, r.abMetrics.Output,
					len(r.temporalPending), len(r.temporalQueue), r.temporalDecoder.PendingCount())
			}
			switch action {
			case "fallback_resolver":
				if r.abMetrics.Temporal.FallbackResolver != 1 {
					t.Error("overflow did not record the configured resolver fallback")
				}
			case "bypass":
				if r.abMetrics.Temporal.OverflowBypass != 1 {
					t.Error("overflow did not record the configured bypass")
				}
			case "abstain":
				if r.abMetrics.Temporal.AbstainLowMargin != 1 {
					t.Error("overflow did not record the configured abstain")
				}
			}
			// The original saved A proposal now has live support A1/B3. Its
			// 25% share fails the existing 45% gate; rejection cannot trigger a
			// second fallback mutation. It still yields one retained B/V outcome.
			r.drainTemporal(fresh.Add(time.Second), false)
			if first.DXCallNorm != "K1ABD" || first.Confidence != "V" ||
				r.abMetrics.Output.ConfidenceCounts.Samples != 2 || r.abMetrics.Temporal.HandoffAttempts != 2 ||
				r.abMetrics.Temporal.Proposed != 1 || r.abMetrics.Temporal.Committed != wantAccepted ||
				r.abMetrics.Temporal.PathSwitches != 0 || r.tracker.CallCorrections() != wantAccepted ||
				len(r.temporalPending) != 0 || len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 {
				t.Errorf("original held request after overflow: call=%s glyph=%s temporal=%+v output=%+v pending=%d queue=%d decoder=%d",
					first.DXCallNorm, first.Confidence, r.abMetrics.Temporal, r.abMetrics.Output,
					len(r.temporalPending), len(r.temporalQueue), r.temporalDecoder.PendingCount())
			}
		})
	}
}

func TestReplayTemporalSuppressedProposalHasNoHandoff(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	cfg := replayTemporalFreshConfig()
	cfg.InvalidAction = "suppress"
	r, key := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	r.ctyDB = db
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	r.drainTemporal(at.Add(2*time.Second), false)
	if r.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 1 {
		t.Fatalf("fixture did not reach requested CTY suppression: %v", r.tracker.CorrectionDecisionReasons())
	}
	if s.DXCallNorm != "K1ABD" || r.abMetrics.Temporal.Proposed != 1 ||
		r.abMetrics.Temporal.Committed != 0 || r.abMetrics.Temporal.HandoffAttempts != 0 ||
		r.abMetrics.Output.ConfidenceCounts.Samples != 0 || r.tracker.CallCorrections() != 0 ||
		r.abMetrics.Temporal.ConfirmedHistoryKeys != 0 || r.temporalDecoder.ConfirmedHistoryMetrics().Keys != 0 ||
		len(r.stabilityCollector.corrections) != 0 || len(r.temporalPending) != 0 ||
		len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 {
		t.Errorf("suppressed proposal reached acceptance/handoff/history: temporal=%+v output=%+v call=%s",
			r.abMetrics.Temporal, r.abMetrics.Output, s.DXCallNorm)
	}
}
