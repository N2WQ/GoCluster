package main

import (
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

// These fixtures exercise the resolver-only replay release path. Their literal
// receiver populations establish evidence integrity, not independent RF truth.
func TestReplayTemporalFreshReleaseAuthority(t *testing.T) {
	for _, mode := range []string{"expired_unswept", "fresh_saved_winner", "fresh_new_winner", "fresh_split"} {
		t.Run(mode, func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			cfg := replayTemporalFreshConfig()
			r, key := replayTemporalFreshRunner(t, cfg, at, time.Second)
			s := replayTemporalFreshSpot("K1ABD", at)
			replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
			releaseAt := at.Add(10 * time.Second)
			replayTemporalFreshProposal(t, r, releaseAt, false, correctionflow.TemporalDecisionActionApply, "K1ABC")
			wantCall, wantGlyph, wantAccepted := "K1ABD", "?", uint64(0)
			if mode == "expired_unswept" {
				// No owner sweep occurs after holding. A real immutable publication
				// still contains four rows whose own eligible window has elapsed.
				published, ok := r.resolver.Lookup(key)
				if !ok || published.TotalReporters != 4 || published.ReceiverEvidence() == nil ||
					published.ReceiverEvidence().RecencyWindow() != time.Second {
					t.Fatal("expired-publication fixture was swept or lost receiver evidence")
				}
			} else {
				winner, runnerCount := "K1ABC", 1
				if mode == "fresh_new_winner" {
					winner = "K1ABE"
					replayTemporalFreshRetire(t, r, key, releaseAt, winner)
				}
				if mode == "fresh_split" {
					runnerCount = 2
				}
				replayTemporalFreshSeed(t, r, key, releaseAt, 2*time.Minute, winner, "K1ABD", runnerCount)
				published, ok := r.resolver.Lookup(key)
				if !ok || published.TotalReporters != 3+runnerCount {
					t.Fatalf("fresh publication fixture: %+v", published)
				}
				if mode == "fresh_split" {
					if published.State != spot.ResolverStateSplit {
						t.Fatalf("A3/B2 did not establish a qualifying narrow split: %+v", published)
					}
					wantGlyph = "P"
				} else {
					if published.Winner != winner || mode == "fresh_new_winner" &&
						correctionflow.ResolverSupportForCall(published, "K1ABC") != 0 {
						t.Fatalf("fresh settled control retained obsolete proposal: %+v", published)
					}
					replayTemporalFreshOrdinaryControl(t, r, key, s, releaseAt, winner)
					wantCall, wantGlyph, wantAccepted = winner, "C", 1
				}
			}
			r.drainTemporal(releaseAt, false)
			replayTemporalFreshReleased(t, r, s, wantCall, wantGlyph, wantAccepted, 1)
		})
	}
}

func TestReplayTemporalAbstainUsesFreshEvidence(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	cfg := replayTemporalFreshConfig()
	cfg.TemporalDecoder.MinMarginScore, cfg.TemporalDecoder.OverflowAction = 50000, "abstain"
	r, key := replayTemporalFreshRunner(t, cfg, at, time.Second)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	releaseAt := at.Add(10 * time.Second)
	replayTemporalFreshProposal(t, r, releaseAt, false, correctionflow.TemporalDecisionActionAbstain, "K1ABC")
	replayTemporalFreshRetire(t, r, key, releaseAt, "K1ABD")
	replayTemporalFreshSeed(t, r, key, releaseAt, 2*time.Minute, "K1ABD", "K1ABC", 1)
	current, ok := r.resolver.Lookup(key)
	if !ok || current.Winner != "K1ABD" || current.WinnerSupport != 3 || current.TotalReporters != 4 {
		t.Fatalf("abstain confidence control must publish B3/A1: %+v", current)
	}
	r.drainTemporal(releaseAt, false)
	replayTemporalFreshReleased(t, r, s, "K1ABD", "V", 0, 0)
	if r.abMetrics.Temporal.AbstainLowMargin != 1 {
		t.Errorf("configured abstain decisions=%d want 1", r.abMetrics.Temporal.AbstainLowMargin)
	}
}

func TestReplayTemporalForceRetiresBeyondEOFClock(t *testing.T) {
	at := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	cfg := replayTemporalFreshConfig()
	cfg.TemporalDecoder.LagSeconds = int((73 * time.Hour).Seconds())
	cfg.TemporalDecoder.MaxWaitSeconds = int((74 * time.Hour).Seconds())
	r, key := replayTemporalFreshRunner(t, cfg, at, time.Second)
	r.dayStart, r.dayEnd = at, at.Add(24*time.Hour)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	// Use the existing replayDay EOF release clock. The queued due time is
	// another 25 hours beyond it; force must retire ownership, not advance time.
	eofAt := r.dayEnd.Add(24 * time.Hour)
	if !r.temporalQueue[0].due.After(eofAt.Add(24 * time.Hour)) {
		t.Fatal("fixture head is not beyond EOF clock plus 24 hours")
	}
	r.drainTemporal(eofAt, false)
	if len(r.temporalPending) != 1 || len(r.temporalQueue) != 1 || r.temporalDecoder.PendingCount() != 1 ||
		r.abMetrics.Output.ConfidenceCounts.Samples != 0 {
		t.Fatal("non-forced pre-deadline control released a held observation")
	}
	replayTemporalFreshProposal(t, r, eofAt, true, correctionflow.TemporalDecisionActionApply, "K1ABC")
	r.drainTemporal(eofAt, true)
	replayTemporalFreshReleased(t, r, s, "K1ABD", "?", 0, 1)
	for i, retained := range r.temporalQueue[:cap(r.temporalQueue)] {
		if retained != nil {
			t.Errorf("retired heap backing slot %d retained request: %+v", i, retained)
		}
	}
	r.drainTemporal(eofAt, true)
	if r.abMetrics.Output.ConfidenceCounts.Samples != 1 || r.tracker.CallCorrections() != 0 ||
		r.abMetrics.Temporal.Proposed != 1 || r.abMetrics.Temporal.HandoffAttempts != 1 {
		t.Fatal("repeated force release duplicated a retired observation")
	}
}

func TestReplayTemporalFreshSupportedProposalCanDifferFromOrdinaryWinner(t *testing.T) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	cfg := replayTemporalFreshConfig()
	// A's live 3/8=37% share passes this explicitly configured ordinary gate.
	// Report count and advantage remain positive: A3 exceeds subject B1 by two.
	cfg.MinConfidencePercent = 25
	r, key := replayTemporalFreshRunner(t, cfg, at, time.Second)
	s := replayTemporalFreshSpot("K1ABD", at)
	replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
	releaseAt := at.Add(10 * time.Second)
	replayTemporalFreshProposal(t, r, releaseAt, false, correctionflow.TemporalDecisionActionApply, "K1ABC")
	replayTemporalFreshSeed(t, r, key, releaseAt, 2*time.Minute, "K1ABC", "K1ABD", 1)
	// Both populations must stay inside the same 500 Hz bucket. Place the
	// refreshed A and C witnesses 400 Hz apart across its center, outside the
	// selected narrow guard; a +350 Hz C witness would belong to another key.
	for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: releaseAt.Add(time.Nanosecond), Key: key, DXCall: "K1ABC", Spotter: receiver, FrequencyKHz: 14019.8, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("fresh A frequency update")
		}
	}
	for _, receiver := range []string{"N1AAA", "N1BBB", "N1CCC", "N1DDD"} {
		if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: releaseAt, Key: key, DXCall: "K1ABE",
			Spotter: receiver, FrequencyKHz: 14020.2, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("fresh competing C receiver was not admitted")
		}
	}
	if r.driver.Drain() != 7 {
		t.Fatal("fresh competing C population was not consumed")
	}
	// Advance past the preceding batch's evaluation deadline before publishing
	// the C4 population. The actual release uses this same deterministic clock.
	releaseAt = releaseAt.Add(time.Microsecond)
	r.driver.Sweep(releaseAt)
	current := correctionflow.SelectResolverPrimarySnapshotForCall(r.resolver, key, cfg, "K1ABD")
	if !current.SnapshotOK || current.Snapshot.State != spot.ResolverStateProbable || current.Snapshot.Winner != "K1ABE" ||
		current.Snapshot.TotalReporters != 8 || correctionflow.ResolverSupportForCall(current.Snapshot, "K1ABC") != 3 {
		t.Fatalf("fresh ordinary winner must be C4 over still eligible A3/B1 without narrow conflict: %+v", current)
	}
	replayTemporalFreshOrdinaryControl(t, r, key, s, releaseAt, "K1ABE")
	// Gate control uses only the published live candidate population. Repointing
	// its winner derives A's actual 37% share from immutable candidate ranks.
	proposal := current
	proposal.Snapshot.Winner, proposal.Snapshot.WinnerSupport = "K1ABC", 3
	proposal.Snapshot.WinnerWeightedSupportMilli = correctionflow.ResolverWeightedSupportForCall(current.Snapshot, "K1ABC")
	gate, evaluated := evaluateResolverPrimaryGateReplay(s, "K1ABD", proposal, cfg, nil, nil, releaseAt)
	if !evaluated || !gate.Allow || gate.WinnerSupport != 3 || gate.SubjectSupport != 1 || gate.WinnerConfidence != 37 {
		t.Fatalf("fresh temporal candidate did not legitimately pass ordinary gates: %+v evaluated=%t", gate, evaluated)
	}
	r.drainTemporal(releaseAt, false)
	replayTemporalFreshReleased(t, r, s, "K1ABC", "C", 1, 1)
	// The final C glyph alone cannot distinguish fresh A37 from saved A75.
	// Observe the actual selection passed to processOutcome, not the gate control.
	if r.abMetrics.Resolver.StateCounts.Probable != 1 || r.abMetrics.Resolver.ProjectedConfidenceCounts.P != 1 ||
		r.abMetrics.Resolver.ProjectedConfidenceCounts.V != 0 {
		t.Errorf("actual released selection reused saved A75 authority: state=%+v projected=%+v",
			r.abMetrics.Resolver.StateCounts, r.abMetrics.Resolver.ProjectedConfidenceCounts)
	}
}

func TestReplayTemporalAcceptedMutationCountersAndHistory(t *testing.T) {
	for _, mode := range []string{"accepted", "cty_rejected", "unchanged"} {
		t.Run(mode, func(t *testing.T) {
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			cfg := replayTemporalFreshConfig()
			r, key := replayTemporalFreshRunner(t, cfg, at, 2*time.Minute)
			subject, wantCall, wantGlyph, firstAccepted := "K1ABD", "K1ABC", "C", uint64(1)
			if mode == "cty_rejected" {
				db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0"?><plist version="1.0"><dict></dict></plist>`))
				if err != nil {
					t.Fatal(err)
				}
				r.ctyDB = db
				wantCall, wantGlyph, firstAccepted = subject, "B", 0
			}
			if mode == "unchanged" {
				subject, wantGlyph, firstAccepted = "K1ABC", "V", 0
			}
			s := replayTemporalFreshSpot(subject, at)
			replayTemporalFreshHold(t, r, key, s, at, "K1ABC")
			replayTemporalFreshProposal(t, r, at.Add(2*time.Second), false, correctionflow.TemporalDecisionActionApply, "K1ABC")
			r.drainTemporal(at.Add(2*time.Second), false)
			replayTemporalFreshReleased(t, r, s, wantCall, wantGlyph, firstAccepted, 1)
			if mode == "cty_rejected" && r.tracker.CorrectionDecisionReasons()[resolverDecisionCTYMiss] != 1 {
				t.Errorf("fixture did not reach CTY rejection: %v", r.tracker.CorrectionDecisionReasons())
			}
			// A rejected or unchanged A proposal must not turn the first accepted
			// B mutation into a path switch. An accepted A then B must count two.
			r.ctyDB = nil
			fresh := at.Add(10 * time.Second)
			replayTemporalFreshRetire(t, r, key, fresh, "K1ABD")
			replayTemporalFreshSeed(t, r, key, fresh, 2*time.Minute, "K1ABD", "K1ABE", 1)
			current, ok := r.resolver.Lookup(key)
			if !ok || current.TotalReporters != 4 || correctionflow.ResolverSupportForCall(current, "K1ABC") != 0 {
				t.Fatalf("second live population retained the obsolete first proposal: %+v", current)
			}
			next := replayTemporalFreshSpot("K1ABE", fresh)
			replayTemporalFreshOrdinaryControl(t, r, key, next, fresh, "K1ABD")
			replayTemporalFreshHold(t, r, key, next, fresh, "K1ABD")
			replayTemporalFreshProposal(t, r, fresh.Add(2*time.Second), false, correctionflow.TemporalDecisionActionApply, "K1ABD")
			r.drainTemporal(fresh.Add(2*time.Second), false)
			if next.DXCallNorm != "K1ABD" || next.Confidence != "C" ||
				r.abMetrics.Temporal.Committed != firstAccepted+1 || r.abMetrics.Temporal.PathSwitches != firstAccepted ||
				r.abMetrics.Temporal.Proposed != 2 || r.abMetrics.Temporal.HandoffAttempts != 2 ||
				r.abMetrics.Temporal.ConfirmedHistoryKeys != 1 || r.abMetrics.Temporal.ConfirmedHistoryEvictions != 0 ||
				r.tracker.CallCorrections() != firstAccepted+1 || r.abMetrics.Output.ConfidenceCounts.Samples != 2 ||
				uint64(len(r.stabilityCollector.corrections)) != firstAccepted+1 {
				t.Errorf("second mutation/history: call=%s glyph=%s temporal=%+v output=%+v corrections=%d stability=%d",
					next.DXCallNorm, next.Confidence, r.abMetrics.Temporal, r.abMetrics.Output,
					r.tracker.CallCorrections(), len(r.stabilityCollector.corrections))
			}
		})
	}
}

func replayTemporalFreshConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, MaxEditDistance: 6, MinConsensusReports: 2,
		MinAdvantage: 1, MinConfidencePercent: 45, FrequencyToleranceHz: 500,
		FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot", InvalidAction: "broadcast",
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 1,
			MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 4, BeamSize: 4, MaxObsCandidates: 4,
			OverflowAction: "fallback_resolver",
		},
	}
}

func replayTemporalFreshRunner(t *testing.T, cfg config.CallCorrectionConfig, at time.Time, window time.Duration) (*replayRunner, spot.ResolverSignalKey) {
	t.Helper()
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatal(err)
	}
	r := &replayRunner{cfg: &config.Config{CallCorrection: cfg}, resolver: resolver, driver: driver,
		tracker: &stats.Tracker{}, temporalNextID: 1, temporalDecoder: correctionflow.NewTemporalDecoder(cfg),
		temporalPending: make(map[uint64]replayTemporalPending), stabilityCollector: newReplayStabilityCollector(replayStabilityConfig{})}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	replayTemporalFreshSeed(t, r, key, at, window, "K1ABC", "K1ABD", 1)
	return r, key
}

func replayTemporalFreshSpot(call string, at time.Time) *spot.Spot {
	s := spot.NewSpotNormalized(call, "N0AAA", 14020, "CW")
	s.Time = at
	return s
}

func replayTemporalFreshSeed(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, at time.Time, window time.Duration, winner, subject string, subjectCount int) {
	t.Helper()
	for _, group := range []struct {
		call      string
		frequency float64
		receivers []string
	}{
		{winner, 14020, []string{"N0AAA", "N0BBB", "N0CCC", "N0FFF"}[:3]},
		{subject, 14020.2, []string{"N0DDD", "N0EEE"}[:subjectCount]},
	} {
		for _, receiver := range group.receivers {
			if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: group.call,
				Spotter: receiver, FrequencyKHz: group.frequency, RecencyWindow: window}) {
				t.Fatal("replay fixture receiver was not admitted")
			}
		}
	}
	if got := r.driver.Drain(); got != 3+subjectCount {
		t.Fatalf("consumed reports=%d want %d", got, 3+subjectCount)
	}
	r.driver.Sweep(at.Add(2 * time.Nanosecond))
}

func replayTemporalFreshRetire(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, at time.Time, winner string) {
	t.Helper()
	// Apply a narrow window before its later expansion so old memberships are
	// pruned from the owner, rather than accidentally restored by a new window.
	if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: winner,
		Spotter: "N0AAA", FrequencyKHz: 14020, RecencyWindow: time.Second}) || r.driver.Drain() != 1 {
		t.Fatal("replay fixture failed to retire obsolete population")
	}
}

func replayTemporalFreshHold(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, s *spot.Spot, at time.Time, winner string) {
	t.Helper()
	if !r.maybeHoldTemporal(s, spot.ResolverEvidence{Key: key}, true, at) || len(r.temporalPending) != 1 ||
		len(r.temporalQueue) != 1 || r.temporalDecoder.PendingCount() != 1 {
		t.Fatal("actual replay temporal admission did not own exactly one observation")
	}
	for _, pending := range r.temporalPending {
		if pending.spot != s || !pending.selection.SnapshotOK || pending.selection.Snapshot.Winner != winner ||
			pending.selection.Snapshot.TotalReporters != 4 || pending.selection.Snapshot.ReceiverEvidence() != nil {
			t.Fatalf("saved compact A3/B1 proposal fixture: %+v", pending)
		}
	}
}

func replayTemporalFreshProposal(t *testing.T, r *replayRunner, releaseAt time.Time, force bool, action correctionflow.TemporalDecisionAction, winner string) {
	t.Helper()
	// Independently evaluate the saved input with another real decoder. This
	// verifies the proposal gate without consuming the runner's owned request.
	control := correctionflow.NewTemporalDecoder(r.cfg.CallCorrection)
	for _, pending := range r.temporalPending {
		observedAt := pending.maxAt.Add(-r.temporalDecoder.MaxWaitDuration())
		if accepted, reason := control.Observe(correctionflow.TemporalObservation{ID: pending.id, ObservedAt: observedAt,
			Key: pending.evidence.Key, SubjectCall: normalizedDXCall(pending.spot), Selection: pending.selection}); !accepted {
			t.Fatalf("saved-proposal control was not admitted: %s", reason)
		}
		decision := control.Evaluate(pending.id, releaseAt, force)
		if decision.Action != action || decision.Winner != winner || decision.Key != pending.evidence.Key {
			t.Fatalf("fixture did not produce required temporal proposal: %+v", decision)
		}
	}
}

func replayTemporalFreshOrdinaryControl(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, s *spot.Spot, at time.Time, winner string) {
	t.Helper()
	copy := s.Clone()
	outcome := maybeApplyResolverCorrectionReplay(copy, r.resolver, spot.ResolverEvidence{Key: key}, true,
		r.cfg.CallCorrection, nil, nil, nil, nil, at)
	if !outcome.Applied || outcome.Winner != winner || !outcome.GateEvaluated || !outcome.Gate.Allow {
		t.Fatalf("ordinary correction positive control failed: %+v", outcome)
	}
}

func replayTemporalFreshReleased(t *testing.T, r *replayRunner, s *spot.Spot, call, glyph string, accepted, proposed uint64) {
	t.Helper()
	if len(r.temporalPending) != 0 || len(r.temporalQueue) != 0 || r.temporalDecoder.PendingCount() != 0 {
		t.Errorf("temporal ownership leaked: pending=%d queue=%d decoder=%d", len(r.temporalPending), len(r.temporalQueue), r.temporalDecoder.PendingCount())
	}
	if s.DXCallNorm != call || s.DXCall != call || s.Confidence != glyph {
		t.Errorf("replay output call=%s normalized=%s glyph=%s want %s/%s", s.DXCall, s.DXCallNorm, s.Confidence, call, glyph)
	}
	wantConfidence := replayConfidenceCounts{Samples: 1}
	switch glyph {
	case "?":
		wantConfidence.Unknown = 1
	case "P":
		wantConfidence.P = 1
	case "V":
		wantConfidence.V = 1
	case "C":
		wantConfidence.C = 1
	case "B":
		wantConfidence.B = 1
	default:
		t.Fatalf("unsupported literal confidence oracle %q", glyph)
	}
	wantHistoryKeys := 0
	if accepted > 0 {
		wantHistoryKeys = 1 // Every fixture holds one literal resolver key.
	}
	if r.abMetrics.Output.ConfidenceCounts != wantConfidence || r.abMetrics.Temporal.Committed != accepted ||
		r.abMetrics.Temporal.Proposed != proposed || r.abMetrics.Temporal.HandoffAttempts != 1 ||
		r.abMetrics.Temporal.ConfirmedHistoryKeys != wantHistoryKeys || r.abMetrics.Temporal.ConfirmedHistoryEvictions != 0 ||
		r.abMetrics.Temporal.PathSwitches != 0 || r.tracker.CallCorrections() != accepted ||
		uint64(len(r.stabilityCollector.corrections)) != accepted {
		t.Errorf("replay accepted/output accounting: temporal=%+v output=%+v mutations=%d stability=%d want accepted=%d switched=%d",
			r.abMetrics.Temporal, r.abMetrics.Output, r.tracker.CallCorrections(), len(r.stabilityCollector.corrections), accepted, uint64(0))
	}
}
