package correctionflow

import (
	"reflect"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestDiagnosticsTemporalCheckedBeforeCutoffAndExtras(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "eleven_identities", true: "duplicate_admission"}[duplicate], func(t *testing.T) {
			d := b31GenerationDecoder()
			d.cfg.MaxObsCandidates, d.cfg.BeamSize, d.cfg.MaxEventsPerKey = 8, 8, 32
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
			calls := []string{"K1A00", "K1A01", "K1A02", "K1A03", "K1A04", "K1A05", "K1A06", "K1A07", "K1BAD", "K1CUT", "K1TAIL"}
			if duplicate {
				calls[1] = calls[0]
			}
			ranks := make([]spot.ResolverCandidateSupport, len(calls))
			for i, call := range calls {
				ranks[i] = spot.ResolverCandidateSupport{Call: call, Support: 1, WeightedSupportMilli: 1000, ReplacementBlocked: i == 8}
			}
			selection := ResolverPrimarySelection{SnapshotOK: true, Snapshot: spot.ResolverSnapshot{Key: key, State: spot.ResolverStateConfident, Winner: "K1WIN",
				SupportBasis: spot.ResolverSupportBasisWeighted, TotalReporters: 8, TotalWeightedSupportMilli: 8000, CandidateRanks: ranks}}
			for i, subject := range []string{"K1FAL", "K1SUB"} {
				if accepted, reason := d.Observe(TemporalObservation{ID: uint64(i + 1), Key: key, ObservedAt: at.Add(time.Duration(i) * 500 * time.Millisecond), SubjectCall: subject, Selection: selection}); !accepted {
					t.Fatal(reason)
				}
			}
			req, st := d.requests[1], d.keyStates[key]
			before := d.Evaluate(1, at.Add(time.Second), false)
			d.requests[1], d.keyStates[key] = req, st
			var capture TemporalEvaluateDiagnostics
			after, err := d.EvaluateWithDiagnostics(1, at.Add(time.Second), false, &capture)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("cutoff diagnostics changed execution: %v", err)
			}
			for event := range capture.Window {
				rows := capture.Window[event].Ranks
				for i := 0; i < 8; i++ {
					if !rows[i].Checked || !rows[i].Allowed || !rows[i].Admitted {
						t.Fatal("actual admitted rank omitted")
					}
				}
				if rows[1].Deduplicated != duplicate || !rows[8].Checked || rows[8].Allowed || rows[8].Admitted || !rows[9].Checked || !rows[9].Allowed || !rows[9].Cutoff || rows[10].Checked || rows[10].Status != "not_visited_after_cutoff" {
					t.Fatalf("checked ninth allowed rank was confused with unvisited suffix: %+v", rows)
				}
				if capture.Window[event].AdmittedRanks != 8 {
					t.Fatal("deduplication changed actual admission count")
				}
			}
			want := 11
			if duplicate {
				want = 10
			}
			if len(capture.Window[1].Candidates) != want || len(capture.Window[1].Extras) != 3 || !capture.Window[1].Extras[0].Added || !capture.Window[1].Extras[1].Added || !capture.Window[1].Extras[2].Added {
				t.Fatal("actual winner/subject/fallback extras were lost")
			}
			if capture.Window[0].Extras[2].Added {
				t.Fatal("first-event fallback duplicate was invented")
			}
			diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
		})
	}
}

func TestDiagnosticsTemporalLateOnlyIsNotStartUniverse(t *testing.T) {
	d := ccv2TemporalOracleDecoder()
	const a, b, c = "DL1AAA", "K9XYZ", "JA7QRS"
	events := []temporalEvent{ccv2TemporalOracleEvent(a, 4000, spot.ResolverCandidateSupport{Call: a, Support: 3, WeightedSupportMilli: 3000}, spot.ResolverCandidateSupport{Call: b, Support: 1, WeightedSupportMilli: 1000}),
		ccv2TemporalOracleEvent(a, 1000, spot.ResolverCandidateSupport{Call: c, Support: 1, WeightedSupportMilli: 1000})}
	events[0].id, events[1].id = 101, 202
	winner, score, margin, count := d.decodeWindowDiagnosticsLocked(events, a, "USB", nil)
	var capture TemporalEvaluateDiagnostics
	gotWinner, gotScore, gotMargin, gotCount := d.decodeWindowDiagnosticsLocked(events, a, "USB", &capture)
	if capture.budget.err != nil || winner != gotWinner || score != gotScore || margin != gotMargin || count != gotCount {
		t.Fatal("trace altered existing independent-oracle execution")
	}
	if len(capture.Window) != 2 || capture.Window[0].ID != 101 || capture.Window[1].ID != 202 {
		t.Fatal("event membership was inferred rather than copied")
	}
	foundLate := false
	for _, call := range capture.Window[1].Candidates {
		foundLate = foundLate || call == c
	}
	for _, row := range capture.StartScores {
		if row.Call == c {
			t.Fatal("later event candidate was falsely described as a starting identity")
		}
	}
	if !foundLate {
		t.Fatal("actual later candidate was omitted")
	}
}

func TestDiagnosticsTemporalDisabledResetAndFallbackParity(t *testing.T) {
	var decoder *TemporalDecoder
	capture := TemporalEvaluateDiagnostics{Window: []DiagnosticTemporalEvent{{ID: 999}}, Winner: "stale"}
	decision, err := decoder.EvaluateWithDiagnostics(7, time.Time{}, false, &capture)
	if err != nil || decision.Reason != "decoder_disabled" || capture.ClockRead || len(capture.Window) != 0 || capture.Winner != "" {
		t.Fatal("disabled capture retained a previous invocation or invented a clock")
	}
	for _, overflow := range []string{"abstain", "bypass", "fallback_resolver"} {
		t.Run(overflow, func(t *testing.T) {
			tc := diagnosticsBaselineEvaluateCases()[3]
			f := newDiagnosticsBaselineEvaluate(t, tc)
			f.decoder.cfg.OverflowAction = overflow
			f.restore()
			before := f.decoder.Evaluate(1, f.now, true)
			f.restore()
			after, err := f.decoder.EvaluateWithDiagnostics(1, f.now, true, &capture)
			if err != nil || !reflect.DeepEqual(before, after) || capture.Action != after.Action || !capture.TimedOut || f.decoder.PendingCount() != 0 {
				t.Fatal("forced diagnostic fallback changed retirement/action")
			}
		})
	}
}

func TestDiagnosticsTemporalClockReadIsActualFallback(t *testing.T) {
	d := b31GenerationDecoder()
	var capture TemporalEvaluateDiagnostics
	provided := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	decision, err := d.EvaluateWithDiagnostics(999, provided, false, &capture)
	if err != nil || decision.Reason != "missing_request" || capture.ClockRead || !capture.EffectiveAt.Equal(provided) {
		t.Fatal("provided replay clock was misreported as a clock read")
	}
	decision, err = d.EvaluateWithDiagnostics(999, time.Time{}, false, &capture)
	if err != nil || decision.Reason != "missing_request" || !capture.ClockRead || capture.EffectiveAt.IsZero() || !capture.RequestedAt.IsZero() {
		t.Fatal("actual time.Now fallback was not captured")
	}
}

func TestDiagnosticsGenerationVectorAndFirstMismatch(t *testing.T) {
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 64, MaxActiveKeys: 8, HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
	t.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for offset := -2; offset <= 2; offset++ {
		for i, call := range []string{"K1ABC", "K1ABC", "K1ABD"} {
			if !r.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: []string{"N0AAA", "N0BBB", "N0CCC"}[i], ObservedAt: at, FrequencyKHz: 7010 + .5*float64(offset), RecencyWindow: time.Minute}) {
				t.Fatal("generation fixture refused")
			}
		}
	}
	if driver.Drain() != 15 {
		t.Fatal("generation fixture lost inputs")
	}
	driver.Sweep(at.Add(time.Millisecond))
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodEnabled = false
	saved := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABE", at.Add(time.Second))
	seen := make(map[uint64]bool)
	for slot := 0; slot < 5; slot++ {
		k := key
		k.Bucket += int64(slot - 2)
		s, ok := r.Lookup(k)
		if !ok || s.Generation == 0 || seen[s.Generation] {
			t.Fatal("fixture must have five distinct actual generations")
		}
		seen[s.Generation] = true
		saved.generationWitnesses[slot] = s.Generation
	}
	proposal := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABD", Selection: saved}
	var capture TemporalReleaseDiagnostics
	selected, err := SelectTemporalReleaseSnapshotWithDiagnostics(r, key, cfg, "K1ABE", proposal, at.Add(time.Second), &capture)
	if err != nil || selected.Snapshot.Winner != "K1ABD" || !capture.GenerationCurrent || capture.ExpectedGenerationWitnesses != saved.generationWitnesses {
		t.Fatal("unequal valid per-key generations were collapsed or rejected")
	}
	for slot := range capture.GenerationChecks {
		if capture.GenerationChecks[slot].Status != "read" || capture.GenerationChecks[slot].Current != saved.generationWitnesses[slot] {
			t.Fatal("generation validation was not the actual consumed read")
		}
	}
	proposal.Selection.generationWitnesses[0]++
	selected, err = SelectTemporalReleaseSnapshotWithDiagnostics(r, key, cfg, "K1ABE", proposal, at.Add(time.Second), &capture)
	if err != nil || selected.Snapshot.Winner != "K1ABC" || capture.GenerationCurrent || capture.GenerationChecks[0].Status != "read" || capture.GenerationChecks[1].Status != "not_reached" || capture.GenerationChecks[1].Expected != saved.generationWitnesses[1] {
		t.Fatal("first mismatch lost untouched expected witnesses or became diagnostic failure")
	}
	proposal.Selection.generationConflict = true
	_, err = SelectTemporalReleaseSnapshotWithDiagnostics(r, key, cfg, "K1ABE", proposal, at.Add(time.Second), &capture)
	if err != nil || !capture.GenerationConflict || capture.GenerationChecks[0].Status != "not_reached" {
		t.Fatal("conflict performed unnecessary validation reads")
	}
}
