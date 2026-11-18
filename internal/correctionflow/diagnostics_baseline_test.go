// File role: Freeze compatibility/allocation controls before diagnostic hooks.
// These fixtures use only the pre-diagnostic public APIs. They qualify the
// measured branch on every invocation; terminal Evaluate calls restore the
// same prebuilt request/event ownership rather than measuring missing_request.
package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type diagnosticsBaselineEvaluateCase struct {
	name           string
	action         TemporalDecisionAction
	reason, winner string
	candidates     int
	force          bool
	latency        time.Duration
}

func diagnosticsBaselineEvaluateCases() []diagnosticsBaselineEvaluateCase {
	return []diagnosticsBaselineEvaluateCase{
		{"decoded_apply", TemporalDecisionActionApply, "proposal", "K1ABC", 2, false, time.Second},
		{"score_defer", TemporalDecisionActionDefer, "low_score", "K1ABC", 2, false, time.Second},
		{"margin_defer", TemporalDecisionActionDefer, "low_margin", "K1ABC", 2, false, time.Second},
		{"forced_fallback", TemporalDecisionActionFallbackResolver, "low_margin", "K1ABC", 2, true, time.Second},
		{"lag_pending", TemporalDecisionActionDefer, "lag_pending", "", 0, false, 500 * time.Millisecond},
		{"missing_request", TemporalDecisionActionBypass, "missing_request", "", 0, false, 0},
		{"missing_key_state", TemporalDecisionActionFallbackResolver, "missing_key_state", "", 0, false, time.Second},
		{"event_evicted", TemporalDecisionActionFallbackResolver, "event_evicted", "", 0, false, time.Second},
	}
}

type diagnosticsBaselineEvaluateFixture struct {
	decoder                      *TemporalDecoder
	request                      temporalRequest
	state                        *temporalKeyState
	now                          time.Time
	missingRequest, missingState bool
}

func newDiagnosticsBaselineEvaluate(tb testing.TB, tc diagnosticsBaselineEvaluateCase) diagnosticsBaselineEvaluateFixture {
	tb.Helper()
	d := b31GenerationDecoder()
	d.cfg.BeamSize, d.cfg.MaxObsCandidates, d.cfg.MaxEventsPerKey = 8, 8, 32
	if tc.name == "score_defer" {
		d.cfg.MinScore = 1 << 30
	}
	if tc.name == "margin_defer" || tc.name == "forced_fallback" {
		d.cfg.MinMarginScore = 1 << 30
	}
	if tc.name == "event_evicted" {
		d.cfg.MaxEventsPerKey = 1
	}
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	for id := uint64(1); id <= 2; id++ {
		if accepted, reason := d.Observe(TemporalObservation{ID: id, Key: key, ObservedAt: at.Add(time.Duration(id-1) * 500 * time.Millisecond),
			SubjectCall: "K1ABD", Selection: testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 4500, "K1ABD", 500)}); !accepted {
			tb.Fatalf("baseline Observe(%d): %s", id, reason)
		}
	}
	// Retiring the later request through its ordinary API leaves the first
	// request owning the event key. No fabricated detached event owner is needed.
	if later := d.Evaluate(2, at.Add(time.Second), true); later.Reason == "missing_request" || later.Action == TemporalDecisionActionDefer {
		tb.Fatalf("baseline setup failed to retire the later request: %+v", later)
	}
	req, exists := d.requests[1]
	st := d.keyStates[key]
	if !exists || st == nil || len(d.requests) != 1 || len(st.events) == 0 {
		tb.Fatal("baseline setup lost the target request/event owner")
	}
	f := diagnosticsBaselineEvaluateFixture{decoder: d, request: req, state: st, now: at.Add(time.Second),
		missingRequest: tc.name == "missing_request", missingState: tc.name == "missing_key_state"}
	if tc.name == "lag_pending" {
		f.now = at.Add(500 * time.Millisecond)
	}
	return f
}

// The test/benchmark is this decoder's sole owner. Restoring map entries shares
// only prebuilt immutable event/rank data; Evaluate retires owners but does not
// mutate those events. Assignments stay inside the timed loop, as in the
// existing retirement benchmark, so timer/allocator sampling is not restarted
// on every operation. Construction and Observe setup remain outside timing.
func (f diagnosticsBaselineEvaluateFixture) restore() {
	if f.missingRequest {
		delete(f.decoder.requests, f.request.id)
	} else {
		f.decoder.requests[f.request.id] = f.request
	}
	if f.missingState {
		delete(f.decoder.keyStates, f.request.key)
	} else {
		f.decoder.keyStates[f.request.key] = f.state
	}
}

func diagnosticsBaselineEvaluateMatches(got TemporalDecision, tc diagnosticsBaselineEvaluateCase) bool {
	return got.ID == 1 && got.Action == tc.action && got.Reason == tc.reason && got.Winner == tc.winner &&
		got.CandidateCount == tc.candidates && got.CommitLatency == tc.latency
}

func TestDiagnosticsBaselineTemporalEvaluate(t *testing.T) {
	for _, tc := range diagnosticsBaselineEvaluateCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiagnosticsBaselineEvaluate(t, tc)
			firstID, lastID := f.state.events[0].id, f.state.events[len(f.state.events)-1].id
			for repeat := 0; repeat < 2; repeat++ {
				f.restore()
				got := f.decoder.Evaluate(1, f.now, tc.force)
				if !diagnosticsBaselineEvaluateMatches(got, tc) {
					t.Fatalf("baseline branch drifted: %+v", got)
				}
				_, pending := f.decoder.requests[1]
				if pending != (tc.action == TemporalDecisionActionDefer) {
					t.Fatal("baseline request retirement changed")
				}
				_, hasState := f.decoder.keyStates[f.request.key]
				if hasState != (tc.action == TemporalDecisionActionDefer || f.missingRequest) {
					t.Fatal("baseline event-key ownership changed")
				}
				if f.state.events[0].id != firstID || f.state.events[len(f.state.events)-1].id != lastID || f.decoder.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{}) {
					t.Fatal("proposal evaluation mutated event data or accepted history")
				}
			}
		})
	}
}

func BenchmarkDiagnosticsBaselineTemporalEvaluate(b *testing.B) {
	for _, tc := range diagnosticsBaselineEvaluateCases() {
		b.Run(tc.name, func(b *testing.B) {
			f := newDiagnosticsBaselineEvaluate(b, tc)
			f.restore()
			if got := f.decoder.Evaluate(1, f.now, tc.force); !diagnosticsBaselineEvaluateMatches(got, tc) {
				b.Fatalf("unqualified Evaluate control: %+v", got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f.restore()
				if got := f.decoder.Evaluate(1, f.now, tc.force); !diagnosticsBaselineEvaluateMatches(got, tc) {
					b.Fatalf("measured branch drifted: %+v", got)
				}
			}
		})
	}
}

type diagnosticsBaselineReleaseFixture struct {
	resolver   *spot.SignalResolver
	key        spot.ResolverSignalKey
	cfg        config.CallCorrectionConfig
	at         time.Time
	proposal   TemporalDecision
	winner     string
	present    bool
	generation uint64
}

func newDiagnosticsBaselineRelease(tb testing.TB, kind string) diagnosticsBaselineReleaseFixture {
	tb.Helper()
	r, key, _ := accountingResolver(tb, 1, "K1ABD", false, nil)
	tb.Cleanup(r.Stop)
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodEnabled = false
	ordinary := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABE", at)
	if !ordinary.SnapshotOK || ordinary.Snapshot.Winner != "K1ABC" || ordinary.Snapshot.Generation == 0 ||
		ordinary.Snapshot.State != spot.ResolverStateConfident || ResolverSupportForCall(ordinary.Snapshot, "K1ABD") != 1 {
		tb.Fatalf("release baseline did not establish a settled, supported alternative: %+v", ordinary)
	}
	saved := ordinary
	saved.Snapshot = saved.Snapshot.Compact()
	f := diagnosticsBaselineReleaseFixture{resolver: r, key: key, cfg: cfg, at: at, winner: "K1ABD", present: true, generation: ordinary.Snapshot.Generation,
		proposal: TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABD", Selection: saved}}
	switch kind {
	case "ordinary_action_guard":
		f.proposal.Action = TemporalDecisionActionFallbackResolver
		f.winner = "K1ABC"
	case "ordinary_missing_guard":
		f.key.Bucket += 10
		f.present = false
		f.winner = ""
		f.generation = 0
	case "witness_mismatch":
		f.proposal.Selection.generationWitnesses[2]++
		f.winner = "K1ABC"
	}
	return f
}

func diagnosticsBaselineReleaseKinds() []string {
	return []string{"supported_other_winner", "ordinary_action_guard", "ordinary_missing_guard", "witness_mismatch"}
}

func diagnosticsBaselineReleaseMatches(got ResolverPrimarySelection, f diagnosticsBaselineReleaseFixture) bool {
	if got.SnapshotOK != f.present || got.Snapshot.Winner != f.winner || got.Snapshot.Generation != f.generation {
		return false
	}
	if !f.present {
		return true
	}
	if got.Snapshot.TotalReporters != 3 || len(got.Snapshot.CandidateRanks) != 2 {
		return false
	}
	if f.winner == "K1ABD" {
		return got.Snapshot.WinnerSupport == 1 && got.Snapshot.WinnerWeightedSupportMilli == 1000 && got.Snapshot.RunnerUp == "K1ABC" &&
			got.Snapshot.RunnerSupport == 2 && got.Snapshot.State == spot.ResolverStateProbable
	}
	return got.Snapshot.WinnerSupport == 2 && got.Snapshot.State == spot.ResolverStateConfident
}

func TestDiagnosticsBaselineTemporalRelease(t *testing.T) {
	for _, kind := range diagnosticsBaselineReleaseKinds() {
		t.Run(kind, func(t *testing.T) {
			f := newDiagnosticsBaselineRelease(t, kind)
			for repeat := 0; repeat < 2; repeat++ {
				got := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at)
				if !diagnosticsBaselineReleaseMatches(got, f) {
					t.Fatalf("release control drifted: %+v", got)
				}
			}
		})
	}
}

func BenchmarkDiagnosticsBaselineTemporalRelease(b *testing.B) {
	for _, kind := range diagnosticsBaselineReleaseKinds() {
		b.Run(kind, func(b *testing.B) {
			f := newDiagnosticsBaselineRelease(b, kind)
			if got := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at); !diagnosticsBaselineReleaseMatches(got, f) {
				b.Fatalf("unqualified release control: %+v", got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at); !diagnosticsBaselineReleaseMatches(got, f) {
					b.Fatalf("measured release branch drifted: %+v", got)
				}
			}
		})
	}
}

type diagnosticsBaselineEarlySelectionFixture struct {
	resolver      *spot.SignalResolver
	key           spot.ResolverSignalKey
	cfg           config.CallCorrectionConfig
	at            time.Time
	anchor        string
	present, loss bool
	state         spot.ResolverState
	anchorMissing int
}

func newDiagnosticsBaselineEarlySelection(tb testing.TB, kind string) diagnosticsBaselineEarlySelectionFixture {
	tb.Helper()
	f := diagnosticsBaselineEarlySelectionFixture{key: spot.NewResolverSignalKey(7010, "40m", "CW", 500), cfg: accountingConfig(),
		at: time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC), anchor: "K1ABC"}
	if kind == "nil_resolver" {
		return f
	}
	if kind == "anchor_missing" {
		f.resolver, f.key, _ = accountingResolver(tb, 1, "K1ABD", false, nil)
		f.key.Bucket++
		f.anchor = ""
		f.anchorMissing = 1
		tb.Cleanup(f.resolver.Stop)
		return f
	}
	maxReporters := 8
	if kind == "loss" {
		maxReporters = 2
		f.loss = true
		f.state = spot.ResolverStateUncertain
	} else {
		f.state = spot.ResolverStateSplit
	}
	f.present = true
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 16, MaxCandidatesPerKey: 2, MaxReportersPerCand: maxReporters,
		HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6,
		MaxEditDistance: 6, DistanceModelCW: "plain"})
	f.resolver = r
	tb.Cleanup(r.Stop)
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		tb.Fatal(err)
	}
	if err := driver.SetProcessElapsed(0); err != nil {
		tb.Fatal(err)
	}
	rows := []struct {
		call, receiver string
		frequency      float64
	}{
		{"K1ABC", "N0AAA", 7010}, {"K1ABC", "N0BBB", 7010}, {"K1ABC", "N0CCC", 7010},
	}
	if kind == "split" {
		rows = append(rows, struct {
			call, receiver string
			frequency      float64
		}{"K1ABD", "N0DDD", 7010.2}, struct {
			call, receiver string
			frequency      float64
		}{"K1ABD", "N0EEE", 7010.2})
	}
	for _, row := range rows {
		if !r.Enqueue(spot.ResolverEvidence{Key: f.key, ObservedAt: f.at.Add(-time.Second), DXCall: row.call, Spotter: row.receiver,
			FrequencyKHz: row.frequency, RecencyWindow: time.Minute}) {
			tb.Fatal("early-return baseline evidence refused")
		}
	}
	if driver.Drain() != len(rows) {
		tb.Fatal("early-return baseline evidence not consumed")
	}
	driver.Sweep(f.at.Add(-time.Second + time.Millisecond))
	if kind == "loss" && r.MetricsSnapshot().CapPressureReporters != 1 {
		tb.Fatal("loss baseline did not exercise reporter-cap loss")
	}
	return f
}

func diagnosticsBaselineEarlySelectionKinds() []string {
	return []string{"loss", "split", "anchor_missing", "nil_resolver"}
}

func diagnosticsBaselineEarlySelectionMatches(got ResolverPrimarySelection, f diagnosticsBaselineEarlySelectionFixture) bool {
	return got.SnapshotOK == f.present && got.Snapshot.EvidenceLoss == f.loss && got.Snapshot.State == f.state &&
		got.Snapshot.Winner == "" && !got.UsedNeighborhood && got.NeighborhoodExcludedAnchorMissing == f.anchorMissing
}

func TestDiagnosticsBaselineSelectorEarlyReturn(t *testing.T) {
	for _, kind := range diagnosticsBaselineEarlySelectionKinds() {
		t.Run(kind, func(t *testing.T) {
			f := newDiagnosticsBaselineEarlySelection(t, kind)
			if got := SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, f.anchor, f.at); !diagnosticsBaselineEarlySelectionMatches(got, f) {
				t.Fatalf("early selector branch drifted: %+v", got)
			}
		})
	}
}

func BenchmarkDiagnosticsBaselineSelectorEarlyReturn(b *testing.B) {
	for _, kind := range diagnosticsBaselineEarlySelectionKinds() {
		b.Run(kind, func(b *testing.B) {
			f := newDiagnosticsBaselineEarlySelection(b, kind)
			if got := SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, f.anchor, f.at); !diagnosticsBaselineEarlySelectionMatches(got, f) {
				b.Fatalf("unqualified early-selector control: %+v", got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, f.anchor, f.at); !diagnosticsBaselineEarlySelectionMatches(got, f) {
					b.Fatalf("measured selector branch drifted: %+v", got)
				}
			}
		})
	}
}
