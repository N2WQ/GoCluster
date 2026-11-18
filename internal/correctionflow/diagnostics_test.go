package correctionflow

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

func diagnosticTestJSON(t *testing.T, value any, bound int) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > bound || bound > diagnosticMaxBytes {
		t.Fatalf("encoded preflight bound=%d actual=%d", bound, len(data))
	}
	return data
}

func TestDiagnosticsEvaluateBaselineParity(t *testing.T) {
	for _, tc := range diagnosticsBaselineEvaluateCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := newDiagnosticsBaselineEvaluate(t, tc)
			f.restore()
			before := f.decoder.Evaluate(1, f.now, tc.force)
			beforeRequests, beforeKeys := len(f.decoder.requests), len(f.decoder.keyStates)
			f.restore()
			var capture TemporalEvaluateDiagnostics
			after, err := f.decoder.EvaluateWithDiagnostics(1, f.now, tc.force, &capture)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) || len(f.decoder.requests) != beforeRequests || len(f.decoder.keyStates) != beforeKeys {
				t.Fatalf("tracing changed decision/lifecycle: before=%+v after=%+v", before, after)
			}
			if !diagnosticsBaselineEvaluateMatches(after, tc) || capture.Exit != tc.reason || capture.Action != after.Action || capture.CandidateCount != after.CandidateCount {
				t.Fatalf("capture lost actual result: %+v", capture)
			}
			decoded := tc.candidates > 0
			if (len(capture.Window) > 0) != decoded || capture.ThresholdChecked != decoded || (len(capture.StartScores) > 0) != decoded {
				t.Fatal("unexecuted window/threshold was invented or actual scoring was omitted")
			}
			if decoded && (len(capture.Window) != 2 || capture.Window[0].ID != 1 || capture.Window[1].ID != 2 || len(capture.Window[0].Candidates) != 2) {
				t.Fatal("actual event IDs/candidate sets were lost")
			}
			diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
			f.restore()
			without, err := f.decoder.EvaluateWithDiagnostics(1, f.now, tc.force, nil)
			if err != nil || !reflect.DeepEqual(before, without) {
				t.Fatal("nil diagnostic API changed ordinary Evaluate")
			}
		})
	}
}

func TestDiagnosticsReleaseBaselineParity(t *testing.T) {
	for _, kind := range diagnosticsBaselineReleaseKinds() {
		t.Run(kind, func(t *testing.T) {
			f := newDiagnosticsBaselineRelease(t, kind)
			before := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at)
			var capture TemporalReleaseDiagnostics
			after, err := SelectTemporalReleaseSnapshotWithDiagnostics(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at, &capture)
			if err != nil || !reflect.DeepEqual(before, after) || !diagnosticsBaselineReleaseMatches(after, f) {
				t.Fatalf("release parity failed: %v before=%+v after=%+v", err, before, after)
			}
			switch kind {
			case "supported_other_winner":
				if capture.Exit != "transformed" || !capture.Transform.Applied || !capture.OpaqueTieChecked || capture.OpaqueTie || capture.OpaqueTieInternals != "private_internals_not_exposed" {
					t.Fatalf("actual transform/opaque check missing: %+v", capture)
				}
			case "witness_mismatch":
				if capture.Exit != "generation_refused" || !capture.GenerationResultChecked || capture.GenerationCurrent || capture.GenerationChecks[2].Status != "read" || capture.GenerationChecks[3].Status != "not_reached" {
					t.Fatal("normal witness refusal became an error or invented later reads")
				}
			default:
				if capture.Exit != "ordinary_guard" || capture.GenerationResultChecked || capture.OpaqueTieChecked {
					t.Fatal("ordinary early return executed later checks")
				}
			}
			diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
			without, err := SelectTemporalReleaseSnapshotWithDiagnostics(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at, nil)
			if err != nil || !reflect.DeepEqual(before, without) {
				t.Fatal("nil release diagnostics changed selection")
			}
		})
	}
}

func TestDiagnosticsSelectorEarlyParity(t *testing.T) {
	exits := map[string]string{"loss": "evidence_loss", "split": "exact_split", "anchor_missing": "anchor_missing", "nil_resolver": "nil_resolver"}
	for _, kind := range diagnosticsBaselineEarlySelectionKinds() {
		t.Run(kind, func(t *testing.T) {
			f := newDiagnosticsBaselineEarlySelection(t, kind)
			before := SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, f.anchor, f.at)
			var capture ResolverSelectionDiagnostics
			after, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(f.resolver, f.key, f.cfg, f.anchor, f.at, &capture)
			if err != nil || !reflect.DeepEqual(before, after) || capture.Exit != exits[kind] {
				t.Fatalf("selector early parity: err=%v exit=%s", err, capture.Exit)
			}
			count := 0
			for i := range capture.Reads {
				if capture.Reads[i].Read {
					count++
				}
				if capture.Reads[i].ComparabilityChecked {
					t.Fatal("early return invented comparability")
				}
			}
			if kind == "nil_resolver" && count != 0 || kind != "nil_resolver" && count != 3 {
				t.Fatalf("actual enabled lookup population changed: %d", count)
			}
			if capture.Merged || len(capture.CanonicalRows) != 0 {
				t.Fatal("early return synthesized a union for explanation")
			}
			diagnosticTestJSON(t, capture, capture.EncodedByteUpperBound())
		})
	}
}

func TestDiagnosticsCaptureConsumedReadsOnce(t *testing.T) {
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	cfg := accountingConfig()
	cfg.ResolverNeighborhoodBucketRadius = 2
	for _, kind := range []string{"exact_only", "loss", "split"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			lookup := func(k spot.ResolverSignalKey) (spot.ResolverSnapshot, bool) {
				calls++
				s := spot.ResolverSnapshot{Key: k, Generation: uint64(calls), Winner: "K1ABC", State: spot.ResolverStateConfident,
					CandidateRanks: []spot.ResolverCandidateSupport{{Call: "K1ABC", ObservedCalls: []string{"K1ABC"}, Support: 2, WeightedSupportMilli: 2000}}}
				if k == key && kind == "loss" {
					s.EvidenceLoss = true
				}
				if k == key && kind == "split" {
					s.State = spot.ResolverStateSplit
				}
				return s, true
			}
			var capture ResolverSelectionDiagnostics
			capture.begin(key, "K1ABC", time.Time{})
			selected := selectResolverPrimarySnapshotFromLookupDiagnostics(key, cfg, "K1ABC", lookup, &capture)
			if capture.budget.err != nil {
				t.Fatal(capture.budget.err)
			}
			if calls != 5 || selected.Snapshot.Generation != 1 {
				t.Fatalf("trace performed second lookup or substituted a later snapshot: calls=%d generation=%d", calls, selected.Snapshot.Generation)
			}
			for slot, want := range [5]uint64{2, 3, 1, 4, 5} {
				if capture.Reads[slot].Snapshot.Generation != want {
					t.Fatal("read generation vector collapsed")
				}
				if capture.Reads[slot].RawEvidenceAvailable || capture.Reads[slot].RecencyWindow != 0 {
					t.Fatal("compact/missing view was assigned an invented recency window")
				}
			}
			if capture.Output.GenerationWitnesses != ([5]uint64{0, 0, 1, 0, 0}) {
				t.Fatal("read slots were confused with contributor witnesses")
			}
			if len(capture.Rules) != 0 || capture.Merged {
				t.Fatal("trace re-executed an unvisited rule/merge")
			}
		})
	}
}

func TestDiagnosticsDistinctConcurrentCaptures(t *testing.T) {
	r, key, _ := accountingResolver(t, 2, "K1ABD", false, nil)
	t.Cleanup(r.Stop)
	cfg := accountingConfig()
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	before := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at)
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for i := 0; i < 8; i++ {
				if worker%2 != 0 {
					if selected := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at); !reflect.DeepEqual(before, selected) {
						t.Error("ordinary concurrent selector changed")
					}
					continue
				}
				var capture ResolverSelectionDiagnostics
				selected, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(r, key, cfg, "K1ABC", at, &capture)
				if err != nil || !reflect.DeepEqual(before, selected) {
					t.Error("concurrent independent capture changed selection")
				}
				if len(capture.CanonicalRows) > 0 {
					capture.CanonicalRows[0].Receiver = "local mutation"
				}
			}
		}(worker)
	}
	wait.Wait()
	if after := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at); !reflect.DeepEqual(before, after) {
		t.Fatal("capture mutation changed publication")
	}
	decoder := b31GenerationDecoder()
	decoder.cfg.MaxPending = 16
	decoder.cfg.MaxObsCandidates, decoder.cfg.MaxEventsPerKey, decoder.cfg.BeamSize = 8, 32, 8
	decoder.cfg.MinScore = 1 << 30
	for id := uint64(1); id <= 8; id++ {
		if accepted, reason := decoder.Observe(TemporalObservation{ID: id, Key: key, ObservedAt: at, SubjectCall: "K1ABD", Selection: before}); !accepted {
			t.Fatal(reason)
		}
	}
	for id := uint64(1); id <= 8; id++ {
		wait.Add(1)
		go func(id uint64) {
			defer wait.Done()
			var capture TemporalEvaluateDiagnostics
			decision, err := decoder.EvaluateWithDiagnostics(id, at, false, &capture)
			if err != nil || decision.Action != TemporalDecisionActionDefer || decision.Reason != "lag_pending" {
				t.Error("concurrent temporal capture altered lag decision")
			}
		}(id)
	}
	wait.Wait()
	if decoder.PendingCount() != 8 {
		t.Fatal("lag diagnostics retired requests")
	}
	var controls [8]TemporalDecision
	for id := uint64(1); id <= 8; id++ {
		controls[id-1] = decoder.Evaluate(id, at.Add(time.Second), false)
		if controls[id-1].Action != TemporalDecisionActionDefer || controls[id-1].Reason != "low_score" {
			t.Fatal("decoded concurrency control did not preserve pending requests")
		}
	}
	for id := uint64(1); id <= 8; id++ {
		wait.Add(1)
		go func(id uint64) {
			defer wait.Done()
			if id%2 != 0 {
				if got := decoder.Evaluate(id, at.Add(time.Second), false); !reflect.DeepEqual(got, controls[id-1]) {
					t.Error("ordinary decoded concurrency changed")
				}
				return
			}
			var capture TemporalEvaluateDiagnostics
			got, err := decoder.EvaluateWithDiagnostics(id, at.Add(time.Second), false, &capture)
			if err != nil || !reflect.DeepEqual(got, controls[id-1]) || len(capture.Window) != 9-int(id) || len(capture.Window[0].Candidates) == 0 {
				t.Error("concurrent capture lost actual decoded window/candidates")
				return
			}
			capture.Window[0].Candidates[0] = "local mutation"
			if len(capture.Window[0].Selection.Snapshot.Candidates) > 0 && len(capture.Window[0].Selection.Snapshot.Candidates[0].ObservedCalls) > 0 {
				capture.Window[0].Selection.Snapshot.Candidates[0].ObservedCalls[0] = "local alias mutation"
			}
		}(id)
	}
	wait.Wait()
	if decoder.PendingCount() != 8 || !reflect.DeepEqual(decoder.Evaluate(1, at.Add(time.Second), false), controls[0]) {
		t.Fatal("decoded capture mutated retained event ownership/data")
	}
}
