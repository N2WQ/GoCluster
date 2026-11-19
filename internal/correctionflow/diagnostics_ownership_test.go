package correctionflow

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dxcluster/spot"
)

func TestDiagnosticsOwnedAliasesStringsAndRows(t *testing.T) {
	backing := strings.Repeat("x", 1<<20) + "K1ABC" + strings.Repeat("y", 1<<20)
	call := backing[1<<20 : (1<<20)+5]
	key := spot.ResolverSignalKey{Band: call, Mode: call, Bucket: 1, ToleranceHz: 500}
	aliases := []string{call}
	ranks := []spot.ResolverCandidateSupport{{Call: call, VoteKey: call, ObservedCalls: aliases, Support: 2, WeightedSupportMilli: 2000}}
	snapshot := spot.ResolverSnapshot{Key: key, State: spot.ResolverStateConfident, Winner: call, CandidateRanks: ranks}
	budget := diagnosticBudget{}
	owned := cloneDiagnosticSnapshot(&budget, snapshot)
	row := cloneDiagnosticReceiver(call, call, spot.ResolverReceiverObservation{Receiver: call})
	if budget.err != nil {
		t.Fatal(budget.err)
	}
	for _, cloned := range []string{owned.Key.Band, owned.Key.Mode, owned.Winner, owned.Candidates[0].Call, owned.Candidates[0].VoteKey, owned.Candidates[0].ObservedCalls[0], row.Call, row.Identity, row.Receiver} {
		if cloned != call || unsafe.StringData(cloned) == unsafe.StringData(call) {
			t.Fatal("captured label pins original large backing storage")
		}
	}
	aliases[0] = "mutated source alias"
	ranks[0].Call = "mutated source rank"
	if owned.Candidates[0].ObservedCalls[0] != call || owned.Candidates[0].Call != call {
		t.Fatal("captured rank or nested alias slice borrowed source storage")
	}
	owned.Candidates[0].ObservedCalls[0] = "mutated capture"
	if aliases[0] != "mutated source alias" {
		t.Fatal("capture alias mutation reached source")
	}
	r, key, _ := accountingResolver(t, 2, "K1ABD", false, nil)
	t.Cleanup(r.Stop)
	var capture ResolverSelectionDiagnostics
	at := time.Date(2026, 10, 10, 12, 0, 1, 0, time.UTC)
	cfg := accountingConfig()
	before := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at)
	_, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(r, key, cfg, "K1ABC", at, &capture)
	if err != nil {
		t.Fatal(err)
	}
	if len(capture.Reads[2].RawRows) == 0 || len(capture.CanonicalRows) == 0 {
		t.Fatal("ownership fixture lacks copied raw/canonical evidence")
	}
	if !capture.Reads[2].RawEvidenceAvailable || capture.Reads[2].RecencyWindow != time.Minute {
		t.Fatal("actual consumed publication recency window was not copied")
	}
	for _, source := range capture.Reads {
		for _, receiver := range source.RawRows {
			if receiver.RawOrigin != "unavailable" {
				t.Fatal("retained receiver was assigned invented archive provenance")
			}
		}
	}
	capture.Reads[2].RawRows[0].Receiver = "changed raw"
	capture.CanonicalRows[0].Receiver = "changed canonical"
	// Published singleton canonical groups legitimately use their Call as the
	// observed spelling and leave ObservedCalls nil. The actual two-key merge
	// constructs an explicit variant list; require and mutate that owned list.
	if len(capture.MergedCandidates) == 0 || len(capture.MergedCandidates[0].ObservedCalls) == 0 {
		t.Fatal("merged ownership fixture did not produce explicit observed aliases")
	}
	capture.MergedCandidates[0].ObservedCalls[0] = "changed alias"
	if after := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", at); !reflect.DeepEqual(before, after) {
		t.Fatal("capture mutation changed published selection")
	}
	first := capture
	frozen, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	_, err = SelectResolverPrimarySnapshotForCallWithDiagnostics(nil, key, cfg, "K1ABC", at, &capture)
	if err != nil || capture.Exit != "nil_resolver" || len(capture.CanonicalRows) != 0 || capture.Reads[2].Read {
		t.Fatal("reset retained previous selector data")
	}
	retained, err := json.Marshal(first)
	if err != nil || string(retained) != string(frozen) {
		t.Fatal("destination reset mutated a retained previous trace")
	}
}

func TestDiagnosticsDestinationResetAfterErrors(t *testing.T) {
	f := newDiagnosticsBaselineRelease(t, "supported_other_winner")
	var selector ResolverSelectionDiagnostics
	before := SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, f.cfg, "K1ABE", f.at)
	selected, err := SelectResolverPrimarySnapshotForCallWithDiagnostics(f.resolver, f.key, f.cfg, strings.Repeat("x", 1025), f.at, &selector)
	if err == nil || !reflect.DeepEqual(before, selected) {
		t.Fatal("label capture failure changed ordinary selection")
	}
	_, err = SelectResolverPrimarySnapshotForCallWithDiagnostics(f.resolver, f.key, f.cfg, "K1ABE", f.at, &selector)
	if err != nil {
		t.Fatal("selector error was not reset:", err)
	}
	var release TemporalReleaseDiagnostics
	_, err = SelectTemporalReleaseSnapshotWithDiagnostics(f.resolver, f.key, f.cfg, "K1ABE", f.proposal, f.at, &release)
	if err != nil {
		t.Fatal(err)
	}
	retained := release
	data, err := json.Marshal(retained)
	if err != nil {
		t.Fatal(err)
	}
	_, err = SelectTemporalReleaseSnapshotWithDiagnostics(nil, f.key, f.cfg, "K1ABE", f.proposal, f.at, &release)
	if err != nil || release.Exit != "ordinary_guard" || release.GenerationResultChecked || release.Transform.Applied {
		t.Fatal("release reset retained actual checks")
	}
	after, err := json.Marshal(retained)
	if err != nil || string(data) != string(after) {
		t.Fatal("release reset mutated a retained trace")
	}
	tc := diagnosticsBaselineEvaluateCases()[0]
	eval := newDiagnosticsBaselineEvaluate(t, tc)
	eval.restore()
	var temporal TemporalEvaluateDiagnostics
	_, err = eval.decoder.EvaluateWithDiagnostics(1, eval.now, false, &temporal)
	if err != nil {
		t.Fatal(err)
	}
	old := temporal
	data, err = json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eval.decoder.EvaluateWithDiagnostics(999, eval.now, false, &temporal)
	if err != nil || temporal.Exit != "missing_request" || len(temporal.Window) != 0 || len(temporal.StartScores) != 0 || temporal.ThresholdChecked {
		t.Fatal("temporal reset retained previous decoded window")
	}
	after, err = json.Marshal(old)
	if err != nil || string(data) != string(after) {
		t.Fatal("temporal destination reused old owned storage")
	}
}

func TestDiagnosticsStructuralAndLabelFailuresPreserveDecision(t *testing.T) {
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for _, kind := range []string{"too_many_ranks", "too_many_aliases", "alias_row_bytes", "label_bytes"} {
		t.Run(kind, func(t *testing.T) {
			ranks := []spot.ResolverCandidateSupport{{Call: "K1ABC", ObservedCalls: []string{"K1ABC"}, Support: 2, WeightedSupportMilli: 2000}}
			switch kind {
			case "too_many_ranks":
				ranks = make([]spot.ResolverCandidateSupport, 81)
			case "too_many_aliases":
				ranks[0].ObservedCalls = make([]string, 81)
			case "alias_row_bytes":
				ranks[0].ObservedCalls = make([]string, 20)
				for i := range ranks[0].ObservedCalls {
					ranks[0].ObservedCalls[i] = strings.Repeat("x", 1024)
				}
			case "label_bytes":
				ranks[0].ObservedCalls[0] = strings.Repeat("x", 1025)
			}
			snapshot := spot.ResolverSnapshot{Key: key, Winner: "K1ABC", State: spot.ResolverStateConfident, CandidateRanks: ranks}
			lookup := func(spot.ResolverSignalKey) (spot.ResolverSnapshot, bool) { return snapshot, true }
			cfg := accountingConfig()
			cfg.ResolverNeighborhoodEnabled = false
			before := selectResolverPrimarySnapshotFromLookup(key, cfg, "K1ABC", lookup)
			var capture ResolverSelectionDiagnostics
			capture.begin(key, "K1ABC", time.Time{})
			after := selectResolverPrimarySnapshotFromLookupDiagnostics(key, cfg, "K1ABC", lookup, &capture)
			if capture.budget.err == nil || !reflect.DeepEqual(before, after) || capture.budget.bytes > diagnosticMaxBytes {
				t.Fatal("invalid capture payload changed normal selector or exceeded its budget")
			}
		})
	}
	for _, kind := range []string{"max_obs", "max_events"} {
		t.Run(kind, func(t *testing.T) {
			tc := diagnosticsBaselineEvaluateCases()[0]
			f := newDiagnosticsBaselineEvaluate(t, tc)
			if kind == "max_obs" {
				f.decoder.cfg.MaxObsCandidates = 9
			} else {
				f.decoder.cfg.MaxEventsPerKey = 33
			}
			f.restore()
			before := f.decoder.Evaluate(1, f.now, false)
			wantRequests, wantKeys := len(f.decoder.requests), len(f.decoder.keyStates)
			f.restore()
			var capture TemporalEvaluateDiagnostics
			after, err := f.decoder.EvaluateWithDiagnostics(1, f.now, false, &capture)
			if err == nil || !reflect.DeepEqual(before, after) || len(f.decoder.requests) != wantRequests || len(f.decoder.keyStates) != wantKeys {
				t.Fatal("unsupported capture bound changed normal decoder/lifecycle")
			}
		})
	}
}

func TestDiagnosticsWholeRecordBudgetAndByteLabels(t *testing.T) {
	budget := diagnosticBudget{}
	if !budget.label(strings.Repeat("é", 512)) || budget.label(strings.Repeat("é", 513)) {
		t.Fatal("label bound used characters instead of UTF-8 bytes")
	}
	budget = diagnosticBudget{}
	if !budget.take(diagnosticMaxBytes) || budget.take(1) || budget.bytes != diagnosticMaxBytes {
		t.Fatal("encoded record quota overflowed")
	}
	d := b31GenerationDecoder()
	d.cfg.BeamSize, d.cfg.MaxObsCandidates, d.cfg.MaxEventsPerKey, d.cfg.MaxPending = 8, 8, 32, 64
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	ranks := make([]spot.ResolverCandidateSupport, 80)
	for i := range ranks {
		ranks[i] = spot.ResolverCandidateSupport{Call: "K1ABC", ObservedCalls: []string{strings.Repeat("x", 1024)}, Support: 1, WeightedSupportMilli: 1000}
	}
	selection := ResolverPrimarySelection{SnapshotOK: true, Snapshot: spot.ResolverSnapshot{Key: key, Winner: "K1ABC", State: spot.ResolverStateConfident,
		SupportBasis: spot.ResolverSupportBasisWeighted, TotalReporters: 1, TotalWeightedSupportMilli: 1000, CandidateRanks: ranks}}
	for id := uint64(1); id <= 32; id++ {
		if accepted, reason := d.Observe(TemporalObservation{ID: id, Key: key, ObservedAt: at.Add(time.Duration(id) * time.Millisecond), SubjectCall: "K1ABD", Selection: selection}); !accepted {
			t.Fatal(reason)
		}
	}
	req, st := d.requests[1], d.keyStates[key]
	// ID 1 is observed at at+1ms, so at+1s is still inside its lag. Select an
	// explicit clock after lag and before max-wait to exercise actual decoding.
	evaluateAt := at.Add(2 * time.Second)
	before := d.Evaluate(1, evaluateAt, false)
	if before.CandidateCount == 0 {
		t.Fatal("record-budget control did not reach decoder scoring")
	}
	requests, keys := len(d.requests), len(d.keyStates)
	d.requests[1], d.keyStates[key] = req, st
	var capture TemporalEvaluateDiagnostics
	after, err := d.EvaluateWithDiagnostics(1, evaluateAt, false, &capture)
	if !capture.ThresholdChecked || len(capture.Window) != 32 {
		t.Fatal("record-budget trace did not reach its actual window/scoring path")
	}
	if err == nil || !strings.Contains(err.Error(), "encoded size") || !reflect.DeepEqual(before, after) || len(d.requests) != requests || len(d.keyStates) != keys || capture.budget.bytes > diagnosticMaxBytes {
		t.Fatalf("whole-record failure did not preserve ordinary execution: %v", err)
	}
}
