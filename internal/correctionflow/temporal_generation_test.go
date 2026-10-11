// File role: Checks temporal proposal generation witnesses across every decoded
// window without changing scoring, confirmed history, or retained ownership.
package correctionflow

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func b31GenerationDecoder() *TemporalDecoder {
	return NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 3,
		BeamSize: 4, MaxObsCandidates: 4, MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 4,
		MinScore: 0, MinMarginScore: 0, OverflowAction: "fallback_resolver",
	}})
}

func b31ObserveGeneration(t *testing.T, d *TemporalDecoder, id uint64, key spot.ResolverSignalKey, at time.Time, selection ResolverPrimarySelection) {
	t.Helper()
	if ok, reason := d.Observe(TemporalObservation{ID: id, Key: key, ObservedAt: at, SubjectCall: "K1ABE", Selection: selection}); !ok {
		t.Fatalf("temporal fixture id=%d rejected: %s", id, reason)
	}
}

func TestB31TemporalGenerationWitnessUnion(t *testing.T) {
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		first    [5]uint64
		second   [5]uint64
		third    [5]uint64
		want     [5]uint64
		conflict bool
	}{
		{"later_windows", [5]uint64{0, 0, 11, 0, 0}, [5]uint64{0, 22, 11, 0, 0}, [5]uint64{33, 0, 0, 44, 55}, [5]uint64{33, 22, 11, 44, 55}, false},
		{"zero_preserves", [5]uint64{0, 0, 11, 22, 0}, [5]uint64{0, 0, 11, 0, 0}, [5]uint64{}, [5]uint64{0, 0, 11, 22, 0}, false},
		{"same_id", [5]uint64{0, 0, 11, 22, 0}, [5]uint64{0, 0, 11, 22, 0}, [5]uint64{}, [5]uint64{0, 0, 11, 22, 0}, false},
		{"later_conflict", [5]uint64{0, 0, 11, 0, 0}, [5]uint64{0, 0, 11, 22, 0}, [5]uint64{0, 0, 11, 33, 0}, [5]uint64{0, 0, 11, 22, 0}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := b31GenerationDecoder()
			for i, witnesses := range [][5]uint64{tc.first, tc.second, tc.third} {
				selection := testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
				selection.generationWitnesses = witnesses
				b31ObserveGeneration(t, d, uint64(i+1), key, at.Add(time.Duration(i)*time.Second), selection)
				selection.generationWitnesses = [5]uint64{99, 99, 99, 99, 99}
			}
			decision := d.Evaluate(1, at.Add(3*time.Second), false)
			if decision.Action != TemporalDecisionActionApply || decision.Winner != "K1ABC" {
				t.Fatalf("fixture did not produce its ordinary proposal: %+v", decision)
			}
			if decision.Selection.generationWitnesses != tc.want || decision.Selection.generationConflict != tc.conflict {
				t.Fatalf("witnesses/conflict=%v/%v, want %v/%v", decision.Selection.generationWitnesses, decision.Selection.generationConflict, tc.want, tc.conflict)
			}
			if d.ConfirmedHistoryMetrics() != (TemporalConfirmedHistoryMetrics{}) {
				t.Fatal("witness union invented accepted temporal history")
			}
		})
	}
}

func TestB31TemporalGenerationMissingFirstAnchor(t *testing.T) {
	f := newB31GenerationFixture(t, 2)
	f.add(t, 0, "K1ABC", "N0AAA", f.at)
	f.driver.Sweep(f.at.Add(time.Millisecond))
	actual := f.selection(f.at.Add(time.Second))
	if !actual.SnapshotOK || actual.generationWitnesses[2] == 0 {
		t.Fatalf("actual later selection has no generation authority: %+v", actual)
	}
	d := b31GenerationDecoder()
	b31ObserveGeneration(t, d, 1, f.key, f.at, ResolverPrimarySelection{})
	b31ObserveGeneration(t, d, 2, f.key, f.at.Add(time.Second), actual)
	decision := d.Evaluate(1, f.at.Add(2*time.Second), false)
	if decision.Selection.Snapshot.Key != f.key || decision.Selection.SnapshotOK ||
		decision.Selection.generationWitnesses[2] != actual.generationWitnesses[2] || decision.Selection.generationConflict {
		t.Fatalf("missing first anchor lost later actual authority or invented a population: %+v", decision.Selection)
	}
}

func TestB31TemporalGenerationRejectsMisalignedAndConflictingSources(t *testing.T) {
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		source   spot.ResolverSignalKey
		conflict bool
	}{
		{"missing_anchor", spot.ResolverSignalKey{}, false},
		{"foreign_anchor", spot.NewResolverSignalKey(14020.5, "20m", "CW", 500), false},
		{"existing_conflict", key, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := b31GenerationDecoder()
			first := testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
			first.generationWitnesses[2] = 11
			second := testTemporalSelection(tc.source, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
			second.generationWitnesses[3] = 22
			second.generationConflict = tc.conflict
			b31ObserveGeneration(t, d, 1, key, at, first)
			b31ObserveGeneration(t, d, 2, key, at.Add(time.Second), second)
			decision := d.Evaluate(1, at.Add(2*time.Second), false)
			if !decision.Selection.generationConflict || decision.Selection.generationWitnesses[2] != 11 {
				t.Fatalf("invalid source anchor/conflict acquired temporal authority: %+v", decision.Selection)
			}
		})
	}
}

func TestB31TemporalForeignFirstAnchorCannotOverrideFreshOrdinary(t *testing.T) {
	f := newB31GenerationFixture(t, 3)
	for _, row := range []struct{ call, receiver string }{
		{"K1ABD", "N0AAA"}, {"K1ABD", "N0BBB"}, {"K1ABC", "N0CCC"},
	} {
		f.add(t, 0, row.call, row.receiver, f.at)
	}
	f.driver.Sweep(f.at.Add(time.Millisecond))
	fresh := f.selection(f.at.Add(time.Second))
	if !fresh.SnapshotOK || fresh.Snapshot.EvidenceLoss || fresh.Snapshot.Winner != "K1ABD" ||
		fresh.generationWitnesses[2] == 0 || ResolverSupportForCall(fresh.Snapshot, "K1ABC") != 1 {
		t.Fatalf("ordinary release population was not independently established: %+v", fresh)
	}
	// Exercise an unreachable first-anchor mismatch. A genuine lifecycle ID
	// cannot authorize that foreign snapshot key for this request's release.
	foreign := spot.NewResolverSignalKey(14020.5, "20m", "CW", 500)
	first := testTemporalSelection(foreign, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
	first.generationWitnesses[2] = fresh.generationWitnesses[2]
	d := b31GenerationDecoder()
	b31ObserveGeneration(t, d, 1, f.key, f.at, first)
	proposal := d.Evaluate(1, f.at.Add(time.Second), false)
	if proposal.Action != TemporalDecisionActionApply || proposal.Winner != "K1ABC" ||
		proposal.Selection.Snapshot.Key != foreign || proposal.Selection.generationWitnesses[2] != fresh.generationWitnesses[2] {
		t.Fatalf("foreign first-anchor proposal fixture was not established: %+v", proposal)
	}
	released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(time.Second))
	if !released.SnapshotOK || released.Snapshot.Winner != "K1ABD" || released.Snapshot.Generation != fresh.Snapshot.Generation {
		t.Fatalf("foreign first anchor overrode the fresh ordinary winner: %+v", released)
	}
	proposal.Selection = fresh
	if allowed := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(time.Second)); allowed.Snapshot.Winner != "K1ABC" {
		t.Fatalf("valid same-generation anchor could not authorize a supported proposal: %+v", allowed)
	}
}

func TestB31TemporalGenerationExcludesOutsideDecodedWindow(t *testing.T) {
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	d := b31GenerationDecoder()
	first := testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
	first.generationWitnesses[2] = 11
	later := first
	later.generationWitnesses[2] = 22
	b31ObserveGeneration(t, d, 1, key, at, first)
	b31ObserveGeneration(t, d, 2, key, at.Add(4*time.Second), later)
	decision := d.Evaluate(1, at.Add(5*time.Second), true)
	if decision.Selection.generationWitnesses != ([5]uint64{0, 0, 11, 0, 0}) || decision.Selection.generationConflict {
		t.Fatalf("outside-window generation contaminated this proposal: %+v", decision.Selection)
	}
}

func TestB31TemporalSelectionPreservesOwnLossProtection(t *testing.T) {
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	base := testTemporalSelection(key, spot.ResolverStateConfident, "K1ABC", 3000, "K1ABD", 1000)
	base.Snapshot.EvidenceLoss = true
	base.Snapshot.Generation = 11
	base.generationWitnesses[2] = 11
	selected := selectionForTemporalWinner(base, "K1ABD")
	if !selected.Snapshot.EvidenceLoss || selected.Snapshot.Generation != 11 || selected.Snapshot.State != spot.ResolverStateUncertain ||
		selected.Snapshot.Winner != "K1ABC" || selected.generationWitnesses != base.generationWitnesses {
		t.Fatalf("temporal relabeling promoted a protected compact snapshot: %+v", selected)
	}
}

func TestB31TemporalLaterWindowRetiredNeighborCannotOverrideFreshOrdinary(t *testing.T) {
	f := newB31GenerationFixture(t, 3)
	f.cfg.ResolverNeighborhoodEnabled = true
	f.cfg.ResolverNeighborhoodBucketRadius = 1
	f.cfg.ResolverNeighborhoodMaxDistance = 1
	for _, row := range []struct{ call, receiver string }{
		{"K1ABC", "N0AAA"}, {"K1ABC", "N0BBB"}, {"K1ABD", "N0CCC"},
	} {
		f.add(t, 0, row.call, row.receiver, f.at)
	}
	f.driver.Sweep(f.at.Add(time.Millisecond))
	first := f.selection(f.at.Add(time.Millisecond))
	if !first.SnapshotOK || first.Snapshot.Winner != "K1ABC" || first.Snapshot.EvidenceLoss || first.generationWitnesses[3] != 0 {
		t.Fatalf("first-window ordinary fixture was not established: %+v", first)
	}
	d := b31GenerationDecoder()
	b31ObserveGeneration(t, d, 1, f.key, f.at, first)
	for _, receiver := range []string{"N0DDD", "N0EEE", "N0FFF", "N0GGG"} {
		f.add(t, 1, "W9XYZ", receiver, f.at.Add(time.Second))
	}
	f.driver.Sweep(f.at.Add(time.Second + time.Millisecond))
	second := f.selection(f.at.Add(time.Second + time.Millisecond))
	if !second.SnapshotOK || !second.Snapshot.EvidenceLoss || second.generationWitnesses[3] == 0 {
		t.Fatalf("later decoded window did not admit actual incomparable-neighbor cap loss: %+v", second)
	}
	b31ObserveGeneration(t, d, 2, f.key, f.at.Add(time.Second), second)
	f.processElapsed(t, 10*time.Minute)
	f.add(t, 0, "K1ABD", "N0HHH", f.at.Add(2*time.Second))
	f.add(t, 0, "K1ABD", "N0III", f.at.Add(2*time.Second+time.Nanosecond))
	// No sweep after retirement: the selector must logically exclude the old
	// neighbor, while admitted exact evidence independently favors ABD 3:2.
	latest := f.selection(f.at.Add(2*time.Second + time.Millisecond))
	if !latest.SnapshotOK || latest.Snapshot.EvidenceLoss || latest.Snapshot.Winner != "K1ABD" ||
		latest.Snapshot.TotalReporters != 5 || latest.generationWitnesses[3] != 0 || ResolverSupportForCall(latest.Snapshot, "K1ABC") != 2 {
		t.Fatalf("latest ordinary fallback fixture was not independently qualified: %+v", latest)
	}
	b31ObserveGeneration(t, d, 3, f.key, f.at.Add(2*time.Second), latest)
	proposal := d.Evaluate(1, f.at.Add(3*time.Second), false)
	if proposal.Action != TemporalDecisionActionApply || proposal.Winner != "K1ABC" ||
		proposal.Selection.generationWitnesses[3] != second.generationWitnesses[3] || proposal.Selection.generationConflict {
		t.Fatalf("decoder lost its later-window generation influence: %+v", proposal)
	}
	released := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(3*time.Second))
	if released.Snapshot.Winner != "K1ABD" || released.Snapshot.EvidenceLoss || released.Snapshot.TotalReporters != 5 {
		t.Fatalf("retired later-window neighbor authorized a stale override: %+v", released)
	}
	proposal.Selection = latest
	if freshProposal := SelectTemporalReleaseSnapshot(f.resolver, f.key, f.cfg, "K1ABE", proposal, f.at.Add(3*time.Second)); freshProposal.Snapshot.Winner != "K1ABC" {
		t.Fatalf("same-generation supported temporal override was universally disabled: %+v", freshProposal)
	}
}
