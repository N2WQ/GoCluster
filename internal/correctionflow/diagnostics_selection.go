// File role: Copy actual selector publications and rule results without reads.
// Row enumeration is over the already consumed immutable view. The first pass
// preflights labels/bytes, the second detaches values; neither evaluates rules.
package correctionflow

import (
	"strings"

	"dxcluster/spot"
)

func lenDiagnosticReadPrefix(d *ResolverSelectionDiagnostics) int {
	count := 0
	for i := range d.Reads {
		if d.Reads[i].Read {
			count++
		}
	}
	return count
}

func diagnosticRows(b *diagnosticBudget, view *spot.ResolverEvidenceView, canonical bool) []DiagnosticReceiver {
	if b.err != nil || view == nil {
		return nil
	}
	if view.Len() > diagnosticMaxIncidences {
		b.fail("diagnostic receiver population exceeds 5120")
		return nil
	}
	count := 0
	preflight := func(call, identity string, row spot.ResolverReceiverObservation) bool {
		count++
		if count > diagnosticMaxIncidences {
			b.fail("diagnostic receiver population exceeds 5120")
			return false
		}
		if !b.label(call) || !b.label(identity) || !b.label(row.Receiver) {
			return false
		}
		return b.take(512 + 6*(len(call)+len(identity)+len(row.Receiver)))
	}
	if canonical {
		view.RangeCanonical(func(identity, display string, row spot.ResolverReceiverObservation) bool {
			return preflight(display, identity, row)
		})
	} else {
		view.Range(func(call string, row spot.ResolverReceiverObservation) bool { return preflight(call, "", row) })
	}
	if b.err != nil {
		return nil
	}
	out := make([]DiagnosticReceiver, 0, count)
	copyRow := func(call, identity string, row spot.ResolverReceiverObservation) bool {
		out = append(out, cloneDiagnosticReceiver(call, identity, row))
		return true
	}
	if canonical {
		view.RangeCanonical(func(identity, display string, row spot.ResolverReceiverObservation) bool {
			return copyRow(display, identity, row)
		})
	} else {
		view.Range(func(call string, row spot.ResolverReceiverObservation) bool { return copyRow(call, "", row) })
	}
	return out
}

func cloneDiagnosticReceiver(call, identity string, row spot.ResolverReceiverObservation) DiagnosticReceiver {
	return DiagnosticReceiver{Call: strings.Clone(call), Identity: strings.Clone(identity), Receiver: strings.Clone(row.Receiver),
		ObservedAt: row.ObservedAt.UTC(), FrequencyKHz: row.FrequencyKHz, WeightMilli: row.WeightMilli, Report: row.Report,
		FrequencyConflict: row.FrequencyConflict, HasReport: row.HasReport, SNRConflict: row.SNRConflict, RawOrigin: "unavailable"}
}

func (d *ResolverSelectionDiagnostics) read(slot, sequence int, key spot.ResolverSignalKey, snapshot spot.ResolverSnapshot, present bool) {
	if d.budget.err != nil {
		return
	}
	r := &d.Reads[slot]
	r.Read, r.Sequence, r.Present = true, sequence, present
	r.RequestedKey = cloneDiagnosticKey(&d.budget, key)
	if !present {
		return
	}
	view := snapshot.ReceiverEvidence()
	r.RawEvidenceAvailable = view != nil
	r.RecencyWindow = view.RecencyWindow()
	if view.Len() > diagnosticMaxIncidences-d.RawIncidences {
		d.budget.fail("diagnostic total raw incidences exceed 5120")
		return
	}
	headerSpellings := 0
	for i := range snapshot.CandidateRanks {
		headerSpellings += max(1, len(snapshot.CandidateRanks[i].ObservedCalls))
	}
	if headerSpellings > diagnosticMaxCandidates-d.candidateSpellings {
		d.budget.fail("diagnostic total candidate spelling metadata exceeds 80")
		return
	}
	spellings := 0
	last := ""
	view.Range(func(call string, _ spot.ResolverReceiverObservation) bool {
		if spellings == 0 || call != last {
			spellings++
			last = call
		}
		return true
	})
	if spellings > diagnosticMaxCandidates-d.RawSpellings {
		d.budget.fail("diagnostic total raw spellings exceed 80")
		return
	}
	r.Snapshot = cloneDiagnosticSnapshot(&d.budget, snapshot)
	r.RawRows = diagnosticRows(&d.budget, view, false)
	d.RawSpellings += spellings
	d.candidateSpellings += headerSpellings
	d.RawIncidences += len(r.RawRows)
}

func (d *ResolverSelectionDiagnostics) admission(slot int, status string, checked, result, override bool, reason string) {
	if d == nil || d.budget.err != nil {
		return
	}
	r := &d.Reads[slot]
	r.Admission = status
	r.ComparabilityChecked = checked
	r.Comparable = result
	r.ExactOverride = override
	r.ComparabilityReason = d.budget.text(reason)
	r.Admitted = status == "admitted"
}

func (d *ResolverSelectionDiagnostics) rule(stage string, slot int, call, other string, result bool, reason string, in spot.ResolverAmbiguityInput) {
	if d == nil || d.budget.err != nil {
		return
	}
	if len(d.Rules) >= diagnosticMaxRules {
		d.budget.fail("diagnostic executed-rule bound exceeded")
		return
	}
	if !d.budget.label(call) || !d.budget.label(other) || !d.budget.label(reason) || !d.budget.take(768+6*(len(call)+len(other)+len(reason))) {
		return
	}
	d.Rules = append(d.Rules, DiagnosticRule{Stage: stage, Slot: slot, Call: strings.Clone(call), Other: strings.Clone(other), Result: result, Reason: strings.Clone(reason), Inputs: in})
}

func (d *ResolverSelectionDiagnostics) nomination(slot int, candidate spot.ResolverCandidateSupport, state spot.ResolverState, nominee, created, updated bool) {
	if d == nil || d.budget.err != nil {
		return
	}
	if len(d.Nominations) >= diagnosticMaxCandidates {
		d.budget.fail("diagnostic nomination bound exceeds 80")
		return
	}
	if !d.budget.take(256) {
		return
	}
	d.Nominations = append(d.Nominations, DiagnosticNomination{Slot: slot, Call: d.budget.text(candidate.Call), Identity: d.budget.text(candidate.VoteKey),
		SourceState: spot.ResolverState(d.budget.text(string(state))), Nominee: nominee, Created: created, BestUpdated: updated})
}

func finishReleaseGuard(d *TemporalReleaseDiagnostics, selection ResolverPrimarySelection, stop string) ResolverPrimarySelection {
	if d != nil {
		d.OrdinaryGuardStop = stop
	}
	return finishReleaseDiagnostic(d, selection, "ordinary_guard")
}

func finishSelectionDiagnostic(d *ResolverSelectionDiagnostics, selection ResolverPrimarySelection, exit string) ResolverPrimarySelection {
	if d != nil {
		d.Exit = exit
		d.Output = cloneDiagnosticSelection(&d.budget, selection)
	}
	return selection
}

func finishReleaseDiagnostic(d *TemporalReleaseDiagnostics, selection ResolverPrimarySelection, exit string) ResolverPrimarySelection {
	if d != nil {
		d.Exit = exit
		d.Output = cloneDiagnosticSelection(&d.budget, selection)
	}
	return selection
}

func finishTransformDiagnostic(d *DiagnosticTransform, selection ResolverPrimarySelection, exit string) ResolverPrimarySelection {
	if d != nil {
		d.Exit = exit
	}
	return selection
}
