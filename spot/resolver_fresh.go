// File role: Projects immutable receiver publications at a release clock.
// Reads never advance resolver hysteresis or resurrect superseded observations.
package spot

import (
	"sort"
	"time"
)

// LookupEligible filters the latest publication to [now-window, now]. The
// publication is the only evidence source: this is not an ingest barrier. A
// compact snapshot cannot authorize release because it lacks timestamped rows.
func (r *SignalResolver) LookupEligible(key ResolverSignalKey, now time.Time) (ResolverSnapshot, bool) {
	snap, ok := r.Lookup(key)
	if !ok || snap.receiverEvidence == nil || snap.receiverEvidence.window <= 0 {
		return ResolverSnapshot{}, false
	}
	view := snap.receiverEvidence
	cutoff := now.Add(-view.window)
	eligible := func(row ResolverReceiverObservation) bool {
		return !row.ObservedAt.Before(cutoff) && !row.ObservedAt.After(now)
	}
	count := 0
	for _, candidate := range view.candidates {
		for _, row := range candidate.receivers {
			if eligible(row) {
				count++
			}
		}
	}
	if count == view.count {
		return snap, true
	}
	projected := ResolverSnapshot{Key: snap.Key, EvaluatedAt: snap.EvaluatedAt, State: ResolverStateUncertain}
	if count == 0 {
		return projected, true
	}
	fresh := &ResolverEvidenceView{window: view.window, count: count,
		candidates: make([]resolverEvidenceCandidate, 0, len(view.candidates))}
	tallies := make(map[string]ResolverCandidateSupport, len(view.candidates))
	receivers := make(map[string]int, count)
	for _, candidate := range view.candidates {
		rows := make([]ResolverReceiverObservation, 0, len(candidate.receivers))
		tally := ResolverCandidateSupport{Call: candidate.call}
		for _, row := range candidate.receivers {
			if !eligible(row) {
				continue
			}
			rows = append(rows, row)
			tally.Support++
			tally.WeightedSupportMilli += row.WeightMilli
			if previous, exists := receivers[row.Receiver]; !exists || row.WeightMilli > previous {
				receivers[row.Receiver] = row.WeightMilli
			}
		}
		if len(rows) > 0 {
			fresh.candidates = append(fresh.candidates, resolverEvidenceCandidate{call: candidate.call, receivers: rows})
			tallies[candidate.call] = tally
		}
	}
	projected.receiverEvidence = fresh
	projected.TotalReporters = len(receivers)
	for _, weight := range receivers {
		projected.TotalWeightedSupportMilli += weight
	}
	// Preserve owner ranking (including confusion tie provenance) for equal
	// filtered tallies. Never sort a published slice or use owner workspaces.
	projected.CandidateRanks = make([]ResolverCandidateSupport, 0, len(tallies))
	for _, previous := range snap.CandidateRanks {
		if tally, exists := tallies[previous.Call]; exists {
			projected.CandidateRanks = append(projected.CandidateRanks, tally)
		}
	}
	sort.SliceStable(projected.CandidateRanks, func(i, j int) bool {
		left, right := projected.CandidateRanks[i], projected.CandidateRanks[j]
		if left.WeightedSupportMilli != right.WeightedSupportMilli {
			return left.WeightedSupportMilli > right.WeightedSupportMilli
		}
		return left.Support > right.Support
	})
	if len(projected.CandidateRanks) == 0 {
		return projected, true
	}
	top := projected.CandidateRanks[0]
	projected.WinnerSupport, projected.WinnerWeightedSupportMilli = top.Support, top.WeightedSupportMilli
	projected.Margin = top.Support
	split := resolverEvidenceHasFrequencyConflict(fresh)
	if len(projected.CandidateRanks) > 1 {
		runner := projected.CandidateRanks[1]
		projected.RunnerUp, projected.RunnerSupport, projected.RunnerWeightedSupportMilli = runner.Call, runner.Support, runner.WeightedSupportMilli
		projected.Margin -= runner.Support
		left, right := normalizeCorrectionCallIdentity(top.Call), normalizeCorrectionCallIdentity(runner.Call)
		_, related := detectCorrectionFamilyByIdentity(left, right, r.cfg.FamilyPolicy)
		a, b, witnessed := closestResolverFrequencyPair(fresh, top.Call, runner.Call)
		overlap := 0
		var leftRows, rightRows []ResolverReceiverObservation
		for _, candidate := range fresh.candidates {
			if candidate.call == top.Call {
				leftRows = candidate.receivers
			}
			if candidate.call == runner.Call {
				rightRows = candidate.receivers
			}
		}
		// Rows retain their receiver ordering, so intersection needs no map.
		for i, j := 0, 0; i < len(leftRows) && j < len(rightRows); {
			switch {
			case leftRows[i].Receiver < rightRows[j].Receiver:
				i++
			case leftRows[i].Receiver > rightRows[j].Receiver:
				j++
			default:
				overlap++
				i++
				j++
			}
		}
		split = split || witnessed && shouldRejectAsAmbiguousMultiSignal(top.Support, runner.Support,
			a, b, r.cfg.FreqGuardMinSeparationKHz, r.cfg.FreqGuardRunnerUpRatio, overlap,
			correctionDistance(left, right, key.Mode, r.cfg.DistanceModelCW, r.cfg.DistanceModelRTTY), r.cfg.MaxEditDistance, related)
	}
	if split {
		projected.State = ResolverStateSplit
		return projected, true
	}
	// Removal may revoke authority, but a reader cannot settle a new winner or
	// advance the owner's pending hysteresis. Only a still-leading settled
	// published identity may retain authority with its newly computed share.
	if snap.Winner == top.Call {
		projected.Winner = top.Call
		if snap.State == ResolverStateConfident || snap.State == ResolverStateProbable {
			projected.State = classifyResolverState(top.WeightedSupportMilli, projected.TotalWeightedSupportMilli)
		}
	}
	return projected, true
}
