// File role: Verify frozen diagnostic rows before importing study-only evidence.
// Captured owner metadata is retained separately; reconstruction creates no
// settled publication, lifecycle, archive provenance or confusion-model order.
package correctionflow

import (
	"fmt"
	"slices"
	"strings"

	"dxcluster/spot"
)

// ImportFrequencySources accepts an already integrity-verified diagnostic DTO.
// File hashes/schema/record bounds remain the diagnostic reader's responsibility.
// This check additionally binds each reconstructed union to captured metadata.
func ImportFrequencySources(d ResolverSelectionDiagnostics, slashThreshold int) ([5]FrequencySource, error) {
	var sources [5]FrequencySource
	totalRows, totalSlots := 0, 0
	for i := range d.Reads {
		read := &d.Reads[i]
		s := &sources[i]
		s.Read, s.Present = read.Read, read.Present
		s.RequestedKey = frequencyDiagnosticKey(read.RequestedKey)
		if !read.Read || !read.Present {
			if read.Present || len(read.RawRows) != 0 || len(read.Snapshot.Candidates) != 0 {
				return sources, fmt.Errorf("frequency import slot %d: evidence without a present read", i)
			}
			continue
		}
		if !read.RawEvidenceAvailable {
			return sources, fmt.Errorf("frequency import slot %d: unavailable raw evidence", i)
		}
		if len(read.RawRows) > 5120-totalRows || len(read.Snapshot.Candidates) > 80-totalSlots {
			return sources, fmt.Errorf("frequency import: raw bounds exceeded")
		}
		rows := make([]spot.ResolverEvidenceRow, 0, len(read.RawRows))
		for _, r := range read.RawRows {
			if r.Identity != "" || r.RawOrigin != "unavailable" {
				return sources, fmt.Errorf("frequency import slot %d: raw identity/origin mismatch", i)
			}
			rows = append(rows, spot.ResolverEvidenceRow{Call: r.Call, Observation: spot.ResolverReceiverObservation{
				Receiver: r.Receiver, ObservedAt: r.ObservedAt, FrequencyKHz: r.FrequencyKHz, WeightMilli: r.WeightMilli,
				Report: r.Report, HasReport: r.HasReport, SNRConflict: r.SNRConflict, FrequencyConflict: r.FrequencyConflict}})
		}
		view, err := spot.NewStudyResolverEvidence(rows, read.RecencyWindow)
		if err != nil {
			return sources, fmt.Errorf("frequency import slot %d: %w", i, err)
		}
		captured := read.Snapshot
		ranks := view.CanonicalSupport(slashThreshold)
		if len(ranks) != len(captured.Candidates) {
			return sources, fmt.Errorf("frequency import slot %d: canonical count mismatch", i)
		}
		seen := make(map[string]bool, len(ranks))
		ownedRanks := make([]spot.ResolverCandidateSupport, 0, len(ranks))
		for _, header := range captured.Candidates {
			identity := header.VoteKey
			if identity == "" {
				identity = spot.CorrectionVoteKey(header.Call)
			}
			found := false
			for _, rank := range ranks {
				if rank.Identity() != identity {
					continue
				}
				if seen[identity] || rank.Call != header.Call || rank.Support != header.Support ||
					rank.WeightedSupportMilli != header.WeightedSupportMilli || rank.ReplacementBlocked != header.ReplacementBlocked ||
					!slices.Equal(rank.ObservedCalls, header.ObservedCalls) {
					return sources, fmt.Errorf("frequency import slot %d: canonical metadata mismatch", i)
				}
				seen[identity], found = true, true
				ownedRanks = append(ownedRanks, spot.ResolverCandidateSupport{Call: strings.Clone(header.Call), VoteKey: strings.Clone(identity),
					ObservedCalls: slices.Clone(rank.ObservedCalls), ReplacementBlocked: rank.ReplacementBlocked,
					Support: rank.Support, WeightedSupportMilli: rank.WeightedSupportMilli})
				break
			}
			if !found {
				return sources, fmt.Errorf("frequency import slot %d: unknown identity", i)
			}
		}
		count, weighted, _ := view.ReceiverTotals()
		if count != captured.TotalReporters || weighted != captured.TotalWeightedSupportMilli || frequencyDiagnosticKey(captured.Key) != s.RequestedKey {
			return sources, fmt.Errorf("frequency import slot %d: source totals/key mismatch", i)
		}
		s.Evidence = view
		s.PendingIncumbent, s.PendingChallenger = strings.Clone(captured.PendingIncumbent), strings.Clone(captured.PendingChallenger)
		s.Snapshot = spot.ResolverSnapshot{Key: s.RequestedKey, Generation: captured.Generation, EvidenceLoss: captured.EvidenceLoss,
			SupportBasis: captured.SupportBasis, EvaluatedAt: captured.EvaluatedAt, State: captured.State,
			Winner: strings.Clone(captured.Winner), RunnerUp: strings.Clone(captured.RunnerUp),
			WinnerSupport: captured.WinnerSupport, RunnerSupport: captured.RunnerSupport, Margin: captured.Margin,
			TotalReporters: count, TotalWeightedSupportMilli: weighted, WinnerWeightedSupportMilli: captured.WinnerWeightedSupportMilli,
			RunnerWeightedSupportMilli: captured.RunnerWeightedSupportMilli, CandidateRanks: ownedRanks}
		totalRows += len(rows)
		// Count source raw spellings, including aliases that fold to one rank.
		raw := make(map[string]bool)
		for _, row := range rows {
			raw[row.Call] = true
		}
		totalSlots += len(raw)
		if totalSlots > 80 {
			return sources, fmt.Errorf("frequency import: raw spelling bound exceeded")
		}
	}
	return sources, nil
}

func frequencyDiagnosticKey(k DiagnosticKey) spot.ResolverSignalKey {
	return spot.ResolverSignalKey{Band: strings.Clone(k.Band), Mode: strings.Clone(k.Mode), ToleranceHz: k.ToleranceHz, Bucket: k.Bucket}
}
