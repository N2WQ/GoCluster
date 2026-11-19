// File role: Own bounded study imports and pure subsets of immutable evidence.
// These operations create no resolver publication, lifecycle or settled verdict.
package spot

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// ResolverEvidenceRow is raw spelling provenance, before canonical receiver joins.
type ResolverEvidenceRow struct {
	Call        string
	Observation ResolverReceiverObservation
}

// NewStudyResolverEvidence owns one source's captured rows. The study contract
// caps raw spellings/receivers before alias folding; malformed rows fail rather
// than being truncated, normalized or used to manufacture owner authority.
func NewStudyResolverEvidence(rows []ResolverEvidenceRow, window time.Duration) (*ResolverEvidenceView, error) {
	if len(rows) > 16*64 || window <= 0 {
		return nil, fmt.Errorf("study evidence: invalid row count or recency window")
	}
	byCall := make(map[string]map[string]ResolverReceiverObservation, 16)
	for _, input := range rows {
		r := input.Observation
		if input.Call == "" || len(input.Call) > 1024 || NormalizeSpotDXCallsign(input.Call) != input.Call ||
			r.Receiver == "" || len(r.Receiver) > 1024 || r.ObservedAt.IsZero() ||
			math.IsNaN(r.FrequencyKHz) || math.IsInf(r.FrequencyKHz, 0) || r.FrequencyKHz < 0 ||
			r.WeightMilli < 0 || r.WeightMilli > resolverReliabilityScale ||
			(r.FrequencyConflict && r.FrequencyKHz != 0) || (r.SNRConflict && r.HasReport) || (!r.HasReport && r.Report != 0) {
			return nil, fmt.Errorf("study evidence: invalid raw observation")
		}
		receivers, found := byCall[input.Call]
		if !found {
			if len(byCall) == 16 {
				return nil, fmt.Errorf("study evidence: more than 16 raw spellings")
			}
			receivers = make(map[string]ResolverReceiverObservation)
			byCall[strings.Clone(input.Call)] = receivers
		}
		if _, duplicate := receivers[r.Receiver]; duplicate || len(receivers) == 64 {
			return nil, fmt.Errorf("study evidence: duplicate receiver or receiver cap")
		}
		r.Receiver = strings.Clone(r.Receiver)
		r.ObservedAt = r.ObservedAt.UTC() // Own the instant without retaining caller location tables.
		receivers[r.Receiver] = r
	}
	view := &ResolverEvidenceView{window: window, count: len(rows)}
	for call, receivers := range byCall {
		candidate := resolverEvidenceCandidate{call: call, receivers: make([]ResolverReceiverObservation, 0, len(receivers))}
		for _, r := range receivers {
			candidate.receivers = append(candidate.receivers, r)
		}
		sort.Slice(candidate.receivers, func(i, j int) bool { return candidate.receivers[i].Receiver < candidate.receivers[j].Receiver })
		view.candidates = append(view.candidates, candidate)
	}
	sort.Slice(view.candidates, func(i, j int) bool { return view.candidates[i].call < view.candidates[j].call })
	view.canonical = buildCanonicalResolverCandidates(view.candidates)
	return view, nil
}

// Filter returns a new raw subset, then reuses the canonical union/slash rules.
// The input stays immutable. In particular, a filter never operates on a single
// favorable cross-source canonical frequency in place of the raw witnesses.
// Receiver slices are newly owned; immutable string backing can be shared.
func (v *ResolverEvidenceView) Filter(keep func(string, ResolverReceiverObservation) bool) *ResolverEvidenceView {
	if v == nil || keep == nil {
		return nil
	}
	out := &ResolverEvidenceView{window: v.window}
	for _, candidate := range v.candidates {
		var rows []ResolverReceiverObservation
		for _, row := range candidate.receivers {
			if keep(candidate.call, row) {
				rows = append(rows, row)
			}
		}
		if len(rows) > 0 {
			out.count += len(rows)
			out.candidates = append(out.candidates, resolverEvidenceCandidate{call: candidate.call, receivers: rows})
		}
	}
	out.canonical = buildCanonicalResolverCandidates(out.candidates)
	return out
}
