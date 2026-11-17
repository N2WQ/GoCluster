// File role: Derives semantic voting populations from the resolver's raw caps.
// Immutable publications own these groups; singleton groups borrow raw rows and
// merged groups retain at most the admitted raw incidences. No registry survives
// its publication. Compact snapshots retain only bounded rank/variant metadata.
package spot

import (
	"sort"
	"time"
)

type resolverCanonicalCandidate struct {
	identity  correctionCallIdentity
	display   string
	variants  []string
	receivers []ResolverReceiverObservation
	lastSeen  time.Time
	weighted  int
	runes     []rune
	borrowed  bool
}

// Identity returns the semantic voting identity while Call remains an observed
// spelling. Unannotated synthetic ranks retain their existing call semantics.
func (c ResolverCandidateSupport) Identity() string {
	if c.VoteKey != "" {
		return c.VoteKey
	}
	return CorrectionVoteKey(c.Call)
}

// CandidateForCall resolves any equivalent spelling to its one receiver union.
func (s ResolverSnapshot) CandidateForCall(call string) (ResolverCandidateSupport, bool) {
	identity := CorrectionVoteKey(NormalizeSpotDXCallsign(call))
	if identity == "" {
		return ResolverCandidateSupport{}, false
	}
	for _, candidate := range s.CandidateRanks {
		if candidate.Identity() == identity {
			return candidate, true
		}
	}
	return ResolverCandidateSupport{}, false
}

// DisplayForCall returns a currently observed representative, never a fabricated
// canonical spelling. Legacy scalar-only snapshots retain their fallback.
func (s ResolverSnapshot) DisplayForCall(call string) string {
	if candidate, ok := s.CandidateForCall(call); ok {
		return candidate.Call
	}
	if len(s.CandidateRanks) > 0 {
		return ""
	}
	identity := CorrectionVoteKey(call)
	if identity != "" && CorrectionVoteKey(s.Winner) == identity {
		return s.Winner
	}
	if identity != "" && CorrectionVoteKey(s.RunnerUp) == identity {
		return s.RunnerUp
	}
	return ""
}

// HasObservedCall preserves literal delivery-witness authority. ObservedCalls is
// bounded by raw spelling admission (16 per key, 80 in a five-key selection).
func (s ResolverSnapshot) HasObservedCall(call string) bool {
	for _, candidate := range s.CandidateRanks {
		if candidate.Call == call {
			return true
		}
		for _, observed := range candidate.ObservedCalls {
			if observed == call {
				return true
			}
		}
	}
	if len(s.CandidateRanks) > 0 {
		return false
	}
	return s.Winner == call || s.RunnerUp == call
}

// AllowsReplacement keeps slash precedence separate from support/confidence.
func (s ResolverSnapshot) AllowsReplacement(call string) bool {
	if candidate, ok := s.CandidateForCall(call); ok {
		return !candidate.ReplacementBlocked
	}
	return len(s.CandidateRanks) == 0
}

func joinResolverReceiverObservation(left, right ResolverReceiverObservation) ResolverReceiverObservation {
	joined := joinResolverReporterObservation(
		resolverReporterObservation{observedAt: left.ObservedAt, frequencyKHz: left.FrequencyKHz, report: left.Report, hasReport: left.HasReport, snrConflict: left.SNRConflict, frequencyConflict: left.FrequencyConflict},
		resolverReporterObservation{observedAt: right.ObservedAt, frequencyKHz: right.FrequencyKHz, report: right.Report, hasReport: right.HasReport, snrConflict: right.SNRConflict, frequencyConflict: right.FrequencyConflict})
	return ResolverReceiverObservation{Receiver: left.Receiver, ObservedAt: joined.observedAt, FrequencyKHz: joined.frequencyKHz,
		WeightMilli: max(left.WeightMilli, right.WeightMilli), Report: joined.report, HasReport: joined.hasReport,
		SNRConflict: joined.snrConflict, FrequencyConflict: joined.frequencyConflict}
}

func canonicalResolverCandidates(view *ResolverEvidenceView) []resolverCanonicalCandidate {
	if view == nil {
		return nil
	}
	if view.canonical != nil {
		return view.canonical
	}
	return buildCanonicalResolverCandidates(view.candidates)
}

func buildCanonicalResolverCandidates(raw []resolverEvidenceCandidate) []resolverCanonicalCandidate {
	groups := make([]resolverCanonicalCandidate, 0, len(raw))
	indices := make(map[string]int, len(raw))
	members := make([][]int, 0, len(raw))
	for i := range raw {
		if len(raw[i].receivers) == 0 {
			continue
		}
		identity := normalizeCorrectionCallIdentity(raw[i].call)
		index, exists := indices[identity.VoteKey]
		if !exists {
			index = len(groups)
			indices[identity.VoteKey] = index
			groups = append(groups, resolverCanonicalCandidate{identity: identity})
			members = append(members, nil)
		}
		members[index] = append(members[index], i)
	}
	for i := range groups {
		group := &groups[i]
		group.runes = []rune(group.identity.VoteKey)
		if len(members[i]) == 1 {
			member := &raw[members[i][0]]
			group.display, group.receivers = member.call, member.receivers
			group.borrowed = true
		} else {
			receivers := make(map[string]ResolverReceiverObservation)
			for _, index := range members[i] {
				for _, row := range raw[index].receivers {
					if previous, exists := receivers[row.Receiver]; exists {
						row = joinResolverReceiverObservation(previous, row)
					}
					receivers[row.Receiver] = row
				}
			}
			group.receivers = make([]ResolverReceiverObservation, 0, len(receivers))
			for _, row := range receivers {
				group.receivers = append(group.receivers, row)
			}
			sort.Slice(group.receivers, func(a, b int) bool { return group.receivers[a].Receiver < group.receivers[b].Receiver })
			variants := make(map[string]map[string]time.Time, len(members[i]))
			for _, index := range members[i] {
				member := &raw[index]
				for _, row := range member.receivers {
					if !row.ObservedAt.Equal(receivers[row.Receiver].ObservedAt) {
						continue
					}
					byReceiver := variants[member.call]
					if byReceiver == nil {
						byReceiver = make(map[string]time.Time)
						variants[member.call] = byReceiver
					}
					byReceiver[row.Receiver] = row.ObservedAt
				}
			}
			bestSupport := -1
			var bestAt time.Time
			for call, byReceiver := range variants {
				var latest time.Time
				for _, at := range byReceiver {
					if at.After(latest) {
						latest = at
					}
				}
				support := len(byReceiver)
				group.variants = append(group.variants, call)
				if support > bestSupport || support == bestSupport && (latest.After(bestAt) || latest.Equal(bestAt) && call < group.display) {
					group.display, bestSupport, bestAt = call, support, latest
				}
			}
			sort.Strings(group.variants)
		}
		for _, row := range group.receivers {
			group.weighted += row.WeightMilli
			if row.ObservedAt.After(group.lastSeen) {
				group.lastSeen = row.ObservedAt
			}
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].identity.VoteKey < groups[j].identity.VoteKey })
	return groups
}

// RangeCanonical visits distinct receiver memberships of semantic candidates.
// Range remains the original raw provenance API. Callback values are copies.
func (v *ResolverEvidenceView) RangeCanonical(visit func(identity, display string, row ResolverReceiverObservation) bool) {
	if visit == nil {
		return
	}
	for _, group := range canonicalResolverCandidates(v) {
		for _, row := range group.receivers {
			if !visit(group.identity.VoteKey, group.display, row) {
				return
			}
		}
	}
}

// CanonicalSupport returns bounded, detached rank headers. Receiver rows remain
// in the view; observed variants borrow its immutable bounded string lists.
func (v *ResolverEvidenceView) CanonicalSupport(slashThreshold int) []ResolverCandidateSupport {
	groups := canonicalResolverCandidates(v)
	ranks := make([]ResolverCandidateSupport, 0, len(groups))
	for _, group := range groups {
		ranks = append(ranks, ResolverCandidateSupport{Call: group.display, VoteKey: group.identity.VoteKey,
			ObservedCalls: group.variants, Support: len(group.receivers), WeightedSupportMilli: group.weighted})
	}
	applyResolverSlashPrecedence(ranks, slashThreshold)
	return ranks
}

func applyResolverSlashPrecedence(ranks []ResolverCandidateSupport, threshold int) {
	if threshold <= 0 {
		threshold = 2
	}
	for i := range ranks {
		identity := normalizeCorrectionCallIdentity(ranks[i].Identity())
		if identity.HasSlash {
			continue
		}
		for _, slash := range ranks {
			other := normalizeCorrectionCallIdentity(slash.Identity())
			if other.HasSlash && other.BaseKey == identity.BaseKey && slash.Support >= threshold {
				ranks[i].ReplacementBlocked = true
				break
			}
		}
	}
}

// MergeResolverEvidenceViews combines the selector's at-most-five admitted raw
// publications. Its temporary rows/maps are bounded by their admitted raw caps;
// callers do not retain this view in synthesized or compact temporal snapshots.
func MergeResolverEvidenceViews(views ...*ResolverEvidenceView) *ResolverEvidenceView {
	merged := &ResolverEvidenceView{}
	for _, view := range views {
		if view == nil {
			continue
		}
		merged.candidates = append(merged.candidates, view.candidates...)
		merged.count += view.count
		for _, group := range canonicalResolverCandidates(view) {
			merged.frequencyWitnesses = append(merged.frequencyWitnesses, resolverEvidenceCandidate{call: group.identity.VoteKey, receivers: group.receivers})
		}
	}
	merged.canonical = buildCanonicalResolverCandidates(merged.candidates)
	for i := range merged.canonical {
		group := &merged.canonical[i]
		if group.borrowed {
			group.receivers = append([]ResolverReceiverObservation(nil), group.receivers...)
			group.borrowed = false
		}
		// Receiver/candidate frequency conflicts remain scoped to their source
		// bucket. Different bucket observations retain their actual witnesses;
		// their frequency difference alone is not a new global split rule.
		for j := range group.receivers {
			row := &group.receivers[j]
			row.FrequencyConflict = false
			chosen := false
			for _, source := range merged.frequencyWitnesses {
				if source.call != group.identity.VoteKey {
					continue
				}
				index := sort.Search(len(source.receivers), func(k int) bool { return source.receivers[k].Receiver >= row.Receiver })
				if index == len(source.receivers) || source.receivers[index].Receiver != row.Receiver {
					continue
				}
				original := source.receivers[index]
				if !original.ObservedAt.Equal(row.ObservedAt) {
					continue
				}
				row.FrequencyConflict = row.FrequencyConflict || original.FrequencyConflict
				if !chosen || original.FrequencyKHz < row.FrequencyKHz {
					row.FrequencyKHz = original.FrequencyKHz
					chosen = true
				}
			}
		}
	}
	return merged
}
func resolverCanonicalRows(view *ResolverEvidenceView, call string) []ResolverReceiverObservation {
	identity := CorrectionVoteKey(call)
	for _, group := range canonicalResolverCandidates(view) {
		if group.identity.VoteKey == identity {
			return group.receivers
		}
	}
	return nil
}

// ClosestFrequencyPair supplies the existing ambiguity guard with exact eligible
// witnesses from this bounded canonical population.
func (v *ResolverEvidenceView) ClosestFrequencyPair(left, right string) (float64, float64, bool) {
	return closestResolverFrequencyPair(v, left, right)
}

func resolverFrequencyRows(view *ResolverEvidenceView, call string) []ResolverReceiverObservation {
	if view == nil || len(view.frequencyWitnesses) == 0 {
		return resolverCanonicalRows(view, call)
	}
	identity := CorrectionVoteKey(call)
	var rows []ResolverReceiverObservation
	for _, source := range view.frequencyWitnesses {
		if source.call == identity {
			rows = append(rows, source.receivers...)
		}
	}
	return rows
}

func rankedCanonicalResolverCandidates(view *ResolverEvidenceView, into []rankedResolverCandidate, threshold int) []rankedResolverCandidate {
	groups := canonicalResolverCandidates(view)
	for i, group := range groups {
		into = append(into, rankedResolverCandidate{call: group.display, support: len(group.receivers), weightedSupportMilli: group.weighted,
			lastSeen: group.lastSeen, receivers: group.receivers, identity: group.identity, callRunes: group.runes,
			observedCalls: group.variants, replacementBlocked: resolverCanonicalBlocked(groups, i, threshold)})
	}
	return into
}

func resolverCanonicalBlocked(groups []resolverCanonicalCandidate, index, threshold int) bool {
	if threshold <= 0 {
		threshold = 2
	}
	identity := groups[index].identity
	if identity.HasSlash {
		return false
	}
	for _, group := range groups {
		if group.identity.HasSlash && group.identity.BaseKey == identity.BaseKey && len(group.receivers) >= threshold {
			return true
		}
	}
	return false
}

func resolverReceiverOverlap(left, right []ResolverReceiverObservation) int {
	overlap := 0
	for i, j := 0, 0; i < len(left) && j < len(right); {
		switch {
		case left[i].Receiver < right[j].Receiver:
			i++
		case left[i].Receiver > right[j].Receiver:
			j++
		default:
			overlap++
			i++
			j++
		}
	}
	return overlap
}

func resolverRankSupport(candidate rankedResolverCandidate) ResolverCandidateSupport {
	return ResolverCandidateSupport{Call: candidate.call, VoteKey: candidate.identity.VoteKey, ObservedCalls: candidate.observedCalls,
		Support: candidate.support, WeightedSupportMilli: candidate.weightedSupportMilli, ReplacementBlocked: candidate.replacementBlocked}
}

func resolverRankIdentity(candidate rankedResolverCandidate) string {
	if candidate.identity.VoteKey != "" {
		return candidate.identity.VoteKey
	}
	return CorrectionVoteKey(candidate.call)
}

func resolverRankDisplay(ranked []rankedResolverCandidate, identity string) string {
	for _, candidate := range ranked {
		if resolverRankIdentity(candidate) == identity {
			return candidate.call
		}
	}
	return ""
}
