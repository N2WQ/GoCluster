// File role: Derives semantic voting populations from the resolver's raw caps.
// Immutable publications own these groups; singleton groups borrow raw rows and
// merged groups retain at most the admitted raw incidences. No registry survives
// its publication. Compact snapshots retain only bounded rank/variant metadata.
package spot

import (
	"math/bits"
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
	groups := canonicalResolverCandidates(v)
	for i := range groups {
		group := &groups[i]
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
	for i := range groups {
		group := &groups[i]
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

type resolverCanonicalMergeSource struct {
	group *resolverCanonicalCandidate
	raw   []resolverEvidenceCandidate
}

// MergeResolverEvidenceViews combines the selector's at-most-five admitted
// publications. Merge their already sorted canonical rows rather than rebuilding
// receiver maps from raw spellings on each read. Raw rows and every source's
// frequency witnesses remain borrowed, immutable provenance. Only multi-source
// canonical unions allocate receiver backing; compact snapshots retain none.
func MergeResolverEvidenceViews(views ...*ResolverEvidenceView) *ResolverEvidenceView {
	merged := &ResolverEvidenceView{}
	var sources []resolverCanonicalMergeSource
	for _, view := range views {
		if view == nil {
			continue
		}
		merged.candidates = append(merged.candidates, view.candidates...)
		merged.count += view.count
		groups := canonicalResolverCandidates(view)
		for i := range groups {
			group := &groups[i]
			sources = append(sources, resolverCanonicalMergeSource{group: group, raw: view.candidates})
			merged.frequencyWitnesses = append(merged.frequencyWitnesses, resolverEvidenceCandidate{call: group.identity.VoteKey, receivers: group.receivers})
		}
	}
	// Stable source ordering preserves the first raw identity's metadata; voting
	// order itself remains lexical and every join is timestamp/receipt independent.
	sort.SliceStable(sources, func(i, j int) bool { return sources[i].group.identity.VoteKey < sources[j].group.identity.VoteKey })
	merged.canonical = make([]resolverCanonicalCandidate, 0, len(sources))
	for first := 0; first < len(sources); {
		end := first + 1
		for end < len(sources) && sources[end].group.identity.VoteKey == sources[first].group.identity.VoteKey {
			end++
		}
		group := *sources[first].group
		if end-first > 1 {
			group.receivers = mergeResolverCanonicalRows(sources[first:end])
			group.weighted, group.lastSeen = 0, time.Time{}
			for _, row := range group.receivers {
				group.weighted += row.WeightMilli
				if row.ObservedAt.After(group.lastSeen) {
					group.lastSeen = row.ObservedAt
				}
			}
			group.display, group.variants = mergedResolverObservedVariants(group, sources[first:end])
		}
		merged.canonical = append(merged.canonical, group)
		first = end
	}
	return merged
}

func mergeResolverCanonicalRows(sources []resolverCanonicalMergeSource) []ResolverReceiverObservation {
	positions := make([]int, len(sources))
	capacity := 0
	for _, source := range sources {
		capacity = max(capacity, len(source.group.receivers))
	}
	// Repeated receivers across buckets are common. Starting at the largest
	// source avoids retaining five copies' worth of backing for one union.
	rows := make([]ResolverReceiverObservation, 0, capacity)
	for {
		receiver, found := "", false
		for i, source := range sources {
			if positions[i] < len(source.group.receivers) {
				next := source.group.receivers[positions[i]].Receiver
				if !found || next < receiver {
					receiver, found = next, true
				}
			}
		}
		if !found {
			return rows
		}
		var joined ResolverReceiverObservation
		var latest time.Time
		frequency, conflict, have := 0.0, false, false
		for i, source := range sources {
			if positions[i] == len(source.group.receivers) || source.group.receivers[positions[i]].Receiver != receiver {
				continue
			}
			row := source.group.receivers[positions[i]]
			positions[i]++
			if !have {
				joined = row
			} else {
				joined = joinResolverReceiverObservation(joined, row)
			}
			// Equal-time SNR joins across sources, but a frequency disagreement
			// across buckets does not invent a source-local conflict. Preserve
			// the original min-frequency/OR rule at the global newest timestamp.
			if !have || row.ObservedAt.After(latest) {
				latest, frequency, conflict = row.ObservedAt, row.FrequencyKHz, row.FrequencyConflict
			} else if row.ObservedAt.Equal(latest) {
				if row.FrequencyKHz < frequency {
					frequency = row.FrequencyKHz
				}
				conflict = conflict || row.FrequencyConflict
			}
			have = true
		}
		joined.FrequencyKHz, joined.FrequencyConflict = frequency, conflict
		rows = append(rows, joined)
	}
}

// Display support belongs to raw spellings at each receiver's global newest
// timestamp. Per-source display winners alone cannot establish that support.
// Raw spelling rows and canonical rows are sorted; each spelling needs only a
// moving receiver cursor and a bounded source list, never another receiver map.
func mergedResolverObservedVariants(group resolverCanonicalCandidate, sources []resolverCanonicalMergeSource) (string, []string) {
	variants := make(map[string][][]ResolverReceiverObservation)
	for _, source := range sources {
		for _, raw := range source.raw {
			observed := raw.call == source.group.display
			for _, call := range source.group.variants {
				observed = observed || raw.call == call
			}
			// A locally superseded spelling cannot become current in a larger
			// union: its global newest timestamp is at least the local newest.
			if observed && len(raw.receivers) > 0 {
				variants[raw.call] = append(variants[raw.call], raw.receivers)
			}
		}
	}
	var observed []string
	display, bestSupport := "", -1
	var bestAt time.Time
	for call, parts := range variants {
		positions := make([]int, len(parts))
		support, canonicalIndex := 0, 0
		var latest time.Time
		for {
			receiver, found := "", false
			for i, rows := range parts {
				if positions[i] < len(rows) && (!found || rows[positions[i]].Receiver < receiver) {
					receiver, found = rows[positions[i]].Receiver, true
				}
			}
			if !found {
				break
			}
			var observedAt time.Time
			haveTime := false
			for i, rows := range parts {
				if positions[i] < len(rows) && rows[positions[i]].Receiver == receiver {
					at := rows[positions[i]].ObservedAt
					positions[i]++
					if !haveTime || at.After(observedAt) {
						observedAt, haveTime = at, true
					}
				}
			}
			for canonicalIndex < len(group.receivers) && group.receivers[canonicalIndex].Receiver < receiver {
				canonicalIndex++
			}
			if canonicalIndex < len(group.receivers) && group.receivers[canonicalIndex].Receiver == receiver && group.receivers[canonicalIndex].ObservedAt.Equal(observedAt) {
				support++
				if observedAt.After(latest) {
					latest = observedAt
				}
			}
		}
		if support == 0 {
			continue
		}
		observed = append(observed, call)
		if support > bestSupport || support == bestSupport && (latest.After(bestAt) || latest.Equal(bestAt) && call < display) {
			display, bestSupport, bestAt = call, support, latest
		}
	}
	sort.Strings(observed)
	return display, observed
}

// ReceiverTotals counts each receiver once across semantic candidates, using
// its maximum weight. Sorted immutable rows avoid per-read receiver set maps.
// Conflicts remain candidate-specific and do not disappear behind another call.
func (v *ResolverEvidenceView) ReceiverTotals() (count, weighted int, frequencyConflict bool) {
	groups := canonicalResolverCandidates(v)
	if len(groups) > 16 {
		return resolverReceiverTotalsHeap(groups)
	}
	positions := make([]int, len(groups))
	for {
		receiver, found := "", false
		for i := range groups {
			if positions[i] < len(groups[i].receivers) {
				next := groups[i].receivers[positions[i]].Receiver
				if !found || next < receiver {
					receiver, found = next, true
				}
			}
		}
		if !found {
			return count, weighted, frequencyConflict
		}
		weight, haveWeight := 0, false
		for i := range groups {
			if positions[i] < len(groups[i].receivers) && groups[i].receivers[positions[i]].Receiver == receiver {
				row := groups[i].receivers[positions[i]]
				positions[i]++
				if !haveWeight || row.WeightMilli > weight {
					weight, haveWeight = row.WeightMilli, true
				}
				frequencyConflict = frequencyConflict || row.FrequencyConflict
			}
		}
		count++
		weighted += weight
	}
}

type resolverReceiverCursor struct {
	rows  []ResolverReceiverObservation
	index int
}

// A five-key neighborhood can contain 80 distinct identities. Repeatedly
// scanning all heads per unique receiver is costly for disjoint populations.
// Keep the cheap small-cohort scan above and merge large cohorts with one local
// heap of slice cursors, without receiver maps or per-receiver allocations.
func resolverReceiverTotalsHeap(groups []resolverCanonicalCandidate) (count, weighted int, frequencyConflict bool) {
	heads := make([]resolverReceiverCursor, 0, len(groups))
	for i := range groups {
		if len(groups[i].receivers) > 0 {
			heads = append(heads, resolverReceiverCursor{rows: groups[i].receivers})
		}
	}
	for i := len(heads)/2 - 1; i >= 0; i-- {
		siftResolverReceiverCursors(heads, i)
	}
	for len(heads) > 0 {
		first := heads[0].rows[heads[0].index]
		receiver, weight := first.Receiver, first.WeightMilli
		count++
		// Many identities can share every receiver. Repeated heap repair for
		// the same key would lose the small scan's advantage in that shape.
		// Heap order lets this bounded probe prune any nonmatching subtree.
		depth := bits.Len(uint(len(heads)))
		threshold := max(2, (len(heads)+depth-1)/depth)
		if equalResolverReceiverHeads(heads, 0, receiver, threshold) >= threshold {
			for i := 0; i < len(heads); {
				row := heads[i].rows[heads[i].index]
				if row.Receiver != receiver {
					i++
					continue
				}
				weight = max(weight, row.WeightMilli)
				frequencyConflict = frequencyConflict || row.FrequencyConflict
				heads[i].index++
				if heads[i].index == len(heads[i].rows) {
					heads[i] = heads[len(heads)-1]
					heads = heads[:len(heads)-1]
				} else {
					i++
				}
			}
			for i := len(heads)/2 - 1; i >= 0; i-- {
				siftResolverReceiverCursors(heads, i)
			}
		} else {
			for len(heads) > 0 && heads[0].rows[heads[0].index].Receiver == receiver {
				row := heads[0].rows[heads[0].index]
				weight = max(weight, row.WeightMilli)
				frequencyConflict = frequencyConflict || row.FrequencyConflict
				heads[0].index++
				if heads[0].index == len(heads[0].rows) {
					heads[0] = heads[len(heads)-1]
					heads = heads[:len(heads)-1]
				}
				if len(heads) > 0 {
					siftResolverReceiverCursors(heads, 0)
				}
			}
		}
		weighted += weight
	}
	return count, weighted, frequencyConflict
}

func equalResolverReceiverHeads(heads []resolverReceiverCursor, index int, receiver string, limit int) int {
	if limit <= 0 || index >= len(heads) || heads[index].rows[heads[index].index].Receiver != receiver {
		return 0
	}
	count := 1
	count += equalResolverReceiverHeads(heads, 2*index+1, receiver, limit-count)
	count += equalResolverReceiverHeads(heads, 2*index+2, receiver, limit-count)
	return count
}

func siftResolverReceiverCursors(heads []resolverReceiverCursor, parent int) {
	for {
		child := 2*parent + 1
		if child >= len(heads) {
			return
		}
		if child+1 < len(heads) && heads[child+1].rows[heads[child+1].index].Receiver < heads[child].rows[heads[child].index].Receiver {
			child++
		}
		if heads[parent].rows[heads[parent].index].Receiver <= heads[child].rows[heads[child].index].Receiver {
			return
		}
		heads[parent], heads[child] = heads[child], heads[parent]
		parent = child
	}
}

// ReceiverOverlap intersects canonical candidate memberships without copying or
// retaining a receiver set. It does not change either candidate's denominator.
func (v *ResolverEvidenceView) ReceiverOverlap(left, right string) int {
	return resolverReceiverOverlap(resolverCanonicalRows(v, left), resolverCanonicalRows(v, right))
}
func resolverCanonicalRows(view *ResolverEvidenceView, call string) []ResolverReceiverObservation {
	identity := CorrectionVoteKey(call)
	groups := canonicalResolverCandidates(view)
	for i := range groups {
		group := &groups[i]
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
	for i := range groups {
		group := &groups[i]
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
	for i := range groups {
		group := &groups[i]
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
	for i := range ranked {
		if resolverRankIdentity(ranked[i]) == identity {
			return ranked[i].call
		}
	}
	return ""
}
