package spot

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"
	"time"
)

// The deliberately slow map construction is the pre-optimization derivation.
// It shares the unchanged raw publication builder, not the sorted merge, display
// cursor, population reduction or overlap implementation under test.
func ccv2MapMergeOracle(views []*ResolverEvidenceView) *ResolverEvidenceView {
	result := &ResolverEvidenceView{}
	for _, view := range views {
		if view == nil {
			continue
		}
		result.candidates = append(result.candidates, view.candidates...)
		result.count += view.count
		for _, group := range canonicalResolverCandidates(view) {
			result.frequencyWitnesses = append(result.frequencyWitnesses, resolverEvidenceCandidate{call: group.identity.VoteKey, receivers: group.receivers})
		}
	}
	result.canonical = buildCanonicalResolverCandidates(result.candidates)
	for i := range result.canonical {
		group := &result.canonical[i]
		group.receivers = append([]ResolverReceiverObservation(nil), group.receivers...)
		for j := range group.receivers {
			row := &group.receivers[j]
			row.FrequencyConflict = false
			chosen := false
			for _, source := range result.frequencyWitnesses {
				if source.call != group.identity.VoteKey {
					continue
				}
				for _, original := range source.receivers {
					if original.Receiver != row.Receiver || !original.ObservedAt.Equal(row.ObservedAt) {
						continue
					}
					row.FrequencyConflict = row.FrequencyConflict || original.FrequencyConflict
					if !chosen || original.FrequencyKHz < row.FrequencyKHz {
						row.FrequencyKHz, chosen = original.FrequencyKHz, true
					}
				}
			}
		}
	}
	return result
}

func ccv2CheckMergedPopulation(t *testing.T, views []*ResolverEvidenceView) {
	t.Helper()
	want, got := ccv2MapMergeOracle(views), MergeResolverEvidenceViews(views...)
	if got.Len() != want.Len() || !reflect.DeepEqual(got.candidates, want.candidates) ||
		!reflect.DeepEqual(got.frequencyWitnesses, want.frequencyWitnesses) || !reflect.DeepEqual(got.canonical, want.canonical) {
		t.Fatalf("sorted merge differs from map derivation\ngot=%+v\nwant=%+v", got.canonical, want.canonical)
	}
	weights := make(map[string]int)
	conflict := false
	members := make(map[string]map[string]bool)
	want.RangeCanonical(func(identity, _ string, row ResolverReceiverObservation) bool {
		if previous, ok := weights[row.Receiver]; !ok || row.WeightMilli > previous {
			weights[row.Receiver] = row.WeightMilli
		}
		if members[identity] == nil {
			members[identity] = make(map[string]bool)
		}
		members[identity][row.Receiver] = true
		conflict = conflict || row.FrequencyConflict
		return true
	})
	weighted := 0
	for _, weight := range weights {
		weighted += weight
	}
	if count, total, hasConflict := got.ReceiverTotals(); count != len(weights) || total != weighted || hasConflict != conflict {
		t.Fatalf("distinct population = %d/%d/%t, want %d/%d/%t", count, total, hasConflict, len(weights), weighted, conflict)
	}
	for left, leftRows := range members {
		for right, rightRows := range members {
			overlap := 0
			for receiver := range leftRows {
				if rightRows[receiver] {
					overlap++
				}
			}
			if got.ReceiverOverlap(left, right) != overlap {
				t.Fatalf("overlap %s/%s differs from independent sets", left, right)
			}
		}
	}
}

func TestCCV2SortedCanonicalMergeMapOracle(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	random := rand.New(rand.NewPCG(0x43435632, 7))
	for iteration := 0; iteration < 128; iteration++ {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			views := make([]*ResolverEvidenceView, 1+iteration%5)
			for source := range views {
				if iteration%13 == 0 && source == 0 {
					continue
				}
				view := &ResolverEvidenceView{window: time.Minute}
				for _, call := range []string{"W1AW", "KH6/W1AW", "W1AW/KH6", "K1ABC/F/2", "K1ABC/2/F", "K1ABC"} {
					raw := resolverEvidenceCandidate{call: call}
					for receiver := 0; receiver < 12; receiver++ {
						if random.IntN(3) == 0 {
							continue
						}
						row := ResolverReceiverObservation{Receiver: fmt.Sprintf("N0%03d", receiver), ObservedAt: at.Add(time.Duration(random.IntN(5)) * time.Second),
							FrequencyKHz: 7010 + float64(source)*.5 + float64(random.IntN(3))*.01, WeightMilli: random.IntN(3) * 500,
							HasReport: random.IntN(4) != 0}
						if row.HasReport {
							row.Report = random.IntN(3) * 9
						}
						if random.IntN(17) == 0 {
							row.FrequencyConflict, row.FrequencyKHz = true, 0
						}
						if random.IntN(17) == 0 {
							row.SNRConflict, row.HasReport, row.Report = true, false, 0
						}
						raw.receivers = append(raw.receivers, row)
					}
					if len(raw.receivers) > 0 {
						view.candidates = append(view.candidates, raw)
						view.count += len(raw.receivers)
					}
				}
				sort.Slice(view.candidates, func(i, j int) bool { return view.candidates[i].call < view.candidates[j].call })
				view.canonical = buildCanonicalResolverCandidates(view.candidates)
				views[source] = view
			}
			ccv2CheckMergedPopulation(t, views)
		})
	}
}

func TestCCV2SortedMergeSupersededDisplayRetainsSourceWitness(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	view := func(raw ...resolverEvidenceCandidate) *ResolverEvidenceView {
		v := &ResolverEvidenceView{candidates: raw}
		for _, candidate := range raw {
			v.count += len(candidate.receivers)
		}
		v.canonical = buildCanonicalResolverCandidates(raw)
		return v
	}
	row := func(seconds int, frequency float64) []ResolverReceiverObservation {
		return []ResolverReceiverObservation{{Receiver: "N0AAA", ObservedAt: at.Add(time.Duration(seconds) * time.Second), FrequencyKHz: frequency, WeightMilli: 1000, HasReport: true}}
	}
	a := view(resolverEvidenceCandidate{call: "KH6/W1AW", receivers: row(3, 7010)}, resolverEvidenceCandidate{call: "W1AW/KH6", receivers: row(2, 7010.1)})
	b := view(resolverEvidenceCandidate{call: "W1AW/KH6", receivers: row(4, 7010.5)})
	ccv2CheckMergedPopulation(t, []*ResolverEvidenceView{a, b})
	got := MergeResolverEvidenceViews(a, b)
	group := got.canonical[0]
	if group.display != "W1AW/KH6" || !reflect.DeepEqual(group.variants, []string{"W1AW/KH6"}) || group.receivers[0].ObservedAt != at.Add(4*time.Second) {
		t.Fatalf("older display resurrected: %+v", group)
	}
	witnesses := resolverFrequencyRows(got, "W1AW/KH6")
	if len(witnesses) != 2 || witnesses[0].FrequencyKHz != 7010 || witnesses[1].FrequencyKHz != 7010.5 {
		t.Fatal("global freshness discarded original source-bucket frequency geometry")
	}
}
