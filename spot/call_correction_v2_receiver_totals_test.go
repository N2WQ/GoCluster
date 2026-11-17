package spot

import (
	"fmt"
	"testing"
	"time"
)

func ccv2DistinctCandidateViews(groups, rows int, overlap bool) []*ResolverEvidenceView {
	views := make([]*ResolverEvidenceView, (groups+15)/16)
	at := time.Unix(1700000000, 0).UTC()
	for source := range views {
		view := &ResolverEvidenceView{window: time.Minute}
		for candidate := source * 16; candidate < min(groups, (source+1)*16); candidate++ {
			raw := resolverEvidenceCandidate{call: fmt.Sprintf("K1A%02d", candidate)}
			for receiver := 0; receiver < rows; receiver++ {
				identity := candidate*rows + receiver
				if overlap {
					identity = receiver
				}
				raw.receivers = append(raw.receivers, ResolverReceiverObservation{Receiver: fmt.Sprintf("R%05d", identity),
					ObservedAt: at, FrequencyKHz: 7010 + float64(source)*.5, WeightMilli: 500 * (candidate % 3)})
			}
			view.candidates = append(view.candidates, raw)
			view.count += len(raw.receivers)
		}
		view.canonical = buildCanonicalResolverCandidates(view.candidates)
		views[source] = view
	}
	return views
}

func ccv2MapReceiverTotals(view *ResolverEvidenceView) (int, int, bool) {
	weights := make(map[string]int)
	conflict := false
	view.RangeCanonical(func(_, _ string, row ResolverReceiverObservation) bool {
		if previous, ok := weights[row.Receiver]; !ok || row.WeightMilli > previous {
			weights[row.Receiver] = row.WeightMilli
		}
		conflict = conflict || row.FrequencyConflict
		return true
	})
	weighted := 0
	for _, weight := range weights {
		weighted += weight
	}
	return len(weights), weighted, conflict
}

func TestCCV2SortedMergeManySemanticCandidates(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		t.Run(fmt.Sprint(overlap), func(t *testing.T) {
			views := ccv2DistinctCandidateViews(80, 64, overlap)
			ccv2CheckMergedPopulation(t, views)
			union := MergeResolverEvidenceViews(views...)
			want := 5120
			if overlap {
				want = 64
			}
			if count, _, _ := union.ReceiverTotals(); count != want || union.Len() != 5120 {
				t.Fatal("five-key raw/canonical/dissent population was recapped or overcounted")
			}
		})
	}
}

func TestCCV2ReceiverTotalsRaggedOverlapConflict(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	views := make([]*ResolverEvidenceView, 2)
	for source := range views {
		v := &ResolverEvidenceView{window: time.Minute}
		for candidate := source * 16; candidate < min(33, (source+1)*16); candidate++ {
			raw := resolverEvidenceCandidate{call: fmt.Sprintf("K1A%02d", candidate)}
			for receiver := 0; receiver <= candidate%5; receiver++ {
				name := fmt.Sprintf("R%05d", candidate*5+receiver)
				if receiver == 0 && candidate%3 != 2 {
					name = "A00000"
				}
				raw.receivers = append(raw.receivers, ResolverReceiverObservation{Receiver: name, ObservedAt: at,
					WeightMilli: 1000 * (candidate % 2), FrequencyConflict: candidate == 0})
			}
			v.candidates = append(v.candidates, raw)
			v.count += len(raw.receivers)
		}
		v.canonical = buildCanonicalResolverCandidates(v.candidates)
		views[source] = v
	}
	ccv2CheckMergedPopulation(t, views)
	if _, _, conflict := MergeResolverEvidenceViews(views...).ReceiverTotals(); !conflict {
		t.Fatal("higher-weight membership hid a different candidate's frequency conflict")
	}
}

func BenchmarkCCV2ReceiverTotals(b *testing.B) {
	for _, groups := range []int{2, 16, 80} {
		for _, overlap := range []bool{false, true} {
			view := MergeResolverEvidenceViews(ccv2DistinctCandidateViews(groups, 64, overlap)...)
			wantCount, wantWeight, wantConflict := ccv2MapReceiverTotals(view)
			for _, method := range []struct {
				name string
				run  func(*ResolverEvidenceView) (int, int, bool)
			}{{"sorted", (*ResolverEvidenceView).ReceiverTotals}, {"map", ccv2MapReceiverTotals}} {
				b.Run(fmt.Sprintf("groups%d/overlap%t/%s", groups, overlap, method.name), func(b *testing.B) {
					if count, weight, conflict := method.run(view); count != wantCount || weight != wantWeight || conflict != wantConflict {
						b.Fatal("population benchmark failed its set oracle")
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						method.run(view)
					}
				})
			}
		}
	}
}
