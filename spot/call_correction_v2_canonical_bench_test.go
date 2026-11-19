package spot

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func ccv2FrequencyPublication(b testing.TB, source, rowsPerGroup int) (*ResolverEvidenceView, string, string) {
	b.Helper()
	at := time.Unix(1700000000, 0).UTC()
	center := 7010 + float64(source)*.5
	key := NewResolverSignalKey(center, "40m", "CW", 500)
	r := NewSignalResolver(SignalResolverConfig{MaxCandidatesPerKey: 16, MaxReportersPerCand: 64})
	st := &resolverKeyState{key: key, recencyWindow: time.Minute, lastSeen: at, candidates: make(map[string]*resolverCandidate), reporterRefs: make(map[string]resolverReporterRef)}
	identities := []string{"W1AW", "K1ABC"}
	for group, base := range identities {
		variants := []string{base}
		if rowsPerGroup == 512 {
			variants = []string{base + "/A/B/C", base + "/A/C/B", base + "/B/A/C", base + "/B/C/A", base + "/C/A/B", base + "/C/B/A", "A/" + base + "/B/C", "B/" + base + "/A/C"}
			identities[group] = CorrectionVoteKey(variants[0])
		}
		for variant, call := range variants {
			candidate := &resolverCandidate{lastSeen: at, identity: normalizeCorrectionCallIdentity(call), callRunes: []rune(call), reporters: make(map[string]resolverReporterObservation)}
			for receiver := 0; receiver < rowsPerGroup/len(variants); receiver++ {
				rx := fmt.Sprintf("R%d%d%d%02d", source, group, variant, receiver)
				candidate.reporters[rx] = resolverReporterObservation{observedAt: at, frequencyKHz: center + []float64{-.16, .16}[group]}
				st.reporterRefs[rx] = resolverReporterRef{refCount: 1, weightMilli: 1000}
			}
			st.candidates[call] = candidate
		}
	}
	r.evaluateKey(st, at)
	snap, ok := r.Lookup(key)
	if !ok || snap.ReceiverEvidence().Len() != 2*rowsPerGroup {
		b.Fatal("published guard fixture lost rows")
	}
	return snap.ReceiverEvidence(), identities[0], identities[1]
}

func BenchmarkCCV2CanonicalFrequencyShapes(b *testing.B) {
	for _, rows := range []int{3, 64, 512, 2560} {
		b.Run(fmt.Sprintf("rows_per_group=%d", rows), func(b *testing.B) {
			var view *ResolverEvidenceView
			var left, right string
			if rows == 2560 {
				views := make([]*ResolverEvidenceView, 5)
				for source := range views {
					views[source], left, right = ccv2FrequencyPublication(b, source, 512)
				}
				view = MergeResolverEvidenceViews(views...)
			} else {
				view, left, right = ccv2FrequencyPublication(b, 0, rows)
			}
			l, r := resolverFrequencyRows(view, left), resolverFrequencyRows(view, right)
			want := math.Inf(1)
			for _, a := range l {
				for _, c := range r {
					want = math.Min(want, math.Abs(a.FrequencyKHz-c.FrequencyKHz))
				}
			}
			a, c, ok := closestResolverFrequencyPair(view, left, right)
			if !ok || len(l) != rows || len(r) != rows || math.Abs(a-c) != want {
				b.Fatal("published frequency shape lacks its exhaustive witness")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				closestResolverFrequencyPair(view, left, right)
			}
		})
	}
}
