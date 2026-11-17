package spot

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestCCV2CanonicalFrequencyWitnessOracle(t *testing.T) {
	// This is a binary64 geometry oracle, not a frequency-to-key admission
	// test. Explicit distinct key fixtures isolate exact .3 boundaries without
	// rounding them through RF bucket arithmetic or integer Hz coordinates.
	at := time.Unix(1700000000, 0).UTC()
	variants := func(base string) []string {
		return []string{base + "/A/B/C", base + "/A/C/B", base + "/B/A/C", base + "/B/C/A", base + "/C/A/B", base + "/C/B/A", "A/" + base + "/B/C", "B/" + base + "/A/C"}
	}
	for _, keys := range []int{1, 5} {
		for _, geometry := range []struct {
			right    float64
			shuffled bool
		}{
			{math.Nextafter(.6, math.Inf(-1)), false}, {.6, false}, {math.Nextafter(.6, math.Inf(1)), false}, {.6, true},
		} {
			t.Run(fmt.Sprintf("keys%d/right%x/shuffled%t", keys, math.Float64bits(geometry.right), geometry.shuffled), func(t *testing.T) {
				resolver := NewSignalResolver(SignalResolverConfig{MaxActiveKeys: 5, MaxCandidatesPerKey: 16, MaxReportersPerCand: 64})
				views := make([]*ResolverEvidenceView, 0, keys)
				var leftWitnesses, rightWitnesses []float64
				for source := 0; source < keys; source++ {
					key := ResolverSignalKey{Band: "40m", Mode: "CW", ToleranceHz: 500, Bucket: int64(source)}
					st := &resolverKeyState{key: key, recencyWindow: time.Minute, candidates: make(map[string]*resolverCandidate), reporterRefs: make(map[string]resolverReporterRef)}
					for group, base := range []string{"W1AW", "K1ABC"} {
						for variant, call := range variants(base) {
							candidate := &resolverCandidate{lastSeen: at, identity: normalizeCorrectionCallIdentity(call), callRunes: []rune(call), reporters: make(map[string]resolverReporterObservation)}
							for receiver := 0; receiver < 64; receiver++ {
								frequency := .3
								if group == 1 {
									frequency = geometry.right
								}
								if geometry.shuffled {
									index := source*512 + variant*64 + receiver
									if group == 0 {
										frequency = 10 + float64((index*137)%(keys*512))*.01
										if index == 137 {
											frequency = 12.3456
										}
									} else {
										frequency = 10.005 + float64((index*73)%(keys*512))*.01
										if index == 233 {
											frequency = 12.3457
										}
									}
								}
								// All rows are distinct receiver incidences. Repeated equal
								// distances intentionally leave witness-pair choice free.
								rx := fmt.Sprintf("R%d%d%d%02d", source, group, variant, receiver)
								candidate.reporters[rx] = resolverReporterObservation{observedAt: at, frequencyKHz: frequency}
								st.reporterRefs[rx] = resolverReporterRef{refCount: 1, weightMilli: 1000}
								if group == 0 {
									leftWitnesses = append(leftWitnesses, frequency)
								} else {
									rightWitnesses = append(rightWitnesses, frequency)
								}
							}
							st.candidates[call] = candidate
						}
					}
					if len(st.candidates) != 16 {
						t.Fatal("oracle exceeded raw spelling cap")
					}
					resolver.evaluateKey(st, at)
					snapshot, ok := resolver.Lookup(key)
					if !ok || snapshot.ReceiverEvidence().Len() != 1024 || len(snapshot.CandidateRanks) != 2 {
						t.Fatal("oracle publication did not retain capped population")
					}
					views = append(views, snapshot.ReceiverEvidence())
				}
				view := views[0]
				if keys == 5 {
					view = MergeResolverEvidenceViews(views...)
				}
				leftCall, rightCall := CorrectionVoteKey("W1AW/A/B/C"), CorrectionVoteKey("K1ABC/A/B/C")
				leftRows, rightRows := resolverFrequencyRows(view, leftCall), resolverFrequencyRows(view, rightCall)
				if len(leftRows) != keys*512 || len(rightRows) != keys*512 || len(leftRows)*len(rightRows) <= 4096 {
					t.Fatal("oracle missed large sorted branch")
				}
				want := math.Inf(1)
				// Independent exhaustive reference; never calls the production
				// two-pointer scan or quantizes an original binary64 observation.
				for _, a := range leftWitnesses {
					for _, b := range rightWitnesses {
						if separation := math.Abs(a - b); separation < want {
							want = separation
						}
					}
				}
				if geometry.shuffled && math.Abs(leftWitnesses[0]-rightWitnesses[0]) <= want {
					t.Fatal("shuffled oracle accidentally permits a first-pair implementation")
				}
				for repeat := 0; repeat < 3; repeat++ {
					a, b, ok := closestResolverFrequencyPair(view, leftCall, rightCall)
					if !ok || math.Abs(a-b) != want {
						t.Fatalf("closest separation %.20g want %.20g", math.Abs(a-b), want)
					}
					in := ResolverAmbiguityInput{WinnerSupport: keys * 512, RunnerSupport: keys * 512, WinnerFrequencyKHz: a, RunnerFrequencyKHz: b, MinSeparationKHz: .3, RunnerUpRatio: .6, Distance: 1, MaxEditDistance: 3}
					if got := RejectResolverAmbiguity(in); got != (want < .3) {
						t.Fatalf("strict boundary verdict=%t separation %.20g", got, want)
					}
				}
			})
		}
	}
}
