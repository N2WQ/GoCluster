package spot

import (
	"fmt"
	"testing"
	"time"
)

// The actual checked-in asset exercises its admitted bins and character table.
// These shapes cover the model-enabled branch, unlike the legacy no-model
// publication/projection benchmarks retained for comparable baseline costs.
func BenchmarkCCV2ResolverShapes(b *testing.B) {
	model, err := LoadConfusionModel("../data/rbn/priors/confusion_model.json")
	if err != nil {
		b.Fatal(err)
	}
	for _, shape := range []struct {
		name                  string
		candidates, receivers int
	}{{"typical", 2, 3}, {"capped", 16, 64}} {
		for _, kind := range []string{"clean", "dirty", "partial_fresh"} {
			b.Run(shape.name+"/"+kind, func(b *testing.B) {
				r := NewSignalResolver(SignalResolverConfig{ConfusionModel: model, ConfusionWeight: 1, HysteresisWindows: 1, FreqGuardMinSeparationKHz: 1, MaxEditDistance: 3})
				at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
				key := NewResolverSignalKey(7010, "40m", "CW", 500)
				st := &resolverKeyState{key: key, recencyWindow: time.Minute, candidates: make(map[string]*resolverCandidate), reporterRefs: make(map[string]resolverReporterRef), lastSeen: at}
				for c := 0; c < shape.candidates; c++ {
					call := fmt.Sprintf("K1AA%c", 'A'+rune(c))
					candidate := &resolverCandidate{lastSeen: at, identity: normalizeCorrectionCallIdentity(call), callRunes: []rune(call), reporters: make(map[string]resolverReporterObservation)}
					for receiver := 0; receiver < shape.receivers; receiver++ {
						rx := fmt.Sprintf("N%04d", c*64+receiver)
						seen := at
						if kind == "partial_fresh" && receiver%2 == 0 {
							seen = at.Add(-31 * time.Second)
						}
						candidate.reporters[rx] = resolverReporterObservation{observedAt: seen, frequencyKHz: 7010, report: receiver % 36, hasReport: true}
						st.reporterRefs[rx] = resolverReporterRef{refCount: 1, weightMilli: 1000}
					}
					st.candidates[call] = candidate
				}
				// Every row is eligible at publication. The later read clock expires
				// only the older half without advancing/pruning the owner.
				r.attachKeyGeneration(st)
				r.evaluatePrunedKey(st, at)
				readAt := at.Add(30 * time.Second)
				if kind == "partial_fresh" {
					snap, ok := r.LookupEligible(key, readAt)
					if !ok || snap.TotalReporters != shape.candidates*(shape.receivers/2) {
						b.Fatal("partial model shape did not filter expected rows")
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if kind == "partial_fresh" {
						r.LookupEligible(key, readAt)
					} else {
						if kind == "dirty" {
							st.receiverEvidenceDirty = true
						}
						r.evaluatePrunedKey(st, at)
					}
				}
			})
		}
	}
}
