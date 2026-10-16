package spot

import (
	"fmt"
	"testing"
	"time"
)

// Common baseline/candidate benchmark: uses only preexisting resolver entry
// points. One operation is a complete guarded sweep, including forced due-state
// setup and population checks. Frozen observation/process clocks prevent expiry
// and idle retirement during calibration; expiry cases replenish outside timing.
func BenchmarkResolverPreparedSweep(b *testing.B) {
	for _, shape := range []struct {
		keys, candidates, reporters int
	}{
		{1, 1, 1}, {64, 2, 8}, {64, 16, 64}, {1000, 2, 8},
	} {
		name := fmt.Sprintf("keys%d_candidates%d_reporters%d", shape.keys, shape.candidates, shape.reporters)
		b.Run(name, func(b *testing.B) {
			r, states, now := newSweepBenchmarkPopulation(b, shape.keys, shape.candidates, shape.reporters)
			r.sweep(states, now) // Warm publication/scratch; not measured.
			previous := make(map[ResolverSignalKey]*ResolverSnapshot, len(states))
			marker := now.Add(-time.Hour)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, st := range states {
					previous[st.key] = st.cell.snapshot.Load()
					st.nextEvalAt, st.lastEvalAt, st.dirty = now, marker, true
				}
				r.sweep(states, now)
				checkSweepBenchmarkPopulation(b, states, previous, now, shape.keys, shape.candidates, shape.reporters)
			}
		})
	}
	b.Run("standalone_candidates2_reporters8", func(b *testing.B) {
		r, states, now := newSweepBenchmarkPopulation(b, 1, 2, 8)
		var st *resolverKeyState
		for _, value := range states {
			st = value
		}
		r.evaluateKey(st, now)
		previous := make(map[ResolverSignalKey]*ResolverSnapshot, 1)
		marker := now.Add(-time.Hour)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			previous[st.key] = st.cell.snapshot.Load()
			st.lastEvalAt = marker
			r.evaluateKey(st, now)
			checkSweepBenchmarkPopulation(b, states, previous, now, 1, 2, 8)
		}
	})
	b.Run("expiry_keys64_candidates2_reporters8", func(b *testing.B) {
		r, states, now := newSweepBenchmarkPopulation(b, 64, 2, 8)
		r.sweep(states, now)
		previous := make(map[ResolverSignalKey]*ResolverSnapshot, len(states))
		old := now.Add(-time.Minute - time.Nanosecond)
		marker := now.Add(-time.Hour)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			for _, st := range states {
				previous[st.key] = st.cell.snapshot.Load()
				for _, candidate := range st.candidates {
					upsertResolverReporter(st, candidate, "N0EXPIRED", old, 7010.1, 1000)
				}
				if st.reporterRefs["N0EXPIRED"].refCount != 2 {
					b.Fatal("expiry fixture was not fully replenished")
				}
				st.nextEvalAt, st.lastEvalAt, st.dirty = now, marker, true
			}
			b.StartTimer()
			r.sweep(states, now)
			checkSweepBenchmarkPopulation(b, states, previous, now, 64, 2, 8)
		}
	})
}

func newSweepBenchmarkPopulation(b *testing.B, keys, candidates, reporters int) (*SignalResolver, map[ResolverSignalKey]*resolverKeyState, time.Time) {
	b.Helper()
	r, d, _, now := lossTestDriver(b, SignalResolverConfig{MaxActiveKeys: keys, MaxCandidatesPerKey: candidates,
		MaxReportersPerCand: reporters + 1, InactiveTTL: time.Hour})
	for keyIndex := 0; keyIndex < keys; keyIndex++ {
		key := NewResolverSignalKey(7010+float64(keyIndex), "40m", "CW", 500)
		st := &resolverKeyState{key: key, recencyWindow: time.Minute, lastSeen: now, dirty: true,
			candidates: make(map[string]*resolverCandidate, candidates), reporterRefs: make(map[string]resolverReporterRef, candidates*reporters)}
		for candidateIndex := 0; candidateIndex < candidates; candidateIndex++ {
			call := fmt.Sprintf("DL%dABC", candidateIndex)
			candidate := &resolverCandidate{lastSeen: now, lastFreqKHz: 7010.1,
				reporters: make(map[string]resolverReporterObservation, reporters+1),
				identity:  normalizeCorrectionCallIdentity(call), callRunes: []rune(call)}
			for reporterIndex := 0; reporterIndex < reporters; reporterIndex++ {
				receiver := fmt.Sprintf("N%dR%d", candidateIndex, reporterIndex)
				upsertResolverReporter(st, candidate, receiver, now, 7010.1, 1000)
			}
			st.candidates[call] = candidate
		}
		d.states[key] = st
	}
	return r, d.states, now
}

func checkSweepBenchmarkPopulation(b *testing.B, states map[ResolverSignalKey]*resolverKeyState, previous map[ResolverSignalKey]*ResolverSnapshot, now time.Time, keys, candidates, reporters int) {
	// Called inside timing: identical guards are part of both measurements.
	if len(states) != keys {
		b.Fatal("sweep retired intended benchmark keys")
	}
	for _, st := range states {
		if st.lastEvalAt != now || len(st.candidates) != candidates || len(st.reporterRefs) != candidates*reporters {
			b.Fatal("sweep skipped evaluation or changed intended population")
		}
		for _, candidate := range st.candidates {
			if len(candidate.reporters) != reporters {
				b.Fatal("sweep lost intended reporter rows")
			}
		}
		s := st.cell.snapshot.Load()
		if s == nil || s == previous[st.key] || s.EvaluatedAt != now || s.TotalReporters != candidates*reporters ||
			s.TotalWeightedSupportMilli != candidates*reporters*1000 || len(s.CandidateRanks) != candidates ||
			s.ReceiverEvidence().Len() != candidates*reporters || s.EvidenceLoss {
			b.Fatal("sweep did not publish the complete intended population")
		}
	}
}
