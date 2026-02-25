package spot

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkSignalResolverEvaluate(b *testing.B) {
	resolver := NewSignalResolver(SignalResolverConfig{
		QueueSize:                 64,
		MaxActiveKeys:             32,
		MaxCandidatesPerKey:       8,
		MaxReportersPerCand:       32,
		InactiveTTL:               time.Minute,
		EvalMinInterval:           5 * time.Millisecond,
		SweepInterval:             5 * time.Millisecond,
		HysteresisWindows:         2,
		FreqGuardMinSeparationKHz: 1.0,
		FreqGuardRunnerUpRatio:    0.6,
		MaxEditDistance:           3,
		DistanceModelCW:           "morse",
		DistanceModelRTTY:         "baudot",
	})
	key := NewResolverSignalKey(7010.0, "40m", "CW", 500)
	st := &resolverKeyState{
		key:           key,
		recencyWindow: 30 * time.Second,
		candidates:    make(map[string]*resolverCandidate, 8),
		reporterRefs:  make(map[string]int, 8*32),
		lastSeen:      time.Now().UTC(),
	}
	now := time.Now().UTC()
	for candidateIdx := 0; candidateIdx < 8; candidateIdx++ {
		call := fmt.Sprintf("DL6L%c", 'A'+candidateIdx)
		reporters := make(map[string]time.Time, 32)
		for reporterIdx := 0; reporterIdx < 32; reporterIdx++ {
			reporter := fmt.Sprintf("K1%03d", candidateIdx*100+reporterIdx)
			reporters[reporter] = now
			st.reporterRefs[reporter]++
		}
		st.candidates[call] = &resolverCandidate{
			lastSeen:    now,
			lastFreqKHz: 7010.0 + float64(candidateIdx)*0.1,
			reporters:   reporters,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolver.evaluateKey(st, now)
	}
}
