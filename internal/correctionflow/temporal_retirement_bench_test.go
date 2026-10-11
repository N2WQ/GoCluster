package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

// Isolate terminal reclamation's bounded pending scan from decoder scoring.
// Other-key requests force the last-owner case to inspect the full population.
func BenchmarkTemporalRetirementPendingScan(b *testing.B) {
	for _, population := range []int{1, 25000} {
		b.Run(fmt.Sprintf("pending_%d", population), func(b *testing.B) {
			d := temporalRetirementDecoder("abstain")
			d.cfg.MaxPending = population
			d.cfg.MaxActiveKeys = 2
			at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			other := key
			other.Bucket++
			if population > 1 {
				d.keyStates[other] = &temporalKeyState{}
			}
			for i := 1; i < population; i++ {
				d.requests[uint64(i)] = temporalRequest{id: uint64(i), key: other, observedAt: at}
			}
			id := uint64(population)
			st := &temporalKeyState{events: []temporalEvent{{id: 0, observedAt: at}}}
			req := temporalRequest{id: id, key: key, observedAt: at, evicted: true}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.requests[id], d.keyStates[key] = req, st
				if got := d.Evaluate(id, at, true); got.Reason != "event_evicted" {
					b.Fatalf("benchmark missed terminal path: %+v", got)
				}
			}
			b.StopTimer()
			if len(d.requests) != population-1 || d.keyStates[key] != nil || population > 1 && d.keyStates[other] == nil {
				b.Fatal("terminal scan lost other owners or retained target state")
			}
		})
	}
}
