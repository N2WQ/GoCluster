package cluster

import (
	"testing"
	"time"

	"dxcluster/spot"
)

// BenchmarkStabilizerPreStartAdmission isolates ordinary single-item admission.
// No scheduler runs: every successful reservation is consumed and retired here
// before the next iteration, so neither overflow nor growing backlog can make
// the terminal-fence comparison appear cheaper. The spot and capacities are
// fixed; the envelope allocation and reservation bookkeeping remain measured.
func BenchmarkStabilizerPreStartAdmission(b *testing.B) {
	s := newTelnetSpotStabilizer(time.Hour, 1024)
	sp := spot.NewSpot("K1ABC", "W1XYZ", 14030, "CW")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !s.Enqueue(sp) {
			b.Fatal("ordinary admission unexpectedly refused")
		}
		select {
		case envelope := <-s.in:
			if envelope == nil || envelope.spot != sp {
				b.Fatal("admission did not retain the owned spot")
			}
			s.pending.Add(-1)
		default:
			b.Fatal("successful admission did not publish its reservation")
		}
	}
	b.StopTimer()
	if s.Pending() != 0 || len(s.in) != 0 || len(s.release) != 0 {
		b.Fatal("benchmark failed to retire every measured reservation")
	}
}
