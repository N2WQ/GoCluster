package correctionflow

import (
	"dxcluster/spot"
	"fmt"
	"testing"
)

func BenchmarkTemporalConfirmedHistory(b *testing.B) {
	for _, capacity := range []int{2, 6000} {
		b.Run(fmt.Sprintf("cap_%d_eviction", capacity), func(b *testing.B) {
			d := temporalConfirmationDecoder(capacity)
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			for i := 0; i < capacity; i++ {
				key.Bucket = int64(i)
				d.ConfirmCorrection(key, "K1ABC")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key.Bucket = int64(capacity + i)
				d.ConfirmCorrection(key, "K1ABC")
			}
			b.StopTimer()
			if m := d.ConfirmedHistoryMetrics(); m.Keys != capacity || m.Evictions != uint64(b.N) {
				b.Fatalf("historychurn=%+v", m)
			}
		})
	}
}
