package archive

import (
	"fmt"
	"path/filepath"
	"testing"

	"dxcluster/config"
	"dxcluster/spot"
)

func BenchmarkOwnedFlush(b *testing.B) {
	for _, kind := range []string{"raw", "normalized", "categorized"} {
		for _, size := range []int{1, 64} {
			b.Run(fmt.Sprintf("%s/rows_%d", kind, size), func(b *testing.B) {
				w, err := NewWriter(config.ArchiveConfig{DBPath: filepath.Join(b.TempDir(), "archive.db"), Synchronous: "off"})
				if err != nil {
					b.Fatal(err)
				}
				defer w.Stop()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					batch := make([]*spot.Spot, size)
					for n := range batch {
						s := ownedFlushInput()
						if kind != "categorized" {
							s.ToxicityCategories = nil
						}
						if kind != "raw" {
							s.EnsureNormalized()
						}
						batch[n] = s.SealForAsync()
					}
					// Overwrite the same bounded set of keys each iteration; fixture
					// construction is excluded from worker flush cost on both versions.
					w.seq = 0
					b.StartTimer()
					w.flush(batch)
				}
				b.ReportMetric(float64(size), "rows/op")
			})
		}
	}
}
