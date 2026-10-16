package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
	"dxcluster/spot"
)

// Both revisions run this complete flush against the same finite Pebble key set.
// Fixture construction is outside the timer; every persisted row is checked.
func BenchmarkEncodingScratchFlush(b *testing.B) {
	for _, mixed := range []bool{false, true} {
		kind := "normalized"
		if mixed {
			kind = "mixed"
		}
		for _, rows := range []int{1, 64, 3000} {
			b.Run(fmt.Sprintf("%s/rows_%d", kind, rows), func(b *testing.B) {
				w, err := NewWriter(config.ArchiveConfig{DBPath: filepath.Join(b.TempDir(), "archive"), Synchronous: "off"})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(w.Stop)
				batch := make([]*spot.Spot, rows)
				expected := make([][]byte, rows)
				for i := range batch {
					s := ownedFlushInput()
					s.ToxicityCategories = nil
					want := ownedFlushRecord()
					want.toxicityCategories = ""
					if mixed {
						s.Comment = fmt.Sprintf("row-%d:%s", i, strings.Repeat("x", []int{1, 4096, 17, 256}[i%4]))
						want.comment = s.Comment
					}
					s.EnsureNormalized()
					batch[i] = s.SealForAsync()
					expected[i] = ownedFlushExpectedBytes(want)
				}
				w.flush(batch)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					w.seq = 0
					w.flush(batch)
				}
				b.StopTimer()
				elapsed := b.Elapsed().Nanoseconds()
				if w.seq != uint32(rows) {
					b.Fatalf("flush wrote %d rows, want %d", w.seq, rows)
				}
				for i, s := range batch {
					raw, closer, err := w.db.Get(spotKeyBytes(normalizeUnixNano(s.Time), uint32(i+1)))
					if err != nil {
						b.Fatal(err)
					}
					equal := bytes.Equal(raw, expected[i])
					if err := closer.Close(); err != nil {
						b.Fatal(err)
					}
					if !equal {
						b.Fatalf("row %d persisted different bytes", i)
					}
				}
				b.ReportMetric(float64(rows), "rows/op")
				if output := os.Getenv("GOCLUSTER_P1_BENCH_RECEIPTS"); output != "" {
					// Each calibration replaces this case's receipt. The final
					// N/elapsed pair must match the completed benchmark's output.
					receipt := struct {
						Case      string `json:"case"`
						N         int    `json:"n"`
						ElapsedNS int64  `json:"elapsed_ns"`
					}{b.Name(), b.N, elapsed}
					data, err := json.Marshal(receipt)
					if err != nil {
						b.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(output, fmt.Sprintf("%s-%d.json", kind, rows)), data, 0o600); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
