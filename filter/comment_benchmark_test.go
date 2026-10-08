package filter

import (
	"fmt"
	"strings"
	"testing"

	"dxcluster/spot"
)

// Measure the actual filter consumer, including its normal gates. Preparation
// stays outside the timed fan-out shape; correctness guards prevent a cheap
// early rejection from masquerading as successful full-list matching.
func BenchmarkCommentFilter(b *testing.B) {
	for _, count := range []int{0, 1, MaxCommentPhrases} {
		b.Run(fmt.Sprintf("phrases-%d", count), func(b *testing.B) {
			f := NewFilter()
			f.Reset()
			sp := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
			sp.Comment = strings.Repeat("x", 128)
			for i := range count {
				f.Comments = append(f.Comments, fmt.Sprintf("needle-%02d", i))
				f.BlockComments = append(f.BlockComments, fmt.Sprintf("excluded-%02d", i))
			}
			if count > 0 {
				sp.Comment += " " + f.Comments[count-1]
			}
			if !f.Matches(sp) {
				b.Fatal("benchmark fixture must pass all filter gates")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if !f.Matches(sp) {
					b.Fatal("full matching result changed")
				}
			}
		})
	}
}
