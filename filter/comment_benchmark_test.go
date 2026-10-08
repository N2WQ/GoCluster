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

// Synthetic sizes include the review's 65,500-byte input, independently of a
// concrete peer frame's smaller comment budget. Every fixture reaches COMMENT.
func BenchmarkCommentHostile(b *testing.B) {
	for _, shape := range []string{"prefix", "suffix", "periodic", "both-lists"} {
		for _, size := range []int{128, 1024, 65500} {
			b.Run(fmt.Sprintf("%s/bytes-%d", shape, size), func(b *testing.B) {
				f := NewFilter()
				f.Reset()
				s := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
				s.Comment = strings.Repeat("A", size)
				for i := range MaxCommentPhrases {
					phrase := strings.Repeat("A", 61) + fmt.Sprintf("%03d", i)
					switch shape {
					case "suffix":
						phrase = fmt.Sprintf("%03d", i) + strings.Repeat("A", 61)
					case "periodic":
						phrase = strings.Repeat("AB", 30) + "A" + fmt.Sprintf("%03d", i)
					case "both-lists":
						f.Comments = append(f.Comments, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i+32))
					}
					f.BlockComments = append(f.BlockComments, phrase)
				}
				if shape == "periodic" {
					s.Comment = strings.Repeat("AB", size/2)
				}
				want := shape != "both-lists"
				if got := f.Matches(s); got != want {
					b.Fatal("hostile fixture did not reach the expected COMMENT result")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if f.Matches(s) != want {
						b.Fatal("hostile result changed")
					}
				}
			})
		}
	}
}

func BenchmarkCommentCheapReject(b *testing.B) {
	for _, gate := range []string{"band", "mode"} {
		for _, size := range []int{128, 1024, 65500} {
			b.Run(fmt.Sprintf("%s/bytes-%d", gate, size), func(b *testing.B) {
				f := NewFilter()
				f.Reset()
				s := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
				for i := range MaxCommentPhrases {
					f.BlockComments = append(f.BlockComments, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i))
				}
				s.Comment = strings.Repeat("A", size)
				if !f.Matches(s) {
					b.Fatal("open-gate control must pass all comment rules")
				}
				if gate == "band" {
					f.SetBand("40m", true)
				} else {
					f.SetMode("FT8", true)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if f.Matches(s) {
						b.Fatal("cheap rejection failed")
					}
				}
			})
		}
	}
}

// Paired components expose the accepted extra ordinary-gate work when a short
// COMMENT would previously reject early. Fixtures are identical across components.
func BenchmarkCommentEarlyReject(b *testing.B) {
	for _, kind := range []string{"first-reject", "pass-miss"} {
		for _, component := range []string{"filter", "ordinary", "matcher"} {
			b.Run(kind+"/"+component, func(b *testing.B) {
				f := NewFilter()
				f.Reset()
				s := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
				s.Comment = "CQ POTA UP 5"
				phrase := "CQ"
				if kind == "pass-miss" {
					phrase = "SOTA"
				}
				if component == "filter" {
					if kind == "first-reject" {
						f.BlockComments = []string{phrase}
					} else {
						f.Comments = []string{phrase}
					}
				}
				evaluate := func() bool { return f.Matches(s) }
				want := component == "ordinary"
				if component == "matcher" {
					evaluate = func() bool { return MatchCommentPhrase(s.Comment, phrase) }
					want = kind == "first-reject"
				}
				if evaluate() != want {
					b.Fatal("invalid paired component fixture")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					if evaluate() != want {
						b.Fatal("paired result changed")
					}
				}
			})
		}
	}
}
