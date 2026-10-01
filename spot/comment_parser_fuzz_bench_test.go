package spot

import (
	"runtime"
	"strings"
	"testing"
)

func FuzzCommentParserLegacyDifferential(f *testing.F) {
	useCommentTaxonomy(f, commentParserTaxonomies(f)[1].taxonomy)
	for _, seed := range []string{
		"ııı XX CW", "ıııı XX XAA", "ȿ CW", "\xff CW", "1200ZFT8 POTA 10 dB",
		"20 WPM CW", "45 BPS RTTY", "SSB -4 73 88 CQ", " \t,;:!.\t ",
		strings.Repeat("A ", 256), strings.Repeat("A", 1024),
	} {
		f.Add(seed, 14020.0)
	}
	f.Fuzz(func(t *testing.T, comment string, frequency float64) {
		// The differential oracle intentionally retains the old allocation-heavy
		// algorithm. Bound its input to the admitted peer envelope; larger shared
		// callers retain the same parser semantics without a new runtime limit.
		if len(comment) > 65536 {
			t.Skip()
		}
		checkCommentParserDifferential(t, comment, frequency)
	})
}

func BenchmarkCommentParserVariants(b *testing.B) {
	taxonomies := commentParserTaxonomies(b)
	cases := []struct {
		name     string
		input    string
		taxonomy *Taxonomy
	}{
		{"Ordinary", "FT8 -12 dB POTA-1234 SOTA-ABC CQ TEST", taxonomies[0].taxonomy},
		{"Unicode", "ıııı XX XAA 1200ZFT8 POTA 10 dB", taxonomies[1].taxonomy},
		{"DenseAlias", strings.Repeat("A ", 2048), taxonomies[2].taxonomy},
		{"SingleAlias", strings.Repeat("A", 4096), taxonomies[2].taxonomy},
		{"MaximumDense", strings.Repeat("\xff ", 32768), taxonomies[2].taxonomy},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			useCommentTaxonomy(b, tc.taxonomy)
			want := legacyCommentParse(tc.input, 14020)
			for _, variant := range []struct {
				name  string
				parse func(string, float64) CommentParseResult
			}{{"Legacy", legacyCommentParse}, {"Streaming", ParseSpotComment}} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(len(tc.input)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						got := variant.parse(tc.input, 14020)
						if got != want {
							b.Fatalf("benchmark changed parser result: %+v, want %+v", got, want)
						}
						runtime.KeepAlive(got)
					}
				})
			}
		})
	}
}
