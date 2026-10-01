package spot

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

type commentTaxonomyCase struct {
	name     string
	taxonomy *Taxonomy
}

func commentParserTaxonomies(t testing.TB) []commentTaxonomyCase {
	t.Helper()
	definitions := defaultTaxonomyFile()
	base, err := buildTaxonomy(definitions)
	if err != nil {
		t.Fatal(err)
	}
	definitions.Modes = append(definitions.Modes, ModeDefinition{Name: "AUDIT", CommentTokens: []string{
		"A", "AA", "DB", "WPM", "BPS", "I", "IC", "C", "W", "Q", "XAA",
	}})
	custom, err := buildTaxonomy(definitions)
	if err != nil {
		t.Fatal(err)
	}
	maximum := taxonomyFile{}
	for mode := 0; mode < 16; mode++ {
		definition := ModeDefinition{Name: fmt.Sprintf("AUDIT%d", mode)}
		for alias := 1; alias <= 32; alias++ {
			definition.CommentTokens = append(definition.CommentTokens, strings.Repeat("A", 32*mode+alias))
		}
		maximum.Modes = append(maximum.Modes, definition)
	}
	all, err := buildTaxonomy(maximum)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.modeCommentTokens) != 512 {
		t.Fatalf("maximum taxonomy contains %d aliases", len(all.modeCommentTokens))
	}
	return []commentTaxonomyCase{{"default", base}, {"custom", custom}, {"maximum", all}}
}

func useCommentTaxonomy(t testing.TB, taxonomy *Taxonomy) {
	t.Helper()
	previous := CurrentTaxonomy()
	ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { ConfigureTaxonomy(previous) })
}

func TestCommentParserUnicodeCoordinates(t *testing.T) {
	taxonomies := commentParserTaxonomies(t)
	cases := []struct {
		name    string
		comment string
		custom  bool
		want    CommentParseResult
	}{
		{"shifted_token", "ııı XX CW", false, CommentParseResult{Mode: "CW", Comment: "ııı CW"}},
		{"suppressed_suffix", "ıııı XX XAA", true, CommentParseResult{Mode: "AUDIT", Comment: "ıııı XX"}},
		{"expanded_width", "ȿ CW", false, CommentParseResult{Mode: "CW", Comment: "ȿ"}},
		{"unchanged_width", "ÿ CW", false, CommentParseResult{Mode: "CW", Comment: "ÿ"}},
		{"ligature", "ﬃ CW", false, CommentParseResult{Mode: "CW", Comment: "ﬃ"}},
		{"invalid_utf8", "i\xff CW", false, CommentParseResult{Mode: "CW", Comment: "i\xff"}},
		{"builtin_db", "+3 DB CW", true, CommentParseResult{Mode: "CW", Report: 3, HasReport: true}},
		{"builtin_wpm", "20 WPM CW", true, CommentParseResult{Mode: "CW", Comment: "20 WPM"}},
		{"builtin_bps", "45 BPS RTTY", true, CommentParseResult{Mode: "RTTY", Comment: "45 BPS"}},
		{"time_event_report", "1200ZFT8 POTA 10 dB", true, CommentParseResult{
			Mode: "FT8", Report: 10, HasReport: true, TimeToken: "1200Z", Comment: "POTA", Events: EventPOTA,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taxonomy := taxonomies[0].taxonomy
			if tc.custom {
				taxonomy = taxonomies[1].taxonomy
			}
			useCommentTaxonomy(t, taxonomy)
			if got := ParseSpotComment(tc.comment, 14020); got != tc.want {
				t.Fatalf("comment %q: got %+v, want %+v", tc.comment, got, tc.want)
			}
		})
	}
}

func TestCommentParserLegacyDifferential(t *testing.T) {
	seeds := []string{
		"", " \t ", ",;:!.", "ÿ CW", "ı CW", "ﬃ CW", "ȿ CW", "ı DB CW", "ı!DB CW",
		"ı CW FT8", "ı 1200ZCW", "ÿ 1200ZCW", "ﬃ 1200ZCW", "ȿ 1200ZCW", "ſS CW", "ß CW",
		"CQ CW", "CQ +12dB CW", "1200ZFT8 POTA 10 dB", "i\xff CW", "ı XXCW", "ı A ACW",
		"ııı XX CW", "ıııı XX XAA", "FT8 73 88 CQ", "RTTY 45 BPS TEST", "SSB CQ", "\u00a0CW\u00a0",
	}
	alphabet := []string{"ı", "ȿ", "ÿ", "ﬃ", "ſ", "ß", " ", "\t", ",", "1200Z", "CW", "DB", "FT8", "A", "AA", "X", "CWT", "BPS", "+3", "\xff"}
	frequencies := []float64{0, 7074, 14020, math.NaN(), math.Inf(1)}
	for _, configuration := range commentParserTaxonomies(t) {
		t.Run(configuration.name, func(t *testing.T) {
			useCommentTaxonomy(t, configuration.taxonomy)
			for _, comment := range seeds {
				for _, frequency := range frequencies {
					checkCommentParserDifferential(t, comment, frequency)
				}
			}
			var state uint64 = 1
			for n := 0; n < 4096; n++ {
				var text strings.Builder
				for word := 0; word < 12; word++ {
					state = state*6364136223846793005 + 1
					text.WriteString(alphabet[(state>>32)%uint64(len(alphabet))])
				}
				checkCommentParserDifferential(t, text.String(), frequencies[n%len(frequencies)])
			}
		})
	}
}

func checkCommentParserDifferential(t testing.TB, comment string, frequency float64) {
	t.Helper()
	want := legacyCommentParse(comment, frequency)
	if got := ParseSpotComment(comment, frequency); got != want {
		t.Fatalf("comment bytes=%d prefix=%q frequency=%v: got %+v, legacy %+v", len(comment), comment[:min(len(comment), 128)], frequency, got, want)
	}
}

func TestCommentCursorTaxonomyVisibility(t *testing.T) {
	configurations := commentParserTaxonomies(t)
	useCommentTaxonomy(t, configurations[0].taxonomy)
	cursor := acCursor{scanner: getKeywordScanner(), text: "CW XAA"}
	ConfigureTaxonomy(configurations[1].taxonomy)
	// Global matching still uses the original scanner. A suffix-only token lookup
	// would incorrectly select AUDIT's C or W aliases here.
	first := tokenizeComment("CW")[0]
	if got, ok := classifyTokenWithFallback(&cursor, first); !ok || got.kind != acTokenMode || got.mode != "CW" {
		t.Fatalf("initial scanner snapshot lost: %+v %v", got, ok)
	}
	second := tokenizeComment("CW XAA")[1]
	if got, ok := classifyTokenWithFallback(&cursor, second); !ok || got.kind != acTokenMode || got.mode != "AUDIT" {
		t.Fatalf("fallback did not observe current taxonomy: %+v %v", got, ok)
	}
}

func TestCommentTokenPreallocation(t *testing.T) {
	cases := []string{"", " \t", ",;:!.", "CW\u00a0FT8", "\u00a0CW\u00a0", "1200ZFT8 -4 DB", "i\xff CW", "ııı XX CW", strings.Repeat("A ", 32768)}
	for _, comment := range cases {
		got := tokenizeComment(comment)
		want := legacyCommentTokenize(comment)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("token metadata changed for %d-byte comment", len(comment))
		}
		if cap(got) != len(got) {
			t.Fatalf("token growth backing retained: len=%d cap=%d", len(got), cap(got))
		}
	}
	if unsafe.Sizeof(commentToken{}) > 80 {
		t.Fatalf("token exceeds the allocation proof: size=%d", unsafe.Sizeof(commentToken{}))
	}
}

func TestCommentCursorNoAllocation(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[1].taxonomy)
	comment := "ı XAA"
	token := tokenizeComment(comment)[1]
	text := strings.ToUpper(comment)
	var pattern acPattern
	var matched bool
	allocations := testing.AllocsPerRun(100, func() {
		cursor := acCursor{scanner: getKeywordScanner(), text: text}
		pattern, matched = classifyTokenWithFallback(&cursor, token)
	})
	if !matched || pattern.mode != "AUDIT" || allocations != 0 {
		t.Fatalf("cursor/fallback result=%+v matched=%v allocations=%v", pattern, matched, allocations)
	}
}

func TestCommentParserConcurrentImmutableScanner(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[1].taxonomy)
	inputs := []string{"ııı XX CW", "ıııı XX XAA", "1200ZFT8 POTA 10 dB", "RTTY 45 BPS TEST", "i\xff CW"}
	want := make([]CommentParseResult, len(inputs))
	for i, input := range inputs {
		want[i] = legacyCommentParse(input, 14020)
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				for i, input := range inputs {
					if got := ParseSpotComment(input, 14020); got != want[i] {
						t.Errorf("concurrent result changed: %+v", got)
						return
					}
				}
			}
		}()
	}
	workers.Wait()
}

func TestCommentParserAllocationEnvelope(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[2].taxonomy)
	cases := []struct{ name, input string }{
		{"single_alias", strings.Repeat("A", 65536)},
		{"dense_alias", strings.Repeat("A ", 32768)},
		{"dense_lowercase", strings.Repeat("a ", 32768)},
		{"dense_invalid_utf8", strings.Repeat("\xff ", 32768)},
		{"single_invalid_utf8", strings.Repeat("\xff", 65536)},
		{"peeled_invalid_utf8", "1200Z" + strings.Repeat("\xff", 65531)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkCommentParserDifferential(t, tc.input, 14020)
			// TotalAlloc intentionally includes all allocation generations, a stronger
			// diagnostic than post-GC retained samples. The peer budget additionally
			// accounts for wire parsing, callback formatting and queued owners. This
			// measured check supplements, rather than replaces, its ownership proof.
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result := ParseSpotComment(tc.input, 14020)
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(result)
			allocated := after.TotalAlloc - before.TotalAlloc
			allowance := uint64(64<<10 + 56*len(tc.input) + 128*commentTokenCount(tc.input))
			if allocated > allowance {
				t.Fatalf("allocated=%d exceeds comment envelope=%d", allocated, allowance)
			}
			t.Logf("input=%d tokens=%d TotalAlloc=%d allowance=%d", len(tc.input), commentTokenCount(tc.input), allocated, allowance)
		})
	}
}
