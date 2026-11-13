package spot

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type humanCommentCase struct {
	name, input string
	want        CommentParseResult
}

func humanCommentCases() []humanCommentCase {
	return []humanCommentCase{
		{"empty", " \t ", CommentParseResult{}},
		{"split_unsigned", "CW UP 2", CommentParseResult{Mode: "CW", Comment: "UP 2"}},
		{"split_signed", "CW UP -2", CommentParseResult{Mode: "CW", Comment: "UP -2"}},
		{"number_before_mode", "60 CW Why not?", CommentParseResult{Mode: "CW", Comment: "60 Why not?"}},
		{"frequency_request", "pse 14 cw", CommentParseResult{Mode: "CW", Comment: "pse 14"}},
		{"ft8", "FT8 -14 PLS EU", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "PLS EU"}},
		{"ft4", "FT4 +02 CQ", CommentParseResult{Mode: "FT4", Report: 2, HasReport: true, Comment: "CQ"}},
		{"ft8_alias", "ft-8 -14", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true}},
		{"ft4_alias", "FT-4 +02", CommentParseResult{Mode: "FT4", Report: 2, HasReport: true}},
		{"negative_zero", "FT8 -00", CommentParseResult{Mode: "FT8", HasReport: true}},
		{"positive_zero", "FT4 +0", CommentParseResult{Mode: "FT4", HasReport: true}},
		{"minimum", "FT8 -30", CommentParseResult{Mode: "FT8", Report: -30, HasReport: true}},
		{"maximum", "FT4 +30", CommentParseResult{Mode: "FT4", Report: 30, HasReport: true}},
		{"below_minimum", "FT8 -31", CommentParseResult{Mode: "FT8", Comment: "-31"}},
		{"above_maximum", "FT4 +31", CommentParseResult{Mode: "FT4", Comment: "+31"}},
		{"unsigned", "FT8 14", CommentParseResult{Mode: "FT8", Comment: "14"}},
		{"decimal", "FT8 +2.0", CommentParseResult{Mode: "FT8", Comment: "+2.0"}},
		{"two_signs", "FT8 +-2", CommentParseResult{Mode: "FT8", Comment: "+-2"}},
		{"overflow", "FT8 +99999999999999999999999999999", CommentParseResult{Mode: "FT8", Comment: "+99999999999999999999999999999"}},
		{"interrupted", "FT8 CQing +2 in NC", CommentParseResult{Mode: "FT8", Comment: "CQing +2 in NC"}},
		{"sent_report", "FT8 Sent: -02", CommentParseResult{Mode: "FT8", Comment: "Sent -02"}},
		{"number_before_ft8", "2 FT8 -14", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "2"}},
		{"report_before_ft8", "-14 FT8", CommentParseResult{Mode: "FT8", Comment: "-14"}},
		{"first_mode", "CW FT8 -14", CommentParseResult{Mode: "CW", Comment: "FT8 -14"}},
		{"second_mode_not_adjacent", "FT8 CQ FT4 -14", CommentParseResult{Mode: "FT8", Comment: "CQ FT4 -14"}},
		{"multiple_modes", "FT8 -14 FT4 +02", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "FT4 +02"}},
		{"other_digital", "FT2 -12", CommentParseResult{Mode: "FT2", Comment: "-12"}},
		{"qrt_time", "QRT 2200Z", CommentParseResult{Comment: "QRT 2200Z"}},
		{"multiple_times", "CW 1200Z 1800Z", CommentParseResult{Mode: "CW", Comment: "1200Z 1800Z"}},
		{"fused_grid", "CW 1200ZIO87", CommentParseResult{Mode: "CW", Comment: "1200ZIO87"}},
		{"fused_mode_not_standalone", "1200ZFT8 -14", CommentParseResult{Mode: "FT8", Comment: "1200ZFT8 -14"}},
		{"fused_number_not_signed_token", "FT8 1200Z-14", CommentParseResult{Mode: "FT8", Comment: "1200Z-14"}},
		{"shifted_scanner_not_source_mode", "ıııııııı FOO -14 FT8", CommentParseResult{Mode: "FT8", Comment: "ıııııııı -14 FT8"}},
		{"fused_explicit_report", "1200Z-14dB CQ", CommentParseResult{Report: -14, HasReport: true, Comment: "1200Z CQ"}},
		{"fused_split_report", "1200Z-14 dB CQ", CommentParseResult{Report: -14, HasReport: true, Comment: "1200Z CQ"}},
		{"cwt", "CWOps - CWT", CommentParseResult{Mode: "CW", Comment: "CWOps - CWT"}},
		{"cwt_number", "CWT +2", CommentParseResult{Mode: "CW", Comment: "CWT +2"}},
		{"speed_order", "CQ CW 20 WPM POTA", CommentParseResult{Mode: "CW", Comment: "CQ 20 WPM POTA", Events: EventPOTA}},
		{"bps", "RTTY 45 BPS TEST", CommentParseResult{Mode: "RTTY", Comment: "45 BPS TEST"}},
		{"explicit_outside_bare_range", "FT8 +31 dB", CommentParseResult{Mode: "FT8", Report: 31, HasReport: true}},
		{"explicit_before_bare", "+12dB FT8 -14", CommentParseResult{Mode: "FT8", Report: 12, HasReport: true, Comment: "-14"}},
		{"explicit_after_bare", "FT8 -14 +31dB", CommentParseResult{Mode: "FT8", Report: 31, HasReport: true, Comment: "-14"}},
		{"embedded_report_is_text", "FT8 -14 label=+31dB", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "label=+31dB"}},
		{"speed_report_state", "20 WPM -12 dB", CommentParseResult{Report: -12, HasReport: true, Comment: "20 WPM"}},
		{"first_time_report_state", "20 1200Z -12 dB", CommentParseResult{Report: -12, HasReport: true, Comment: "20 1200Z"}},
		{"second_time_report_state", "1000Z 20 1200Z -12 dB", CommentParseResult{Report: -12, HasReport: true, Comment: "1000Z 20 1200Z"}},
		{"nearest_number", "UP 2 -12 dB", CommentParseResult{Report: -12, HasReport: true, Comment: "UP 2"}},
		{"interrupted_db", "-12 PLS dB", CommentParseResult{Comment: "-12 PLS dB"}},
		{"punctuation_interrupts_db", "-12 ... dB", CommentParseResult{Comment: "-12 dB"}},
		{"orphan_db", "dB CQ", CommentParseResult{Comment: "dB CQ"}},
		{"unsigned_explicit", "12 DB", CommentParseResult{Report: 12, HasReport: true}},
		{"explicit_zero", "-0dB", CommentParseResult{HasReport: true}},
		{"explicit_minimum", "-200dB", CommentParseResult{Report: -200, HasReport: true}},
		{"explicit_maximum", "+200 dB", CommentParseResult{Report: 200, HasReport: true}},
		{"explicit_below_minimum", "-201dB", CommentParseResult{Comment: "-201dB"}},
		{"explicit_above_maximum", "+201 dB", CommentParseResult{Comment: "+201 dB"}},
		{"no_numeric_suffix", "1000dB", CommentParseResult{Comment: "1000dB"}},
		{"explicit_overflow", "+999999999999999999999999dB", CommentParseResult{Comment: "+999999999999999999999999dB"}},
		{"explicit_decimal", "-12.5 dB", CommentParseResult{Comment: "-12.5 dB"}},
		{"explicit_unit_suffix", "-12dBm", CommentParseResult{Comment: "-12dBm"}},
		{"s9_separate", "S9 +10 dB", CommentParseResult{Comment: "S9 +10 dB"}},
		{"s9_compact", "S9+10dB", CommentParseResult{Comment: "S9+10dB"}},
		{"s9_case_punctuation", "s9: +10DB", CommentParseResult{Comment: "s9 +10DB"}},
		{"59_compact", "59+20dB", CommentParseResult{Comment: "59+20dB"}},
		{"59_separate", "59 +20 dB", CommentParseResult{Comment: "59 +20 dB"}},
		{"strength_in_ft8", "FT8 S9 +10 dB", CommentParseResult{Mode: "FT8", Comment: "S9 +10 dB"}},
		{"strength_then_valid", "S9 +10 dB -12dB", CommentParseResult{Report: -12, HasReport: true, Comment: "S9 +10 dB"}},
		{"first_explicit_wins", "-12dB -8 dB", CommentParseResult{Report: -12, HasReport: true, Comment: "-8 dB"}},
		{"speed_after_report", "-12 dB WPM", CommentParseResult{Report: -12, HasReport: true, Comment: "WPM"}},
		{"fused_after_other_time", "1000Z 1200Z-14dB", CommentParseResult{Report: -14, HasReport: true, Comment: "1000Z 1200Z"}},
		{"invalid_then_shorthand", "FT8 -14 +201dB", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "+201dB"}},
		{"normalization", " \tCW \tUP 2., 2200Z\t ", CommentParseResult{Mode: "CW", Comment: "UP 2 2200Z"}},
		{"events", "FT8 -14 POTA-1234 SOTA-ABC", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "POTA-1234 SOTA-ABC", Events: EventPOTA | EventSOTA}},
	}
}

func TestParseHumanSpotCommentContract(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[0].taxonomy)
	for _, tc := range humanCommentCases() {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseHumanSpotComment(tc.input, 14074); got != tc.want {
				t.Fatalf("input=%q: got %+v, want %+v", tc.input, got, tc.want)
			}
		})
	}
}

func TestHumanCommentTaxonomyPermissionAndAliases(t *testing.T) {
	definitions := defaultTaxonomyFile()
	for index := range definitions.Modes {
		if definitions.Modes[index].Name == "FT8" {
			definitions.Modes[index].CommentTokens = append(definitions.Modes[index].CommentTokens, "HUMANFT")
			definitions.Modes[index].BareNumericReport = false
		}
	}
	taxonomy, err := buildTaxonomy(definitions)
	if err != nil {
		t.Fatal(err)
	}
	useCommentTaxonomy(t, taxonomy)
	for _, tc := range []humanCommentCase{
		{"configured_alias_disabled", "HUMANFT -14", CommentParseResult{Mode: "FT8", Comment: "-14"}},
		{"explicit_still_allowed", "HUMANFT -14 dB", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true}},
		{"other_mode_permission", "FT4 -14", CommentParseResult{Mode: "FT4", Report: -14, HasReport: true}},
	} {
		if got := ParseHumanSpotComment(tc.input, 14074); got != tc.want {
			t.Fatalf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
	for index := range definitions.Modes {
		if definitions.Modes[index].Name == "FT8" {
			definitions.Modes[index].BareNumericReport = true
		}
	}
	taxonomy, err = buildTaxonomy(definitions)
	if err != nil {
		t.Fatal(err)
	}
	ConfigureTaxonomy(taxonomy)
	if got := ParseHumanSpotComment("HUMANFT -14", 14074); got != (CommentParseResult{Mode: "FT8", Report: -14, HasReport: true}) {
		t.Fatalf("configured alias ignored: %+v", got)
	}
}

func TestHumanCommentExplicitDBCompatibility(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[0].taxonomy)
	for _, input := range []string{
		"CW +5 dB CQ", "FT8 -14dB POTA", "RTTY 45 BPS -12 dB TEST", "20 WPM -12 dB",
		"1000Z 20 1200Z -12 dB", "FT8 +31 dB", "1200Z-14dB CQ",
	} {
		want := legacyCommentParse(input, 14074)
		got := ParseHumanSpotComment(input, 14074)
		if got.Mode != want.Mode || got.Report != want.Report || got.HasReport != want.HasReport || got.Events != want.Events || got.TimeToken != "" {
			t.Fatalf("input=%q: human=%+v legacy=%+v", input, got, want)
		}
	}
}

func TestHumanCommentConcurrentImmutableScanner(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[0].taxonomy)
	cases := humanCommentCases()
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 16; iteration++ {
				for _, tc := range cases {
					if got := ParseHumanSpotComment(tc.input, 14074); got != tc.want {
						t.Errorf("concurrent %s: got %+v, want %+v", tc.name, got, tc.want)
						return
					}
				}
			}
		}()
	}
	workers.Wait()
}

func TestHumanCommentSourceAliasEvidence(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[0].taxonomy)
	// Existing legacy Unicode offsets land FOO's span on a later FT8 match.
	// Preserve that separate compatibility boundary, but never promote its
	// adjacent number into new human shorthand evidence.
	got := ParseHumanSpotComment("ıııııııı FOO -14 FT8", 14074)
	if got.Mode != "FT8" || got.HasReport || !strings.Contains(got.Comment, "-14") {
		t.Fatalf("a shifted match manufactured a human report: %+v", got)
	}
}

func TestHumanCommentAllocationEnvelope(t *testing.T) {
	useCommentTaxonomy(t, commentParserTaxonomies(t)[0].taxonomy)
	input := "CW " + strings.Repeat("1200Z 20 WPM ", 4096)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := ParseHumanSpotComment(input, 14074)
	runtime.ReadMemStats(&after)
	if got.HasReport || got.Comment != strings.TrimPrefix(strings.TrimSpace(input), "CW ") {
		t.Fatal("allocation witness did not preserve its dense human comment")
	}
	// As for the legacy envelope, total allocated bytes include every scratch
	// generation. This supplements the explicit input-sized token/mask bound.
	allowance := uint64(64<<10 + 56*len(input) + 128*commentTokenCount(input))
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > allowance {
		t.Fatalf("human allocation=%d exceeds input-sized envelope=%d", allocated, allowance)
	}
	runtime.KeepAlive(got)
}

func FuzzHumanCommentRawInvariants(f *testing.F) {
	useCommentTaxonomy(f, commentParserTaxonomies(f)[0].taxonomy)
	for _, raw := range []string{"FT8 -14", "CW UP 2", "FT8 ... -14", "1000Z 20 1200Z -12 dB", "ıııııııı FOO -14 FT8", "\xff 1200ZFT8 -14", "CWT QRT 1800Z 20 WPM"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 65536 {
			return
		}
		got := ParseHumanSpotComment(raw, 14074)
		if got.TimeToken != "" || len(got.Comment) > len(raw) || got.HasReport && (got.Report < -200 || got.Report > 200) {
			t.Fatalf("human parser invented a timestamp or grew source text: %+v", got)
		}
		if got.HasReport && !strings.Contains(strings.ToUpper(raw), "DB") &&
			((got.Mode != "FT8" && got.Mode != "FT4") || got.Report < -30 || got.Report > 30) {
			t.Fatalf("unannotated human report escaped mode/range bounds: %+v", got)
		}
	})
}

func FuzzHumanCommentShorthand(f *testing.F) {
	useCommentTaxonomy(f, commentParserTaxonomies(f)[0].taxonomy)
	for _, value := range []int32{-31, -30, -1, 0, 1, 30, 31} {
		f.Add(value, uint8(0), uint8(0), "sample")
	}
	f.Fuzz(func(t *testing.T, value int32, modeChoice, form uint8, noise string) {
		if len(noise) > 1024 {
			t.Skip()
		}
		modes := [...]string{"FT8", "FT-8", "FT4", "FT-4", "CW", "FT2"}
		mode := modes[int(modeChoice)%len(modes)]
		canonical := strings.ReplaceAll(mode, "-", "")
		signed := form&1 == 0
		adjacent := form&2 == 0
		number := fmt.Sprintf("%+d", value)
		if !signed {
			magnitude := int64(value)
			if magnitude < 0 {
				magnitude = -magnitude
			}
			number = strconv.FormatInt(magnitude, 10)
		}
		// The inert suffix varies length without accidentally introducing a
		// mode, dB annotation, number or EVENT that would change the oracle.
		suffix := "note_" + strings.Repeat("x", len(noise))
		text := number + " " + suffix
		if !adjacent {
			text = "CQing " + text
		}
		want := CommentParseResult{Mode: canonical, Comment: text}
		if signed && adjacent && (canonical == "FT8" || canonical == "FT4") && value >= -30 && value <= 30 {
			want.Report, want.HasReport, want.Comment = int(value), true, suffix
		}
		if got := ParseHumanSpotComment(mode+" "+text, 14074); got != want {
			t.Fatalf("signed=%v adjacent=%v input=%q: got %+v, want %+v", signed, adjacent, mode+" "+text, got, want)
		}
	})
}

func BenchmarkHumanCommentParser(b *testing.B) {
	useCommentTaxonomy(b, commentParserTaxonomies(b)[0].taxonomy)
	for _, tc := range []humanCommentCase{
		{"Bare", "FT8 -14 PLS EU", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "PLS EU"}},
		{"Explicit", "FT8 -14 dB POTA-1234", CommentParseResult{Mode: "FT8", Report: -14, HasReport: true, Comment: "POTA-1234", Events: EventPOTA}},
		{"Preserved", "CWT QRT 2200Z UP 2 20 WPM", CommentParseResult{Mode: "CW", Comment: "CWT QRT 2200Z UP 2 20 WPM"}},
		{"Dense", "CW " + strings.Repeat("1200Z 20 WPM ", 4096), CommentParseResult{Mode: "CW", Comment: strings.TrimSpace(strings.Repeat("1200Z 20 WPM ", 4096))}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.input)))
			for iteration := 0; iteration < b.N; iteration++ {
				got := ParseHumanSpotComment(tc.input, 14074)
				if got != tc.want {
					b.Fatalf("input=%q: got %+v, want %+v", tc.input, got, tc.want)
				}
				runtime.KeepAlive(got)
			}
		})
	}
}
