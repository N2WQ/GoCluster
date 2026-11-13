package spot

import (
	"strconv"
	"testing"
)

func TestHumanExplicitReportSelectedModeOwnership(t *testing.T) {
	definitions := defaultTaxonomyFile()
	definitions.Modes = append(definitions.Modes, ModeDefinition{Name: "AUDIT", CommentTokens: []string{"-12", "-13DB", "DB"}})
	taxonomy, err := buildTaxonomy(definitions)
	if err != nil {
		t.Fatal(err)
	}
	useCommentTaxonomy(t, taxonomy)
	for _, tc := range []humanCommentCase{
		{"numeric_mode", "-12 dB", CommentParseResult{Mode: "AUDIT", Comment: "dB"}},
		{"compact_mode", "-13DB", CommentParseResult{Mode: "AUDIT"}},
		{"fused_numeric_mode", "1200Z-12 dB", CommentParseResult{Mode: "AUDIT", Comment: "1200Z-12 dB"}},
		{"fused_compact_mode", "1200Z-13DB", CommentParseResult{Mode: "AUDIT", Comment: "1200Z-13DB"}},
		{"unselected_alias_report", "FT8 -13DB", CommentParseResult{Mode: "FT8", Report: -13, HasReport: true}},
		{"builtin_db_precedence", "+3 DB CW", CommentParseResult{Mode: "CW", Report: 3, HasReport: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ParseHumanSpotComment(tc.input, 14074); got != tc.want {
				t.Fatalf("input=%q: got %+v, want %+v", tc.input, got, tc.want)
			}
		})
	}
}

// Construct declared forms rather than deriving expectations from the parser
// or its token recognizer. The independent raw fuzz target covers arbitrary
// bytes; this target falsifies association, preservation and range errors.
func FuzzHumanExplicitReport(f *testing.F) {
	useCommentTaxonomy(f, commentParserTaxonomies(f)[0].taxonomy)
	for _, value := range []int32{-201, -200, -12, 0, 12, 200, 201, 1000} {
		for form := uint8(0); form < 10; form++ {
			f.Add(value, form)
		}
	}
	f.Fuzz(func(t *testing.T, value int32, form uint8) {
		number := strconv.FormatInt(int64(value), 10)
		input, remaining := number+"dB", ""
		present := value >= -200 && value <= 200
		report := int(value)
		switch form % 10 {
		case 1:
			input = number + " dB"
		case 2:
			input, present = "label="+number+"dB", false
		case 3:
			input, present = "S9 "+number+"dB", false
		case 4:
			input, present = "59 "+number+" dB", false
		case 5:
			input, remaining = "UP 2 "+number+" dB", "UP 2"
		case 6:
			input, present = number+" PLS dB", false
		case 7:
			input, remaining = "1200Z"+number+"dB", "1200Z"
		case 8:
			input, present = number+"dBm", false
		case 9:
			input, remaining = "S9 "+number+" dB -12dB", "S9 "+number+" dB"
			present, report = true, -12
		}
		if !present {
			report, remaining = 0, input
		}
		want := CommentParseResult{Mode: "CW", Report: report, HasReport: present, Comment: remaining}
		if got := ParseHumanSpotComment("CW "+input, 14074); got != want {
			t.Fatalf("input=%q: got %+v, want %+v", input, got, want)
		}
	})
}
