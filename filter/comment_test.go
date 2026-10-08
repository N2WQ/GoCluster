package filter

import (
	"dxcluster/spot"
	"fmt"
	"strings"
	"testing"
)

func TestCommentPhraseMatching(t *testing.T) {
	for _, tc := range []struct {
		comment, phrase string
		want            bool
	}{
		{"POTA-1234", "pota", true}, {"UP  5 PLEASE", "up  5", true},
		{"UP  5", "up 5", false}, {"* ? , \"", "* ? , \"", true},
		{"anything", "*", false}, {"", "POTA", false}, {"anything", "", false},
		{"anything", " ", false}, {"POTA", "\tPOTA", false},
		{"cafÉ", "CAFÉ", false}, {strings.Repeat("A", 65), strings.Repeat("A", 65), false},
	} {
		if got := MatchCommentPhrase(tc.comment, tc.phrase); got != tc.want {
			t.Errorf("Match(%q,%q)=%v", tc.comment, tc.phrase, got)
		}
	}
	if !ValidCommentPhrase(strings.Repeat("a", 64)) || !ValidCommentPhrase(" POTA ") {
		t.Fatal("valid exact phrase rejected")
	}
	if got := testing.AllocsPerRun(100, func() {
		if !MatchCommentPhrase("CQ POTA UP 5", "pota") {
			panic("wrong result")
		}
	}); got != 0 {
		t.Fatalf("allocated %v", got)
	}
}

func TestCommentFilterTruthTableAndReset(t *testing.T) {
	f := NewFilter()
	s := &spot.Spot{Comment: "POTA QRT", Band: "20m", BandNorm: "20m", Mode: "CW"}
	for _, tc := range []struct {
		allow, reject []string
		comment       string
		want          bool
	}{
		{nil, nil, "", true}, {[]string{"SOTA", "POTA"}, nil, "pota", true},
		{[]string{"POTA"}, nil, "", false}, {[]string{"POTA"}, nil, "ordinary", false},
		{nil, []string{"QRT"}, "", true}, {[]string{"POTA"}, []string{"qrt"}, "POTA QRT", false},
		{[]string{"pota", "POTA"}, []string{"POTA"}, "pota", false},
	} {
		f.Comments, f.BlockComments, s.Comment = tc.allow, tc.reject, tc.comment
		if got := f.MatchesWithPath(s, ""); got != tc.want {
			t.Errorf("allow=%v reject=%v comment=%q got=%v", tc.allow, tc.reject, tc.comment, got)
		}
	}
	f.Comments = []string{"POTA"}
	f.BlockComments = []string{"QRT"}
	f.Reset()
	if len(f.Comments)+len(f.BlockComments) != 0 {
		t.Fatal("global reset retained comments")
	}
	for i := 0; i < 1000; i++ {
		f.Comments = []string{fmt.Sprint(i)}
		f.BlockComments = []string{fmt.Sprint(i)}
		f.Reset()
	}
	if len(f.Comments)+len(f.BlockComments) != 0 {
		t.Fatal("churn retained rules")
	}
}

func TestCommentMatcherAdversarialBoundaries(t *testing.T) {
	for _, size := range []int{1, 63, 64} {
		phrase := strings.Repeat("a", size-1) + "B"
		for _, comment := range []string{
			strings.Repeat("A", 128), strings.Repeat("A", 65500),
			strings.Repeat("A", 65500-size) + phrase,
			strings.Repeat("AB", 32750),
			"\xff" + phrase, phrase[:size-1] + "\x80B",
			phrase[:size-1], phrase + "\x7f",
		} {
			want := strings.Contains(asciiLower(comment), asciiLower(phrase))
			if got := MatchCommentPhrase(comment, phrase); got != want {
				t.Fatalf("size=%d comment bytes=%d: got %v want %v", size, len(comment), got, want)
			}
		}
	}
	for _, tc := range []struct{ comment, phrase string }{
		{"ABABABABAC", "ABABAC"}, {"aaaaaab", "aaab"},
		{"ABABABABAB", "ABABAC"}, {"\xc3\xa9POTA", "pota"},
		{"PO\xc3\xa9TA", "pota"}, {"AA\x00AA", "AAAA"},
		{strings.Repeat("A", 65500), "B" + strings.Repeat("A", 63)},
	} {
		want := strings.Contains(asciiLower(tc.comment), asciiLower(tc.phrase))
		if got := MatchCommentPhrase(tc.comment, tc.phrase); got != want {
			t.Fatalf("phrase=%q comment bytes=%d: got %v want %v", tc.phrase, len(tc.comment), got, want)
		}
	}
	for _, phrase := range []string{"", " ", "\t", "POTA\n", "POTA\x7f", "POTA\u00a0", strings.Repeat("A", 65)} {
		if ValidCommentPhrase(phrase) || MatchCommentPhrase(phrase, phrase) {
			t.Fatalf("invalid phrase accepted: %q", phrase)
		}
	}
	comment, phrase := strings.Repeat("A", 65500), strings.Repeat("A", 61)+"000"
	if got := testing.AllocsPerRun(10, func() { MatchCommentPhrase(comment, phrase) }); got != 0 {
		t.Fatalf("long matcher allocated %v", got)
	}
}

func TestCommentMaximumListsAndOtherGates(t *testing.T) {
	f := NewFilter()
	f.Reset()
	s := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	for i := range MaxCommentPhrases {
		f.BlockComments = append(f.BlockComments, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i))
		f.Comments = append(f.Comments, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i+32))
	}
	s.Comment = strings.Repeat("A", 65500-64) + f.Comments[31]
	if !f.Matches(s) {
		t.Fatal("last PASS phrase at final bytes did not match")
	}
	if got := testing.AllocsPerRun(3, func() { f.Matches(s) }); got != 0 {
		t.Fatalf("normalized full filter allocated %v", got)
	}
	s.Comment += f.BlockComments[0]
	if f.Matches(s) {
		t.Fatal("REJECT lost precedence")
	}
	s.Comment = strings.Repeat("A", 65500-64) + f.Comments[31]
	f.SetBand("40m", true)
	if f.Matches(s) {
		t.Fatal("COMMENT bypassed band allowlist")
	}
	f.Reset()
	f.Comments = []string{"000"}
	s.Comment = "000"
	f.SetMode("FT8", true)
	if f.Matches(s) {
		t.Fatal("COMMENT bypassed mode allowlist")
	}
}

func TestCommentConfigurationBoundsAndOwnership(t *testing.T) {
	c := ConfigurationFromFilter(NewFilter(), SettingsConfiguration{})
	c.Filters.Comments = make([]string, MaxCommentPhrases)
	for i := range c.Filters.Comments {
		c.Filters.Comments[i] = strings.Repeat("a", MaxCommentPhraseBytes)
	}
	c.Filters.BlockComments = append([]string(nil), c.Filters.Comments...)
	if c.ValidateCommentRules() != nil {
		t.Fatal("maximum rejected")
	}
	if c.MinimumSizeFits(4095) {
		t.Fatal("preflight omitted phrase bytes")
	}
	copy := c.Clone()
	copy.Filters.Comments[0] = "other"
	if c.Filters.Comments[0] == "other" || c.Equal(copy) || c.Fingerprint() == copy.Fingerprint() {
		t.Fatal("clone alias or undetected edit")
	}
	c.Filters.Comments = append(c.Filters.Comments, "extra")
	if c.ValidateCommentRules() == nil {
		t.Fatal("duplicate entries bypassed count cap")
	}
	if _, err := c.Preset(); err == nil {
		t.Fatal("oversized preset accepted")
	}
	c.Filters.Comments = []string{strings.Repeat("a", 65)}
	if c.ValidateCommentRules() == nil {
		t.Fatal("oversized phrase accepted")
	}
}

func BenchmarkMatchCommentPhrase(b *testing.B) {
	comment := strings.Repeat("x", 80) + " POTA UP 5"
	if !MatchCommentPhrase(comment, "pota up 5") || MatchCommentPhrase(comment, "sota") {
		b.Fatal("incorrect matcher")
	}
	b.ReportAllocs()
	for b.Loop() {
		if !MatchCommentPhrase(comment, "pota up 5") {
			b.Fatal("incorrect match")
		}
	}
}
