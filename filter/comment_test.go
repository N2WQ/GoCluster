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
