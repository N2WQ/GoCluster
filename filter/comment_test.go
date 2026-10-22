package filter

import (
	"dxcluster/spot"
	"fmt"
	"strings"
	"testing"
)

// Preserve the existing stress workload independently of retired list limits.
const commentWorkloadPhrases = 32

func nativeCommentFilter(t testing.TB, allow, reject []string) *Filter {
	t.Helper()
	var definitions []RuleDefinition
	for i, phrases := range [][]string{allow, reject} {
		action := RulePass
		if i == 1 {
			action = RuleReject
		}
		for len(phrases) > 0 {
			count := min(len(phrases), 12)
			quoted := make([]string, count)
			for i := range count {
				quoted[i] = `"` + strings.ReplaceAll(phrases[i], `"`, `""`) + `"`
			}
			definitions = append(definitions, RuleDefinition{len(definitions), action, "COMMENT=" + strings.Join(quoted, ",")})
			phrases = phrases[count:]
		}
	}
	return nativeRuleFilter(t, definitions...)
}

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
	s := &spot.Spot{Comment: "POTA QRT", Band: "20m", BandNorm: "20m", Mode: "CW", IsHuman: true}
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
		f := nativeCommentFilter(t, tc.allow, tc.reject)
		s.Comment = tc.comment
		if got := f.MatchesWithPath(s, ""); got != tc.want {
			t.Errorf("allow=%v reject=%v comment=%q got=%v", tc.allow, tc.reject, tc.comment, got)
		}
	}
	f := nativeCommentFilter(t, []string{"POTA"}, []string{"QRT"})
	f.Reset()
	if f.PreparedRules() != nil {
		t.Fatal("global reset retained comments")
	}
	for i := 0; i < 1000; i++ {
		f = nativeCommentFilter(t, []string{fmt.Sprint(i)}, []string{fmt.Sprint(i)})
		f.Reset()
	}
	if f.PreparedRules() != nil {
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
	s := spot.NewSpot("K1ABC", "N2ABC", 14025, "CW")
	var allow, reject []string
	for i := range commentWorkloadPhrases {
		reject = append(reject, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i))
		allow = append(allow, strings.Repeat("A", 61)+fmt.Sprintf("%03d", i+32))
	}
	f := nativeCommentFilter(t, allow, reject)
	s.Comment = strings.Repeat("A", 65500-64) + allow[31]
	if !f.Matches(s) {
		t.Fatal("last PASS phrase at final bytes did not match")
	}
	if got := testing.AllocsPerRun(3, func() { f.Matches(s) }); got != 0 {
		t.Fatalf("normalized full filter allocated %v", got)
	}
	s.Comment += reject[0]
	if f.Matches(s) {
		t.Fatal("REJECT lost precedence")
	}
	s.Comment = strings.Repeat("A", 65500-64) + allow[31]
	f.preparedRules, _ = f.preparedRules.WithRule(RuleDefinition{9, RuleReject, "BAND=20m"})
	if f.Matches(s) {
		t.Fatal("COMMENT bypassed band allowlist")
	}
	f = nativeRuleFilter(t, RuleDefinition{0, RulePass, `MODE=FT8 AND COMMENT="000"`})
	s.Comment = "000"
	if f.Matches(s) {
		t.Fatal("COMMENT bypassed mode allowlist")
	}
}

func TestCommentConfigurationBoundsAndOwnership(t *testing.T) {
	f := nativeCommentFilter(t, []string{"POTA"}, []string{"QRT"})
	c := ConfigurationFromFilter(f, SettingsConfiguration{})
	copy := c.Clone()
	f.preparedRules = mustPrepareRuleSet(t, RuleDefinition{0, RulePass, `COMMENT="SOTA"`})
	if !copy.Equal(c) || copy.Equal(ConfigurationFromFilter(f, SettingsConfiguration{})) {
		t.Fatal("comment edit mutated a prior immutable configuration")
	}
	for _, source := range []string{`COMMENT=""`, `COMMENT=" "`, `COMMENT="` + strings.Repeat("a", 65) + `"`} {
		if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, source}}); err == nil {
			t.Fatal("invalid phrase accepted")
		}
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
