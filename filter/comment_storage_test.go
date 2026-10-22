package filter

import (
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestNativeCommentStorageExactRoundtrip(t *testing.T) {
	usePresetTestDir(t)
	f := nativeCommentFilter(t, []string{" POTA ", "pota", "up  5", "*,?\""}, []string{"qrt"})
	wantLiteralRule := `COMMENT=" POTA ","pota","up  5","*,?"""`
	if got := f.PreparedRules().Definitions()[0].Expr; got != wantLiteralRule {
		t.Fatalf("literal grammar changed: got %q, want %q", got, wantLiteralRule)
	}
	c := ConfigurationFromFilter(f, SettingsConfiguration{})
	baseline, err := c.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveConfiguration("N2WQ-1", c, &PresetReference{Name: "TEST", Baseline: baseline}, nil); err != nil {
		t.Fatal(err)
	}
	r, err := LoadUserRecord("N2WQ-1")
	if err != nil || !c.Equal(recordConfiguration(r)) || !c.Equal(ConfigurationFromPreset(r.Preset.Baseline)) {
		t.Fatal("native comment definition/association roundtrip changed", err)
	}
	if !r.Matches(&spot.Spot{Comment: `*,?"`, IsHuman: true}) || r.Matches(&spot.Spot{Comment: `*,?\`, IsHuman: true}) {
		t.Fatal("quoted literal changed meaning during storage")
	}
	if err := SavePreset("N2WQ", "TEST", baseline); err != nil {
		t.Fatal(err)
	}
	saved, err := LoadPreset("N2WQ-2", "TEST")
	if err != nil || !c.Equal(ConfigurationFromPreset(saved)) {
		t.Fatal("native comment library roundtrip changed", err)
	}
}

func TestNativeCommentLegacyFieldsProtected(t *testing.T) {
	usePresetTestDir(t)
	for _, raw := range []string{
		"comments: [POTA]\n",
		"configuration_version: 4\ncomments: [POTA]\n",
		"configuration_version: 5\nconfiguration: {filters: {rules: [], comments: [POTA]}, settings: {}}\n",
		"configuration_version: 5\nconfiguration: {filters: {rules: [{slot: 0, action: pass, expr: 'COMMENT=\"\"'}]}, settings: {}}\n",
	} {
		assertMinSNRStorageProtected(t, raw)
	}
}

func FuzzMatchCommentPhrase(f *testing.F) {
	f.Add("POTA UP 5", "pota")
	f.Add("UP  5", "up 5")
	f.Add(strings.Repeat("A", 1024), strings.Repeat("A", 61)+"000")
	f.Add(strings.Repeat("A", 64), strings.Repeat("a", 64))
	f.Add("PO\x80TA", "pota")
	f.Add("ABABABABAC", "ABABAC")
	f.Fuzz(func(t *testing.T, comment, phrase string) {
		want := ValidCommentPhrase(phrase) && strings.Contains(asciiLower(comment), asciiLower(phrase))
		if MatchCommentPhrase(comment, phrase) != want {
			t.Fatal("literal matching disagrees with reference")
		}
	})
}

func asciiLower(s string) string {
	bytes := []byte(s)
	for i, b := range bytes {
		if b >= 'A' && b <= 'Z' {
			bytes[i] = b + 32
		}
	}
	return string(bytes)
}
