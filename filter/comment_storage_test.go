package filter

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"slices"
	"strings"
	"testing"
)

func TestCommentStorageExactRoundtrip(t *testing.T) {
	usePresetTestDir(t)
	f := NewFilter()
	f.Comments = []string{" POTA ", "pota", "POTA", "up  5", "*,?\""}
	f.BlockComments = []string{"POTA", "qrt"}
	c := ConfigurationFromFilter(f, SettingsConfiguration{})
	baseline, err := c.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if err = SaveConfiguration("N2WQ-1", c, &PresetReference{Name: "TEST", Baseline: baseline}, nil); err != nil {
		t.Fatal(err)
	}
	r, err := LoadUserRecord("N2WQ-1")
	if err != nil {
		t.Fatal(err)
	}
	if !c.Equal(ConfigurationFromFilter(&r.Filter, SettingsConfiguration{})) || !c.Equal(ConfigurationFromPreset(r.Preset.Baseline)) {
		t.Fatal("exact values changed")
	}
	r.Comments[0] = "changed"
	if baseline.Comments[0] == "changed" || r.Preset.Baseline.Comments[0] == "changed" {
		t.Fatal("baseline shares comment list")
	}
	if err = SavePreset("N2WQ", "TEST", baseline); err != nil {
		t.Fatal(err)
	}
	saved, err := LoadPreset("N2WQ-2", "TEST")
	if err != nil || !slices.Equal(saved.Comments, f.Comments) || !slices.Equal(saved.BlockComments, f.BlockComments) {
		t.Fatalf("preset changed: %v", err)
	}
}

func TestCommentStorageVersionsAndProtection(t *testing.T) {
	usePresetTestDir(t)
	for version := 0; version <= 3; version++ {
		marker := ""
		if version > 0 {
			marker = fmt.Sprintf("configuration_version: %d\n", version)
		}
		body := "bands: {20m: false}\nallbands: false\n"
		if version == 3 {
			body += "min_snr: {CW: 0, FT8: -10}\n"
		}
		raw := marker + body + "preset:\n  name: TEST\n  baseline:\n"
		for _, line := range strings.Split(strings.TrimSuffix(marker+body, "\n"), "\n") {
			raw += "    " + line + "\n"
		}
		if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		r, err := LoadUserRecord("N2WQ-1")
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Comments)+len(r.BlockComments)+len(r.Preset.Baseline.Comments)+len(r.Preset.Baseline.BlockComments) != 0 {
			t.Fatal("old version acquired rules")
		}
		if version == 3 && (r.MinSNR["FT8"] != -10 || r.Preset.Baseline.MinSNR["FT8"] != -10 || len(r.MinSNR) != 2) {
			t.Fatal("v3 minima lost")
		}
		for _, field := range []string{"comments: []\n", "block_comments: [POTA]\n", "<<: {comments: [POTA]}\n", "old: &old {comments: [POTA]}\n<<: *old\n"} {
			assertMinSNRStorageProtected(t, marker+field)
		}
	}
	for _, field := range []string{"comments: null", "comments: {}", "comments: [42]", "comments: ['']", "comments: [' ']", "comments: [\"tab\\tvalue\"]", "comments: [café]", "block_comments: ['" + strings.Repeat("a", 65) + "']", "preset: {name: TEST, baseline: {configuration_version: 3, comments: []}}"} {
		assertMinSNRStorageProtected(t, "configuration_version: 4\n"+field+"\n")
	}
	for _, count := range []int{32, 33} {
		body := "comments: &rules [" + strings.TrimSuffix(strings.Repeat("POTA,", count), ",") + "]\nblock_comments: *rules\n"
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(body), &node); err != nil {
			t.Fatal(err)
		}
		if got := validateStoredCommentFields(node.Content[0], 4); (got == nil) != (count == 32) {
			t.Fatalf("node count %d: %v", count, got)
		}
	}
}

func FuzzMatchCommentPhrase(f *testing.F) {
	f.Add("POTA UP 5", "pota")
	f.Add("UP  5", "up 5")
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
