package filter

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestRuleDefaultsCheckedInShape(t *testing.T) {
	modes := []string{"CW", "LSB", "USB", "UNKNOWN", "RTTY", "MSK144", "PSK31", "SSTV"}
	sources := []string{"HUMAN"}
	before := configuredDefaultRules.Load()
	set, err := PrepareDefaultRuleSet(modes, sources)
	if err != nil {
		t.Fatal(err)
	}
	want := []RuleDefinition{{0, RulePass, "MODE=CW,LSB,USB,UNKNOWN,RTTY,MSK144,PSK,SSTV AND SOURCE=HUMAN"}}
	if got := set.Definitions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("visible default got %#v, want %#v", got, want)
	}
	if got := set.Counts().Program; got.Atoms != 2 || got.Values != 9 {
		t.Fatalf("eight-mode/HUMAN default counts: %+v", got)
	}
	for _, nearby := range []bool{false, true} {
		for _, probe := range []struct {
			mode  string
			human bool
			want  bool
		}{
			{"CW", true, true}, {"PSK63", true, true}, {"MSK144", true, true},
			{"FT8", true, false}, {"CW", false, false}, {"", true, true},
		} {
			if got := set.Matches(&spot.Spot{Mode: probe.mode, IsHuman: probe.human}, EvaluationContext{PathClass: ""}); got != probe.want {
				t.Fatalf("mode=%q human=%v nearby=%v got %v, want %v", probe.mode, probe.human, nearby, got, probe.want)
			}
		}
	}
	modes[0], sources[0] = "FT8", "SKIMMER"
	if !reflect.DeepEqual(set.Definitions(), want) || configuredDefaultRules.Load() != before {
		t.Fatal("factory retained caller slices or changed published defaults")
	}
	reset, err := PrepareRuleSet(nil)
	if err != nil || reset != nil || !reset.Matches(&spot.Spot{Mode: "FT8"}, EvaluationContext{PathClass: ""}) {
		t.Fatalf("explicit empty reset restored defaults: %v", err)
	}
	assertPreparedRulePrograms(t, set)
}

func TestRuleDefaultsNormalization(t *testing.T) {
	for _, test := range []struct {
		name           string
		modes, sources []string
		want           string
	}{
		{"variants", []string{" psk31 ", "PSK63", "PSK", "cw", "CW", ""}, []string{" human ", "HUMAN"},
			"MODE=PSK,CW,UNKNOWN AND SOURCE=HUMAN"},
		{"empty-source", []string{"CW"}, nil, "MODE=CW AND SOURCE=HUMAN,SKIMMER"},
		{"blank-source", []string{"CW"}, []string{"", "  "}, "MODE=CW AND SOURCE=HUMAN,SKIMMER"},
		{"all-source", []string{"CW"}, []string{" all "}, "MODE=CW AND SOURCE=HUMAN,SKIMMER"},
		{"all-with-human", []string{"CW"}, []string{"HUMAN", "ALL"}, "MODE=CW AND SOURCE=HUMAN,SKIMMER"},
		{"both-sources", []string{"CW"}, []string{"SKIMMER", "HUMAN", "SKIMMER"}, "MODE=CW AND SOURCE=HUMAN,SKIMMER"},
		{"skimmer", []string{"CW"}, []string{"", " skimmer "}, "MODE=CW AND SOURCE=SKIMMER"},
		{"blank-mode", []string{""}, nil, "MODE=UNKNOWN AND SOURCE=HUMAN,SKIMMER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			set, err := PrepareDefaultRuleSet(test.modes, test.sources)
			if err != nil {
				t.Fatal(err)
			}
			if got := set.Definitions()[0]; got.Slot != 0 || got.Action != RulePass || got.Expr != test.want {
				t.Fatalf("default got %+v, want %q", got, test.want)
			}
			for _, human := range []bool{false, true} {
				want := true
				if strings.HasSuffix(test.want, "SOURCE=HUMAN") {
					want = human
				} else if strings.HasSuffix(test.want, "SOURCE=SKIMMER") {
					want = !human
				}
				mode := strings.Split(strings.TrimPrefix(test.want, "MODE="), ",")[0]
				mode = strings.Split(mode, " ")[0]
				if got := set.Matches(&spot.Spot{Mode: mode, IsHuman: human}, EvaluationContext{PathClass: ""}); got != want {
					t.Fatalf("source default human=%v got %v, want %v", human, got, want)
				}
			}
		})
	}
	empty, err := PrepareDefaultRuleSet(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "MODE=" + strings.Join(builtInDefaultModeSelection(), ",") + " AND SOURCE=HUMAN,SKIMMER"
	if empty.Definitions()[0].Expr != want || empty.Counts().Program.Values != len(builtInDefaultModeSelection())+2 {
		t.Fatal("empty mode list did not use built-in defaults")
	}
	modes := make([]string, 4096)
	for i := range modes {
		modes[i] = "PSK31"
	}
	duplicates, err := PrepareDefaultRuleSet(modes, []string{"ALL"})
	if err != nil || duplicates.Definitions()[0].Expr != "MODE=PSK AND SOURCE=HUMAN,SKIMMER" || duplicates.Counts().Program.Values != 3 {
		t.Fatalf("raw duplicate count enlarged effective selection: %v", err)
	}
}

func TestRuleDefaultsRejectInvalid(t *testing.T) {
	for _, test := range []struct {
		modes, sources []string
	}{
		{[]string{"bad"}, nil}, {[]string{"CW", "bad"}, nil}, {[]string{"ALL"}, nil},
		{[]string{"SSB"}, nil}, {[]string{"CW OR MODE=FT8"}, nil},
		{[]string{strings.Repeat("X", 1025)}, nil},
		{nil, []string{"bad"}}, {nil, []string{"HUMAN", "bad"}}, {nil, []string{"ALL", "bad"}},
		{nil, []string{"HUMAN", "SKIMMER", "bad"}}, {nil, []string{strings.Repeat("X", 1025)}},
	} {
		if got, err := PrepareDefaultRuleSet(test.modes, test.sources); err == nil || got != nil {
			t.Fatalf("invalid selection was dropped or broadened: modes=%q sources=%q", test.modes, test.sources)
		}
	}
}

func TestRuleDefaultsConfiguredBounds(t *testing.T) {
	modes := make([]string, 33)
	longModes := make([]string, 32)
	for i := range modes {
		modes[i] = fmt.Sprintf("RULEMODE%02d", i)
	}
	for i := range longModes {
		longModes[i] = fmt.Sprintf("LONG%02d", i) + strings.Repeat("X", 26)
	}
	sourceBytes := len(" AND SOURCE=HUMAN,SKIMMER")
	exactName, overName := strings.Repeat("L", 1019-sourceBytes), strings.Repeat("M", 1020-sourceBytes)
	configured := append(append(append([]string(nil), modes...), longModes...), exactName, overName)
	configureRuleDefaultTestTaxonomy(t, configured)
	exactCount, err := PrepareDefaultRuleSet(modes[:32], nil)
	if err != nil || exactCount.Counts().Program.Values != 34 {
		t.Fatalf("exact effective value limit rejected: %v", err)
	}
	if got, err := PrepareDefaultRuleSet(modes, nil); err == nil || got != nil {
		t.Fatal("33 effective configured modes admitted")
	}
	exactBytes, err := PrepareDefaultRuleSet([]string{exactName}, nil)
	if err != nil || len(exactBytes.Definitions()[0].Expr) != 1024 {
		t.Fatalf("1024-byte configured default rejected: %v", err)
	}
	for _, test := range []struct {
		modes, sources []string
	}{
		{[]string{overName}, nil},
		{[]string{overName}, []string{"HUMAN", "SKIMMER"}},
		{longModes, nil},
	} {
		if got, err := PrepareDefaultRuleSet(test.modes, test.sources); err == nil || got != nil {
			t.Fatal("configured selection exceeded canonical bytes")
		}
	}
	if got, err := PrepareDefaultRuleSet(longModes[:30], []string{"HUMAN"}); err != nil || len(got.Definitions()[0].Expr) > 1024 {
		t.Fatalf("bounded long configured names rejected: %v", err)
	}
}

// The taxonomy loader is file-based. Keep its one scratch fixture in the package
// working directory (never the OS temporary directory), and remove it on cleanup.
func configureRuleDefaultTestTaxonomy(t *testing.T, modes []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "data", "config", "spot_taxonomy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var addition strings.Builder
	addition.WriteString("modes:\n")
	for _, mode := range modes {
		fmt.Fprintf(&addition, "  - name: %s\n    filter_visible: true\n", mode)
	}
	content := strings.Replace(strings.ReplaceAll(string(raw), "\r\n", "\n"), "modes:\n", addition.String(), 1)
	file, err := os.CreateTemp(".", "rule_defaults_taxonomy_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	t.Cleanup(func() {
		if err := os.Remove(path); err != nil {
			t.Errorf("remove rule default taxonomy fixture: %v", err)
		}
	})
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(before) })
}
