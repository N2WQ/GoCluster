package filter

import (
	"errors"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func indentStoredFixture(raw string) string {
	return "    " + strings.ReplaceAll(strings.TrimSuffix(raw, "\n"), "\n", "\n    ") + "\n"
}

func TestNativeStoredOwnersUseV5Only(t *testing.T) {
	for _, marker := range []string{"", "0", "1", "2", "3", "4", "6", "null", `"5"`} {
		raw := "configuration: {filters: {rules: []}, settings: {}}\n"
		if marker != "" {
			raw = "configuration_version: " + marker + "\n" + raw
		}
		for _, output := range []any{&UserRecord{}, &SavedPreset{}} {
			if err := yaml.Unmarshal([]byte(raw), output); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("%T accepted non-v5 marker %q: %v", output, marker, err)
			}
		}
	}
	raw := []byte(v5StorageMinimalRecord)
	var record UserRecord
	var preset SavedPreset
	if err := yaml.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &preset); err != nil {
		t.Fatal(err)
	}
	if record.PreparedRules() != nil || preset.PreparedRules() != nil ||
		record.ConfigurationVersion != 5 || preset.ConfigurationVersion != 5 {
		t.Fatal("native owner callbacks installed defaults or lost explicit v5")
	}
}

func TestNativeOwnerRuleReadbackAttestation(t *testing.T) {
	set := testSavedPreset()
	clone, err := set.Clone()
	if err != nil || clone.PreparedRules() != set.PreparedRules() {
		t.Fatal("native clone recompiled prepared programs", err)
	}
	for range 10 {
		cfg := ConfigurationFromPreset(set).Clone()
		readback := cfg.V5()
		_, runtime, err := PrepareV5Configuration(readback)
		if err != nil || runtime.PreparedRules() != set.PreparedRules() {
			t.Fatal("native readback/save boundary lost attestation", err)
		}
		readback.Filters.Rules[0].Expr = "MODE=FT8"
		_, changed, err := PrepareV5Configuration(readback)
		if err != nil || changed.PreparedRules() == set.PreparedRules() {
			t.Fatal("public definition edit reused stale programs", err)
		}
	}
}
