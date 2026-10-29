package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestConfigurationExactDetachedSnapshot(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW"})
	f.SetWWVEnabled(false)
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{Dialect: "cc", Grid: "FN31"})
	clone := cfg.Clone()
	f.SetWWVEnabled(true)
	f.preparedRules = nil
	set, err := clone.Preset()
	if err != nil || set.PreparedRules() != cfg.Filters.preparedRules || set.AllowWWV == nil || *set.AllowWWV {
		t.Fatal("native snapshot lost immutable programs or detached controls", err)
	}
	if clone.V6().Fingerprint() != clone.Fingerprint() || !clone.Equal(ConfigurationFromPreset(set)) {
		t.Fatal("native identity differs from exact v6 identity")
	}
}

func TestConfigurationIdentityAndSerialization(t *testing.T) {
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=CW"})
	base := ConfigurationFromFilter(f, SettingsConfiguration{})
	changed := base.Clone()
	if !base.Equal(changed) || base.Fingerprint() != changed.Fingerprint() {
		t.Fatal("cloning changed active identity")
	}
	for _, value := range []any{f, base, changed} {
		data, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"bands:", "comments:", "dx_states:", "allmodes", "prepared", "program"} {
			if bytes.Contains(data, []byte(forbidden)) {
				t.Fatalf("active serialization contains %q", forbidden)
			}
		}
		if !bytes.Contains(data, []byte("MODE=CW")) {
			t.Fatal("serialization omitted active rules")
		}
	}
	if _, err := changed.Preset(); err != nil {
		t.Fatal("cloning changed active admission", err)
	}
	for _, mutate := range []func(*Configuration){
		func(c *Configuration) { c.Filters.preparedRules = nil },
		func(c *Configuration) { c.Filters.AllowWWV = DefaultBoolFalse },
		func(c *Configuration) { c.Settings.SolarSummaryMinutes = 15 },
	} {
		changed := base.Clone()
		mutate(&changed)
		if base.Equal(changed) || base.Fingerprint() == changed.Fingerprint() {
			t.Fatal("native identity omitted active value")
		}
	}
}

func TestDefaultBoolLiteralYAML(t *testing.T) {
	encoded, err := yaml.Marshal(map[string]DefaultBool{"default": DefaultBoolDefault, "off": DefaultBoolFalse, "on": DefaultBoolTrue})
	if err != nil || string(encoded) != "default: DEFAULT\n\"off\": false\n\"on\": true\n" {
		t.Fatalf("unexpected representation: %s, %v", encoded, err)
	}
	for _, raw := range []string{"null", "~", "1", "yes", "[]", "{}", "\"false\""} {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &node); err != nil {
			t.Fatal(err)
		}
		var value DefaultBool
		if err := value.UnmarshalYAML(node.Content[0]); err == nil {
			t.Fatalf("invalid selection accepted: %s", raw)
		}
	}
	if _, err := yaml.Marshal(DefaultBool(9)); err == nil {
		t.Fatal("invalid internal selection encoded")
	}
}

func TestConfigurationMinimumSizePreflight(t *testing.T) {
	if (Configuration{Settings: SettingsConfiguration{Grid: strings.Repeat("A", 65537)}}).MinimumSizeFits(65536) {
		t.Fatal("oversized setting passed preflight")
	}
	if !(Configuration{}).MinimumSizeFits(0) || (Configuration{}).MinimumSizeFits(-1) {
		t.Fatal("empty preflight boundary changed")
	}
}

func TestUnsupportedUserRecordMarkersRemainUnchanged(t *testing.T) {
	usePresetTestDir(t)
	for _, marker := range []string{"", "0", "1", "2", "3", "4", "5", "7", "-1", "null", "\"6\"", "[]", "true"} {
		raw := "configuration: {filters: {rules: []}, settings: {}}\n"
		if marker != "" {
			raw = "configuration_version: " + marker + "\n" + raw
		}
		assertMinSNRStorageProtected(t, raw)
		if _, err := LoadUserRecord("N2WQ-1"); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
			t.Fatalf("marker %s error class changed: %v", marker, err)
		}
	}
}

func TestMalformedUserRecordFramingAndNulls(t *testing.T) {
	usePresetTestDir(t)
	for _, raw := range []string{
		"null\n", "[]\n", v6StorageMinimalRecord + "---\n{}\n",
		v6StorageMinimalRecord + "configuration_version: 6\n",
		strings.Replace(v6StorageMinimalRecord, "rules: []", "rules: null", 1),
		strings.Replace(v6StorageMinimalRecord, "settings: {}", "settings: {grid: null}", 1),
		"configuration_version: 6\nconfiguration: {filters: {rules: [], allow_wwv: null}, settings: {}}\n",
	} {
		assertMinSNRStorageProtected(t, raw)
	}
}

func TestSaveConfigurationPersistsExactBaselineAndMetadata(t *testing.T) {
	usePresetTestDir(t)
	login := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	if err := SaveUserRecord("N2WQ-1", &UserRecord{Filter: *NewFilter(), LastLoginUTC: login, RecentIPs: []string{"192.0.2.1"}}); err != nil {
		t.Fatal(err)
	}
	f := nativeRuleFilter(t, RuleDefinition{0, RulePass, "MODE=FT8"})
	f.SetWWVEnabled(false)
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "MED"})
	baseline, err := cfg.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveConfiguration("N2WQ-1", cfg, &PresetReference{Name: "CONTEST", Baseline: baseline}, []string{"192.0.2.2"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadUserRecord("N2WQ-1")
	if err != nil || !cfg.Equal(recordConfiguration(loaded)) ||
		!cfg.Equal(ConfigurationFromPreset(loaded.Preset.Baseline)) || !loaded.LastLoginUTC.Equal(login) ||
		strings.Join(loaded.RecentIPs, ",") != "192.0.2.2,192.0.2.1" {
		t.Fatal("native record/baseline/metadata changed", err)
	}
	if err := SaveUserPreferences("N2WQ-1", &SavedPreset{Filter: *NewFilter(), NoiseClass: "QUIET"}, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadUserRecord("N2WQ-1")
	if err != nil || loaded.Preset.Baseline.NoiseClass != "URBAN" || loaded.NoiseClass != "QUIET" {
		t.Fatal("preference save lost applied baseline", err)
	}
	before, _ := os.ReadFile(userRecordPath("N2WQ-1"))
	failure := errors.New("replacement failure")
	if err := saveConfiguration("N2WQ-1", cfg, nil, nil, func(string, []byte) error { return failure }); !errors.Is(err, failure) {
		t.Fatal("write failure lost", err)
	}
	after, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed commit changed disk")
	}
}

func TestAppliedBaselineValidation(t *testing.T) {
	usePresetTestDir(t)
	for _, baseline := range []string{"null", "{}", "{configuration_version: 4}", "{configuration_version: 7}",
		"{configuration_version: 6, configuration: {filters: {rules: []}, settings: {grid: " + strings.Repeat("A", MaxPresetBytes+1) + "}}}"} {
		assertMinSNRStorageProtected(t, v6StorageMinimalRecord+fmt.Sprintf("preset: {name: TEST, baseline: %s}\n", baseline))
	}
}
