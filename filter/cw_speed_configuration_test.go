package filter

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestCWSpeedV7ProfilePresetAndV6Boundary(t *testing.T) {
	cfg, err := DefaultV7Configuration(SettingsConfiguration{Dialect: "go"})
	if err != nil {
		t.Fatal(err)
	}
	initial := cfg.Clone()
	cfg.Settings.CWSpeed = true
	if cfg.Equal(initial) || cfg.Fingerprint() == initial.Fingerprint() {
		t.Fatal("CW speed did not affect v7 equality and revision fingerprint")
	}
	base := Configuration{Settings: initial.Settings}
	changed := Configuration{Settings: cfg.Settings}
	if base.Equal(changed) || base.Fingerprint() == changed.Fingerprint() {
		t.Fatal("CW speed did not affect active configuration fingerprint")
	}
	record := &V7UserRecord{ConfigurationVersion: V7ConfigurationVersion, Configuration: cfg}
	encoded, err := EncodeV7UserRecord(record)
	if err != nil || !strings.Contains(string(encoded), "cw_speed: true") {
		t.Fatalf("v7 profile lost enabled CW speed: %v\n%s", err, encoded)
	}
	decoded, err := DecodeV7UserRecord(encoded)
	if err != nil || !decoded.Configuration.Settings.CWSpeed {
		t.Fatalf("v7 profile did not restore CW speed: %v", err)
	}
	legacy := &V6UserRecord{ConfigurationVersion: V6ConfigurationVersion, Configuration: V6Configuration{Filters: cfg.Filters.GO, Settings: cfg.Settings}}
	v6, err := EncodeV6UserRecord(legacy)
	if err != nil || strings.Contains(string(v6), "cw_speed") {
		t.Fatalf("v6 codec exposed CW speed: %v\n%s", err, v6)
	}
	for _, value := range []string{"true", "false"} {
		input := "configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: {cw_speed: " + value + "}}\n"
		if _, err := DecodeV6UserRecord([]byte(input)); err == nil {
			t.Fatalf("frozen v6 codec accepted cw_speed: %s", value)
		}
	}
	settings, err := yaml.Marshal(cfg.Settings)
	if err != nil || !strings.Contains(string(settings), "cw_speed: true") {
		t.Fatalf("v7 settings marshal did not preserve CW speed: %v", err)
	}
	set, err := V7PresetFromConfiguration(cfg, "go")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := ApplyV7Preset(initial, set)
	if err != nil || !restored.Settings.CWSpeed {
		t.Fatalf("preset did not restore CW speed: %v", err)
	}
}
