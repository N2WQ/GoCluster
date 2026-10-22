package filter

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func v5StorageTestConfiguration() V5Configuration {
	return V5Configuration{
		Filters: V5FilterConfiguration{
			Rules: []RuleDefinition{
				{9, RuleReject, `comment="  MiXeD  text  "`},
				{0, RulePass, "mode=cw and band=20m"},
			},
			NearbyEnabled: true, IncludeBeacons: DefaultBoolDefault,
			AllowWWV: DefaultBoolFalse, AllowWCY: DefaultBoolTrue,
			AllowAnnounce: DefaultBoolDefault, AllowSelf: DefaultBoolFalse, AllowToxic: DefaultBoolTrue,
		},
		Settings: SettingsConfiguration{Dialect: "", Grid: "fn31", NoiseClass: "mixed",
			DedupePolicy: "", PathMinObservationCount: 0, SolarSummaryMinutes: 0},
	}
}

func TestV5StorageUserRoundtrip(t *testing.T) {
	input := &V5UserRecord{
		Configuration: v5StorageTestConfiguration(),
		Preset: &V5PresetReference{Name: "CONTEST", Baseline: &V5SavedPreset{
			Configuration: v5StorageTestConfiguration(),
		}},
		RecentIPs:    []string{"  one  ", "", "one", "four", "five", "trimmed"},
		LastLoginUTC: time.Date(2026, 10, 21, 12, 32, 19, 123456789, time.UTC),
	}
	before, err := yaml.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV5UserRecord(input)
	if err != nil {
		t.Fatal(err)
	}
	after, err := yaml.Marshal(input)
	if err != nil || !bytes.Equal(before, after) || input.Configuration.Filters.preparedRules != nil ||
		input.Preset.Baseline.ConfigurationVersion != 0 || len(input.RecentIPs) != 6 {
		t.Fatal("encoding mutated caller data")
	}
	if !bytes.Contains(data, []byte("configuration_version: 5\nconfiguration:\n")) ||
		bytes.Contains(data, []byte("prepared")) || bytes.Contains(data, []byte("program")) ||
		bytes.Contains(data, []byte("allbands")) {
		t.Fatalf("not plain nested v5 YAML: %s", data)
	}
	decoded, err := DecodeV5UserRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	wantRules := []RuleDefinition{{0, RulePass, "MODE=CW AND BAND=20M"}, {9, RuleReject, `COMMENT="  MiXeD  text  "`}}
	if decoded.ConfigurationVersion != 5 || decoded.Preset.Baseline.ConfigurationVersion != 5 ||
		!slices.Equal(decoded.Configuration.Filters.Rules, wantRules) ||
		!decoded.Configuration.Equal(decoded.Preset.Baseline.Configuration) ||
		decoded.Configuration.Settings != input.Configuration.Settings ||
		decoded.Configuration.Filters.toggles() != input.Configuration.Filters.toggles() ||
		!decoded.Configuration.Filters.NearbyEnabled ||
		!slices.Equal(decoded.RecentIPs, input.RecentIPs[:5]) ||
		decoded.LastLoginUTC != input.LastLoginUTC {
		t.Fatalf("roundtrip changed exact values: %#v", decoded)
	}
	reencoded, err := EncodeV5UserRecord(decoded)
	if err != nil || !bytes.Equal(reencoded, data) {
		t.Fatal("canonical encoding was not stable", err)
	}
	input.Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	input.RecentIPs[0] = "changed"
	input.Preset.Baseline.Configuration.Filters.Rules[0].Expr = "MODE=USB"
	if !slices.Equal(decoded.Configuration.Filters.Rules, wantRules) ||
		decoded.RecentIPs[0] != "  one  " || !decoded.Configuration.Equal(decoded.Preset.Baseline.Configuration) {
		t.Fatal("roundtrip borrowed caller backing storage")
	}
}

func TestV5StorageEmptyAndAllControls(t *testing.T) {
	for value := DefaultBoolDefault; value <= DefaultBoolTrue; value++ {
		input := &V5UserRecord{}
		for _, control := range v5ControlFields(&input.Configuration.Filters) {
			*control = value
		}
		data, err := EncodeV5UserRecord(input)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"rules: []", "nearby_enabled: false", "include_beacons:", "allow_wwv:",
			"allow_wcy:", "allow_announce:", "allow_self:", "allow_toxic:", `dialect: ""`, `grid: ""`,
			`noise_class: ""`, `dedupe_policy: ""`, "path_min_observation_count: 0", "solar_summary_minutes: 0"} {
			if !bytes.Contains(data, []byte(key)) {
				t.Fatalf("missing canonical field %s", key)
			}
		}
		for _, key := range []string{"preset:", "recent_ips:", "last_login_utc:"} {
			if bytes.Contains(data, []byte(key)) {
				t.Fatalf("absent metadata emitted: %s", key)
			}
		}
		decoded, err := DecodeV5UserRecord(data)
		if err != nil {
			t.Fatal(err)
		}
		if len(decoded.Configuration.Filters.Rules) != 0 || decoded.Configuration.Filters.preparedRules != nil ||
			decoded.Configuration.Settings != (SettingsConfiguration{}) || decoded.Preset != nil {
			t.Fatal("empty configuration acquired defaults")
		}
		for _, control := range decoded.Configuration.Filters.toggles() {
			if control != value {
				t.Fatal("DEFAULT/false/true changed")
			}
		}
	}
}

func TestV5StorageExactSettings(t *testing.T) {
	settings := SettingsConfiguration{
		Dialect: "cc", Grid: "fn31", NoiseClass: "mixed", DedupePolicy: "slow",
		PathMinObservationCount: 40, SolarSummaryMinutes: 30,
	}
	record := &V5UserRecord{Configuration: V5Configuration{Settings: settings},
		Preset: &V5PresetReference{Name: "TEST", Baseline: &V5SavedPreset{Configuration: V5Configuration{Settings: settings}}}}
	data, err := EncodeV5UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV5UserRecord(data)
	if err != nil || got.Configuration.Settings != settings || got.Preset.Baseline.Configuration.Settings != settings {
		t.Fatal("storage normalized settings or changed nonzero values", err)
	}
}

const v5StorageMinimalConfiguration = "{filters: {rules: []}, settings: {}}"
const v5StorageMinimalRecord = "configuration_version: 5\nconfiguration: " + v5StorageMinimalConfiguration + "\n"

func TestV5StorageStrictPresenceAndTypes(t *testing.T) {
	for _, test := range []struct{ name, raw string }{
		{"empty", ""},
		{"scalar", "true\n"},
		{"sequence", "[]\n"},
		{"document-null", "null\n"},
		{"multiple", v5StorageMinimalRecord + "---\n{}\n"},
		{"unknown", v5StorageMinimalRecord + "extra: true\n"},
		{"duplicate", v5StorageMinimalRecord + "configuration: {}\n"},
		{"missing-config", "configuration_version: 5\n"},
		{"null-config", "configuration_version: 5\nconfiguration: null\n"},
		{"missing-filters", "configuration_version: 5\nconfiguration: {settings: {}}\n"},
		{"missing-settings", "configuration_version: 5\nconfiguration: {filters: {rules: []}}\n"},
		{"null-filters", "configuration_version: 5\nconfiguration: {filters: null, settings: {}}\n"},
		{"null-settings", "configuration_version: 5\nconfiguration: {filters: {rules: []}, settings: null}\n"},
		{"missing-rules", "configuration_version: 5\nconfiguration: {filters: {}, settings: {}}\n"},
		{"null-rules", "configuration_version: 5\nconfiguration: {filters: {rules: null}, settings: {}}\n"},
		{"map-rules", "configuration_version: 5\nconfiguration: {filters: {rules: {}}, settings: {}}\n"},
		{"unknown-config", "configuration_version: 5\nconfiguration: {filters: {rules: []}, settings: {}, extra: false}\n"},
		{"null-preset", v5StorageMinimalRecord + "preset: null\n"},
		{"null-ips", v5StorageMinimalRecord + "recent_ips: null\n"},
		{"null-ip", v5StorageMinimalRecord + "recent_ips: [null]\n"},
		{"numeric-ip", v5StorageMinimalRecord + "recent_ips: [1]\n"},
		{"null-login", v5StorageMinimalRecord + "last_login_utc: null\n"},
		{"bad-login", v5StorageMinimalRecord + "last_login_utc: not-a-date\n"},
		{"numeric-login", v5StorageMinimalRecord + "last_login_utc: 42\n"},
		{"missing-baseline", v5StorageMinimalRecord + "preset: {name: TEST}\n"},
		{"missing-name", v5StorageMinimalRecord + "preset: {baseline: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + "}}\n"},
		{"null-baseline", v5StorageMinimalRecord + "preset: {name: TEST, baseline: null}\n"},
		{"name-case", v5StorageMinimalRecord + "preset: {name: test, baseline: {configuration_version: 5, configuration: " + v5StorageMinimalConfiguration + "}}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := DecodeV5UserRecord([]byte(test.raw)); err == nil || got != nil {
				t.Fatalf("invalid input admitted: %#v, %v", got, err)
			}
		})
	}
	for _, settings := range []string{`{dialect: null}`, `{grid: 42}`, `{noise_class: false}`,
		`{dedupe_policy: []}`, `{path_min_observation_count: "0"}`, `{solar_summary_minutes: false}`, `{unknown: 0}`,
		`{grid: "", grid: ""}`} {
		raw := "configuration_version: 5\nconfiguration: {filters: {rules: []}, settings: " + settings + "}\n"
		if _, err := DecodeV5UserRecord([]byte(raw)); err == nil {
			t.Fatalf("invalid settings admitted: %s", settings)
		}
	}
	for _, controls := range []string{`nearby_enabled: "false"`, `include_beacons: null`, `allow_wwv: "false"`,
		`allow_wcy: 0`, `allow_announce: default`, `allow_self: []`, `allow_toxic: {}`, `unknown: false`,
		`allow_wwv: true, allow_wwv: false`} {
		raw := "configuration_version: 5\nconfiguration: {filters: {rules: [], " + controls + "}, settings: {}}\n"
		if _, err := DecodeV5UserRecord([]byte(raw)); err == nil {
			t.Fatalf("invalid controls admitted: %s", controls)
		}
	}
}

func TestV5StorageVersionBoundaries(t *testing.T) {
	markers := []string{"", "0", "1", "2", "3", "4", "6", "-1", "null", `"5"`, "true", "5.0", "999999999999999999999999"}
	for _, marker := range markers {
		field := ""
		if marker != "" {
			field = "configuration_version: " + marker + "\n"
		}
		nested := ""
		if marker != "" {
			nested = "    " + field
		}
		nested += "    configuration: " + v5StorageMinimalConfiguration + "\n"
		for _, raw := range []string{
			field + "configuration: " + v5StorageMinimalConfiguration + "\n",
			v5StorageMinimalRecord + "preset:\n  name: TEST\n  baseline:\n" + nested,
			"presets:\n  TEST:\n" + nested,
		} {
			var err error
			if strings.HasPrefix(raw, "presets:") {
				_, err = DecodeV5PresetCollection([]byte(raw))
			} else {
				_, err = DecodeV5UserRecord([]byte(raw))
			}
			if !errors.Is(err, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("marker %q did not visibly refuse version: %v\n%s", marker, err, raw)
			}
		}
	}
	for _, version := range []int{-1, 1, 2, 3, 4, 6} {
		for _, record := range []*V5UserRecord{
			{ConfigurationVersion: version},
			{Preset: &V5PresetReference{Name: "TEST", Baseline: &V5SavedPreset{ConfigurationVersion: version}}},
		} {
			if _, err := EncodeV5UserRecord(record); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("encoded unsupported version %d: %v", version, err)
			}
		}
		if _, err := EncodeV5PresetCollection(&V5PresetCollection{Presets: map[string]*V5SavedPreset{"TEST": {ConfigurationVersion: version}}}); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
			t.Fatalf("collection encoded unsupported version %d: %v", version, err)
		}
	}
}

func TestV5RecordPreparedPayloadAndFailure(t *testing.T) {
	usePresetTestDir(t)
	input := &V5UserRecord{Configuration: v5StorageTestConfiguration(), RecentIPs: []string{"one"},
		LastLoginUTC: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)}
	path, err := filepath.Abs(userRecordPath("N2WQ-1"))
	if err != nil {
		t.Fatal(err)
	}
	// A prospective payload has no authority to replace these legacy bytes.
	legacy := []byte("configuration_version: 4\ngrid: old\n")
	if err := os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareV5UserRecord("  n2wq-1  ", input)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := EncodeV5UserRecord(input)
	if err != nil || prepared.path != path || prepared.ByteLen() != len(expected) || !bytes.Equal(prepared.data, expected) {
		t.Fatal("incorrect captured path/bytes", err)
	}
	input.Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	input.RecentIPs[0] = "changed"
	if disk, err := os.ReadFile(path); err != nil || !bytes.Equal(disk, legacy) {
		t.Fatal("preparation wrote to disk")
	}
	if _, err := DecodeV5UserRecord(legacy); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
		t.Fatal("legacy input was converted")
	}
	failure := errors.New("injected replacement failure")
	err = prepared.commitWith(func(target string, data []byte) error {
		return writeAtomicUserFileWith(target, data, writeUserFileBytes, func(_, _ string) error { return failure })
	})
	if !errors.Is(err, failure) {
		t.Fatal("commit failure was hidden", err)
	}
	if disk, err := os.ReadFile(path); err != nil || !bytes.Equal(disk, legacy) {
		t.Fatal("failed commit changed authoritative bytes")
	}
	entries, err := os.ReadDir(UserDataDir)
	if err != nil || len(entries) != 1 {
		t.Fatal("failed commit retained a staging file", err)
	}
	// Remove only the test-owned legacy destination. No product coordinator is
	// activated here; the existing payload writer is tested on a fresh target.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	previousDir := UserDataDir
	UserDataDir = filepath.Join(previousDir, "different")
	t.Cleanup(func() { UserDataDir = previousDir })
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(disk, expected) {
		t.Fatal("commit did not use captured bytes/path", err)
	}
	got, err := DecodeV5UserRecord(disk)
	if err != nil || got.RecentIPs[0] != "one" || got.LastLoginUTC != input.LastLoginUTC ||
		got.Configuration.Settings != input.Configuration.Settings {
		t.Fatal("captured metadata changed", err)
	}
	UserDataDir = previousDir
	if loaded, err := LoadUserRecord("N2WQ-1"); err != nil || !recordConfiguration(loaded).V5().Equal(got.Configuration) {
		t.Fatal("native entry point did not bind exact v5 configuration", err)
	}
	if final, err := os.ReadFile(path); err != nil || !bytes.Equal(final, expected) {
		t.Fatal("native v5 load rewrote bytes")
	}
	for _, invalid := range []*V5UserRecord{nil, {ConfigurationVersion: 4}} {
		if got, err := PrepareV5UserRecord("N2WQ", invalid); err == nil || got != nil {
			t.Fatal("invalid preparation returned a payload")
		}
	}
	if _, err := PrepareV5UserRecord(" ", &V5UserRecord{}); err == nil {
		t.Fatal("empty callsign admitted")
	}
}

func TestV5RecordDecodeFailurePreservesFile(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ")
	for _, raw := range []string{
		"configuration_version: 4\ngrid: old\n",
		"configuration_version: 6\nconfiguration: " + v5StorageMinimalConfiguration + "\n",
		"configuration_version: 5\nconfiguration: {filters: {rules: [null]}, settings: {}}\n",
		"configuration_version: 5\nconfiguration: {filters: {rules: []}, settings: {grid: null}}\n",
	} {
		before := []byte(raw)
		if err := os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
		read, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeV5UserRecord(read); err == nil {
			t.Fatal("malformed stored record admitted")
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("decode failure changed preexisting file bytes")
		}
	}
}

func TestV5RecordInvalidCallsignCannotPrepare(t *testing.T) {
	for _, callsign := range []string{"", " ", "../escape", `..\escape`, `C:\escape`, `N2WQ\..\escape`, "bad name"} {
		if prepared, err := PrepareV5UserRecord(callsign, &V5UserRecord{}); err == nil || prepared != nil {
			t.Fatalf("invalid target identity admitted: %q", callsign)
		}
	}
}

func TestV5PresetClonePreparedReuse(t *testing.T) {
	cfg, _ := mustPrepareV5Configuration(t, v5StorageTestConfiguration())
	set := &V5SavedPreset{Configuration: cfg}
	clone, err := set.Clone()
	if err != nil {
		t.Fatal(err)
	}
	if clone.Configuration.Filters.preparedRules != cfg.Filters.preparedRules ||
		&clone.Configuration.Filters.Rules[0] == &cfg.Filters.Rules[0] {
		t.Fatal("clone recompiled attested programs or borrowed definitions")
	}
	clone.Configuration.Filters.Rules[0].Expr = "MODE=USB"
	edited, err := clone.Clone()
	if err != nil || edited.Configuration.Filters.preparedRules == cfg.Filters.preparedRules ||
		!reflect.DeepEqual(set.Configuration.Filters.Rules, cfg.Filters.Rules) {
		t.Fatal("clone trusted edited definitions or mutated source", err)
	}
	if _, err := (*V5SavedPreset)(nil).Clone(); err == nil {
		t.Fatal("nil preset admitted")
	}
}
