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

func v6StorageTestConfiguration() V6Configuration {
	return V6Configuration{
		Filters: V6FilterConfiguration{
			Rules: []RuleDefinition{
				{9, RuleReject, `comment="  MiXeD  text  "`},
				{0, RulePass, "mode=cw and band=20m"},
			},
			IncludeBeacons: DefaultBoolDefault,
			AllowWWV:       DefaultBoolFalse, AllowWCY: DefaultBoolTrue,
			AllowAnnounce: DefaultBoolDefault, AllowSelf: DefaultBoolFalse, AllowToxic: DefaultBoolTrue,
		},
		Settings: SettingsConfiguration{Dialect: "", Grid: "fn31", NoiseClass: "mixed",
			DedupePolicy: "", PathMinObservationCount: 0, SolarSummaryMinutes: 0},
	}
}

func TestV6StorageUserRoundtrip(t *testing.T) {
	input := &V6UserRecord{
		Configuration: v6StorageTestConfiguration(),
		Preset: &V6PresetReference{Name: "CONTEST", Baseline: &V6SavedPreset{
			Configuration: v6StorageTestConfiguration(),
		}},
		RecentIPs:    []string{"  one  ", "", "one", "four", "five", "trimmed"},
		LastLoginUTC: time.Date(2026, 10, 21, 12, 32, 19, 123456789, time.UTC),
	}
	before, err := yaml.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV6UserRecord(input)
	if err != nil {
		t.Fatal(err)
	}
	after, err := yaml.Marshal(input)
	if err != nil || !bytes.Equal(before, after) || input.Configuration.Filters.preparedRules != nil ||
		input.Preset.Baseline.ConfigurationVersion != 0 || len(input.RecentIPs) != 6 {
		t.Fatal("encoding mutated caller data")
	}
	if !bytes.Contains(data, []byte("configuration_version: 6\nconfiguration:\n")) ||
		bytes.Contains(data, []byte("prepared")) || bytes.Contains(data, []byte("program")) ||
		bytes.Contains(data, []byte("allbands")) {
		t.Fatalf("not plain nested v6 YAML: %s", data)
	}
	decoded, err := DecodeV6UserRecord(data)
	if err != nil {
		t.Fatal(err)
	}
	wantRules := []RuleDefinition{{0, RulePass, "MODE=CW AND BAND=20M"}, {9, RuleReject, `COMMENT="  MiXeD  text  "`}}
	if decoded.ConfigurationVersion != 6 || decoded.Preset.Baseline.ConfigurationVersion != 6 ||
		!slices.Equal(decoded.Configuration.Filters.Rules, wantRules) ||
		!decoded.Configuration.Equal(decoded.Preset.Baseline.Configuration) ||
		decoded.Configuration.Settings != input.Configuration.Settings ||
		decoded.Configuration.Filters.toggles() != input.Configuration.Filters.toggles() ||
		!slices.Equal(decoded.RecentIPs, input.RecentIPs[:5]) ||
		decoded.LastLoginUTC != input.LastLoginUTC {
		t.Fatalf("roundtrip changed exact values: %#v", decoded)
	}
	reencoded, err := EncodeV6UserRecord(decoded)
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

func TestV6StorageNearbyRuleRoundtrip(t *testing.T) {
	cfg := V6Configuration{Filters: V6FilterConfiguration{Rules: []RuleDefinition{{Slot: 0, Action: RulePass, Expr: "nearby OR (MODE=CW AND NOT nearby)"}}}}
	record := &V6UserRecord{Configuration: cfg, Preset: &V6PresetReference{Name: "NEAR", Baseline: &V6SavedPreset{Configuration: cfg}}}
	raw, err := EncodeV6UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("nearby_enabled")) || !bytes.Contains(raw, []byte("configuration_version: 6")) {
		t.Fatal("native predicate roundtrip retained the retired toggle or old version")
	}
	loaded, err := DecodeV6UserRecord(raw)
	if err != nil || loaded.Configuration.Filters.Rules[0].Expr != "NEARBY OR MODE=CW AND NOT NEARBY" ||
		!loaded.Configuration.Equal(loaded.Preset.Baseline.Configuration) {
		t.Fatal("native predicate or applied baseline changed on reload", err)
	}
}

func TestV6StorageEmptyAndAllControls(t *testing.T) {
	for value := DefaultBoolDefault; value <= DefaultBoolTrue; value++ {
		input := &V6UserRecord{}
		for _, control := range v6ControlFields(&input.Configuration.Filters) {
			*control = value
		}
		data, err := EncodeV6UserRecord(input)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"rules: []", "include_beacons:", "allow_wwv:",
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
		decoded, err := DecodeV6UserRecord(data)
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

func TestV6StorageExactSettings(t *testing.T) {
	settings := SettingsConfiguration{
		Dialect: "cc", Grid: "fn31", NoiseClass: "mixed", DedupePolicy: "slow",
		PathMinObservationCount: 40, SolarSummaryMinutes: 30,
	}
	record := &V6UserRecord{Configuration: V6Configuration{Settings: settings},
		Preset: &V6PresetReference{Name: "TEST", Baseline: &V6SavedPreset{Configuration: V6Configuration{Settings: settings}}}}
	data, err := EncodeV6UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV6UserRecord(data)
	if err != nil || got.Configuration.Settings != settings || got.Preset.Baseline.Configuration.Settings != settings {
		t.Fatal("storage normalized settings or changed nonzero values", err)
	}
}

const v6StorageMinimalConfiguration = "{filters: {rules: []}, settings: {}}"
const v6StorageMinimalRecord = "configuration_version: 6\nconfiguration: " + v6StorageMinimalConfiguration + "\n"

func TestV6StorageStrictPresenceAndTypes(t *testing.T) {
	for _, test := range []struct{ name, raw string }{
		{"empty", ""},
		{"scalar", "true\n"},
		{"sequence", "[]\n"},
		{"document-null", "null\n"},
		{"multiple", v6StorageMinimalRecord + "---\n{}\n"},
		{"unknown", v6StorageMinimalRecord + "extra: true\n"},
		{"duplicate", v6StorageMinimalRecord + "configuration: {}\n"},
		{"missing-config", "configuration_version: 6\n"},
		{"null-config", "configuration_version: 6\nconfiguration: null\n"},
		{"missing-filters", "configuration_version: 6\nconfiguration: {settings: {}}\n"},
		{"missing-settings", "configuration_version: 6\nconfiguration: {filters: {rules: []}}\n"},
		{"null-filters", "configuration_version: 6\nconfiguration: {filters: null, settings: {}}\n"},
		{"null-settings", "configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: null}\n"},
		{"missing-rules", "configuration_version: 6\nconfiguration: {filters: {}, settings: {}}\n"},
		{"null-rules", "configuration_version: 6\nconfiguration: {filters: {rules: null}, settings: {}}\n"},
		{"map-rules", "configuration_version: 6\nconfiguration: {filters: {rules: {}}, settings: {}}\n"},
		{"unknown-config", "configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: {}, extra: false}\n"},
		{"null-preset", v6StorageMinimalRecord + "preset: null\n"},
		{"null-ips", v6StorageMinimalRecord + "recent_ips: null\n"},
		{"null-ip", v6StorageMinimalRecord + "recent_ips: [null]\n"},
		{"numeric-ip", v6StorageMinimalRecord + "recent_ips: [1]\n"},
		{"null-login", v6StorageMinimalRecord + "last_login_utc: null\n"},
		{"bad-login", v6StorageMinimalRecord + "last_login_utc: not-a-date\n"},
		{"numeric-login", v6StorageMinimalRecord + "last_login_utc: 42\n"},
		{"missing-baseline", v6StorageMinimalRecord + "preset: {name: TEST}\n"},
		{"missing-name", v6StorageMinimalRecord + "preset: {baseline: {configuration_version: 6, configuration: " + v6StorageMinimalConfiguration + "}}\n"},
		{"null-baseline", v6StorageMinimalRecord + "preset: {name: TEST, baseline: null}\n"},
		{"name-case", v6StorageMinimalRecord + "preset: {name: test, baseline: {configuration_version: 6, configuration: " + v6StorageMinimalConfiguration + "}}\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := DecodeV6UserRecord([]byte(test.raw)); err == nil || got != nil {
				t.Fatalf("invalid input admitted: %#v, %v", got, err)
			}
		})
	}
	for _, settings := range []string{`{dialect: null}`, `{grid: 42}`, `{noise_class: false}`,
		`{dedupe_policy: []}`, `{path_min_observation_count: "0"}`, `{solar_summary_minutes: false}`, `{unknown: 0}`,
		`{grid: "", grid: ""}`} {
		raw := "configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: " + settings + "}\n"
		if _, err := DecodeV6UserRecord([]byte(raw)); err == nil {
			t.Fatalf("invalid settings admitted: %s", settings)
		}
	}
	for _, controls := range []string{`nearby_enabled: false`, `nearby_enabled: true`, `nearby_enabled: "false"`, `include_beacons: null`, `allow_wwv: "false"`,
		`allow_wcy: 0`, `allow_announce: default`, `allow_self: []`, `allow_toxic: {}`, `unknown: false`,
		`allow_wwv: true, allow_wwv: false`} {
		raw := "configuration_version: 6\nconfiguration: {filters: {rules: [], " + controls + "}, settings: {}}\n"
		if _, err := DecodeV6UserRecord([]byte(raw)); err == nil {
			t.Fatalf("invalid controls admitted: %s", controls)
		}
	}
}

func TestV6StorageVersionBoundaries(t *testing.T) {
	markers := []string{"", "0", "1", "2", "3", "4", "5", "7", "-1", "null", `"6"`, "true", "6.0", "999999999999999999999999"}
	for _, marker := range markers {
		field := ""
		if marker != "" {
			field = "configuration_version: " + marker + "\n"
		}
		nested := ""
		if marker != "" {
			nested = "    " + field
		}
		nested += "    configuration: " + v6StorageMinimalConfiguration + "\n"
		for _, raw := range []string{
			field + "configuration: " + v6StorageMinimalConfiguration + "\n",
			v6StorageMinimalRecord + "preset:\n  name: TEST\n  baseline:\n" + nested,
			"presets:\n  TEST:\n" + nested,
		} {
			var err error
			if strings.HasPrefix(raw, "presets:") {
				_, err = DecodeV6PresetCollection([]byte(raw))
			} else {
				_, err = DecodeV6UserRecord([]byte(raw))
			}
			if !errors.Is(err, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("marker %q did not visibly refuse version: %v\n%s", marker, err, raw)
			}
		}
	}
	for _, version := range []int{-1, 1, 2, 3, 4, 5, 7} {
		for _, record := range []*V6UserRecord{
			{ConfigurationVersion: version},
			{Preset: &V6PresetReference{Name: "TEST", Baseline: &V6SavedPreset{ConfigurationVersion: version}}},
		} {
			if _, err := EncodeV6UserRecord(record); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("encoded unsupported version %d: %v", version, err)
			}
		}
		if _, err := EncodeV6PresetCollection(&V6PresetCollection{Presets: map[string]*V6SavedPreset{"TEST": {ConfigurationVersion: version}}}); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
			t.Fatalf("collection encoded unsupported version %d: %v", version, err)
		}
	}
}

func TestV6RecordPreparedPayloadAndFailure(t *testing.T) {
	usePresetTestDir(t)
	input := &V6UserRecord{Configuration: v6StorageTestConfiguration(), RecentIPs: []string{"one"},
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
	prepared, err := PrepareV6UserRecord("  n2wq-1  ", input)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := EncodeV6UserRecord(input)
	if err != nil || prepared.path != path || prepared.ByteLen() != len(expected) || !bytes.Equal(prepared.data, expected) {
		t.Fatal("incorrect captured path/bytes", err)
	}
	input.Configuration.Filters.Rules[0].Expr = "MODE=FT8"
	input.RecentIPs[0] = "changed"
	if disk, err := os.ReadFile(path); err != nil || !bytes.Equal(disk, legacy) {
		t.Fatal("preparation wrote to disk")
	}
	if _, err := DecodeV6UserRecord(legacy); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
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
	got, err := DecodeV6UserRecord(disk)
	if err != nil || got.RecentIPs[0] != "one" || got.LastLoginUTC != input.LastLoginUTC ||
		got.Configuration.Settings != input.Configuration.Settings {
		t.Fatal("captured metadata changed", err)
	}
	UserDataDir = previousDir
	if loaded, err := LoadUserRecord("N2WQ-1"); err != nil || !recordConfiguration(loaded).V6().Equal(got.Configuration) {
		t.Fatal("native entry point did not bind exact v6 configuration", err)
	}
	if final, err := os.ReadFile(path); err != nil || !bytes.Equal(final, expected) {
		t.Fatal("native v6 load rewrote bytes")
	}
	for _, invalid := range []*V6UserRecord{nil, {ConfigurationVersion: 4}} {
		if got, err := PrepareV6UserRecord("N2WQ", invalid); err == nil || got != nil {
			t.Fatal("invalid preparation returned a payload")
		}
	}
	if _, err := PrepareV6UserRecord(" ", &V6UserRecord{}); err == nil {
		t.Fatal("empty callsign admitted")
	}
}

func TestV6RecordDecodeFailurePreservesFile(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ")
	for _, raw := range []string{
		"configuration_version: 4\ngrid: old\n",
		"configuration_version: 7\nconfiguration: " + v6StorageMinimalConfiguration + "\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: [null]}, settings: {}}\n",
		"configuration_version: 6\nconfiguration: {filters: {rules: []}, settings: {grid: null}}\n",
	} {
		before := []byte(raw)
		if err := os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
		read, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeV6UserRecord(read); err == nil {
			t.Fatal("malformed stored record admitted")
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("decode failure changed preexisting file bytes")
		}
	}
}

func TestV6RecordInvalidCallsignCannotPrepare(t *testing.T) {
	for _, callsign := range []string{"", " ", "../escape", `..\escape`, `C:\escape`, `N2WQ\..\escape`, "bad name"} {
		if prepared, err := PrepareV6UserRecord(callsign, &V6UserRecord{}); err == nil || prepared != nil {
			t.Fatalf("invalid target identity admitted: %q", callsign)
		}
	}
}

func TestV6PresetClonePreparedReuse(t *testing.T) {
	cfg, _ := mustPrepareV6Configuration(t, v6StorageTestConfiguration())
	set := &V6SavedPreset{Configuration: cfg}
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
	if _, err := (*V6SavedPreset)(nil).Clone(); err == nil {
		t.Fatal("nil preset admitted")
	}
}
