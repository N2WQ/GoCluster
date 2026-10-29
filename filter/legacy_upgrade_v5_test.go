package filter

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Historical fixtures deliberately use literal version 5 and its old shape,
// never active encoders, version constants, or generation-6 DTOs.
const retiredV5LiteralConfiguration = `{filters: {rules: [{slot: 3, action: pass, expr: 'MODE=CW AND COMMENT="NEARBY"'}], nearby_enabled: true, include_beacons: false, allow_wwv: true, allow_wcy: DEFAULT, allow_announce: false, allow_self: true, allow_toxic: DEFAULT}, settings: {dialect: cc, grid: FN31, noise_class: URBAN, dedupe_policy: SLOW, path_min_observation_count: 42, solar_summary_minutes: 15}}`

const retiredV5LiteralRecord = "configuration_version: 5\nconfiguration: " + retiredV5LiteralConfiguration + "\n" +
	"preset: {name: OLD, baseline: {configuration_version: 5, configuration: " + retiredV5LiteralConfiguration + "}}\n" +
	"recent_ips: [192.0.2.1, 198.51.100.2]\nlast_login_utc: 2026-10-21T12:00:00Z\n"

func TestLoginUpgradeRetiredV5ExactPreservationAndOnce(t *testing.T) {
	for _, sentinel := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "sentinels"}[sentinel], func(t *testing.T) {
			usePresetTestDir(t)
			raw := retiredV5LiteralRecord
			want := SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW", PathMinObservationCount: 42, SolarSummaryMinutes: 15}
			if sentinel {
				raw = strings.ReplaceAll(raw, "dialect: cc, grid: FN31, noise_class: URBAN, dedupe_policy: SLOW, path_min_observation_count: 42, solar_summary_minutes: 15",
					"dialect: '', grid: '', noise_class: '', dedupe_policy: '', path_min_observation_count: 0, solar_summary_minutes: 0")
				want = SettingsConfiguration{}
			}
			path := writeUpgradeFixture(t, "LZ3NY-11", []byte(raw))
			priorBackup := []byte("verified earlier-generation backup")
			if err := os.WriteFile(path+".pre-v5.bak", priorBackup, 0o600); err != nil {
				t.Fatal(err)
			}
			record, outcome, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
			if err != nil || record == nil || outcome != (LoginUpgradeOutcome{ProfileReset: true}) {
				t.Fatalf("retired-v5 reset outcome %+v: %v", outcome, err)
			}
			cfg := ConfigurationFromFilter(&record.Filter, recordConfiguration(record).Settings)
			if cfg.Settings != want || cfg.Filters.toggles() != ([6]DefaultBool{DefaultBoolFalse, DefaultBoolTrue, DefaultBoolDefault, DefaultBoolFalse, DefaultBoolTrue, DefaultBoolDefault}) ||
				record.Preset != nil || record.PreparedRules() != newFilterDefaultRules() ||
				!reflect.DeepEqual(record.RecentIPs, []string{"192.0.2.1", "198.51.100.2"}) ||
				!record.LastLoginUTC.Equal(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)) {
				t.Fatal("reset changed retained selections, metadata, or configured rule 0")
			}
			assertUpgradeBytes(t, path+".pre-v6.bak", []byte(raw))
			assertUpgradeBytes(t, path+".pre-v5.bak", priorBackup)
			for _, empty := range []bool{false, true} {
				if empty {
					record.Reset()
				} else {
					record.preparedRules = mustPrepareRuleSet(t, RuleDefinition{9, RulePass, "NEARBY OR MODE=RTTY"})
				}
				if err := SaveUserRecord("LZ3NY-11", record); err != nil {
					t.Fatal(err)
				}
				loaded, next, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
				if err != nil || next != (LoginUpgradeOutcome{}) || !recordConfiguration(loaded).Equal(recordConfiguration(record)) {
					t.Fatalf("current-generation rules reset on subsequent login: %+v %v", next, err)
				}
			}
		})
	}
}

func TestLoginUpgradeRetiredV5MalformedProtected(t *testing.T) {
	for name, raw := range map[string]string{
		"predicate":      strings.Replace(retiredV5LiteralRecord, `MODE=CW AND COMMENT="NEARBY"`, "NEARBY", 1),
		"unknown-field":  strings.Replace(retiredV5LiteralRecord, "MODE=CW", "FUTURE=CW", 1),
		"bad-expression": strings.Replace(retiredV5LiteralRecord, "MODE=CW", "MODE=BAD", 1),
		"bad-slot":       strings.Replace(retiredV5LiteralRecord, "slot: 3", "slot: 10", 1),
		"bad-action":     strings.Replace(retiredV5LiteralRecord, "action: pass", "action: PASS", 1),
		"duplicate-slot": strings.Replace(retiredV5LiteralRecord, "rules: [", "rules: [{slot: 3, action: reject, expr: 'MODE=RTTY'},", 1),
		"missing-rules":  strings.Replace(retiredV5LiteralRecord, `rules: [{slot: 3, action: pass, expr: 'MODE=CW AND COMMENT="NEARBY"'}], `, "", 1),
		"null-control":   strings.Replace(retiredV5LiteralRecord, "include_beacons: false", "include_beacons: null", 1),
		"baseline-predicate": strings.Replace(retiredV5LiteralRecord, "baseline: {configuration_version: 5, configuration: "+retiredV5LiteralConfiguration,
			"baseline: {configuration_version: 5, configuration: "+strings.Replace(retiredV5LiteralConfiguration, `MODE=CW AND COMMENT="NEARBY"`, "NEARBY", 1), 1),
		"baseline-current-marker": strings.Replace(retiredV5LiteralRecord, "baseline: {configuration_version: 5", "baseline: {configuration_version: 6", 1),
		"baseline-missing-marker": strings.Replace(retiredV5LiteralRecord, "baseline: {configuration_version: 5, ", "baseline: {", 1),
		"baseline-flat-shape":     strings.Replace(retiredV5LiteralRecord, "baseline: {configuration_version: 5, configuration: "+retiredV5LiteralConfiguration+"}", "baseline: {configuration_version: 5, grid: FN31}", 1),
		"oversized-expression":    strings.Replace(retiredV5LiteralRecord, `MODE=CW AND COMMENT="NEARBY"`, strings.Repeat("A", 1025), 1),
		"oversized-standalone":    strings.Replace(retiredV5LiteralRecord, "grid: FN31", "grid: "+strings.Repeat("A", MaxPresetBytes+1), 1),
	} {
		t.Run(name, func(t *testing.T) {
			usePresetTestDir(t)
			path := writeUpgradeFixture(t, "LZ3NY-11", []byte(raw))
			if _, _, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", func(*PreparedUserRecord) error {
				t.Fatal("protected input reached profile commit")
				return nil
			}); err == nil {
				t.Fatal("invalid retired-v5 input granted replacement authority")
			}
			assertUpgradeBytes(t, path, []byte(raw))
			if _, err := os.Stat(path + ".pre-v6.bak"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("protected input created a backup", err)
			}
		})
	}
}

func TestLoginUpgradeRetiredV5AliasedMarkerAndMixedLibrary(t *testing.T) {
	usePresetTestDir(t)
	aliased := "configuration: {filters: {rules: []}, settings: {path_min_observation_count: &v5 5}}\nconfiguration_version: *v5\n"
	profile := writeUpgradeFixture(t, "LZ3NY-11", []byte(aliased))
	fresh, err := V6SavedPresetFromRuntime(testSavedPreset())
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV6PresetCollection(&V6PresetCollection{Presets: map[string]*V6SavedPreset{"FRESH": fresh}})
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte("    OLD: {configuration_version: 5, configuration: "+retiredV5LiteralConfiguration+"}\n")...)
	library := writeUpgradeLibrary(t, data)
	record, outcome, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || outcome != (LoginUpgradeOutcome{ProfileReset: true, PresetLibraryCleaned: true}) || record.PathMinObservationCount != 5 {
		t.Fatalf("historically valid alias or mixed library refused: %+v %v", outcome, err)
	}
	assertUpgradeBytes(t, profile+".pre-v6.bak", []byte(aliased))
	assertUpgradeBytes(t, library+".pre-v6.bak", data)
	loaded, err := LoadPreset("LZ3NY-12", "FRESH")
	if err != nil || !ConfigurationFromPreset(loaded).Equal(ConfigurationFromPreset(testSavedPreset())) {
		t.Fatal("fresh v6 preset changed while removing retired-v5 entry", err)
	}
	if names, err := ListPresets("LZ3NY-12"); err != nil || !reflect.DeepEqual(names, []string{"FRESH"}) {
		t.Fatal("retired-v5 preset remained active", names, err)
	}
}

func TestLoginUpgradeRetiredV5SharedBaselineWork(t *testing.T) {
	configuration := "{filters: {rules: []}, settings: {grid: &text " + strings.Repeat("A", 32<<10) + "}}"
	baseline := "\npreset: {name: TEST, baseline: {configuration_version: 5, configuration: *cfg}}"
	raw := "configuration_version: 5\nconfiguration: &cfg " + configuration + baseline + "\nrecent_ips: [*text,*text]\n"
	decode := func(source string, budget uint64) error {
		node, state, err := parseUpgradeDocument([]byte(source), MaxUserRecordBytes, budget)
		if err != nil {
			return err
		}
		_, err = decodeRetiredV5Login(node, state, newFilterDefaultRules())
		return err
	}
	if err := decode(raw, 180_000); err != nil {
		t.Fatal("bounded shared alias fixture refused", err)
	}
	if err := decode(strings.Replace(raw, baseline, "", 1), 150_000); err != nil {
		t.Fatal("root alone did not fit the reduced budget", err)
	}
	if err := decode(raw, 150_000); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("nested retired-v5 baseline opened a separate work budget", err)
	}
}
