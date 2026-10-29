package filter

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"dxcluster/spot"
)

func TestLoginUpgradeResetUsesExactConfiguredDefaults(t *testing.T) {
	before := configuredDefaultRules.Load()
	t.Cleanup(func() { configuredDefaultRules.Store(before) })
	for _, sources := range [][]string{{"HUMAN"}, {"SKIMMER"}, {"ALL"}, {"HUMAN", "SKIMMER"}, nil} {
		t.Run("sources", func(t *testing.T) {
			usePresetTestDir(t)
			if err := ConfigureDefaultRules([]string{"CW", "RTTY"}, sources); err != nil {
				t.Fatal(err)
			}
			// All retired source selections, including opposite and disabled
			// selections, are discarded rather than carried into Boolean rules.
			for _, old := range []string{
				"sources: {SKIMMER: true}\n", "sources: {HUMAN: true}\n",
				"allsources: true\n", "sources: {SKIMMER: false, HUMAN: false}\n",
			} {
				writeUpgradeFixture(t, "LZ3NY", []byte("configuration_version: 4\nnearby_enabled: true\n"+old))
				record, outcome, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY", nil)
				if err != nil || outcome != (LoginUpgradeOutcome{ProfileReset: true}) {
					t.Fatalf("reset outcome %+v: %v", outcome, err)
				}
				fresh := NewFilter()
				if record.PreparedRules() != fresh.PreparedRules() ||
					!reflect.DeepEqual(record.PreparedRules().Definitions(), fresh.PreparedRules().Definitions()) {
					t.Fatal("retired profile reset diverged from new-user defaults")
				}
				for _, human := range []bool{false, true} {
					value := &spot.Spot{Mode: "CW", IsHuman: human}
					if record.Matches(value) != fresh.Matches(value) {
						t.Fatal("retired source choice survived configured reset")
					}
				}
				// This loop intentionally creates another original legacy file.
				if err := os.Remove(userRecordPath("LZ3NY") + legacyBackupSuffix); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestLoginUpgradeOutcomeSeparatesProfileAndLibrary(t *testing.T) {
	for _, test := range []struct {
		name    string
		legacy  bool
		library bool
		rules   []RuleDefinition
		want    LoginUpgradeOutcome
	}{
		{"profile-only", true, false, nil, LoginUpgradeOutcome{ProfileReset: true}},
		{"profile-and-library", true, true, nil, LoginUpgradeOutcome{ProfileReset: true, PresetLibraryCleaned: true}},
		{"v6-empty", false, false, nil, LoginUpgradeOutcome{}},
		{"v6-empty-library", false, true, nil, LoginUpgradeOutcome{PresetLibraryCleaned: true}},
		{"v6-custom-library", false, true, []RuleDefinition{{3, RulePass, "MODE=CW"}}, LoginUpgradeOutcome{PresetLibraryCleaned: true}},
		{"v6-explicit-human", false, false, []RuleDefinition{{0, RulePass, "MODE=CW AND SOURCE=HUMAN"}}, LoginUpgradeOutcome{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			usePresetTestDir(t)
			if test.legacy {
				writeUpgradeFixture(t, "LZ3NY-11", []byte("configuration_version: 4\n"))
			} else if err := SaveUserRecord("LZ3NY-11", &UserRecord{Filter: *nativeRuleFilter(t, test.rules...)}); err != nil {
				t.Fatal(err)
			}
			if test.library {
				writeUpgradeLibrary(t, []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n"))
			}
			path := userRecordPath("LZ3NY-11")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			record, outcome, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
			if err != nil || record == nil || outcome != test.want {
				t.Fatalf("outcome %+v, want %+v; %v", outcome, test.want, err)
			}
			if !test.legacy {
				assertUpgradeBytes(t, path, before)
				if !reflect.DeepEqual(record.PreparedRules().Definitions(), test.rules) {
					t.Fatal("current v6 empty/custom saved rules were reset")
				}
				if len(test.rules) > 0 {
					if !record.Matches(&spot.Spot{Mode: "CW", IsHuman: true}) ||
						record.Matches(&spot.Spot{Mode: "CW"}) != (test.name != "v6-explicit-human") {
						t.Fatal("restored rule did not retain its explicit source selection")
					}
				}
			}
			_, retry, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
			if err != nil || retry != (LoginUpgradeOutcome{}) {
				t.Fatalf("repeat login reannounced a reset: %+v %v", retry, err)
			}
		})
	}
}

func TestLoginUpgradeOutcomePartialCommitAndNewSSIDCleanup(t *testing.T) {
	usePresetTestDir(t)
	path := writeUpgradeFixture(t, "LZ3NY-11", []byte("configuration_version: 4\n"))
	library := writeUpgradeLibrary(t, []byte("presets:\n  OLD: {configuration_version: 4}\n"))
	failure := errors.New("profile commit failed")
	record, outcome, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", func(*PreparedUserRecord) error { return failure })
	if record != nil || !errors.Is(err, failure) ||
		outcome != (LoginUpgradeOutcome{PresetLibraryCleaned: true}) {
		t.Fatalf("partial outcome %+v: %v", outcome, err)
	}
	assertUpgradeBytes(t, path, []byte("configuration_version: 4\n"))
	_, retry, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-11", nil)
	if err != nil || retry != (LoginUpgradeOutcome{ProfileReset: true}) {
		t.Fatalf("retry outcome %+v: %v", retry, err)
	}
	// A missing SSID must not touch the shared library; after creation a second
	// login-owned cleanup must still report the library change, not profile reset.
	if err := os.Remove(library + legacyBackupSuffix); err != nil {
		t.Fatal(err)
	}
	raw := []byte("presets:\n  OLD: {configuration_version: 4}\n")
	writeUpgradeLibrary(t, raw)
	_, missing, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-12", nil)
	if !errors.Is(err, ErrLoginUpgradeProfileMissing) || missing != (LoginUpgradeOutcome{}) {
		t.Fatalf("missing outcome %+v: %v", missing, err)
	}
	assertUpgradeBytes(t, library, raw)
	if err := SaveUserRecord("LZ3NY-12", &UserRecord{Filter: *NewFilter()}); err != nil {
		t.Fatal(err)
	}
	_, created, err := UpgradeUserRecordForLoginWithOutcome("LZ3NY-12", nil)
	if err != nil || created != (LoginUpgradeOutcome{PresetLibraryCleaned: true}) {
		t.Fatalf("new SSID second cleanup outcome %+v: %v", created, err)
	}
}
