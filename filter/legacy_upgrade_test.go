package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

// These are literal yaml.v3 flat keys from HEAD's Filter/UserRecord, not v5
// spellings or a re-created legacy Go filter implementation.
const oldLoginFixture = `bands: {20m: true}
blockbands: {40m: true}
modes: {FT8: true}
blockmodes: {CW: true}
allbands: false
allmodes: false
sources: {SKIMMER: true}
minconfidence: 70
allsources: false
dxcontinents: {EU: true}
callsigns: [K1*]
nearby_enabled: true
include_beacons: false
allow_wwv: false
allow_wcy: false
allow_announce: false
allow_self: false
allow_toxic: false
dialect: cc
grid: FN31
noise_class: URBAN
dedupe_policy: MED
path_min_observation_count: 42
solar_summary_minutes: 15
recent_ips: [192.0.2.1, 198.51.100.2]
last_login_utc: 2026-10-21T12:00:00Z
`

func writeUpgradeFixture(t *testing.T, callsign string, data []byte) string {
	t.Helper()
	path := userRecordPath(callsign)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeUpgradeLibrary(t *testing.T, data []byte) string {
	t.Helper()
	path, err := presetCollectionPath("LZ3NY")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertUpgradeBytes(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("bytes changed at %s: %v", path, err)
	}
}

func TestLoginUpgradeActualFlatVersions(t *testing.T) {
	oldDefaults := configuredDefaultRules.Load()
	t.Cleanup(func() { configuredDefaultRules.Store(oldDefaults) })
	if err := ConfigureDefaultRules([]string{"CW", "RTTY"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	for version := 0; version <= 4; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			usePresetTestDir(t)
			raw := oldLoginFixture
			if version != 0 {
				raw = fmt.Sprintf("configuration_version: %d\n", version) + raw
			}
			if version >= 2 {
				raw += "dxstates: {MA: true}\nblockalldestates: true\n"
			}
			if version >= 3 {
				raw += "min_snr: {CW: 0}\n"
			}
			if version >= 4 {
				raw += "comments: [POTA]\nblock_comments: [TEST]\n"
			}
			raw += "preset:\n  name: OLD\n  baseline:\n    configuration_version: 1\n    modes: {FT8: true}\n"
			path := writeUpgradeFixture(t, "LZ3NY-11", []byte(raw))
			if _, err := LoadUserRecord("LZ3NY-11"); err == nil {
				t.Fatal("strict loader accepted old profile")
			}
			assertUpgradeBytes(t, path, []byte(raw))
			record, upgraded, err := UpgradeUserRecordForLogin("LZ3NY-11", nil)
			if err != nil || !upgraded {
				t.Fatalf("upgrade: %v, %v", upgraded, err)
			}
			if record.Dialect != "cc" || record.Grid != "FN31" || record.NoiseClass != "URBAN" ||
				record.DedupePolicy != "MED" || record.PathMinObservationCount != 42 || record.SolarSummaryMinutes != 15 ||
				record.Preset != nil || record.NearbyEnabled || len(record.RecentIPs) != 2 ||
				!record.LastLoginUTC.Equal(time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)) {
				t.Fatalf("settings/history/reset mismatch: %+v", record)
			}
			for _, selection := range []*bool{record.IncludeBeacons, record.AllowSelf, record.AllowToxic,
				record.AllowWWV, record.AllowWCY, record.AllowAnnounce} {
				if selection == nil || *selection {
					t.Fatal("explicit false control lost")
				}
			}
			want := NewFilter().PreparedRules().Definitions()
			if !reflect.DeepEqual(record.PreparedRules().Definitions(), want) ||
				!record.Matches(&spot.Spot{Mode: "CW", IsHuman: true}) ||
				record.Matches(&spot.Spot{Mode: "CW"}) || record.Matches(&spot.Spot{Mode: "FT8", IsHuman: true}) {
				t.Fatal("reset did not install configured new-user defaults", record.PreparedRules().Definitions())
			}
			assertUpgradeBytes(t, path+legacyBackupSuffix, []byte(raw))
			stored, err := LoadV5UserRecord("LZ3NY-11")
			if err != nil || !reflect.DeepEqual(stored.Configuration.Filters.Rules, want) {
				t.Fatal("upgrade did not create writable strict v5", err)
			}
			record.preparedRules = mustPrepareRuleSet(t, RuleDefinition{2, RulePass, "MODE=FT8"})
			if err := SaveUserRecord("LZ3NY-11", record); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			again, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", func(*PreparedUserRecord) error {
				t.Fatal("v5 profile reached upgrade writer")
				return nil
			})
			if err != nil || changed || again.PreparedRules().Definitions()[0].Slot != 2 {
				t.Fatalf("v5 reset on second login: %v %v", changed, err)
			}
			assertUpgradeBytes(t, path, before)
			assertUpgradeBytes(t, path+legacyBackupSuffix, []byte(raw))
		})
	}
	if !strings.Contains(NewFilter().PreparedRules().Definitions()[0].Expr, "SOURCE=HUMAN") {
		t.Fatal("new-user source default was changed")
	}
	empty := NewFilter()
	empty.Reset()
	if empty.PreparedRules() != nil {
		t.Fatal("explicit RESET FILTER installed defaults")
	}
}

func TestLoginUpgradeExactMissingAndSentinelPreferences(t *testing.T) {
	for _, fixture := range []struct {
		name, raw, dialect, dedupe string
	}{
		{"unversioned defaults", "allmodes: false\n", "go", "MED"},
		{"marked missing", "configuration_version: 1\nallmodes: false\n", "", ""},
		{"marked empty zero", "configuration_version: 4\ndialect: ''\ngrid: ''\nnoise_class: ''\ndedupe_policy: ''\npath_min_observation_count: 0\nsolar_summary_minutes: 0\nallow_self: false\nallow_toxic: true\n", "", ""},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			usePresetTestDir(t)
			writeUpgradeFixture(t, "LZ3NY", []byte(fixture.raw))
			record, upgraded, err := UpgradeUserRecordForLogin("LZ3NY", nil)
			if err != nil || !upgraded || record.Dialect != fixture.dialect || record.DedupePolicy != fixture.dedupe ||
				record.Grid != "" || record.NoiseClass != "" || record.PathMinObservationCount != 0 || record.SolarSummaryMinutes != 0 {
				t.Fatalf("sentinel mismatch: %+v %v", record, err)
			}
			if record.IncludeBeacons != nil || record.AllowWWV != nil || record.AllowWCY != nil || record.AllowAnnounce != nil {
				t.Fatal("missing controls no longer DEFAULT")
			}
			if fixture.name == "marked empty zero" && (record.AllowSelf == nil || *record.AllowSelf || record.AllowToxic == nil || !*record.AllowToxic) {
				t.Fatal("explicit controls changed")
			}
		})
	}
}

func TestLoginUpgradeKeepsSixControlsSeparate(t *testing.T) {
	names := []string{"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic"}
	for selected, name := range names {
		t.Run(name, func(t *testing.T) {
			usePresetTestDir(t)
			writeUpgradeFixture(t, "LZ3NY", []byte("configuration_version: 4\n"+name+": false\n"))
			record, _, err := UpgradeUserRecordForLogin("LZ3NY", nil)
			if err != nil {
				t.Fatal(err)
			}
			stored := V5ConfigurationFromFilter(&record.Filter, SettingsConfiguration{})
			for index, value := range stored.Filters.toggles() {
				want := DefaultBoolDefault
				if index == selected {
					want = DefaultBoolFalse
				}
				if value != want {
					t.Fatalf("%s changed separate control %s: %v", name, names[index], value)
				}
			}
		})
	}
}

func TestLoginUpgradeRefusesUnknownUnsafeOrMalformed(t *testing.T) {
	for _, raw := range []string{
		"null\n", "[]\n", "configuration_version: null\n", "configuration_version: '4'\n",
		"configuration_version: 0\n", "configuration_version: 6\n", "configuration_version: -1\n",
		"configuration_version: 4\nconfiguration_version: 4\n", "configuration_version: 4\nrules: []\n",
		"configuration_version: 4\nallow_self: null\n", "configuration_version: 4\nallow_self: 'false'\n",
		"configuration_version: 4\ndialect: null\n", "configuration_version: 1\ndxstates: {MA: true}\n",
		"configuration_version: 2\nmin_snr: {CW: 0}\n", "configuration_version: 3\ncomments: [abc]\n",
		"configuration_version: 4\nmodes: [CW]\n", "configuration_version: 4\npreset: {name: OLD, baseline: {configuration_version: 6}}\n",
		"configuration_version: 4\n---\n{}\n", "configuration_version: [\n",
		strings.Repeat("#", MaxUserRecordBytes+1),
		"callsigns: [&x " + strings.Repeat("a", 50000) + "]\nblock_callsigns: [" + strings.Repeat("*x,", 50) + "]\n",
		"modes: &r {CW: true}\nblockmodes: *r\nrecent_ips: &ips [*ips]\n",
	} {
		t.Run(fmt.Sprintf("case-%d", len(raw)), func(t *testing.T) {
			usePresetTestDir(t)
			path := writeUpgradeFixture(t, "LZ3NY", []byte(raw))
			if _, upgraded, err := UpgradeUserRecordForLogin("LZ3NY", func(*PreparedUserRecord) error {
				t.Fatal("refused profile reached commit")
				return nil
			}); err == nil || upgraded {
				t.Fatal("unsafe input authorized upgrade")
			}

			assertUpgradeBytes(t, path, []byte(raw))
			if _, err := os.Stat(path + legacyBackupSuffix); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("refused input created backup", err)
			}
		})
	}
}

func TestLoginUpgradeMixedLibraryPreservesV5(t *testing.T) {
	usePresetTestDir(t)
	writeUpgradeFixture(t, "LZ3NY-11", []byte("configuration_version: 4\nallmodes: false\n"))
	fresh, err := V5SavedPresetFromRuntime(testSavedPreset())
	if err != nil {
		t.Fatal(err)
	}
	data, err := EncodeV5PresetCollection(&V5PresetCollection{Presets: map[string]*V5SavedPreset{"FRESH": fresh}})
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, []byte("    OLD: {configuration_version: 4, modes: {FT8: true}}\n")...)
	path := writeUpgradeLibrary(t, data)
	if _, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); err != nil || !changed {
		t.Fatal("mixed library upgrade failed", err)
	}
	assertUpgradeBytes(t, path+legacyBackupSuffix, data)
	loaded, err := LoadPreset("LZ3NY-12", "FRESH")
	if err != nil || !ConfigurationFromPreset(loaded).Equal(ConfigurationFromPreset(testSavedPreset())) {
		t.Fatal("v5 preset changed during old-entry removal", err)
	}
	if names, err := ListPresets("LZ3NY"); err != nil || !reflect.DeepEqual(names, []string{"FRESH"}) {
		t.Fatal("old preset not removed", names, err)
	}
}

func TestLoginUpgradeBoundsAliasWorkAndSupportsBoundedLegacyMerge(t *testing.T) {
	usePresetTestDir(t)
	raw := []byte("callsigns: [&x " + strings.Repeat("a", 50000) + "]\nblock_callsigns: [" + strings.Repeat("*x,", 50) + "]\n")
	path := writeUpgradeFixture(t, "LZ3NY-11", raw)
	if _, _, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("alias-expanded discarded filter work was not bounded", err)
	}
	assertUpgradeBytes(t, path, raw)
	merged := []byte("<<: &old {grid: FN31, allow_self: false, allmodes: true}\nallow_toxic: false\n")
	writeUpgradeFixture(t, "LZ3NY-12", merged)
	record, changed, err := UpgradeUserRecordForLogin("LZ3NY-12", nil)
	if err != nil || !changed || record.Grid != "FN31" || record.AllowSelf == nil || *record.AllowSelf ||
		record.AllowToxic == nil || *record.AllowToxic || record.Dialect != "go" {
		t.Fatal("bounded legacy merge did not preserve preferences", err)
	}
}

func TestLoginUpgradeSharedLibraryRetryAndFreshPreset(t *testing.T) {
	usePresetTestDir(t)
	raw := []byte("configuration_version: 4\n" + oldLoginFixture)
	path := writeUpgradeFixture(t, "LZ3NY-11", raw)
	other := writeUpgradeFixture(t, "LZ3NY-12", raw)
	libraryRaw := []byte("presets:\n  OLD:\n    configuration_version: 4\n    modes: {FT8: true}\n")
	library := writeUpgradeLibrary(t, libraryRaw)
	failure := errors.New("record callback failure")
	_, upgraded, err := UpgradeUserRecordForLogin("LZ3NY-11", func(prepared *PreparedUserRecord) error {
		if _, err := DecodeV5UserRecord(prepared.data); err != nil {
			t.Fatal("callback received unadmitted bytes", err)
		}
		return failure
	})
	if !errors.Is(err, failure) || upgraded {
		t.Fatal("failed callback reported successful upgrade", err)
	}
	assertUpgradeBytes(t, path, raw)
	assertUpgradeBytes(t, library+legacyBackupSuffix, libraryRaw)
	names, err := ListPresets("LZ3NY")
	if err != nil || len(names) != 0 {
		t.Fatal("library did not commit first", names, err)
	}
	if err := SavePreset("LZ3NY-12", "FRESH", testSavedPreset()); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(library)
	if err != nil {
		t.Fatal(err)
	}
	for _, callsign := range []string{"LZ3NY-11", "LZ3NY-12"} {
		if _, changed, err := UpgradeUserRecordForLogin(callsign, nil); err != nil || !changed {
			t.Fatal("retry failed", err)
		}
		assertUpgradeBytes(t, library, before)
	}
	assertUpgradeBytes(t, other+legacyBackupSuffix, raw)
	assertUpgradeBytes(t, library+legacyBackupSuffix, libraryRaw)
}

func TestLoginUpgradeV5OnlyLibraryCleanupAndMissingIsolation(t *testing.T) {
	usePresetTestDir(t)
	if err := SaveUserRecord("LZ3NY-11", &UserRecord{Filter: *nativeRuleFilter(t, RuleDefinition{3, RuleReject, "MODE=FT8"}), Grid: "FN31"}); err != nil {
		t.Fatal(err)
	}
	path := userRecordPath("LZ3NY-11")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	library := writeUpgradeLibrary(t, []byte("presets:\n  OLD: {configuration_version: 1, modes: {CW: true}}\n"))
	if _, changed, err := UpgradeUserRecordForLogin("LZ3NY-99", nil); !errors.Is(err, os.ErrNotExist) || changed {
		t.Fatal("missing profile inspected or reset library", err)
	}
	if _, err := os.Stat(library + legacyBackupSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("new login touched library")
	}
	record, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", func(*PreparedUserRecord) error {
		t.Fatal("v5 record rewritten")
		return nil
	})
	if err != nil || !changed || record.Grid != "FN31" || record.PreparedRules().Definitions()[0].Slot != 3 {
		t.Fatal("library cleanup changed v5 record", err)
	}
	assertUpgradeBytes(t, path, before)
	if _, changed, err := UpgradeUserRecordForLogin("LZ3NY-11", nil); err != nil || changed {
		t.Fatal("library was reset twice", err)
	}
}

func TestLoginUpgradeConcurrentSSIDPreservesFreshLibrary(t *testing.T) {
	usePresetTestDir(t)
	for _, call := range []string{"LZ3NY-11", "LZ3NY-12"} {
		writeUpgradeFixture(t, call, []byte("configuration_version: 4\nallmodes: true\n"))
	}
	writeUpgradeLibrary(t, []byte("presets:\n  OLD: {configuration_version: 4, allbands: true}\n"))
	entered, release := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Go(func() {
		_, _, err := UpgradeUserRecordForLogin("LZ3NY-11", func(record *PreparedUserRecord) error {
			close(entered)
			<-release
			return record.Commit()
		})
		errs <- err
	})
	<-entered
	wg.Go(func() {
		err := SavePreset("LZ3NY-12", "FRESH", testSavedPreset())
		if err == nil {
			_, _, err = UpgradeUserRecordForLogin("LZ3NY-12", nil)
		}
		errs <- err
	})
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if names, err := ListPresets("LZ3NY"); err != nil || !reflect.DeepEqual(names, []string{"FRESH"}) {
		t.Fatal("concurrent SSID wiped new v5 preset", names, err)
	}
}
