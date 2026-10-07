package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

func TestConfigurationExactDetachedSnapshot(t *testing.T) {
	on, off := true, false
	f := &Filter{
		Bands: map[string]bool{"20m": false}, BlockBands: map[string]bool{"40m": true},
		Modes: map[string]bool{"USB": false, "CW": true}, Events: map[string]bool{"SOTA": false},
		DXZones: map[int]bool{5: false}, BlockDXDXCC: map[int]bool{291: true},
		DXCallsigns: []string{"K1*", "W1*", "K1*"}, IncludeBeacons: nil, AllowWWV: &off, AllowWCY: &on,
		NearbyEnabled: true, NearbySnapshot: &NearbyLocationSnapshot{AllDXZones: true},
		NearbyUserFine: 10, NearbyUserCoarse: 11, LegacyMinConfidence: 30,
	}
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW"})
	clone := cfg.Clone()
	f.Bands["20m"] = true
	f.DXCallsigns[0] = "CHANGED"
	off = true
	if clone.Filters.Bands.Allow["20m"] || clone.Filters.DXCallsigns[0] != "K1*" || clone.Filters.AllowWWV != DefaultBoolFalse {
		t.Fatal("snapshot aliases its source")
	}
	set, err := clone.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if set.AllBands || set.AllModes || set.AllDXZones || set.Bands["20m"] || set.DXZones[5] || set.Modes["USB"] {
		t.Fatal("exact snapshot normalized explicit false or empty selections")
	}
	if set.IncludeBeacons != nil || set.AllowWWV == nil || *set.AllowWWV || set.AllowWCY == nil || !*set.AllowWCY {
		t.Fatal("snapshot lost DEFAULT/false/true distinction")
	}
	if set.NearbySnapshot != nil || set.NearbyUserFine != pathreliability.InvalidCell || set.NearbyUserCoarse != pathreliability.InvalidCell || set.LegacyMinConfidence != 0 {
		t.Fatal("snapshot contains runtime or migration state")
	}
	set.DXCallsigns[0] = "AGAIN"
	set.Modes["CW"] = false
	if clone.Filters.DXCallsigns[0] != "K1*" || !clone.Filters.Modes.Allow["CW"] {
		t.Fatal("preset aliases detached configuration")
	}
}

func TestConfigurationEqualityAndFingerprint(t *testing.T) {
	base := Configuration{Filters: FilterConfiguration{
		Bands:       StringRules{Allow: map[string]bool{"20m": false, "40m": true}},
		DXCallsigns: []string{"K1*", "W1*", "K1*"}, DECallsigns: []string{"N2*", "G*"},
	}}
	reordered := base.Clone()
	reordered.Filters.DXCallsigns = []string{"K1*", "K1*", "W1*"}
	reordered.Filters.DECallsigns = []string{"G*", "N2*"}
	reordered.Filters.Bands.Allow = map[string]bool{"40m": true, "20m": false}
	if !base.Equal(reordered) || base.Fingerprint() != reordered.Fingerprint() {
		t.Fatal("equivalent rule order changed configuration")
	}
	for _, mutate := range []func(*Configuration){
		func(c *Configuration) { c.Filters.DXCallsigns = []string{"K1*", "W1*", "W1*"} },
		func(c *Configuration) { delete(c.Filters.Bands.Allow, "20m") },
		func(c *Configuration) { c.Filters.IncludeBeacons = DefaultBoolTrue },
		func(c *Configuration) { c.Settings.SolarSummaryMinutes = 15 },
		func(c *Configuration) { c.Filters.NearbyEnabled = true },
	} {
		changed := base.Clone()
		mutate(&changed)
		if base.Equal(changed) || base.Fingerprint() == changed.Fingerprint() {
			t.Fatal("changed value or duplicate multiplicity compared unchanged")
		}
	}
	empty := Configuration{}
	explicit := Configuration{Filters: FilterConfiguration{Bands: StringRules{Allow: map[string]bool{}}, DXCallsigns: []string{}}}
	if !empty.Equal(explicit) || empty.Fingerprint() != explicit.Fingerprint() {
		t.Fatal("nil/empty collections changed configuration")
	}
}

func TestDefaultBoolLiteralYAML(t *testing.T) {
	encoded, err := yaml.Marshal(map[string]DefaultBool{"default": DefaultBoolDefault, "off": DefaultBoolFalse, "on": DefaultBoolTrue})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "default: DEFAULT\n\"off\": false\n\"on\": true\n" {
		t.Fatalf("unexpected output: %s", encoded)
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
	for _, cfg := range []Configuration{
		{Settings: SettingsConfiguration{Grid: strings.Repeat("A", 65537)}},
		{Filters: FilterConfiguration{DXCallsigns: []string{strings.Repeat("A", 65537)}}},
		{Filters: FilterConfiguration{Bands: StringRules{Allow: map[string]bool{strings.Repeat("A", 65537): false}}}},
	} {
		if cfg.MinimumSizeFits(65536) {
			t.Fatal("oversized token passed preparation preflight")
		}
	}
	if !((Configuration{}).MinimumSizeFits(0)) || (Configuration{}).MinimumSizeFits(-1) {
		t.Fatal("invalid empty/preflight boundary")
	}
}

func TestExactMarkedUserRecordAndLegacyMigration(t *testing.T) {
	usePresetTestDir(t)
	marked := "configuration_version: 1\nbands: {}\nallbands: false\nmodes: {USB: false, CW: true}\nallmodes: false\nevents: {SOTA: false}\nallevents: false\ninclude_beacons: false\nallow_wwv: false\npath_min_observation_count: 0\nsolar_summary_minutes: 0\n"
	if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte(marked), 0o644); err != nil {
		t.Fatal(err)
	}
	record, err := LoadUserRecord("N2WQ-1")
	if err != nil {
		t.Fatal(err)
	}
	if record.AllBands || record.AllModes || record.AllEvents || len(record.Modes) != 2 || record.Modes["USB"] || record.Events["SOTA"] || record.AllowWCY != nil {
		t.Fatal("marked record underwent legacy normalization")
	}
	if err := SaveUserRecord("N2WQ-1", record); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"configuration_version: 2\n", "allbands: false\n", "USB: false\n", "SOTA: false\n", "allow_wwv: false\n"} {
		if !bytes.Contains(disk, []byte(literal)) {
			t.Fatalf("disk omitted exact literal %q", literal)
		}
	}
	legacy := "bands: {}\nallbands: false\nmodes: {CW: true}\nallmodes: false\n"
	if err := os.WriteFile(userRecordPath("N2WQ-2"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	record, err = LoadUserRecord("N2WQ-2")
	if err != nil || !record.AllBands || !record.Modes["UNKNOWN"] || record.ConfigurationVersion != CurrentConfigurationVersion {
		t.Fatalf("legacy migration changed: record=%+v err=%v", record, err)
	}
}

func TestUnsupportedUserRecordMarkersRemainUnchanged(t *testing.T) {
	usePresetTestDir(t)
	for _, marker := range []string{"0", "3", "-1", "null", "\"1\"", "[]", "true"} {
		raw := "configuration_version: " + marker + "\nbands: {}\n"
		path := userRecordPath("N2WQ-1")
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadUserRecord("N2WQ-1"); !errors.Is(err, ErrUnsupportedConfigurationVersion) {
			t.Fatalf("marker %s accepted or misclassified: %v", marker, err)
		}
		if err := SaveConfiguration("N2WQ-1", Configuration{}, nil, nil); err == nil {
			t.Fatalf("write overwrote marker %s", marker)
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != raw {
			t.Fatalf("unsupported record changed: marker=%s", marker)
		}
	}
}

func TestMalformedUserRecordFramingAndImplicitMarkers(t *testing.T) {
	usePresetTestDir(t)
	for _, raw := range []string{
		"null\n", "[]\n", "configuration_version: 1\nunknown: true\n", "{}\n---\n{}\n",
		"configuration_version: 1\nconfiguration_version: 1\n",
		"configuration_version: 1\ndxzones: {1: true, 01: false}\n",
		"configuration_version: 1\nsources: {&primary HUMAN: true, *primary: false}\n",
		"legacy: &metadata {configuration_version: 2}\n<<: *metadata\n",
	} {
		if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadUserRecord("N2WQ-1"); err == nil {
			t.Fatalf("malformed record accepted: %s", raw)
		}
	}
}

func TestSaveUserRecordAtomicFailuresPreserveTarget(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ-1")
	if err := os.WriteFile(path, []byte("original bytes\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"write", "sync", "close", "replace"} {
		t.Run(stage, func(t *testing.T) {
			failure := errors.New("injected failure")
			called := []string{}
			stages := atomicUserFileStages{
				write: func(file *os.File, data []byte) error {
					called = append(called, "write")
					if stage == "write" {
						if err := writeUserFileBytes(file, []byte("partial")); err != nil {
							return err
						}
						return failure
					}
					return writeUserFileBytes(file, data)
				},
				sync: func(file *os.File) error {
					called = append(called, "sync")
					if stage == "sync" {
						return failure
					}
					return file.Sync()
				},
				close: func(file *os.File) error {
					called = append(called, "close")
					if stage == "close" {
						return failure
					}
					return file.Close()
				},
				replace: func(source, target string) error {
					called = append(called, "replace")
					return failure
				},
			}
			err := saveUserRecord("N2WQ-1", &UserRecord{NoiseClass: "URBAN"}, func(target string, data []byte) error {
				return writeAtomicUserFileWithStages(target, data, stages)
			})
			if !errors.Is(err, failure) {
				t.Fatalf("%s failure reported success", stage)
			}
			expected := map[string]string{"write": "write", "sync": "write,sync", "close": "write,sync,close", "replace": "write,sync,close,replace"}
			if strings.Join(called, ",") != expected[stage] {
				t.Fatalf("%s continued past its failed stage: %v", stage, called)
			}
			disk, err := os.ReadFile(path)
			if err != nil || string(disk) != "original bytes\n" {
				t.Fatalf("%s damaged the previous record", stage)
			}
			files, err := os.ReadDir(UserDataDir)
			if err != nil || len(files) != 1 || files[0].Name() != "N2WQ-1.yaml" {
				t.Fatalf("%s retained a temporary file: %v", stage, files)
			}
		})
	}
}

func TestCurrentDiskNullsRejectWithoutChangingLegacyPolicy(t *testing.T) {
	usePresetTestDir(t)
	for _, field := range []string{
		"dialect: null", "grid: null", "noise_class: null", "dedupe_policy: null",
		"path_min_observation_count: null", "solar_summary_minutes: null", "allbands: null",
		"bands: null", "bands: {20m: null}", "callsigns: null", "allow_wwv: null", "nearby_enabled: null",
	} {
		if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte("configuration_version: 1\n"+field+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadUserRecord("N2WQ-1"); err == nil {
			t.Fatalf("current null silently became a zero value: %s", field)
		}
	}
	if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte("allbands: null\nbands: null\nallow_wwv: null\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	legacy, err := LoadUserRecord("N2WQ-1")
	if err != nil || !legacy.AllBands || legacy.AllowWWV == nil || !*legacy.AllowWWV {
		t.Fatalf("legacy null/default policy changed: %v", err)
	}
}

func TestSaveConfigurationPersistsExactBaselineAndMetadata(t *testing.T) {
	usePresetTestDir(t)
	login := time.Date(2026, 10, 5, 14, 0, 0, 0, time.UTC)
	if err := SaveUserRecord("N2WQ-1", &UserRecord{Filter: *NewFilter(), LastLoginUTC: login, RecentIPs: []string{"192.0.2.1"}}); err != nil {
		t.Fatal(err)
	}
	cfg := Configuration{Settings: SettingsConfiguration{Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "MED"}, Filters: FilterConfiguration{
		Bands: StringRules{Allow: map[string]bool{"20m": false}}, DXCallsigns: []string{"K1*", "W1*", "K1*"}, AllowWWV: DefaultBoolFalse,
	}}
	baseline, err := cfg.Preset()
	if err != nil {
		t.Fatal(err)
	}
	ref := &PresetReference{Name: "CONTEST", Baseline: baseline}
	if err := SaveConfiguration("N2WQ-1", cfg, ref, []string{"192.0.2.2"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"configuration_version: 2\n", "name: CONTEST\n", "noise_class: URBAN\n", "last_login_utc: 2026-10-05T14:00:00Z\n", "20m: false\n", "allow_wwv: false\n"} {
		if !bytes.Contains(data, []byte(literal)) {
			t.Fatalf("missing durable literal %q", literal)
		}
	}
	loaded, err := LoadUserRecord("N2WQ-1")
	if err != nil || loaded.Preset == nil || loaded.Preset.Name != "CONTEST" || loaded.AllBands || loaded.Preset.Baseline.AllBands || !loaded.LastLoginUTC.Equal(login) || strings.Join(loaded.RecentIPs, ",") != "192.0.2.2,192.0.2.1" {
		t.Fatalf("restored state changed: loaded=%+v err=%v", loaded, err)
	}
	if err := SaveUserPreferences("N2WQ-1", &SavedPreset{Filter: *NewFilter(), NoiseClass: "QUIET"}, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadUserRecord("N2WQ-1")
	if err != nil || loaded.Preset == nil || loaded.Preset.Baseline.NoiseClass != "URBAN" || loaded.NoiseClass != "QUIET" {
		t.Fatalf("ordinary preference save lost baseline: loaded=%+v err=%v", loaded, err)
	}
	before, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected replacement failure")
	if err := saveConfiguration("N2WQ-1", cfg, nil, nil, func(_ string, _ []byte) error { return failure }); !errors.Is(err, failure) {
		t.Fatalf("injected error lost: %v", err)
	}
	after, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed commit changed disk association or baseline")
	}
}

func TestAppliedBaselineValidation(t *testing.T) {
	usePresetTestDir(t)
	for _, raw := range []string{
		"configuration_version: 1\npreset: {name: CONTEST, baseline: null}\n",
		"configuration_version: 1\npreset: {name: lower, baseline: {configuration_version: 1}}\n",
		"configuration_version: 1\npreset: {name: CONTEST, baseline: {configuration_version: 3}}\n",
		"configuration_version: 1\npreset: {name: CONTEST, baseline: {configuration_version: 1, grid: " + strings.Repeat("A", MaxPresetBytes+1) + "}}\n",
	} {
		if err := os.WriteFile(userRecordPath("N2WQ-1"), []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadUserRecord("N2WQ-1"); err == nil {
			t.Fatal("invalid applied baseline accepted")
		}
	}
}

func TestMixedPresetVersionsAndUnsupportedMutation(t *testing.T) {
	usePresetTestDir(t)
	path, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(UserDataDir+"/presets", 0o755); err != nil {
		t.Fatal(err)
	}
	raw := "presets:\n  LEGACY:\n    allbands: false\n    bands: {}\n  EXACT:\n    configuration_version: 1\n    allbands: false\n    bands: {20m: false}\n"
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	legacy, err := LoadPreset("N2WQ-1", "LEGACY")
	if err != nil || !legacy.AllBands {
		t.Fatalf("legacy entry did not migrate: %v", err)
	}
	exact, err := LoadPreset("N2WQ-2", "EXACT")
	if err != nil || exact.AllBands || exact.Bands["20m"] {
		t.Fatalf("exact entry changed: %v", err)
	}
	if err := SavePreset("N2WQ", "NEW", testSavedPreset()); err != nil {
		t.Fatalf("supported mixed collection unusable: %v", err)
	}
	for _, marker := range []string{"0", "3", "null", "\"1\""} {
		bad := fmt.Sprintf("presets:\n  EXACT: {configuration_version: 1}\n  FUTURE: {configuration_version: %s}\n", marker)
		if err := os.WriteFile(path, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := SavePreset("N2WQ", "NEW", testSavedPreset()); err == nil {
			t.Fatal("save rewrote unsupported collection")
		}
		if err := DeletePreset("N2WQ", "EXACT"); err == nil {
			t.Fatal("delete rewrote unsupported collection")
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != bad {
			t.Fatal("unsupported collection changed")
		}
	}
}
