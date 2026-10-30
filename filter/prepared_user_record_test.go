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
)

func TestPreparedUserRecordCapturesBytesAndPath(t *testing.T) {
	const capturedExpr = `BAND=20M AND COMMENT="CW !"`
	usePresetTestDir(t)
	directory := UserDataDir
	off, on := false, true
	record := &UserRecord{
		Filter: *NewFilter(), RecentIPs: []string{"a", "b", "c", "d", "e", "trimmed"},
		LastLoginUTC: time.Date(2026, 10, 9, 12, 34, 56, 0, time.UTC),
		Dialect:      "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "SLOW",
		Preset: &PresetReference{Name: "CONTEST", Baseline: testSavedPreset()},
	}
	record.preparedRules = mustPrepareRuleSet(t, RuleDefinition{0, RulePass, `BAND=20m AND COMMENT="CW !"`})
	wantBaselineRules := record.Preset.Baseline.PreparedRules().Definitions()
	record.IncludeBeacons = &off
	record.AllowWWV, record.AllowWCY = &on, nil
	prepared, err := PrepareUserRecord(" n2wq-1 ", record)
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(prepared.data)
	if prepared.ByteLen() != len(expected) || prepared.path != filepath.Join(directory, "N2WQ-1.yaml") {
		t.Fatal("prepared record did not capture its exact bytes/path")
	}
	for _, literal := range []string{capturedExpr, "include_beacons: false\n", "name: CONTEST\n", "last_login_utc: 2026-10-09T12:34:56Z\n"} {
		if !bytes.Contains(expected, []byte(literal)) {
			t.Fatalf("prepared bytes omit %q", literal)
		}
	}
	files, err := os.ReadDir(directory)
	if err != nil || len(files) != 0 || len(record.RecentIPs) != 6 || record.ConfigurationVersion != 0 {
		t.Fatal("preparation changed durable state or caller metadata")
	}
	record.RecentIPs[0] = "CHANGED"
	record.preparedRules = mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=FT8"})
	off, on = true, false
	record.AllowWCY = &on
	record.LastLoginUTC, record.Grid, record.Dialect = time.Time{}, "CHANGED", "go"
	record.Preset.Name = "CHANGED"
	record.Preset.Baseline.preparedRules = nil
	otherDirectory := t.TempDir()
	UserDataDir = otherDirectory
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(filepath.Join(directory, "N2WQ-1.yaml"))
	if err != nil || !bytes.Equal(disk, expected) || prepared.ByteLen() != len(expected) {
		t.Fatal("commit changed the captured payload")
	}
	if _, err := os.Stat(filepath.Join(otherDirectory, "N2WQ-1.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("commit used the later UserDataDir")
	}
	UserDataDir = directory
	loaded, err := LoadUserRecord("N2WQ-1")
	if err != nil || loaded.IncludeBeacons == nil || *loaded.IncludeBeacons ||
		loaded.AllowWWV == nil || !*loaded.AllowWWV || loaded.AllowWCY != nil ||
		len(loaded.PreparedRules().Definitions()) != 1 || loaded.PreparedRules().Definitions()[0].Expr != capturedExpr ||
		len(loaded.RecentIPs) != 5 || loaded.RecentIPs[0] != "a" || loaded.Preset == nil || loaded.Preset.Name != "CONTEST" ||
		!slices.Equal(loaded.Preset.Baseline.PreparedRules().Definitions(), wantBaselineRules) {
		t.Fatalf("captured record did not restore its original values: %v", err)
	}
}

func TestPreparedUserRecordCapturesRelativeDirectory(t *testing.T) {
	usePresetTestDir(t)
	t.Chdir(t.TempDir())
	UserDataDir = "relative-users"
	prepared, err := PrepareUserRecord("N2WQ-1", &UserRecord{})
	if err != nil {
		t.Fatal(err)
	}
	originalPath := prepared.path
	if !filepath.IsAbs(originalPath) {
		t.Fatal("relative target was not captured absolutely")
	}
	if _, err := os.Stat(UserDataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("preparation created its target directory")
	}
	t.Chdir(t.TempDir())
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(originalPath); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(UserDataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("working-directory change redirected commit")
	}
}

func TestPreparedPreferencesMetadataAndAssociation(t *testing.T) {
	for _, configuration := range []bool{false, true} {
		t.Run(map[bool]string{false: "preferences retain", true: "configuration clears"}[configuration], func(t *testing.T) {
			usePresetTestDir(t)
			login := time.Date(2026, 10, 9, 1, 2, 3, 0, time.UTC)
			stored := &UserRecord{
				Filter: *NewFilter(), LastLoginUTC: login,
				RecentIPs: []string{"old-a", "shared", "old-b", "old-c", "old-d"},
				Preset:    &PresetReference{Name: "CONTEST", Baseline: testSavedPreset()},
			}
			if err := SaveUserRecord("N2WQ-1", stored); err != nil {
				t.Fatal(err)
			}
			sibling := &UserRecord{Filter: *NewFilter(), Grid: "IO91", LastLoginUTC: login.Add(time.Hour), RecentIPs: []string{"sibling"}}
			if err := SaveUserRecord("N2WQ-2", sibling); err != nil {
				t.Fatal(err)
			}
			set := testSavedPreset()
			ips := []string{"new", "shared", " "}
			var prepared *PreparedUserRecord
			var err error
			if configuration {
				prepared, err = PrepareConfiguration(" n2wq-1 ", ConfigurationFromPreset(set), nil, ips)
			} else {
				prepared, err = PrepareUserPreferences(" n2wq-1 ", set, ips)
			}
			if err != nil {
				t.Fatal(err)
			}
			expected := bytes.Clone(prepared.data)
			set.Grid, ips[0] = "CHANGED", "CHANGED"
			set.preparedRules = mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=FT8"})
			// A changed record after preparation must not cause commit to reload or
			// merge again. This deliberately controlled edit grants no concurrency
			// authority to real callers, which still hold their SSID lease.
			stored.LastLoginUTC, stored.RecentIPs = login.Add(time.Hour), []string{"later"}
			if err := SaveUserRecord("N2WQ-1", stored); err != nil {
				t.Fatal(err)
			}
			if err := prepared.Commit(); err != nil {
				t.Fatal(err)
			}
			disk, err := os.ReadFile(userRecordPath("N2WQ-1"))
			if err != nil || !bytes.Equal(disk, expected) {
				t.Fatal("preference commit reloaded or encoded again")
			}
			loaded, err := LoadUserRecord("N2WQ-1")
			if err != nil || !loaded.LastLoginUTC.Equal(login) || loaded.Grid != "FN31" ||
				!reflect.DeepEqual(loaded.RecentIPs, []string{"new", "shared", "old-a", "old-b", "old-c"}) {
				t.Fatalf("full-SSID metadata or literal IP ordering/cap changed: %+v, %v", loaded, err)
			}
			if configuration && loaded.Preset != nil || !configuration && (loaded.Preset == nil || loaded.Preset.Name != "CONTEST") {
				t.Fatal("association retain/clear contract changed")
			}
			other, err := LoadUserRecord("N2WQ-2")
			if err != nil || other.Grid != "IO91" || !other.LastLoginUTC.Equal(login.Add(time.Hour)) || other.RecentIPs[0] != "sibling" {
				t.Fatal("preparation changed a sibling SSID's metadata")
			}
		})
	}
}

func TestPreparedConfigurationCapturesExplicitReference(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	ref := &PresetReference{Name: "CONTEST", Baseline: set}
	prepared, err := PrepareConfiguration("N2WQ-1", ConfigurationFromPreset(set), ref, []string{"new"})
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(prepared.data)
	ref.Name, set.Grid = "CHANGED", "CHANGED"
	set.preparedRules = nil
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	disk, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil || !bytes.Equal(disk, expected) {
		t.Fatal("explicit baseline mutation changed committed bytes")
	}
	loaded, err := LoadUserRecord("N2WQ-1")
	if err != nil || !loaded.LastLoginUTC.IsZero() || loaded.Preset == nil || loaded.Preset.Name != "CONTEST" ||
		loaded.Preset.Baseline.Grid != "FN31" || !loaded.Preset.Baseline.PreparedRules().Equal(testSavedPreset().PreparedRules()) {
		t.Fatalf("missing-record metadata or captured baseline changed: %v", err)
	}
}

func TestPreparedRecordLimitsActive(t *testing.T) {
	usePresetTestDir(t)
	large := strings.Repeat("A", (1<<20)+1)
	for _, prepare := range []func() (*PreparedUserRecord, error){
		func() (*PreparedUserRecord, error) { return PrepareUserRecord("N2WQ-1", &UserRecord{Grid: large}) },
		func() (*PreparedUserRecord, error) {
			return PrepareConfiguration("N2WQ-1", Configuration{Settings: SettingsConfiguration{Grid: large}}, nil, nil)
		},
		func() (*PreparedUserRecord, error) {
			return PrepareUserPreferences("N2WQ-1", &SavedPreset{Grid: large}, nil)
		},
	} {
		prepared, err := prepare()
		if !errors.Is(err, ErrStorageAdmission) || prepared != nil {
			t.Fatalf("ordinary-record/configuration quota was bypassed: %v", err)
		}
	}
}

func TestPreparedRecordValidationBoundaries(t *testing.T) {
	usePresetTestDir(t)
	if _, err := PrepareUserRecord("", nil); err == nil || err.Error() != "nil user record" {
		t.Fatal("record nil-before-callsign order changed")
	}
	if _, err := PrepareUserRecord("not a login", &UserRecord{}); err == nil {
		t.Fatal("ordinary record preparation bypassed full-SSID identity validation")
	}
	if _, err := PrepareUserPreferences("not a login", nil, nil); err == nil || err.Error() != "invalid login callsign" {
		t.Fatal("preference login-before-nil order changed")
	}
	if _, err := PrepareUserPreferences("N2WQ-1", &SavedPreset{ConfigurationVersion: V6ConfigurationVersion + 1}, nil); err != nil {
		t.Fatal("preference preparation added input preset-version validation")
	}
	path := userRecordPath("N2WQ-1")
	for _, raw := range []string{"configuration_version: 6\n", "[malformed"} {
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareConfiguration("N2WQ-1", Configuration{}, nil, nil); err == nil || !strings.HasPrefix(err.Error(), "load current user record:") {
			t.Fatal("configuration preparation accepted malformed authoritative state")
		}
		if _, err := PrepareUserPreferences("N2WQ-1", &SavedPreset{}, nil); err == nil || !strings.HasPrefix(err.Error(), "load current user record:") {
			t.Fatal("preference preparation accepted malformed/future authoritative state")
		}
		unchanged, err := os.ReadFile(path)
		if err != nil || string(unchanged) != raw {
			t.Fatal("refused preparation changed authoritative bytes")
		}
		files, err := os.ReadDir(UserDataDir)
		if err != nil || len(files) != 1 {
			t.Fatal("refused preparation left additional files")
		}
	}
}

func TestPreparedRecordRefusalHasNoEffects(t *testing.T) {
	usePresetTestDir(t)
	UserDataDir = filepath.Join(UserDataDir, "not-created")
	for _, record := range []*UserRecord{
		{ConfigurationVersion: V6ConfigurationVersion + 1},
		{Filter: Filter{AllowWWV: nil}, Grid: strings.Repeat("A", MaxPresetBytes+1)},
		{Preset: &PresetReference{Name: "lower", Baseline: testSavedPreset()}},
	} {
		before := *record
		called := false
		if err := saveUserRecord("N2WQ-1", record, func(_ string, _ []byte) error { called = true; return nil }); err == nil || called {
			t.Fatal("refused record reached a writer")
		}
		if !reflect.DeepEqual(before, *record) {
			t.Fatal("refused preparation mutated its input")
		}
		if _, err := os.Stat(UserDataDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("refused preparation created a directory or file")
		}
	}
}

func TestPreparedPresetCollectionCapturesBytes(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	collection := &presetCollection{Presets: map[string]*SavedPreset{"CONTEST": set}}
	prepared, err := preparePresetCollection(collection)
	if err != nil {
		t.Fatal(err)
	}
	expected := bytes.Clone(prepared)
	set.Grid, set.preparedRules = "CHANGED", nil
	delete(collection.Presets, "CONTEST")
	if !bytes.Equal(prepared, expected) || !bytes.Contains(prepared, []byte("CONTEST:\n")) || !bytes.Contains(prepared, []byte("grid: FN31\n")) {
		t.Fatal("private prepared collection retains mutable input")
	}
	files, err := os.ReadDir(UserDataDir)
	if err != nil || len(files) != 0 {
		t.Fatal("private collection preparation had durable effects")
	}
}
