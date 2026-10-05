package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

func usePresetTestDir(t *testing.T) {
	t.Helper()
	previous := UserDataDir
	UserDataDir = t.TempDir()
	t.Cleanup(func() { UserDataDir = previous })
}

func TestPresetUnderscoreNamesRejected(t *testing.T) {
	usePresetTestDir(t)
	if err := SavePreset("N2WQ", "GOOD-NAME", testSavedPreset()); err != nil {
		t.Fatal(err)
	}
	path, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := SavePreset("N2WQ", "GOOD_BAD", testSavedPreset()); err == nil {
		t.Fatal("SAVE accepted an underscore")
	}
	if _, err := LoadPreset("N2WQ", "GOOD_BAD"); err == nil {
		t.Fatal("LOAD accepted an underscore")
	}
	if err := DeletePreset("N2WQ", "GOOD_BAD"); err == nil {
		t.Fatal("DELETE accepted an underscore")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("invalid name changed the collection")
	}
}

func testSavedPreset() *SavedPreset {
	f := NewFilter()
	f.SetBand("20m", true)
	f.SetMode("CW", true)
	f.SetSource("HUMAN", true)
	f.AddDXCallsignPattern("K1*")
	f.AddBlockDECallsignPattern("W1XYZ")
	f.SetDXDXCC(291, true)
	f.SetDXContinent("EU", false)
	f.SetDEZone(5, true)
	f.SetDXGrid2Prefix("FN", true)
	f.SetConfidenceSymbol("?", false)
	f.SetBeaconEnabled(false)
	f.SetWWVEnabled(false)
	f.SetWCYEnabled(false)
	f.SetAnnounceEnabled(false)
	f.SetSelfEnabled(false)
	f.SetToxicEnabled(false)
	return &SavedPreset{ConfigurationVersion: CurrentConfigurationVersion, Filter: *f, Dialect: "cc", DedupePolicy: "SLOW", Grid: "FN31", NoiseClass: "URBAN", PathMinObservationCount: 40, SolarSummaryMinutes: 30}
}

func TestPresetsOwnershipAndCRUD(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	for _, name := range []string{"zulu", "Contest", "alpha"} {
		if err := SavePreset("N2WQ-1", name, set); err != nil {
			t.Fatal(err)
		}
	}
	names, err := ListPresets("n2wq-2")
	if err != nil || strings.Join(names, ",") != "ALPHA,CONTEST,ZULU" {
		t.Fatalf("names=%v err=%v", names, err)
	}
	loaded, err := LoadPreset("N2WQ", "contest")
	if err != nil || loaded.Grid != "FN31" || loaded.Dialect != "cc" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	set.Grid = "IO91"
	if err := SavePreset("N2WQ-2", "CONTEST", set); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadPreset("N2WQ-1", "contest")
	if err != nil || loaded.Grid != "IO91" {
		t.Fatalf("replacement=%+v err=%v", loaded, err)
	}
	if err := DeletePreset("N2WQ-3", "alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPreset("N2WQ", "alpha"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted name: %v", err)
	}
	if err := DeletePreset("N2WQ", "missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing delete: %v", err)
	}
	other, err := ListPresets("K1ABC")
	if err != nil || len(other) != 0 {
		t.Fatalf("other owner=%v err=%v", other, err)
	}
	if _, err := LoadPreset("K1ABC", "contest"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("other owner load: %v", err)
	}
	for _, call := range []string{"EA8/N2WQ-1", "N2WQ-X"} {
		if err := SavePreset(call, "CON", set); err != nil {
			t.Fatalf("portable/device name: %v", err)
		}
	}
	if _, err := LoadPreset("EA8/N2WQ-2", "con"); err != nil {
		t.Fatal(err)
	}
	base, _ := presetCollectionPath("N2WQ")
	portable, _ := presetCollectionPath("EA8/N2WQ")
	nonNumeric, _ := presetCollectionPath("N2WQ-X")
	if base == portable || base == nonNumeric || filepath.Dir(portable) != filepath.Join(UserDataDir, "presets") {
		t.Fatalf("unsafe/collapsed paths: %q %q %q", base, portable, nonNumeric)
	}
	if err := SavePreset("../BAD", "one", set); err == nil {
		t.Fatal("invalid owner accepted")
	}
}

func TestPresetPreferencesRoundTrip(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	set.NearbyEnabled = true
	set.NearbySnapshot = &NearbyLocationSnapshot{AllDXContinents: true}
	set.NearbyUserFine, set.NearbyUserCoarse = 10, 11
	want, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if err := SavePreset("N2WQ", "all", set); err != nil {
		t.Fatal(err)
	}
	set.Bands["40m"] = true
	set.DXCallsigns[0] = "NEW*"
	*set.AllowWWV = true
	loaded, err := LoadPreset("N2WQ-1", "ALL")
	if err != nil {
		t.Fatal(err)
	}
	got, err := yaml.Marshal(loaded)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("round trip changed preferences: err=%v\nwant=%s\ngot=%s", err, want, got)
	}
	if loaded.NearbySnapshot != nil || loaded.NearbyUserFine != pathreliability.InvalidCell || loaded.NearbyUserCoarse != pathreliability.InvalidCell {
		t.Fatal("runtime NEARBY caches survived serialization")
	}
	loaded.Bands["80m"] = true
	loaded.DXCallsigns[0] = "MUTATED*"
	*loaded.AllowToxic = true
	again, err := LoadPreset("N2WQ-2", "all")
	if err != nil {
		t.Fatal(err)
	}
	got, err = yaml.Marshal(again)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("loaded snapshot aliases another load or the store")
	}
	path, _ := presetCollectionPath("N2WQ")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"recent_ips", "last_login_utc", "nearby_snapshot", "nearby_user", "diag_mode", "read_pause"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("snapshot includes %s", forbidden)
		}
	}
}

func TestPresetCountAndSizeBounds(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	for i := range 20 {
		if err := SavePreset("N2WQ", fmt.Sprintf("SET%d", i), set); err != nil {
			t.Fatal(err)
		}
	}
	path, _ := presetCollectionPath("N2WQ")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := SavePreset("N2WQ-1", "EXTRA", set); err == nil {
		t.Fatal("21st set accepted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("rejected save changed collection")
	}
	set.Grid = "IO91"
	if err := SavePreset("N2WQ-1", "SET0", set); err != nil {
		t.Fatalf("replacement at capacity: %v", err)
	}
	if err := DeletePreset("N2WQ", "SET1"); err != nil {
		t.Fatal(err)
	}
	if err := SavePreset("N2WQ-2", "EXTRA", set); err != nil {
		t.Fatal(err)
	}
	large := testSavedPreset()
	large.DXCallsigns = []string{"A"}
	encoded, err := yaml.Marshal(large)
	if err != nil {
		t.Fatal(err)
	}
	large.DXCallsigns[0] = strings.Repeat("A", (256<<10)-len(encoded)+1)
	encoded, err = yaml.Marshal(large)
	if err != nil || len(encoded) != 256<<10 {
		t.Fatalf("boundary fixture size=%d err=%v", len(encoded), err)
	}
	if err := SavePreset("K1ABC", "LARGE", large); err != nil {
		t.Fatalf("exact boundary: %v", err)
	}
	large.DXCallsigns[0] += "A"
	if err := SavePreset("K1ABC", "LARGE", large); err == nil {
		t.Fatal("oversized replacement accepted")
	}
	loaded, err := LoadPreset("K1ABC", "large")
	if err != nil || len(loaded.DXCallsigns[0]) != len(large.DXCallsigns[0])-1 {
		t.Fatal("oversized replacement damaged prior snapshot")
	}
}

func TestPresetCorruptCollections(t *testing.T) {
	usePresetTestDir(t)
	tooMany := presetCollection{Presets: make(map[string]*SavedPreset)}
	for i := range 21 {
		tooMany.Presets[fmt.Sprintf("S%d", i)] = testSavedPreset()
	}
	tooManyBytes, err := yaml.Marshal(tooMany)
	if err != nil {
		t.Fatal(err)
	}
	oversized := testSavedPreset()
	oversized.DXCallsigns = []string{strings.Repeat("A", 256<<10)}
	oversizedBytes, err := yaml.Marshal(presetCollection{Presets: map[string]*SavedPreset{"LARGE": oversized}})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"", "presets: null\n", "presets: []\n", "presets: {lower: {}}\n", "presets: {GOOD_BAD: {}}\n", "presets: {VALID: null}\n", "presets: {}\nunknown: true\n", "presets: {}\n---\npresets: {}\n", "presets: {VALID: {recent_ips: [1.2.3.4]}}\n", string(tooManyBytes), string(oversizedBytes), strings.Repeat(" ", (8<<20)+1)} {
		path, err := presetCollectionPath("N2WQ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(input), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := ListPresets("N2WQ"); err == nil {
			t.Fatalf("corrupt collection accepted (%d bytes)", len(input))
		}
		if err := SavePreset("N2WQ", "NEW", testSavedPreset()); err == nil {
			t.Fatal("save overwrote corrupt collection")
		}
		if err := DeletePreset("N2WQ", "VALID"); err == nil {
			t.Fatal("delete overwrote corrupt collection")
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != input {
			t.Fatal("corrupt collection modified")
		}
	}
}

func TestPresetCollectionReadBoundary(t *testing.T) {
	usePresetTestDir(t)
	path, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	content := "presets: {}\n"
	content += strings.Repeat(" ", (8<<20)-len(content))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	if names, err := ListPresets("N2WQ-1"); err != nil || len(names) != 0 {
		t.Fatalf("exact 8 MiB collection rejected: %v %v", names, err)
	}
}

func TestPresetNormalizedSizeBound(t *testing.T) {
	usePresetTestDir(t)
	set := testSavedPreset()
	set.ConfigurationVersion = 0                // This fixture exercises absent-marker legacy migration.
	set.Grid = strings.Repeat("\u023f", 90_000) // Uppercase uses three UTF-8 bytes instead of two.
	data, err := yaml.Marshal(presetCollection{Presets: map[string]*SavedPreset{"GROW": set}})
	if err != nil {
		t.Fatal(err)
	}
	path, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPreset("N2WQ-1", "GROW"); err == nil {
		t.Fatal("normalization bypassed the serialized preference bound")
	}
}

func TestPresetOversizedLiveInput(t *testing.T) {
	for _, kind := range []string{"pattern", "map", "metadata"} {
		set := testSavedPreset()
		switch kind {
		case "pattern":
			set.DXCallsigns = []string{strings.Repeat("A", 1<<20)}
		case "map":
			set.Bands = map[string]bool{strings.Repeat("A", 1<<20): true}
		case "metadata":
			set.Grid = strings.Repeat("A", 1<<20)
		}
		if presetFitsMinimumSize(set) {
			t.Fatalf("%s bypassed early bound", kind)
		}
		if _, err := set.Clone(); err == nil {
			t.Fatalf("%s oversized input accepted", kind)
		}
	}
}

func TestPresetConcurrentUpdates(t *testing.T) {
	usePresetTestDir(t)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 20)
	for i := range 20 {
		wg.Go(func() {
			errorsCh <- SavePreset(fmt.Sprintf("N2WQ-%d", i), fmt.Sprintf("S%d", i), testSavedPreset())
		})
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	names, err := ListPresets("N2WQ")
	if err != nil || len(names) != 20 {
		t.Fatalf("lost concurrent saves: %v %v", names, err)
	}
	for i := range 10 {
		wg.Go(func() {
			if err := DeletePreset("N2WQ-1", fmt.Sprintf("S%d", i)); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() {
			if err := SavePreset("N2WQ-2", fmt.Sprintf("S%d", i+10), testSavedPreset()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for i := range 10 {
		if err := SavePreset("N2WQ-3", fmt.Sprintf("NEW%d", i), testSavedPreset()); err != nil {
			t.Fatal(err)
		}
	}
	names, err = ListPresets("N2WQ")
	if err != nil || len(names) != 20 || len(presetLocks) != 64 {
		t.Fatalf("churn violated bounds: names=%v err=%v", names, err)
	}
	for i := 10; i < 20; i++ {
		if _, err := LoadPreset("N2WQ", fmt.Sprintf("S%d", i)); err != nil {
			t.Fatalf("lost unrelated set: %v", err)
		}
	}
}

func TestAtomicUserFileFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "record.yaml")
	if err := os.WriteFile(path, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("injected failure")
	for _, stage := range []string{"write", "replace"} {
		write := writeUserFileBytes
		replace := replaceUserFile
		if stage == "write" {
			write = func(file *os.File, _ []byte) error {
				if err := writeUserFileBytes(file, []byte("partial")); err != nil {
					return err
				}
				return failure
			}
		}
		if stage == "replace" {
			replace = func(_, _ string) error { return failure }
		}
		if err := writeAtomicUserFileWith(path, []byte("new"), write, replace); !errors.Is(err, failure) {
			t.Fatalf("%s failure=%v", stage, err)
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != "original" {
			t.Fatalf("%s damaged target", stage)
		}
		files, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".preset-*.tmp"))
		if err != nil || len(files) != 0 {
			t.Fatalf("%s leaked temp files: %v", stage, files)
		}
	}
	if err := writeAtomicUserFile(path, []byte("committed")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "committed" {
		t.Fatal("replacement did not commit")
	}
}

func TestPresetFailedReplace(t *testing.T) {
	usePresetTestDir(t)
	if err := SavePreset("N2WQ", "ONE", testSavedPreset()); err != nil {
		t.Fatal(err)
	}
	path, _ := presetCollectionPath("N2WQ")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	store := presetStore{write: func(path string, data []byte) error {
		return writeAtomicUserFileWith(path, data, writeUserFileBytes, func(_, _ string) error { return errors.New("injected replace failure") })
	}}
	if err := store.save("N2WQ-1", "ONE", &SavedPreset{Filter: *NewFilter(), Grid: "IO91"}); err == nil {
		t.Fatal("replacement failure ignored")
	}
	if err := store.delete("N2WQ-2", "ONE"); err == nil {
		t.Fatal("delete failure ignored")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatal("failed update damaged collection")
	}
}

func TestSaveUserPreferences(t *testing.T) {
	usePresetTestDir(t)
	login := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	original := &UserRecord{Filter: *NewFilter(), RecentIPs: []string{"192.0.2.1"}, LastLoginUTC: login, Grid: "IO91"}
	if err := SaveUserRecord("N2WQ-2", original); err != nil {
		t.Fatal(err)
	}
	if err := SaveUserPreferences("N2WQ-2", testSavedPreset(), []string{"192.0.2.2"}); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadUserRecord("N2WQ-2")
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.LastLoginUTC.Equal(login) || strings.Join(loaded.RecentIPs, ",") != "192.0.2.2,192.0.2.1" || loaded.Grid != "FN31" || loaded.Dialect != "cc" || loaded.SolarSummaryMinutes != 30 {
		t.Fatalf("metadata/preferences incorrect: %+v", loaded)
	}
	before, err := os.ReadFile(userRecordPath("N2WQ-2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := saveUserPreferences("N2WQ-2", testSavedPreset(), nil, func(_ string, _ []byte) error { return errors.New("injected persistence failure") }); err == nil {
		t.Fatal("write error ignored")
	}
	after, err := os.ReadFile(userRecordPath("N2WQ-2"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("failed preferences write changed default")
	}
	if err := SaveUserPreferences("../N2WQ", testSavedPreset(), nil); err == nil {
		t.Fatal("unsafe login path accepted")
	}
	if err := SaveUserPreferences("EA8/N2WQ-2", testSavedPreset(), nil); err != nil {
		t.Fatalf("portable login preferences: %v", err)
	}
}

func FuzzNormalizePresetName(f *testing.F) {
	for _, seed := range []string{"contest", "1-main", "1_main", "CON", "", "-one", "two words", "../escape", "Å", strings.Repeat("A", 32), strings.Repeat("A", 33)} {
		f.Add(seed)
	}
	grammar := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,31}$`)
	f.Fuzz(func(t *testing.T, name string) {
		canonical, err := NormalizePresetName(name)
		valid := grammar.MatchString(name)
		if valid != (err == nil) {
			t.Fatalf("grammar mismatch for %q: %v", name, err)
		}
		if valid && canonical != strings.ToUpper(name) {
			t.Fatalf("incorrect canonical key %q", canonical)
		}
	})
}
