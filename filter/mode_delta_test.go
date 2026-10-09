// File role: Matcher-backed pointwise MODE delta and marked-storage oracles.
package filter

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"dxcluster/spot"
)

func modeProbe(mode string) *spot.Spot {
	return &spot.Spot{Band: "20m", BandNorm: "20m", Mode: mode, ModeNorm: mode}
}

func ternaryModeMap(code int) map[string]bool {
	values := make(map[string]bool)
	for _, token := range []string{"CW", "FT8", "CUSTOM"} {
		if state := code % 3; state != 0 {
			values[token] = state == 2
		}
		code /= 3
	}
	return values
}

func TestModeDeltaPreservesPointwiseVisibility(t *testing.T) {
	observed := []string{"CW", "FT8", "CUSTOM", "FUTURE", UnknownModeToken}
	for flags := range 4 {
		for allow := range 27 {
			for block := range 27 {
				for _, selected := range []string{"CW", "FT8"} {
					for _, enabled := range []bool{false, true} {
						f := NewFilter()
						f.Modes, f.BlockModes = ternaryModeMap(allow), ternaryModeMap(block)
						f.AllModes, f.BlockAllModes = flags&1 != 0, flags&2 != 0
						before := make([]bool, len(observed))
						for i, mode := range observed {
							before[i] = f.Matches(modeProbe(mode))
						}
						f.SetMode(selected, enabled)
						for i, mode := range observed {
							want := before[i]
							if mode == selected {
								want = enabled
							}
							if got := f.Matches(modeProbe(mode)); got != want {
								t.Fatalf("flags=%d allow=%d block=%d %s=%v changed %s to %v, want %v", flags, allow, block, selected, enabled, mode, got, want)
							}
						}
						if f.UnknownModeVisible() != before[len(before)-1] {
							t.Fatal("unlisted UNKNOWN feedback disagrees with matcher")
						}
					}
				}
			}
		}
	}
}

func TestModeDeltaRetainsFallbackAfterTaxonomyChange(t *testing.T) {
	old := spot.CurrentTaxonomy()
	t.Cleanup(func() { spot.ConfigureTaxonomy(old) })
	raw, err := os.ReadFile(filepath.Join("..", "data", "config", "spot_taxonomy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	raw = bytes.ReplaceAll(raw, []byte("\r\n"), []byte("\n"))
	addition := []byte("modes:\n  - name: FUTUREMODE\n    display: FUTUREMODE\n    filter_visible: true\n")
	raw = bytes.Replace(raw, []byte("modes:\n"), addition, 1)
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	open, closed := NewFilter(), NewFilter()
	open.ResetModes()
	closed.ResetModes()
	closed.BlockAllModes = true
	complete := NewFilter()
	complete.Modes = make(map[string]bool)
	for _, mode := range SupportedModes() {
		complete.Modes[mode] = true
	}
	for _, f := range []*Filter{open, closed, complete} {
		f.SetMode("FT8", false)
		f.SetMode("FT8", true)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	spot.ConfigureTaxonomy(taxonomy)
	if !IsSupportedMode("FUTUREMODE") || !open.Matches(modeProbe("FUTUREMODE")) || closed.Matches(modeProbe("FUTUREMODE")) || complete.Matches(modeProbe("FUTUREMODE")) {
		t.Fatal("named deltas did not preserve future open/restrictive fallback")
	}
}

func TestModeDeltaMarkedOwnersRetainExactRules(t *testing.T) {
	old := UserDataDir
	UserDataDir = t.TempDir()
	t.Cleanup(func() { UserDataDir = old })
	for index, rules := range []StringRules{
		{AllowAll: true},
		{Allow: map[string]bool{"FT8": true}},
		{AllowAll: true, Allow: map[string]bool{"CW": false}},
		{AllowAll: true, Allow: map[string]bool{"CW": false, "CUSTOM": true}},
		{BlockAll: true, Allow: map[string]bool{"CW": true, "CUSTOM": true}},
	} {
		f := NewFilter()
		f.Modes, f.BlockModes, f.AllModes, f.BlockAllModes = rules.Allow, rules.Block, rules.AllowAll, rules.BlockAll
		f.SetMode("FT8", false)
		cfg := ConfigurationFromFilter(f, SettingsConfiguration{})
		set, err := cfg.Preset()
		if err != nil {
			t.Fatal(err)
		}
		if err := SaveUserRecord("N2WQ-1", &UserRecord{ConfigurationVersion: CurrentConfigurationVersion, Filter: *f, Preset: &PresetReference{Name: "MODES", Baseline: set}}); err != nil {
			t.Fatal(err)
		}
		if err := SavePreset("N2WQ-1", "MODES", set); err != nil {
			t.Fatal(err)
		}
		record, err := LoadUserRecord("N2WQ-1")
		if err != nil {
			t.Fatal(err)
		}
		loaded, err := LoadPreset("N2WQ-1", "MODES")
		if err != nil {
			t.Fatal(err)
		}
		want := cfg.Filters.Modes
		if record.Preset == nil || record.Preset.Baseline == nil {
			t.Fatal("marked baseline was lost")
		}
		for _, actual := range []*Filter{&record.Filter, &loaded.Filter, &record.Preset.Baseline.Filter} {
			if got := ConfigurationFromFilter(actual, SettingsConfiguration{}).Filters.Modes; !reflect.DeepEqual(got, want) {
				t.Fatalf("case %d marked owner changed exact modes: got %+v want %+v", index, got, want)
			}
			for _, mode := range []string{"CW", "FT8", "CUSTOM", "FUTURE", UnknownModeToken} {
				if actual.Matches(modeProbe(mode)) != f.Matches(modeProbe(mode)) {
					t.Fatalf("case %d reload changed %s", index, mode)
				}
			}
		}
	}
}
