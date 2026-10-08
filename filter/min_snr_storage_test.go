package filter

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMinSNRStoredRoundTrip(t *testing.T) {
	usePresetTestDir(t)
	f := NewFilter()
	f.MinSNR = map[string]int{"FT8": -10, "CW": 0, "REMOVED-MODE_1": 20}
	cfg := ConfigurationFromFilter(f, SettingsConfiguration{Dialect: "cc"}).Clone()
	baseline, err := cfg.Preset()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveConfiguration("N2WQ-1", cfg, &PresetReference{Name: "TEST", Baseline: baseline}, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(userRecordPath("N2WQ-1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"configuration_version: 4\n", "min_snr:\n", "FT8: -10\n", "CW: 0\n", "REMOVED-MODE_1: 20\n"} {
		if !bytes.Contains(raw, []byte(literal)) {
			t.Fatalf("v3 disk encoding omitted %q", literal)
		}
	}
	record, err := LoadUserRecord("N2WQ-1")
	if err != nil || record == nil || record.Preset == nil || !maps.Equal(record.MinSNR, f.MinSNR) || !maps.Equal(record.Preset.Baseline.MinSNR, f.MinSNR) {
		t.Fatalf("reconnect/baseline lost thresholds: %v", err)
	}
	if !ConfigurationFromFilter(&record.Filter, cfg.Settings).Equal(ConfigurationFromPreset(record.Preset.Baseline)) {
		t.Fatal("round trip marked unchanged preset modified")
	}
	record.MinSNR["FT8"] = 10
	if record.Preset.Baseline.MinSNR["FT8"] != -10 {
		t.Fatal("record and applied baseline share mutable thresholds")
	}
	if ConfigurationFromFilter(&record.Filter, cfg.Settings).Equal(ConfigurationFromPreset(record.Preset.Baseline)) {
		t.Fatal("threshold modification not distinguished from preset")
	}
	if err := SavePreset("N2WQ-1", "TEST", baseline); err != nil {
		t.Fatal(err)
	}
	saved, err := LoadPreset("N2WQ-2", "TEST")
	if err != nil || !maps.Equal(saved.MinSNR, f.MinSNR) {
		t.Fatalf("named preset round trip lost exact thresholds: %v", err)
	}
	saved.MinSNR["CW"] = 100
	again, err := LoadPreset("N2WQ", "TEST")
	if err != nil || again.MinSNR["CW"] != 0 {
		t.Fatal("loaded preset thresholds alias stored snapshot")
	}
}

func TestMinSNRStorageVersions(t *testing.T) {
	usePresetTestDir(t)
	for _, version := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			marker, rules := "", "bands: {20m: false}\nallbands: false\n"
			if version != 0 {
				marker = fmt.Sprintf("configuration_version: %d\n", version)
			}
			if version == 2 {
				rules += "dxstates: {TX: false, ON: true}\nalldxstates: false\ndestates: {AB: false}\nalldestates: false\n"
			}
			raw := marker + rules + "preset:\n  name: TEST\n  baseline:\n"
			for _, line := range strings.Split(strings.TrimSuffix(marker+rules, "\n"), "\n") {
				raw += "    " + line + "\n"
			}
			path := userRecordPath("N2WQ-1")
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			record, err := LoadUserRecord("N2WQ-1")
			if err != nil {
				t.Fatal(err)
			}
			if record.ConfigurationVersion != CurrentConfigurationVersion || record.Preset.Baseline.ConfigurationVersion != CurrentConfigurationVersion || len(record.MinSNR) != 0 || len(record.Preset.Baseline.MinSNR) != 0 {
				t.Fatal("legacy thresholds were not disabled in both record and baseline")
			}
			if version == 2 {
				for _, set := range []*Filter{&record.Filter, &record.Preset.Baseline.Filter} {
					if set.AllDXStates || set.AllDEStates || len(set.DXStates) != 2 || set.DXStates["TX"] || !set.DXStates["ON"] || len(set.DEStates) != 1 || set.DEStates["AB"] {
						t.Fatal("v2 state migration was incorrectly tied to latest version")
					}
				}
			} else if !record.AllDXStates || !record.AllDEStates || !record.Preset.Baseline.AllDXStates || !record.Preset.Baseline.AllDEStates {
				t.Fatal("pre-state versions lost unrestricted migration")
			}
			if !ConfigurationFromFilter(&record.Filter, SettingsConfiguration{}).Equal(ConfigurationFromFilter(&record.Preset.Baseline.Filter, SettingsConfiguration{})) {
				t.Fatal("legacy migration changed matching preset baseline")
			}
			data, err := os.ReadFile(path)
			if err != nil || string(data) != raw {
				t.Fatal("read rewrote legacy disk bytes")
			}
			if err := SaveUserRecord("N2WQ-1", record); err != nil {
				t.Fatal(err)
			}
			data, err = os.ReadFile(path)
			if err != nil || !bytes.Contains(data, []byte("configuration_version: 4\n")) || bytes.Contains(data, []byte("min_snr:")) {
				t.Fatal("legacy successful save did not mark v3 disabled thresholds")
			}
		})
	}
	for _, marker := range []string{"", "configuration_version: 1\n", "configuration_version: 2\n"} {
		for _, body := range []string{
			"min_snr: {FT8: -10}\n",
			"min_snr: {}\n",
			"<<: {min_snr: {FT8: -10}}\n",
			"legacy: &legacy {min_snr: {FT8: -10}}\n<<: *legacy\n",
			"legacy: &legacy {min_snr: {FT8: -10}}\n<<: [{bands: {20m: true}}, *legacy]\n",
			"legacy: &legacy {min_snr: {FT8: -10}}\n<<: {<<: *legacy}\n",
			"key: &key min_snr\n? *key\n: {FT8: -10}\n",
		} {
			assertMinSNRStorageProtected(t, marker+body)
		}
	}
	for _, raw := range []string{
		"configuration_version: 3\npreset: {name: TEST, baseline: {configuration_version: 2, min_snr: {FT8: -10}}}\n",
		"configuration_version: 3\npreset: {name: TEST, baseline: {configuration_version: 5}}\n",
		"configuration_version: 3\nmin_snr: {FT8: null}\n",
		"configuration_version: 3\nmin_snr: {FT8: \"-10\"}\n",
		"configuration_version: 3\nmin_snr: {FT8: 99999999999999999999999999999999}\n",
		"configuration_version: 3\nmin_snr: {FT8: 0, FT8: 1}\n",
		"configuration_version: 3\nmin_snr: {&mode FT8: 0, *mode: 1}\n",
	} {
		assertMinSNRStorageProtected(t, raw)
	}
	// An unused legacy anchor or quoted << field never supplies filter rules.
	for _, raw := range []string{
		"legacy: &legacy {min_snr: {BAD: null}}\nbands: {20m: true}\nallbands: false\n",
		"\"<<\": {min_snr: {BAD: null}}\nbands: {20m: true}\nallbands: false\n",
		"key: &key <<\n? *key\n: {min_snr: {BAD: null}}\nbands: {20m: true}\nallbands: false\n",
	} {
		var record UserRecord
		if err := yaml.Unmarshal([]byte(raw), &record); err != nil || len(record.MinSNR) != 0 || !record.Bands["20m"] || record.AllBands {
			t.Fatalf("ordinary legacy decoder contract changed: %v", err)
		}
	}
}

func assertMinSNRStorageProtected(t *testing.T, raw string) {
	t.Helper()
	path := userRecordPath("N2WQ-1")
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ-1"); err == nil {
		t.Fatal("admitted invalid stored MINSNR/version contract")
	}
	if err := SaveConfiguration("N2WQ-1", ConfigurationFromFilter(NewFilter(), SettingsConfiguration{}), nil, nil); err == nil {
		t.Fatal("write replaced protected record")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != raw {
		t.Fatal("protected disk bytes changed")
	}
}

func TestMinSNRStoragePredecodeBounds(t *testing.T) {
	for count, wantValid := range map[int]bool{128: true, 129: false} {
		var entries strings.Builder
		for i := 0; i < count; i++ {
			fmt.Fprintf(&entries, "MODE%d: 0, ", i)
		}
		for _, raw := range []string{
			"configuration_version: 3\nmin_snr: {" + entries.String() + "}\n",
			"configuration_version: 3\n<<: {min_snr: &rules {" + entries.String() + "}}\nmin_snr: *rules\n",
		} {
			var doc yaml.Node
			if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
				t.Fatal(err)
			}
			if valid := validateStoredMinSNRFields(doc.Content[0], 3) == nil; valid != wantValid {
				t.Fatalf("predecode count %d: accepted=%v want=%v", count, valid, wantValid)
			}
		}
	}
	for byteCount, wantValid := range map[int]bool{65536: true, 65537: false} {
		// Explicit YAML complex keys accept large scalar names.
		raw := "configuration_version: 3\nmin_snr:\n  ? &mode " + strings.Repeat("A", byteCount-1) + "\n  : 0\n  B: -10\n"
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatal(err)
		}
		if valid := validateStoredMinSNRFields(doc.Content[0], 3) == nil; valid != wantValid {
			t.Fatalf("predecode raw bytes %d: accepted=%v want=%v", byteCount, valid, wantValid)
		}
		if wantValid {
			var record UserRecord
			if err := doc.Content[0].Decode(&record); err != nil || len(record.MinSNR) != 2 {
				t.Fatalf("bounded exact bytes failed typed decode: %v", err)
			}
		}
	}
	// Resolve key aliases before counting their bytes; the anchor definition is
	// unrelated metadata to this precheck and not part of the threshold map.
	aliasRaw := "key: &mode " + strings.Repeat("A", 65536) + "\nmin_snr:\n  ? *mode\n  : 0\n  B: 1\n"
	var aliasDoc yaml.Node
	if err := yaml.Unmarshal([]byte(aliasRaw), &aliasDoc); err != nil {
		t.Fatal(err)
	}
	if err := validateStoredMinSNRFields(aliasDoc.Content[0], 3); err == nil || !strings.Contains(err.Error(), "mode-key bytes") {
		t.Fatalf("aliased raw key bytes were not bounded: %v", err)
	}
	for _, raw := range []string{
		"configuration_version: 3\nmin_snr: null\n",
		"configuration_version: 3\nmin_snr: []\n",
		"configuration_version: 3\nmin_snr: {ft8: 0}\n",
		"configuration_version: 3\nmin_snr: {\"FT8 \": 0}\n",
		"configuration_version: 3\nmin_snr: {42: 0}\n",
		"configuration_version: 3\nmin_snr: {\"\": 0}\n",
	} {
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
			t.Fatal(err)
		}
		if validateStoredMinSNRFields(doc.Content[0], 3) == nil {
			t.Fatal("malformed min_snr map reached typed decode")
		}
	}
}
