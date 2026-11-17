package filter

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func metaBandStorageFixture(t *testing.T) (V7Configuration, *V7UserRecord, *V7PresetCollection) {
	t.Helper()
	c := v7TestConfiguration(t)
	var err error
	c.Filters.CC, err = c.Filters.CC.WithDXBM(CCPass, []string{"UHF"})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Filters.CC.DXBM.FrequencyOverrides) == 0 {
		t.Fatal("feature fixture has no frequency overrides")
	}
	goSet, err := V7PresetFromConfiguration(c, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccSet, err := V7PresetFromConfiguration(c, "cc")
	if err != nil {
		t.Fatal(err)
	}
	record := &V7UserRecord{Configuration: c, Presets: V7PresetReferences{GO: &V7PresetReference{Name: "GO", Baseline: goSet}, CC: &V7PresetReference{Name: "CC", Baseline: ccSet}}, RecentIPs: []string{"192.0.2.33"}, LastLoginUTC: time.Unix(1700000000, 0).UTC(), Notice: V7NoticeState{ProfileResetGeneration: 7, ProfileAcknowledgedGeneration: 7}}
	library := &V7PresetCollection{ResetGeneration: 7, Presets: map[string]*V7SavedPreset{"GO": goSet, "CC": ccSet}}
	return c, record, library
}

func TestMetaBandStorageRoundTripsAndOwnership(t *testing.T) {
	c, record, library := metaBandStorageFixture(t)
	profile, err := EncodeV7UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeV7UserRecord(profile)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Configuration.Equal(c) || got.Configuration.Fingerprint() != c.Fingerprint() || got.Configuration.Settings != c.Settings || !got.Presets.GO.Baseline.MatchesConfiguration(c) || !got.Presets.CC.Baseline.MatchesConfiguration(c) || got.Notice != record.Notice || !got.LastLoginUTC.Equal(record.LastLoginUTC) || strings.Join(got.RecentIPs, ",") != "192.0.2.33" {
		t.Fatal("profile lost feature, independent baselines, preferences or metadata")
	}
	configuration, err := EncodeV7Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	libraryBytes, err := EncodeV7PresetCollection(library)
	if err != nil {
		t.Fatal(err)
	}
	for kind, raw := range [][]byte{configuration, profile, libraryBytes} {
		if !bytes.Contains(raw, []byte("frequency_overrides:")) {
			t.Fatalf("owner %d omitted feature", kind)
		}
		canonical, err := canonicalV7FuzzOwner(uint8(kind), raw)
		if err != nil || !bytes.Equal(canonical, raw) {
			t.Fatalf("owner %d unstable roundtrip: %v", kind, err)
		}
	}
	gotLibrary, err := DecodeV7PresetCollection(libraryBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !gotLibrary.Presets["GO"].MatchesConfiguration(c) || !gotLibrary.Presets["CC"].MatchesConfiguration(c) {
		t.Fatal("mixed library lost bank or shared settings")
	}
	before := got.Configuration.Fingerprint()
	for key, value := range record.Configuration.Filters.CC.DXBM.FrequencyOverrides {
		record.Configuration.Filters.CC.DXBM.FrequencyOverrides[key] = !value
		break
	}
	for key, value := range record.Presets.CC.Baseline.CC.DXBM.FrequencyOverrides {
		record.Presets.CC.Baseline.CC.DXBM.FrequencyOverrides[key] = !value
		break
	}
	if got.Configuration.Fingerprint() != before || !got.Presets.CC.Baseline.MatchesConfiguration(got.Configuration) || !gotLibrary.Presets["CC"].MatchesConfiguration(got.Configuration) {
		t.Fatal("decoded profile/library retained caller maps")
	}
	clone := got.Configuration.Clone()
	for key, value := range clone.Filters.CC.DXBM.FrequencyOverrides {
		clone.Filters.CC.DXBM.FrequencyOverrides[key] = !value
		break
	}
	if clone.Equal(got.Configuration) || clone.Fingerprint() == before || got.Configuration.Fingerprint() != before {
		t.Fatal("override clone/equality/fingerprint omitted state or aliased original")
	}
}

// Walk the encoded owner rather than relying on indentation or a particular
// first cell ID. Mutations below target the actual admitted optional field.
func metaBandOverrideMapping(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.MappingNode {
		for i := 0; i < len(node.Content); i += 2 {
			if node.Content[i].Value == "frequency_overrides" {
				return node.Content[i+1]
			}
		}
	}
	for _, child := range node.Content {
		if found := metaBandOverrideMapping(child); found != nil {
			return found
		}
	}
	return nil
}

func TestMetaBandStorageStrictOverrides(t *testing.T) {
	c, record, library := metaBandStorageFixture(t)
	configBytes, err := EncodeV7Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	profileBytes, err := EncodeV7UserRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	libraryBytes, err := EncodeV7PresetCollection(library)
	if err != nil {
		t.Fatal(err)
	}
	for kind, raw := range [][]byte{configBytes, profileBytes, libraryBytes} {
		for _, mutation := range []string{"null", "unknown", "duplicate", "duplicate-case", "wrong-bool", "oversized"} {
			t.Run(fmt.Sprintf("owner=%d/%s", kind, mutation), func(t *testing.T) {
				var document yaml.Node
				if err := yaml.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				mapping := metaBandOverrideMapping(&document)
				if mapping == nil || len(mapping.Content) == 0 {
					t.Fatal("missing override fixture")
				}
				switch mutation {
				case "null":
					*mapping = yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}
				case "unknown":
					mapping.Content[0].Value = "P:123456789"
				case "duplicate", "duplicate-case":
					key, value := *mapping.Content[0], *mapping.Content[1]
					if mutation == "duplicate-case" {
						key.Value = strings.ToLower(key.Value)
					}
					mapping.Content = append(mapping.Content, &key, &value)
				case "wrong-bool":
					mapping.Content[1].Tag, mapping.Content[1].Value = "!!str", "true"
				case "oversized":
					mapping.Content = nil
					for i := 0; i <= len(CCSupportedFrequencyCells()); i++ {
						mapping.Content = append(mapping.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: fmt.Sprintf("P:%d", i)}, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
					}
				}
				bad, err := yaml.Marshal(&document)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := canonicalV7FuzzOwner(uint8(kind), bad); err == nil {
					t.Fatal("malformed override document accepted")
				}
			})
		}
	}
}

func TestMetaBandStorageLegacyOmissionAndRefusalPreservesFiles(t *testing.T) {
	usePresetTestDir(t)
	legacy, err := EncodeV7UserRecord(&V7UserRecord{Configuration: v7TestConfiguration(t)})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(legacy, []byte("frequency_overrides")) {
		t.Fatal("unused feature changed legacy shape")
	}
	for _, feature := range []bool{false, true} {
		_, record, library := metaBandStorageFixture(t)
		profile := legacy
		if feature {
			profile, err = EncodeV7UserRecord(record)
			if err != nil {
				t.Fatal(err)
			}
		}
		badProfile := append([]byte("unsupported_feature: true\n"), profile...)
		path := writeUpgradeFixture(t, "K1META-1", badProfile)
		lib, err := EncodeV7PresetCollection(library)
		if err != nil {
			t.Fatal(err)
		}
		libraryPath := writeUpgradeLibrary(t, lib)
		_, outcome, err := UpgradeV7UserRecordForLoginWithOutcome("K1META-1", nil)
		if err == nil || outcome.ProfileReset || outcome.PresetLibraryReset {
			t.Fatalf("unsupported profile entered reset path: %+v %v", outcome, err)
		}
		assertUpgradeBytes(t, path, badProfile)
		assertUpgradeBytes(t, libraryPath, lib)
		if _, err := os.Stat(path + v7BackupSuffix); !os.IsNotExist(err) {
			t.Fatalf("refusal created reset backup: %v", err)
		}
		if _, err := os.Stat(libraryPath + v7BackupSuffix); !os.IsNotExist(err) {
			t.Fatalf("refusal touched library backup: %v", err)
		}
	}
}
