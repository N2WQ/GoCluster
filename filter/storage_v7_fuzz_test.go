package filter

import (
	"bytes"
	"testing"
)

func FuzzV7StorageOwners(f *testing.F) {
	cfg, err := DefaultV7Configuration(SettingsConfiguration{Dialect: "cc"})
	if err != nil {
		f.Fatal(err)
	}
	set, err := V7PresetFromConfiguration(cfg, "cc")
	if err != nil {
		f.Fatal(err)
	}
	config, err := EncodeV7Configuration(cfg)
	if err != nil {
		f.Fatal(err)
	}
	profile, err := EncodeV7UserRecord(&V7UserRecord{Configuration: cfg, Presets: V7PresetReferences{CC: &V7PresetReference{Name: "NATIVE", Baseline: set}}, Notice: V7NoticeState{ProfileResetGeneration: 7}})
	if err != nil {
		f.Fatal(err)
	}
	library, err := EncodeV7PresetCollection(&V7PresetCollection{ResetGeneration: 7, Presets: map[string]*V7SavedPreset{"NATIVE": set}})
	if err != nil {
		f.Fatal(err)
	}
	for kind, seed := range [][]byte{config, profile, library} {
		f.Add(uint8(kind), seed)
	}
	// These guaranteed-valid owners force the optional feature through every
	// storage entry point; malformed mutations must not be the only coverage.
	cfg.Filters.CC, err = cfg.Filters.CC.WithDXBM(CCPass, []string{"UHF"})
	if err != nil || len(cfg.Filters.CC.DXBM.FrequencyOverrides) == 0 {
		f.Fatalf("feature fixture: %v", err)
	}
	set, err = V7PresetFromConfiguration(cfg, "cc")
	if err != nil {
		f.Fatal(err)
	}
	goSet, err := V7PresetFromConfiguration(cfg, "go")
	if err != nil {
		f.Fatal(err)
	}
	config, err = EncodeV7Configuration(cfg)
	if err != nil {
		f.Fatal(err)
	}
	profile, err = EncodeV7UserRecord(&V7UserRecord{Configuration: cfg, Presets: V7PresetReferences{GO: &V7PresetReference{Name: "GO", Baseline: goSet}, CC: &V7PresetReference{Name: "NATIVE", Baseline: set}}})
	if err != nil {
		f.Fatal(err)
	}
	library, err = EncodeV7PresetCollection(&V7PresetCollection{Presets: map[string]*V7SavedPreset{"NATIVE": set, "GO": goSet}})
	if err != nil {
		f.Fatal(err)
	}
	for kind, seed := range [][]byte{config, profile, library} {
		if _, err := canonicalV7FuzzOwner(uint8(kind), seed); err != nil {
			f.Fatalf("invalid feature seed: %v", err)
		}
		f.Add(uint8(kind), seed)
		f.Add(uint8(kind), bytes.Replace(seed, []byte("frequency_overrides:"), []byte("frequency_overrides: null\ninvalid_overrides:"), 1))
		f.Add(uint8(kind), bytes.Replace(seed, []byte("P:"), []byte("UNKNOWN:"), 1))
		f.Add(uint8(kind), append(bytes.Clone(seed), []byte("configuration_version: 7\nconfiguration_version: 7\n")...))
	}
	f.Add(uint8(1), []byte("configuration_version: 8\n"))
	f.Add(uint8(2), []byte("configuration_version: 7\npresets: &loop {NATIVE: *loop}\n"))
	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		if len(data) > MaxUserRecordBytes {
			return
		}
		original := bytes.Clone(data)
		canonical, err := canonicalV7FuzzOwner(kind%3, data)
		if err != nil {
			return
		}
		if !bytes.Equal(data, original) {
			t.Fatal("decoder changed caller bytes")
		}
		reencoded, err := canonicalV7FuzzOwner(kind%3, canonical)
		if err != nil || !bytes.Equal(canonical, reencoded) {
			t.Fatalf("accepted owner is not strict canonical round-trip: %v", err)
		}
	})
}

func canonicalV7FuzzOwner(kind uint8, data []byte) ([]byte, error) {
	switch kind {
	case 0:
		value, err := DecodeV7Configuration(data)
		if err != nil {
			return nil, err
		}
		return EncodeV7Configuration(value)
	case 1:
		value, err := DecodeV7UserRecord(data)
		if err != nil {
			return nil, err
		}
		return EncodeV7UserRecord(value)
	default:
		value, err := DecodeV7PresetCollection(data)
		if err != nil {
			return nil, err
		}
		return EncodeV7PresetCollection(value)
	}
}
