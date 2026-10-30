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
