package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func storageEmptyPresets(count int) string {
	var raw strings.Builder
	raw.WriteString("presets:\n")
	for i := range count {
		fmt.Fprintf(&raw, "  P%06d: {configuration_version: 6, configuration: {filters: {rules: []}, settings: {}}}\n", i)
	}
	return raw.String()
}

func writeStorageCollection(t *testing.T, raw string) string {
	t.Helper()
	path, err := presetCollectionPath("N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestStorageCollectionVisibleCardinality(t *testing.T) {
	usePresetTestDir(t)
	for _, count := range []int{20, 21, 1000} {
		raw := storageEmptyPresets(count)
		path := writeStorageCollection(t, raw)
		names, err := ListPresets("N2WQ-1")
		if count == 20 && (err != nil || len(names) != count) ||
			count > 20 && !errors.Is(err, ErrStorageAdmission) {
			t.Fatalf("native count %d: %v", count, err)
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != raw {
			t.Fatal("count refusal changed bytes")
		}
	}
}

func TestStorageCollectionWorkActiveAcrossAllPresetAPIs(t *testing.T) {
	usePresetTestDir(t)
	raw := "presets:\n  P0: &set {configuration_version: 6, configuration: {filters: {rules: []}, settings: {grid: &text " +
		strings.Repeat("A", 256<<10) + ", dialect: *text, noise_class: *text, dedupe_policy: *text}}}\n"
	for i := 1; i < MaxPresets; i++ {
		raw += fmt.Sprintf("  P%d: *set\n", i)
	}
	path := writeStorageCollection(t, raw)
	for _, operation := range []func() error{
		func() error { _, err := ListPresets("N2WQ-1"); return err },
		func() error { _, err := LoadPreset("N2WQ-1", "P0"); return err },
		func() error { return DeletePreset("N2WQ-1", "P0") },
		func() error { return SavePreset("N2WQ-1", "OTHER", testSavedPreset()) },
	} {
		if err := operation(); !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "work") {
			t.Fatal("native preset API bypassed shared work budget", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, []byte(raw)) {
			t.Fatal("work refusal changed library")
		}
	}
}

func TestStorageCollectionCardinalityObserverSaturates(t *testing.T) {
	state := &storageDecodeState{limits: storageDecodeLimits{presets: MaxPresets}}
	entries := &storagePresetEntries{keys: make(map[string]bool)}
	for cycle := range 10 {
		for i := range 1000 {
			state.observePresetEntry(entries, fmt.Sprintf("%d-%d", cycle, i))
		}
		if len(entries.keys) != MaxPresets+1 || !entries.overflow || !state.presetCountExceeded {
			t.Fatal("operation scratch cardinality grew with historical rejected entries")
		}
	}
}

func BenchmarkStorageCollectionAdmissionScale(b *testing.B) {
	for _, count := range []int{20, 21, 1000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			raw := []byte(storageEmptyPresets(count))
			b.ReportAllocs()
			for b.Loop() {
				_, err := DecodeV6PresetCollection(raw)
				if count == 20 && err != nil || count > 20 && !errors.Is(err, ErrStorageAdmission) {
					b.Fatal(err)
				}
			}
		})
	}
}
