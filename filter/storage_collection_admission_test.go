package filter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func storageEmptyPresets(count int) string {
	var raw strings.Builder
	raw.WriteString("presets:\n")
	for i := range count {
		fmt.Fprintf(&raw, "  P%06d: {}\n", i)
	}
	return raw.String()
}

// Each distinct alias key resolves to FN31. Losing values remain traversed;
// their default allocations are intentionally not eliminated by cardinality.
func storageRepeatedPresets(count, scalarBytes int) string {
	var raw strings.Builder
	raw.WriteString("presets:\n  FN31: {grid: &a0 FN31")
	if scalarBytes > 0 {
		fmt.Fprintf(&raw, ", callsigns: [&text %s]", strings.Repeat("A", scalarBytes))
	}
	raw.WriteString("}\n")
	for i := 1; i <= count; i++ {
		fmt.Fprintf(&raw, "  ? *a%d\n  : {grid: &a%d FN31", i-1, i)
		if scalarBytes > 0 {
			raw.WriteString(", callsigns: [*text]")
		}
		raw.WriteString("}\n")
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
	merge := "presets: {P0: {}, <<: [{"
	for i := 1; i < 20; i++ {
		if i > 1 {
			merge += ", "
		}
		merge += fmt.Sprintf("P%d: {}", i)
	}
	merge += "}, {P0: {configuration_version: 99}, P1: {configuration_version: 99}}]}\n"
	fixtures := []struct {
		name, raw, want string
		count           int
	}{
		{"twenty", storageEmptyPresets(20), "", 20},
		{"twenty one", storageEmptyPresets(21), "invalid preset collection count", 0},
		{"many interpreted aliases", storageRepeatedPresets(30, 0), "", 1},
		{"skipped null key", storageEmptyPresets(20) + "  ? null\n  : {configuration_version: 99}\n", "", 20},
		{"inserted null over count", storageEmptyPresets(20) + "  NULL-ENTRY: null\n", "invalid preset collection count", 0},
		{"inserted null within count", storageEmptyPresets(19) + "  NULL-ENTRY: null\n", "nil saved preset", 0},
		{"merged visible keys", merge, "", 20},
		{"root merge", "<<: {presets: {TEST: {}}}\n", "", 1},
		{"overflow then type failure", storageEmptyPresets(21) + "  BAD: {bands: 42}\n", "cannot unmarshal", 0},
		{"overflow then future marker", storageEmptyPresets(21) + "  BAD: {configuration_version: 99}\n", "unsupported configuration version", 0},
		{"overflow then unknown root", storageEmptyPresets(21) + "legacy: true\n", "field legacy not found", 0},
		{"overflow then raw duplicate", storageEmptyPresets(21) + "  P000000: {}\n", "mapping key", 0},
		{"overflow then trailing document", storageEmptyPresets(21) + "---\n{}\n", "preset collection must contain one YAML document", 0},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			// In particular, an unquoted key NULL itself resolves to !!null
			// and is skipped. NULL-ENTRY is a valid name for null-value insertion.
			_, frozenErr := frozenDecodePresetCollection([]byte(fixture.raw))
			if fixture.want == "" && frozenErr != nil || fixture.want != "" && (frozenErr == nil || !strings.Contains(frozenErr.Error(), fixture.want)) {
				t.Fatalf("fixture disagrees with frozen insertion/error oracle: %v", frozenErr)
			}
			path := writeStorageCollection(t, fixture.raw)
			collection, err := readPresetCollection(path)
			if fixture.want == "" {
				if err != nil || len(collection.Presets) != fixture.count {
					t.Fatalf("visible count: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), fixture.want) {
				t.Fatalf("refusal precedence: %v", err)
			}
			if fixture.want == "invalid preset collection count" && (err.Error() != fixture.want || !errors.Is(err, ErrStorageAdmission)) {
				t.Fatalf("count error prefix/class changed: %v", err)
			}
			if fixture.name == "overflow then type failure" {
				var typed *yaml.TypeError
				if !errors.As(err, &typed) {
					t.Fatalf("type error class changed: %v", err)
				}
			}
			after, readErr := os.ReadFile(path)
			if readErr != nil || !bytes.Equal(after, []byte(fixture.raw)) {
				t.Fatal("cardinality read changed bytes")
			}
		})
	}
}

func TestStorageCollectionWorkActiveAcrossAllPresetAPIs(t *testing.T) {
	usePresetTestDir(t)
	raw := storageRepeatedPresets(600, 32<<10)
	if len(raw) >= maxPresetCollectionBytes {
		t.Fatal("work fixture also exceeds raw bound")
	}
	path := writeStorageCollection(t, raw)
	// Frozen stock decoding accepts one bounded final preset; the new shared
	// collection work limit must refuse despite fresh guards for each owner.
	if sets, err := frozenDecodePresetCollection([]byte(raw)); err != nil || len(sets) != 1 {
		t.Fatalf("old guard control: %v", err)
	}
	operations := []func() error{
		func() error { _, err := ListPresets("N2WQ-1"); return err },
		func() error { _, err := LoadPreset("N2WQ-1", "FN31"); return err },
		func() error { return DeletePreset("N2WQ-1", "FN31") },
		func() error { return SavePreset("N2WQ-1", "OTHER", &SavedPreset{}) },
	}
	for _, operation := range operations {
		if err := operation(); !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "decoding work") {
			t.Fatalf("production collection work bypass: %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, []byte(raw)) {
			t.Fatal("preset refusal changed bytes")
		}
	}
}

func TestStorageCollectionCardinalityAccountingAndSaturation(t *testing.T) {
	for _, count := range []int{21, 1000} {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(storageEmptyPresets(count)), &document); err != nil {
			t.Fatal(err)
		}
		state, err := newStorageDecodeStateWithLimits(&document, storageDecodeLimits{work: maxPresetCollectionWork, presets: MaxPresets})
		if err != nil {
			t.Fatal(err)
		}
		guard := newStorageDecodeGuard(true)
		if _, _, err := state.decode(&document, reflect.TypeFor[presetCollection](), reflect.Value{}, guard); err != nil {
			t.Fatal(err)
		}
		if !state.presetCountExceeded || state.materialize || state.work.P != uint64(11+9*count) || state.work.E != uint64(11+10*count) {
			t.Fatalf("observer changed hand-counted work: %#v", state.work)
		}
		entries := &storagePresetEntries{keys: make(map[string]bool)}
		for i := range count {
			state.observePresetEntry(entries, fmt.Sprint(i))
		}
		if len(entries.keys) != 21 || !entries.overflow {
			t.Fatalf("count observer did not saturate: %d", len(entries.keys))
		}
	}
}

// Parsing is outside the timed section to expose decoder/default allocation
// costs separately. Production file tests above retain the physical read/AST
// boundary. Neither work units nor these measurements are heap-byte guarantees.
func BenchmarkStorageCollectionAdmissionScale(b *testing.B) {
	for _, shape := range []string{"distinct refused", "repeated admitted"} {
		for _, count := range []int{21, 1000, 100_000} {
			b.Run(fmt.Sprintf("%s/%d", shape, count), func(b *testing.B) {
				raw := storageEmptyPresets(count)
				if shape == "repeated admitted" {
					raw = storageRepeatedPresets(count, 0)
				}
				if len(raw) > maxPresetCollectionBytes {
					b.Fatal("fixture exceeds raw bound")
				}
				var document yaml.Node
				if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
					b.Fatal(err)
				}
				check := func() {
					collection, work, err := decodeStoredPresetCollectionDocument(&document)
					if shape == "distinct refused" {
						if collection != nil || !errors.Is(err, errPresetCollectionCount) {
							b.Fatalf("wrong refusal: %v", err)
						}
					} else if err != nil || len(collection.Presets) != 1 || collection.Presets["FN31"].Grid != "FN31" {
						b.Fatalf("wrong admitted result: %v", err)
					}
					if work.P+work.E > maxPresetCollectionWork {
						b.Fatal("fixture exceeded work bound")
					}
				}
				check()
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for range b.N {
					check()
				}
				b.StopTimer()
				check()
			})
		}
	}
}
