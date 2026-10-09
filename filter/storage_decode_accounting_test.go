package filter

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStorageDecodeLiteralWork(t *testing.T) {
	// Literal totals follow the frozen stock unmarshal trace, not this engine's
	// implementation passes. Custom dispatch plus each fresh plain-root visit
	// are distinct E visits; physical document nodes belong only to P.
	fixtures := []struct {
		name       string
		raw        string
		collection bool
		want       storageDecodeWork
	}{
		{"empty record", "{}\n", false, storageDecodeWork{P: 2, E: 1}},
		{"ordinary map", "bands: {20m: true}\n", false, storageDecodeWork{P: 18, E: 17}},
		{"ignored map", "legacy: {x: abc}\n", false, storageDecodeWork{P: 16, E: 8}},
		{"aliased map", "legacy: &a {20m: true}\nbands: *a\n", false, storageDecodeWork{P: 26, E: 25}},
		{"merged map", "<<: {bands: {20m: true}}\n", false, storageDecodeWork{P: 22, E: 21}},
		{"nested baseline", "preset: {name: TEST, baseline: {bands: {20m: true}}}\n", false, storageDecodeWork{P: 46, E: 47}},
		{"empty collection", "presets: {}\n", true, storageDecodeWork{P: 11, E: 11}},
		{"collection custom dispatch", "presets: {TEST: {bands: {20m: true}}}\n", true, storageDecodeWork{P: 33, E: 34}},
		{"decoded UTF8 bytes", "legacy: \"\\u00e9\"\n", false, storageDecodeWork{P: 12, E: 8}},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			var document yaml.Node
			if err := yaml.Unmarshal([]byte(fixture.raw), &document); err != nil {
				t.Fatal(err)
			}
			var work storageDecodeWork
			var err error
			if fixture.collection {
				_, work, err = decodeStoredPresetCollectionDocument(&document)
			} else {
				_, work, err = decodeStoredUserRecordDocument(&document)
			}
			if err != nil || work != fixture.want {
				t.Fatalf("work=%+v want=%+v err=%v", work, fixture.want, err)
			}
		})
	}
}

func TestStorageDecodeWorkOverflow(t *testing.T) {
	value := uint64(math.MaxUint64)
	if err := checkedStorageAdd(&value, 1); err == nil || value != math.MaxUint64 {
		t.Fatal("overflow was accepted or wrapped")
	}
	state := &storageDecodeState{work: storageDecodeWork{P: math.MaxUint64}}
	if err := state.visit(&yaml.Node{Kind: yaml.ScalarNode}, newStorageDecodeGuard(false)); err == nil {
		t.Fatal("combined physical/logical work overflow was accepted")
	}
}

func TestStorageDecodeCallbackErrorBoundary(t *testing.T) {
	typeError := &yaml.TypeError{Errors: []string{"invalid typed field"}}
	guard := newStorageDecodeGuard(false)
	if err := storedDecodeError(guard, typeError); err != nil || len(guard.typeErrors) != 1 {
		t.Fatalf("direct typed error was not accumulated: %v, %v", err, guard.typeErrors)
	}
	guard = newStorageDecodeGuard(false)
	wrapped := fmt.Errorf("stored owner: %w", typeError)
	if err := storedDecodeError(guard, wrapped); !errors.Is(err, wrapped) || len(guard.typeErrors) != 0 {
		t.Fatalf("wrapped owner failure lost its terminal boundary: %v, %v", err, guard.typeErrors)
	}
}

func TestStorageDecodeNestedFailureBoundaries(t *testing.T) {
	fixtures := []struct {
		name       string
		raw        string
		collection bool
		wantError  string
	}{
		{"comment before future marker", "presets: {FIRST: {configuration_version: 4, comments: ['é']}, LATER: {configuration_version: 5}}\n", true, "invalid phrase"},
		{"comment before typed failure", "presets: {FIRST: {configuration_version: 4, comments: ['é']}, LATER: {bands: 42}}\n", true, "invalid phrase"},
		{"reference name before typed failure", "preset: {name: lower, baseline: {}}\nbands: 42\n", false, "invalid applied preset name"},
		{"baseline size before typed failure", "preset: {name: TEST, baseline: {callsigns: [" + strings.Repeat("K", MaxPresetBytes+1) + "]}}\nbands: 42\n", false, "preset exceeds 256 KiB"},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			var document yaml.Node
			if err := yaml.Unmarshal([]byte(fixture.raw), &document); err != nil {
				t.Fatal(err)
			}
			var oldErr, newErr error
			if fixture.collection {
				_, oldErr = frozenDecodePresetCollection([]byte(fixture.raw))
				_, _, newErr = decodeStoredPresetCollectionDocument(&document)
			} else {
				_, oldErr = frozenDecodeUserRecord([]byte(fixture.raw))
				_, _, newErr = decodeStoredUserRecordDocument(&document)
			}
			var oldType, newType *yaml.TypeError
			if oldErr == nil || newErr == nil || !strings.Contains(oldErr.Error(), fixture.wantError) || !strings.Contains(newErr.Error(), fixture.wantError) ||
				errors.As(oldErr, &oldType) || errors.As(newErr, &newType) ||
				errors.Is(oldErr, ErrUnsupportedConfigurationVersion) || errors.Is(newErr, ErrUnsupportedConfigurationVersion) {
				t.Fatalf("early normal refusal changed: frozen=%v candidate=%v", oldErr, newErr)
			}
		})
	}
}

func TestStorageDecodeLegacyTimestampMapping(t *testing.T) {
	usePresetTestDir(t)
	assertUserRecordCompat(t, storageCompatCase{raw: "last_login_utc: {unknown: {x: 1, x: 2}}\n"})
	assertUserRecordCompat(t, storageCompatCase{raw: "last_login_utc: {unknown: true, unknown: false}\n", wantError: "mapping key"})
}
