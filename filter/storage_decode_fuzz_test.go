package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// Read-only in-memory equivalents of the file loaders keep fuzzing independent
// of mutable UserDataDir. Trailing-document and collection checks are retained.
func candidateStorageRecord(raw []byte) (*UserRecord, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	record, _, err := decodeStoredUserRecordDocument(&document)
	if err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("user record must contain one YAML document")
	}
	record.RecentIPs = trimRecentIPs(record.RecentIPs)
	return record, nil
}

func candidateStorageCollection(raw []byte) (map[string]*SavedPreset, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	collection, _, err := decodeStoredPresetCollectionDocument(&document)
	if err != nil {
		return nil, fmt.Errorf("invalid preset collection: %w", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("preset collection must contain one YAML document")
	}
	if collection.Presets == nil || len(collection.Presets) > MaxPresets {
		return nil, errors.New("invalid preset collection count")
	}
	for name, set := range collection.Presets {
		canonical, err := NormalizePresetName(name)
		if err != nil || name != canonical {
			return nil, fmt.Errorf("invalid stored preset name %q", name)
		}
		if err := checkPresetSize(set); err != nil {
			return nil, fmt.Errorf("invalid stored preset %s: %w", name, err)
		}
	}
	return collection.Presets, nil
}

// yaml.v3 may panic on invalid composite generic merge keys. A frozen panic
// never licenses a candidate panic: the candidate must refuse that input.
func frozenStorageAttempt[T any](fn func() (T, error)) (result T, panicked any, err error) {
	defer func() { panicked = recover() }()
	result, err = fn()
	return result, nil, err
}

func checkStorageDifferential[T any](t *testing.T, want, got T, frozenErr, candidateErr error, frozenPanic any) {
	t.Helper()
	if frozenPanic != nil {
		if candidateErr == nil {
			t.Fatalf("candidate accepted input that panicked in frozen decoding: %v", frozenPanic)
		}
		return
	}
	if (frozenErr == nil) != (candidateErr == nil) {
		t.Fatalf("acceptance changed: frozen=%v candidate=%v", frozenErr, candidateErr)
	}
	if frozenErr == nil {
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("complete value changed: frozen=%#v candidate=%#v", want, got)
		}
		return
	}
	var frozenType, candidateType *yaml.TypeError
	if errors.As(frozenErr, &frozenType) != errors.As(candidateErr, &candidateType) ||
		errors.Is(frozenErr, ErrUnsupportedConfigurationVersion) != errors.Is(candidateErr, ErrUnsupportedConfigurationVersion) {
		t.Fatalf("refusal class changed: frozen=%v candidate=%v", frozenErr, candidateErr)
	}
	if frozenErr.Error() == "yaml: document contains excessive aliasing" && candidateErr.Error() != frozenErr.Error() {
		t.Fatalf("old guard refusal changed: frozen=%v candidate=%v", frozenErr, candidateErr)
	}
}

func TestStorageDecodeInvalidCompositeMergeKeys(t *testing.T) {
	for _, raw := range []string{
		"? {bad: key}\n: false\n<<: {bands: {20m: true}}\n",
		"? [bad, key]\n: false\n<<: {bands: {20m: true}}\n",
	} {
		_, frozenPanic, _ := frozenStorageAttempt(func() (*UserRecord, error) {
			return frozenDecodeUserRecord([]byte(raw))
		})
		if frozenPanic == nil {
			t.Fatal("pinned generic merge-key panic control no longer reproduces")
		}
		if _, err := candidateStorageRecord([]byte(raw)); err == nil {
			t.Fatal("candidate accepted an invalid composite merge key")
		}
	}
}

func FuzzStorageDecodeCompatibility(f *testing.F) {
	for _, fixture := range frozenRecordCases() {
		if len(fixture.raw) <= 4096 {
			f.Add([]byte(fixture.raw), false)
		}
	}
	for _, raw := range nullTagCompatibilityCases() {
		f.Add([]byte(raw), false)
	}
	for version := 0; version <= 4; version++ {
		body := frozenVersionBody(version)
		f.Add([]byte(body), false)
		f.Add([]byte(body+"preset:\n  name: TEST\n  baseline:\n"+frozenIndent(body)), false)
		f.Add([]byte("presets:\n  TEST:\n"+frozenIndent(body)), true)
	}
	for _, raw := range []string{
		"presets: {}\n", "presets: {TEST: null}\n", "{}\n", "null\n",
		"<<: {presets: {TEST: {bands: {20m: true}}}}\n",
		"presets: {TEST: {bands: {<<: {20m: true}, 40m: false}}}\n",
		"presets: {TEST: {}}\nlegacy: {x: 1, x: 2}\n",
		"presets: !!null {TEST: {}}\n", "presets: {TEST: !!null {bands: {20m: true}}}\n",
	} {
		f.Add([]byte(raw), true)
	}
	f.Fuzz(func(t *testing.T, raw []byte, collection bool) {
		if len(raw) > 4096 {
			t.Skip()
		}
		if collection {
			want, frozenPanic, frozenErr := frozenStorageAttempt(func() (map[string]*SavedPreset, error) {
				return frozenDecodePresetCollection(raw)
			})
			got, candidateErr := candidateStorageCollection(raw)
			checkStorageDifferential(t, want, got, frozenErr, candidateErr, frozenPanic)
			return
		}
		want, frozenPanic, frozenErr := frozenStorageAttempt(func() (*UserRecord, error) {
			return frozenDecodeUserRecord(raw)
		})
		got, candidateErr := candidateStorageRecord(raw)
		checkStorageDifferential(t, want, got, frozenErr, candidateErr, frozenPanic)
	})
}

// Keep well-formed YAML in play as bytes mutate. Raw-byte fuzzing complements
// this corpus with syntax damage; this target stresses typed interpretation.
func FuzzStorageDecodeCombinations(f *testing.F) {
	for _, seed := range [][]byte{{0, 0}, {4, 0, 0, 10, 8, 7}, {0, 1, 3, 8},
		{0, 0, 7, 7, 6, 2}, {2, 0, 4, 5, 10}, {0, 0, 9, 10, 9, 10}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, choices []byte) {
		if len(choices) < 2 || len(choices) > 50 {
			t.Skip()
		}
		fragments := []string{
			"bands: *a\n", "blockbands: *a\n", "bands: null\n",
			"modes: {CW: yes, FT8: off}\n", "dxzones: {1: true, 01: false}\n",
			"dxzones: {1.0: true, <<: {1: false}}\n", "allow_wwv: null\n",
			"legacy: {x: 1, x: 2}\n", "preset: {name: TEST, baseline: {bands: *a}}\n",
			"last_login_utc: {unknown: true}\n", "<<: {blockbands: {80m: true}}\n",
			"callsigns: [K1ABC, null, *a]\n", "bands: {20m: null, 40m: false}\n",
			"? [bad, key]\n: {x: true}\n", "? {bad: key}\n: false\n",
			"blockbands: !!null {80m: true}\n", "allow_wwv: !!null {x: true}\n",
			"recent_ips: !!null [192.0.2.1]\n", "dialect: !!null bogus\n",
		}
		body := ""
		if version := choices[0] % 6; version != 0 {
			body = fmt.Sprintf("configuration_version: %d\n", version)
		}
		body += "bands: &a {20m: true, 40m: false}\n"
		for _, choice := range choices[2:] {
			body += fragments[int(choice)%len(fragments)]
		}
		raw := []byte(body)
		if choices[1]%2 != 0 {
			raw = []byte("presets:\n  TEST:\n" + frozenIndent(body))
			want, frozenPanic, frozenErr := frozenStorageAttempt(func() (map[string]*SavedPreset, error) {
				return frozenDecodePresetCollection(raw)
			})
			got, candidateErr := candidateStorageCollection(raw)
			checkStorageDifferential(t, want, got, frozenErr, candidateErr, frozenPanic)
			return
		}
		want, frozenPanic, frozenErr := frozenStorageAttempt(func() (*UserRecord, error) {
			return frozenDecodeUserRecord(raw)
		})
		got, candidateErr := candidateStorageRecord(raw)
		checkStorageDifferential(t, want, got, frozenErr, candidateErr, frozenPanic)
	})
}

func nullTagCompatibilityCases() []string {
	return []string{
		"bands: !!null {20m: true}\n", "recent_ips: !!null [192.0.2.1]\n",
		"preset: !!null {name: TEST, baseline: {}}\n", "allow_wwv: !!null {x: true}\n",
		"bands: !!null bogus\n", "dialect: !!null bogus\n", "!!null {dialect: cc}\n",
		"!!null {bands: {20m: true}}\n",
	}
}

func TestStorageDecodeNullTagCompatibility(t *testing.T) {
	for _, raw := range nullTagCompatibilityCases() {
		t.Run(raw, func(t *testing.T) {
			want, frozenPanic, frozenErr := frozenStorageAttempt(func() (*UserRecord, error) {
				return frozenDecodeUserRecord([]byte(raw))
			})
			got, candidateErr := candidateStorageRecord([]byte(raw))
			checkStorageDifferential(t, want, got, frozenErr, candidateErr, frozenPanic)
		})
	}
}
