package filter

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStorageDecodeLiteralWork(t *testing.T) {
	for _, fixture := range []struct {
		raw  string
		typ  reflect.Type
		want storageDecodeWork
	}{
		{"presets: {}\n", reflect.TypeFor[V5PresetCollection](), storageDecodeWork{P: 11, E: 11}},
		{v5StorageMinimalRecord, reflect.TypeFor[V5UserRecord](), storageDecodeWork{P: 67, E: 67}},
	} {
		document := storageMetadataDocument(t, fixture.raw)
		state, err := newStorageDecodeState(document)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := state.decode(document, fixture.typ, reflect.Value{}, newStorageDecodeGuard(true)); err != nil {
			t.Fatal(err)
		}
		if state.work != fixture.want {
			t.Fatalf("work=%+v want=%+v", state.work, fixture.want)
		}
		total := fixture.want.P + fixture.want.E
		for _, budget := range []uint64{total, total - 1} {
			out := reflect.New(fixture.typ).Interface()
			err := decodeV5StorageDocument([]byte(fixture.raw), MaxUserRecordBytes, storageDecodeLimits{work: budget}, out)
			if budget == total && err != nil || budget < total && !errors.Is(err, ErrStorageAdmission) {
				t.Fatalf("literal work boundary %d/%d: %v", budget, total, err)
			}
		}
	}
}

func TestStorageDecodeWorkOverflow(t *testing.T) {
	value := uint64(math.MaxUint64)
	if err := checkedStorageAdd(&value, 1); err == nil || value != math.MaxUint64 {
		t.Fatal("overflow accepted or wrapped")
	}
	state := &storageDecodeState{work: storageDecodeWork{P: math.MaxUint64}}
	if err := state.visit(&yaml.Node{Kind: yaml.ScalarNode}, newStorageDecodeGuard(false)); err == nil {
		t.Fatal("combined physical/logical work overflow accepted")
	}
}

func TestStorageDecodeCallbackErrorBoundary(t *testing.T) {
	typed := &yaml.TypeError{Errors: []string{"invalid typed field"}}
	guard := newStorageDecodeGuard(false)
	if err := storedDecodeError(guard, typed); err != nil || len(guard.typeErrors) != 1 {
		t.Fatal("typed failure accumulation changed")
	}
	guard = newStorageDecodeGuard(false)
	wrapped := fmt.Errorf("stored owner: %w", typed)
	if err := storedDecodeError(guard, wrapped); !errors.Is(err, wrapped) || len(guard.typeErrors) != 0 {
		t.Fatal("wrapped owner failure lost terminal boundary")
	}
}
