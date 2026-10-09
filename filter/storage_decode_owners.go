// File role: Preserve stored-type custom-unmarshal boundaries and finalization.
// Each owner keeps its original prechecks/migrations; nested callbacks borrow
// one operation state instead of opening another physical/logical work budget.
package filter

import (
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

func storageOwnerVersion(node *yaml.Node, typ reflect.Type) (int, error) {
	version, err := storedConfigurationVersion(node)
	if err != nil {
		return 0, err
	}
	if err := validateStoredStateFields(node, version); err != nil {
		return 0, err
	}
	if err := validateStoredMinSNRFields(node, version); err != nil {
		return 0, err
	}
	if err := validateStoredCommentFields(node, version); err != nil {
		return 0, err
	}
	if version != 0 || typ == reflect.TypeFor[SavedPreset]() {
		if err := validateStoredMapping(node, typ); err != nil {
			return 0, err
		}
	}
	if version != 0 {
		if err := validateCurrentStoredValues(node, typ); err != nil {
			return 0, err
		}
	}
	return version, nil
}

func storagePlainOwnerType(typ reflect.Type) reflect.Type {
	switch typ {
	case reflect.TypeFor[UserRecord]():
		return reflect.TypeFor[storagePlainUserRecord]()
	case reflect.TypeFor[SavedPreset]():
		return reflect.TypeFor[storagePlainSavedPreset]()
	default:
		return reflect.TypeFor[storagePlainPresetReference]()
	}
}

func (state *storageDecodeState) preflightOwner(node *yaml.Node, typ reflect.Type) error {
	version := 0
	if typ == reflect.TypeFor[PresetReference]() {
		if err := validateStoredMapping(node, typ); err != nil {
			return err
		}
		if err := validateCurrentStoredValues(node, typ); err != nil {
			return err
		}
	} else {
		var err error
		version, err = storageOwnerVersion(node, typ)
		if err != nil {
			return err
		}
	}
	guard := newStorageDecodeGuard(false)
	if _, _, err := state.decode(node, storagePlainOwnerType(typ), reflect.Value{}, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	if typ != reflect.TypeFor[PresetReference]() {
		if guard.marker != version {
			return fmt.Errorf("%w: marker must be an explicit field", ErrUnsupportedConfigurationVersion)
		}
		return nil
	}
	// Reference.Clone has an existing snapshot-size/name boundary before the
	// parent's remaining fields. Finalize this single nested reference after its
	// complete accounted walk, preserving that stop boundary. This temporary
	// materialization uses only this engine's alias-free scalar conversions; it
	// opens no guard/budget and performs no additional Node.Decode expansion.
	previous := state.materialize
	state.materialize = true
	defer func() { state.materialize = previous }()
	var ref PresetReference
	return ref.unmarshalStored(node, state)
}

func (state *storageDecodeState) decodeOwner(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) (bool, bool, error) {
	if typ != reflect.TypeFor[UserRecord]() && typ != reflect.TypeFor[SavedPreset]() && typ != reflect.TypeFor[PresetReference]() {
		return false, false, nil
	}
	var err error
	if !state.materialize {
		err = state.preflightOwner(node, typ)
	} else {
		switch owner := out.Addr().Interface().(type) {
		case *UserRecord:
			err = owner.unmarshalStored(node, state)
		case *SavedPreset:
			err = owner.unmarshalStored(node, state)
		case *PresetReference:
			err = owner.unmarshalStored(node, state)
		default:
			err = errors.New("invalid stored decoding owner")
		}
	}
	if err != nil {
		return true, false, storedDecodeError(guard, err)
	}
	return true, true, nil
}

func (state *storageDecodeState) decodePlain(node *yaml.Node, output any) error {
	out := reflect.ValueOf(output).Elem()
	guard := newStorageDecodeGuard(false)
	if _, _, err := state.decode(node, out.Type(), out, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return nil
}

func decodeStoredOwnerNode(node *yaml.Node, output any) error {
	state, err := newStorageDecodeState(node)
	if err != nil {
		return err
	}
	out := reflect.ValueOf(output).Elem()
	if err := state.preflightOwner(node, out.Type()); err != nil {
		return err
	}
	state.materialize = true
	guard := newStorageDecodeGuard(false)
	_, _, err = state.decodeOwner(node, out.Type(), out, guard)
	if err == nil && len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return err
}

func decodeStoredUserRecordDocument(document *yaml.Node) (*UserRecord, storageDecodeWork, error) {
	state, err := newStorageDecodeState(document)
	if err != nil {
		return nil, storageDecodeWork{}, err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return nil, state.work, errors.New("user record must contain one YAML mapping")
	}
	node := document.Content[0]
	if err := state.preflightOwner(node, reflect.TypeFor[UserRecord]()); err != nil {
		return nil, state.work, err
	}
	state.materialize = true
	var record UserRecord
	if err := record.unmarshalStored(node, state); err != nil {
		return nil, state.work, err
	}
	return &record, state.work, nil
}

func decodeStoredPresetCollectionDocument(document *yaml.Node) (*presetCollection, storageDecodeWork, error) {
	state, err := newStorageDecodeState(document)
	if err != nil {
		return nil, storageDecodeWork{}, err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, reflect.TypeFor[presetCollection](), reflect.Value{}, guard); err != nil {
		return nil, state.work, err
	}
	if len(guard.typeErrors) != 0 {
		return nil, state.work, &yaml.TypeError{Errors: guard.typeErrors}
	}
	state.materialize = true
	var collection presetCollection
	guard = newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, reflect.TypeFor[presetCollection](), reflect.ValueOf(&collection).Elem(), guard); err != nil {
		return nil, state.work, err
	}
	if len(guard.typeErrors) != 0 {
		return nil, state.work, &yaml.TypeError{Errors: guard.typeErrors}
	}
	return &collection, state.work, nil
}
