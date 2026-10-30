// File role: Base-call-striped v7 preset operations. Reset and mutation reread
// authoritative bytes under the same existing stripe; no library cache is kept.
package filter

import (
	"errors"
	"sort"
)

type V7PresetSummary struct {
	Name    string
	Dialect string
}

// LoadV7PresetCollection is a strict read, suitable for acknowledgement checks.
// It never resets or creates a library; callers distinguish absence themselves.
func LoadV7PresetCollection(callsign string) (*V7PresetCollection, error) {
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, err
	}
	data, err := readUpgradeFile(path, maxPresetCollectionBytes)
	if err != nil {
		return nil, err
	}
	return DecodeV7PresetCollection(data)
}

func LoadV7Preset(callsign, name string) (*V7SavedPreset, error) {
	name, err := NormalizePresetName(name)
	if err != nil {
		return nil, err
	}
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, _, err := (v7UpgradeStore{}).resetLibraryOwned(path)
	if err != nil {
		return nil, err
	}
	set := collection.Presets[name]
	if set == nil {
		return nil, ErrPresetNotFound
	}
	return set.Clone()
}

func ListV7PresetSummaries(callsign string) ([]V7PresetSummary, error) {
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, _, err := (v7UpgradeStore{}).resetLibraryOwned(path)
	if err != nil {
		return nil, err
	}
	result := make([]V7PresetSummary, 0, len(collection.Presets))
	for name, set := range collection.Presets {
		result = append(result, V7PresetSummary{Name: name, Dialect: set.Dialect})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func ListV7Presets(callsign string) ([]string, error) {
	summaries, err := ListV7PresetSummaries(callsign)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(summaries))
	for i, summary := range summaries {
		names[i] = summary.Name
	}
	return names, nil
}

func SaveV7Preset(callsign, name string, set *V7SavedPreset) error {
	_, err := (v7PresetStore{}).save(callsign, name, set, nil, nil, false)
	return err
}

// SaveV7PresetAndRecord preserves the established library-first partial-success
// result. Callers retain both old associations unless the profile commit succeeds.
func SaveV7PresetAndRecord(callsign, name string, set *V7SavedPreset, prepare func() (*PreparedUserRecord, error), commit func(*PreparedUserRecord) error) (bool, error) {
	return (v7PresetStore{}).save(callsign, name, set, prepare, commit, true)
}

type v7PresetStore struct{ upgrade v7UpgradeStore }

func (store v7PresetStore) save(callsign, name string, set *V7SavedPreset, prepare func() (*PreparedUserRecord, error), commit func(*PreparedUserRecord) error, withRecord bool) (bool, error) {
	if withRecord && prepare == nil {
		return false, errors.New("nil v7 user record preparation")
	}
	name, err := NormalizePresetName(name)
	if err != nil {
		return false, err
	}
	snapshot, err := set.Clone()
	if err != nil {
		return false, err
	}
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return false, err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, _, err := store.upgrade.resetLibraryOwned(path)
	if err != nil {
		return false, err
	}
	if _, exists := collection.Presets[name]; !exists && len(collection.Presets) >= MaxPresets {
		return false, storageAdmissionf("preset count exceeds %d", MaxPresets)
	}
	collection.Presets[name] = snapshot
	data, err := EncodeV7PresetCollection(collection)
	if err != nil {
		return false, err
	}
	var record *PreparedUserRecord
	if withRecord {
		record, err = prepare()
		if err != nil {
			return false, err
		}
		if err := record.validate(); err != nil {
			return false, err
		}
		if _, err := DecodeV7UserRecord(record.data); err != nil {
			return false, err
		}
	}
	if err := store.upgrade.replace(path, data); err != nil {
		return false, err
	}
	if withRecord {
		if commit != nil {
			return true, commit(record)
		}
		return true, record.commitWith(store.upgrade.replace)
	}
	return true, nil
}

func DeleteV7Preset(callsign, name string) error {
	name, err := NormalizePresetName(name)
	if err != nil {
		return err
	}
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, _, err := (v7UpgradeStore{}).resetLibraryOwned(path)
	if err != nil {
		return err
	}
	if _, exists := collection.Presets[name]; !exists {
		return ErrPresetNotFound
	}
	delete(collection.Presets, name)
	data, err := EncodeV7PresetCollection(collection)
	if err != nil {
		return err
	}
	return writeAtomicUserFile(path, data)
}
