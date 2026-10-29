// File role: Explicit login-only reset of recognized retired profiles/libraries.
// The caller owns the full-SSID lease. This operation then owns the existing
// base-call preset stripe; callbacks must not reenter preset APIs.
package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"

	"gopkg.in/yaml.v3"
)

const legacyBackupSuffix = ".pre-v6.bak"

// ErrLoginUpgradeLibrary distinguishes shared-library cleanup failure from an
// invalid profile. A nonnil record with this error is an unchanged valid v6
// profile: login may retain its ordinary writable preferences and report the
// library warning separately. A nil record still denotes a protected failure.
var ErrLoginUpgradeLibrary = errors.New("old preset library cleanup failed")

// ErrLoginUpgradeProfileMissing grants the new-user path only when the initial
// authoritative profile read finds no file. Later backup/library/commit errors
// may also match os.ErrNotExist, but never grant this creation authority.
var ErrLoginUpgradeProfileMissing = errors.New("login upgrade profile missing")

// UpgradeUserRecordForLogin re-reads authoritative disk state before granting
// any replacement authority. Existing v6 records are never reset, but an old
// shared library is cleaned even for v6 SSIDs after a partial earlier upgrade.
// Missing records match ErrLoginUpgradeProfileMissing and os.ErrNotExist without
// inspecting the shared library. Callers must use the former for new-user flow.
// upgraded is true only after all required durable writes succeed. Each file is
// atomic, not the pair: the library commits first, and retry preserves new v6
// presets. A nil callback uses PreparedUserRecord.Commit.
func UpgradeUserRecordForLogin(callsign string, commit func(*PreparedUserRecord) error) (*UserRecord, bool, error) {
	return (loginUpgradeStore{}).upgrade(callsign, commit)
}

// LoginUpgradeOutcome reports successful durable changes from this operation.
// Library cleanup is distinct from a first Boolean profile reset. If a later
// profile commit fails, PresetLibraryCleaned can still be true; callers must
// handle the error before publishing a profile or announcing its reset.
type LoginUpgradeOutcome struct {
	ProfileReset         bool
	PresetLibraryCleaned bool
}

// UpgradeUserRecordForLoginWithOutcome has the same lease, persistence and error
// contract as UpgradeUserRecordForLogin, with precise results for login notices.
// A current v6 profile never reports ProfileReset, including library-only cleanup.
func UpgradeUserRecordForLoginWithOutcome(callsign string, commit func(*PreparedUserRecord) error) (*UserRecord, LoginUpgradeOutcome, error) {
	return (loginUpgradeStore{}).upgradeWithOutcome(callsign, commit)
}

// Per-operation seams share the existing atomic writer's failure boundaries.
// No mutable hooks, decoded cache, or per-historical-user registry is retained.
type loginUpgradeStore struct {
	write  func(string, []byte) error
	backup func(string, []byte) error
}

func (store loginUpgradeStore) upgrade(callsign string, commit func(*PreparedUserRecord) error) (*UserRecord, bool, error) {
	record, outcome, err := store.upgradeWithOutcome(callsign, commit)
	return record, err == nil && (outcome.ProfileReset || outcome.PresetLibraryCleaned), err
}

func (store loginUpgradeStore) upgradeWithOutcome(callsign string, commit func(*PreparedUserRecord) error) (*UserRecord, LoginUpgradeOutcome, error) {
	var outcome LoginUpgradeOutcome
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, outcome, err
	}
	original, err := readUpgradeFile(path, MaxUserRecordBytes)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, outcome, fmt.Errorf("%w: %w", ErrLoginUpgradeProfileMissing, err)
		}
		return nil, outcome, err
	}
	node, state, err := parseUpgradeDocument(original, MaxUserRecordBytes, maxUserRecordWork)
	if err != nil {
		return nil, outcome, err
	}
	version, err := upgradeStoredVersion(node)
	if err != nil {
		return nil, outcome, err
	}
	var record *UserRecord
	if version == CurrentConfigurationVersion {
		var stored *V6UserRecord
		stored, err = DecodeV6UserRecord(original)
		if err == nil {
			record, err = RuntimeUserRecordFromV6(stored)
		}
	} else {
		record, err = decodeRetiredLogin(node, version, state, newFilterDefaultRules())
	}
	if err != nil {
		return nil, outcome, err
	}
	var prepared *PreparedUserRecord
	if version != CurrentConfigurationVersion {
		prepared, err = PrepareUserRecord(callsign, record)
		if err != nil {
			return nil, outcome, fmt.Errorf("prepare upgraded profile: %w", err)
		}
	}
	libraryPath, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, outcome, err
	}
	mu := presetCollectionLock(libraryPath)
	mu.Lock()
	defer mu.Unlock()
	libraryOriginal, libraryData, err := prepareLegacyLibraryUpgrade(libraryPath)
	if err != nil {
		return upgradeLibraryFailure(record, version, "prepare old preset library reset", err)
	}
	// Both replacement payloads are fully admitted before any backup or write.
	backup := store.backup
	if backup == nil {
		backup = writeLegacyOriginalBackup
	}
	if libraryData != nil {
		if err := backup(libraryPath+legacyBackupSuffix, libraryOriginal); err != nil {
			return upgradeLibraryFailure(record, version, "back up old preset library", err)
		}
	}
	if prepared != nil {
		if err := backup(path+legacyBackupSuffix, original); err != nil {
			return nil, outcome, fmt.Errorf("back up old profile: %w", err)
		}
	}
	write := store.write
	if write == nil {
		write = writeAtomicUserFile
	}
	if libraryData != nil {
		if err := write(libraryPath, libraryData); err != nil {
			return upgradeLibraryFailure(record, version, "reset old preset library", err)
		}
		outcome.PresetLibraryCleaned = true
	}
	if prepared != nil {
		if commit == nil {
			commit = func(value *PreparedUserRecord) error { return value.commitWith(write) }
		}
		if err := commit(prepared); err != nil {
			return nil, outcome, fmt.Errorf("commit upgraded profile (preset library may already be reset): %w", err)
		}
		outcome.ProfileReset = true
	}
	return record, outcome, nil
}

func upgradeLibraryFailure(record *UserRecord, version int, stage string, cause error) (*UserRecord, LoginUpgradeOutcome, error) {
	if version != CurrentConfigurationVersion {
		record = nil
	}
	return record, LoginUpgradeOutcome{}, fmt.Errorf("%w: %s: %w", ErrLoginUpgradeLibrary, stage, cause)
}

func readUpgradeFile(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > limit {
		return nil, storageAdmissionf("upgrade file exceeds %d bytes", limit)
	}
	return data, nil
}

func prepareLegacyLibraryUpgrade(path string) ([]byte, []byte, error) {
	original, err := readUpgradeFile(path, maxPresetCollectionBytes)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	node, state, err := parseUpgradeDocument(original, maxPresetCollectionBytes, maxPresetCollectionWork)
	if err != nil {
		return nil, nil, err
	}
	if len(node.Content) != 2 || node.Content[0].Kind != yaml.ScalarNode ||
		node.Content[0].Tag != "!!str" || node.Content[0].Value != "presets" {
		return nil, nil, errors.New("invalid preset library mapping")
	}
	entries := node.Content[1]
	if entries.Kind != yaml.MappingNode || entries.Tag != "!!map" {
		return nil, nil, errors.New("presets must be a mapping")
	}
	if len(entries.Content)/2 > MaxPresets {
		return nil, nil, storageAdmissionf("preset count exceeds %d", MaxPresets)
	}
	if err := state.rawDuplicates(entries); err != nil {
		return nil, nil, err
	}
	kept := &V6PresetCollection{Presets: make(map[string]*V6SavedPreset)}
	removed := false
	for i := 0; i < len(entries.Content); i += 2 {
		key, value := v6StoredNode(entries.Content[i]), v6StoredNode(entries.Content[i+1])
		name, err := NormalizePresetName(key.Value)
		if err != nil || key.Kind != yaml.ScalarNode || key.Tag != "!!str" || name != key.Value {
			return nil, nil, errors.New("invalid stored preset name")
		}
		version, err := upgradeStoredVersion(value)
		if err != nil {
			return nil, nil, err
		}
		if version != CurrentConfigurationVersion {
			if err := validateRetiredPreset(value, version, state); err != nil {
				return nil, nil, fmt.Errorf("legacy preset %s: %w", name, err)
			}
			removed = true
			continue
		}
		set, err := decodeUpgradeV6Preset(value, state)
		if err != nil {
			return nil, nil, fmt.Errorf("v6 preset %s: %w", name, err)
		}
		kept.Presets[name] = set
	}
	if !removed {
		return nil, nil, nil
	}
	data, err := EncodeV6PresetCollection(kept)
	return original, data, err
}

func validateRetiredPreset(node *yaml.Node, version int, state *storageDecodeState) error {
	if version == retiredV5Version {
		return validateRetiredV5Preset(node, state)
	}
	if err := validateLegacyOwner(node, version, state, true); err != nil {
		return err
	}
	return admitLegacyPresetNode(node)
}

func decodeUpgradeV6Preset(node *yaml.Node, state *storageDecodeState) (*V6SavedPreset, error) {
	typ := reflect.TypeFor[V6SavedPreset]()
	if err := validateStoredValueWithFields(node, typ, state.validateV6StoredFields); err != nil {
		return nil, err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(node, typ, reflect.Value{}, guard); err != nil {
		return nil, err
	}
	if len(guard.typeErrors) != 0 {
		return nil, &yaml.TypeError{Errors: guard.typeErrors}
	}
	var set V6SavedPreset
	state.materialize = true
	_, _, err := state.decode(node, typ, reflect.ValueOf(&set).Elem(), newStorageDecodeGuard(true))
	state.materialize = false
	if err != nil {
		return nil, err
	}
	return set.Clone()
}

// An exclusive fixed-name backup never replaces a different original. Failed
// writes remove their own partial file. Retry verifies the complete bounded bytes
// and syncs the existing file, including a previous interrupted durability step.
func writeLegacyOriginalBackup(path string, original []byte) error {
	return writeLegacyOriginalBackupWithStages(path, original, atomicUserFileStages{
		write: writeUserFileBytes, sync: (*os.File).Sync, close: (*os.File).Close,
	})
}

func writeLegacyOriginalBackupWithStages(path string, original []byte, stages atomicUserFileStages) error {
	if len(original) > maxPresetCollectionBytes {
		return storageAdmissionf("original backup exceeds byte limit")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return statErr
		}
		if !info.Mode().IsRegular() {
			return errors.New("original backup is not a regular file")
		}
		saved, readErr := readUpgradeFile(path, maxPresetCollectionBytes)
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(saved, original) {
			return errors.New("existing original backup differs; refusing replacement")
		}
		file, err = os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		syncErr := stages.sync(file)
		closeErr := stages.close(file)
		_ = file.Close()
		return errors.Join(syncErr, closeErr)
	}
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf("Warning: failed to remove incomplete original backup: %v", err)
			}
		}
	}()
	if err := stages.write(file, original); err != nil {
		return err
	}
	if err := stages.sync(file); err != nil {
		return err
	}
	if err := stages.close(file); err != nil {
		return err
	}
	saved, err := readUpgradeFile(path, maxPresetCollectionBytes)
	if err != nil {
		return err
	}
	if !bytes.Equal(saved, original) {
		return errors.New("new original backup verification failed")
	}
	complete = true
	return nil
}
