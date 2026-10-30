// File role: Explicit login/preset-access reset, with independent file outcomes.
// Caller owns its full-SSID lease before this code takes the base-call stripe.
package filter

import (
	"errors"
	"fmt"
	"os"
)

const v7BackupSuffix = ".pre-v7.bak"

type V7UpgradeOutcome struct {
	ProfileReset           bool
	PresetLibraryReset     bool
	LibraryResetGeneration int
	LibraryError           error
}

type V7LibraryResetOutcome struct {
	Reset           bool
	ResetGeneration int
}

// Per-operation fault seams never outlive their operation or alter global IO.
type v7UpgradeStore struct {
	read         func(string, int) ([]byte, error)
	backup       func(string, []byte) error
	write        func(string, []byte) error
	admitProfile func(*V7UserRecord) error
}

func (store v7UpgradeStore) readFile(path string, limit int) ([]byte, error) {
	if store.read != nil {
		return store.read(path, limit)
	}
	return readUpgradeFile(path, limit)
}
func (store v7UpgradeStore) backUp(path string, data []byte) error {
	if store.backup != nil {
		return store.backup(path+v7BackupSuffix, data)
	}
	return writeLegacyOriginalBackup(path+v7BackupSuffix, data)
}
func (store v7UpgradeStore) replace(path string, data []byte) error {
	if store.write != nil {
		return store.write(path, data)
	}
	return writeAtomicUserFile(path, data)
}

// UpgradeV7UserRecordForLoginWithOutcome never grants creation authority except
// for the initial authoritative profile-missing sentinel. Library errors are
// reported separately so an independently admitted/backed-up profile can reset.
func UpgradeV7UserRecordForLoginWithOutcome(callsign string, commit func(*PreparedUserRecord) error) (*V7UserRecord, V7UpgradeOutcome, error) {
	return (v7UpgradeStore{}).upgrade(callsign, commit)
}

// UpgradeV7UserRecordForLoginWithAdmission lets the server admit its complete
// human/wrapped-machine views before any reset backup or replacement. The
// callback receives detached state and is never called for an existing v7 file.
func UpgradeV7UserRecordForLoginWithAdmission(callsign string, commit func(*PreparedUserRecord) error, admit func(*V7UserRecord) error) (*V7UserRecord, V7UpgradeOutcome, error) {
	return (v7UpgradeStore{admitProfile: admit}).upgrade(callsign, commit)
}

func (store v7UpgradeStore) upgrade(callsign string, commit func(*PreparedUserRecord) error) (*V7UserRecord, V7UpgradeOutcome, error) {
	var outcome V7UpgradeOutcome
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, outcome, err
	}
	original, err := store.readFile(path, MaxUserRecordBytes)
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
	if err := admitV7UpgradeNodes(node, v7ProfileNodes); err != nil {
		return nil, outcome, err
	}
	version, err := v7UpgradeVersion(node)
	if err != nil {
		return nil, outcome, err
	}
	var record *V7UserRecord
	var prepared *PreparedUserRecord
	if version == V7ConfigurationVersion {
		record, err = DecodeV7UserRecord(original)
	} else {
		settings, ips, lastLogin, decodeErr := decodeV7DiscardProfile(node, version, state)
		if decodeErr != nil {
			return nil, outcome, decodeErr
		}
		configuration, prepareErr := DefaultV7Configuration(settings)
		if prepareErr != nil {
			return nil, outcome, prepareErr
		}
		record = &V7UserRecord{ConfigurationVersion: V7ConfigurationVersion, Configuration: configuration, RecentIPs: ips, LastLoginUTC: lastLogin, Notice: V7NoticeState{ProfileResetGeneration: V7ConfigurationVersion}}
		prepared, err = PrepareV7UserRecord(callsign, record)
	}
	if err != nil {
		return nil, outcome, err
	}
	if prepared != nil && store.admitProfile != nil {
		candidate, decodeErr := DecodeV7UserRecord(prepared.data)
		if decodeErr != nil {
			return nil, outcome, decodeErr
		}
		if admissionErr := store.admitProfile(candidate); admissionErr != nil {
			return nil, outcome, fmt.Errorf("admit reset profile views: %w", admissionErr)
		}
	}
	libraryPath, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, outcome, err
	}
	mu := presetCollectionLock(libraryPath)
	mu.Lock()
	defer mu.Unlock()
	collection, libraryOriginal, libraryData, removed, libraryErr := store.prepareLibrary(libraryPath)
	if libraryErr == nil && collection != nil && libraryData == nil {
		outcome.LibraryResetGeneration = collection.ResetGeneration
	}
	// Each file needs its own admitted payload and verified original backup.
	// When both are eligible, both backups precede the library-first writes.
	if libraryErr == nil && libraryData != nil {
		libraryErr = store.backUp(libraryPath, libraryOriginal)
	}
	if libraryErr != nil {
		outcome.LibraryError = fmt.Errorf("preset reset pending: %w", libraryErr)
	}
	if prepared != nil {
		if err := store.backUp(path, original); err != nil {
			return nil, outcome, fmt.Errorf("back up old profile: %w", err)
		}
	}
	if libraryErr == nil && libraryData != nil {
		libraryErr = store.replace(libraryPath, libraryData)
		if libraryErr == nil {
			outcome.PresetLibraryReset = removed
			outcome.LibraryResetGeneration = collection.ResetGeneration
		}
	}
	if libraryErr != nil {
		outcome.LibraryError = fmt.Errorf("preset reset pending: %w", libraryErr)
		outcome.LibraryResetGeneration = 0
	}
	if prepared != nil {
		if commit != nil {
			err = commit(prepared)
		} else {
			err = prepared.commitWith(store.replace)
		}
		if err != nil {
			return nil, outcome, fmt.Errorf("commit reset profile: %w", err)
		}
		outcome.ProfileReset = true
	}
	return record, outcome, nil
}

func (store v7UpgradeStore) prepareLibrary(path string) (*V7PresetCollection, []byte, []byte, bool, error) {
	original, err := store.readFile(path, maxPresetCollectionBytes)
	if errors.Is(err, os.ErrNotExist) {
		return &V7PresetCollection{ConfigurationVersion: V7ConfigurationVersion, Presets: map[string]*V7SavedPreset{}}, nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, nil, false, err
	}
	collection, data, removed, err := prepareV7LibraryReset(original)
	return collection, original, data, removed, err
}

// resetLibraryOwned requires the existing base-call stripe. Preset callers use
// it before access; no old preset can fall through into a LOAD adapter.
func (store v7UpgradeStore) resetLibraryOwned(path string) (*V7PresetCollection, V7LibraryResetOutcome, error) {
	collection, original, data, removed, err := store.prepareLibrary(path)
	if err != nil {
		return nil, V7LibraryResetOutcome{}, err
	}
	if data != nil {
		if err := store.backUp(path, original); err != nil {
			return nil, V7LibraryResetOutcome{}, err
		}
		if err := store.replace(path, data); err != nil {
			return nil, V7LibraryResetOutcome{}, err
		}
	}
	return collection, V7LibraryResetOutcome{Reset: removed, ResetGeneration: collection.ResetGeneration}, nil
}

func ResetV7PresetLibraryForLogin(callsign string) (V7LibraryResetOutcome, error) {
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return V7LibraryResetOutcome{}, err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	_, outcome, err := (v7UpgradeStore{}).resetLibraryOwned(path)
	return outcome, err
}
