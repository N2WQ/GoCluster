// File role: Owns bounded named preset/settings snapshots shared by numeric SSIDs.
// Collections are read per operation; only a fixed lock array lives for the process.
// Related docs: telnet/README.md, docs/decisions/ADR-0238-named-presets.md.
package filter

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"

	"dxcluster/spot"
	"dxcluster/strutil"
	"gopkg.in/yaml.v3"
)

const (
	// MaxPresets bounds the number of presets shared by a callsign's SSIDs.
	MaxPresets = 20
	// MaxPresetBytes bounds the standalone YAML encoding of one preset.
	MaxPresetBytes           = 256 << 10
	maxPresetCollectionBytes = 8 << 20
)

// ErrPresetNotFound identifies a missing name, distinct from filesystem failures.
// It also matches os.ErrNotExist for callers using the usual persistence contract.
var ErrPresetNotFound = fmt.Errorf("saved preset not found: %w", os.ErrNotExist)

// SavedPreset contains preferences only. Login history and runtime caches
// must never move between SSIDs when a preset is loaded.
type SavedPreset struct {
	ConfigurationVersion    int `yaml:"configuration_version,omitempty"`
	Filter                  `yaml:",inline"`
	Dialect                 string `yaml:"dialect,omitempty"`
	DedupePolicy            string `yaml:"dedupe_policy,omitempty"`
	Grid                    string `yaml:"grid,omitempty"`
	NoiseClass              string `yaml:"noise_class,omitempty"`
	PathMinObservationCount int    `yaml:"path_min_observation_count,omitempty"`
	SolarSummaryMinutes     int    `yaml:"solar_summary_minutes,omitempty"`
}

// Clone returns a detached, exact preference snapshot. Callers must guard
// the source maps while cloning; runtime-only Filter fields are rebuilt on LOAD.
func (set *SavedPreset) Clone() (*SavedPreset, error) {
	if set == nil {
		return nil, errors.New("nil saved preset")
	}
	if set.ConfigurationVersion != 0 && set.ConfigurationVersion != CurrentConfigurationVersion {
		return nil, ErrUnsupportedConfigurationVersion
	}
	return ConfigurationFromPreset(set).Preset()
}

// UnmarshalYAML migrates legacy snapshots and adds unrestricted states to v1.
// Version two preserves every configured state rule; version three adds MINSNR.
func (set *SavedPreset) UnmarshalYAML(node *yaml.Node) error {
	version, err := storedConfigurationVersion(node)
	if err != nil {
		return err
	}
	if err := validateStoredStateFields(node, version); err != nil {
		return err
	}
	if err := validateStoredMinSNRFields(node, version); err != nil {
		return err
	}
	if err := validateStoredMapping(node, reflect.TypeFor[SavedPreset]()); err != nil {
		return err
	}
	if version != 0 {
		if err := validateCurrentStoredValues(node, reflect.TypeFor[SavedPreset]()); err != nil {
			return err
		}
	}
	type plain SavedPreset
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	if decoded.ConfigurationVersion != version {
		return fmt.Errorf("%w: marker must be an explicit field", ErrUnsupportedConfigurationVersion)
	}
	*set = SavedPreset(decoded)
	if version == 0 {
		set.normalize()
	}
	if version < stateConfigurationVersion {
		set.ResetDXStates()
		set.ResetDEStates()
	}
	if err := ConfigurationFromFilter(&set.Filter, SettingsConfiguration{}).ValidateStateRules(); err != nil {
		return err
	}
	if err := ConfigurationFromFilter(&set.Filter, SettingsConfiguration{}).ValidateMinSNRRules(); err != nil {
		return err
	}
	set.ConfigurationVersion = CurrentConfigurationVersion
	return nil
}

func (set *SavedPreset) normalize() {
	set.migrateLegacyConfidence()
	set.normalizeDefaults()
	if strings.TrimSpace(set.Dialect) == "" {
		set.Dialect = "go"
	}
	set.DedupePolicy = NormalizeDedupePolicy(set.DedupePolicy)
	set.Grid = strutil.NormalizeUpper(set.Grid)
	set.NoiseClass = strutil.NormalizeUpper(set.NoiseClass)
	set.PathMinObservationCount = normalizePathMinObservationCount(set.PathMinObservationCount)
	set.SolarSummaryMinutes = normalizeSolarSummaryMinutes(set.SolarSummaryMinutes)
}

// NormalizePresetName validates the ASCII command identifier and returns its
// uppercase key. Whitespace is rejected rather than silently changing the name.
func NormalizePresetName(name string) (string, error) {
	if len(name) < 1 || len(name) > 32 {
		return "", errors.New("preset name must contain 1-32 letters, digits or hyphens")
	}
	for i := range len(name) {
		ch := name[i]
		alnum := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !alnum && (i == 0 || ch != '-') {
			return "", errors.New("preset name must start with a letter or digit and contain only letters, digits or hyphens")
		}
	}
	return strings.ToUpper(name), nil
}

type presetCollection struct {
	Presets map[string]*SavedPreset `yaml:"presets"`
}

// Stripes serialize collection read/modify/replace across SSIDs without retaining
// a lock or decoded collection per historical callsign. Collisions only delay commands.
var presetLocks [64]sync.Mutex

type presetStore struct{ write func(string, []byte) error }

func presetCollectionPath(callsign string) (string, error) {
	owner := spot.NormalizeOwnCallsign(callsign)
	if !spot.IsValidNormalizedCallsign(owner) {
		return "", errors.New("invalid preset owner callsign")
	}
	return filepath.Join(UserDataDir, "presets", hex.EncodeToString([]byte(owner))+".yaml"), nil
}

func presetCollectionLock(path string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return &presetLocks[h.Sum32()%uint32(len(presetLocks))]
}

// SavePreset saves or replaces a detached snapshot for the baseline callsign.
func SavePreset(callsign, name string, set *SavedPreset) error {
	return (presetStore{}).save(callsign, name, set)
}

func (store presetStore) save(callsign, name string, set *SavedPreset) error {
	name, err := NormalizePresetName(name)
	if err != nil {
		return err
	}
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return err
	}
	snapshot, err := set.Clone()
	if err != nil {
		return err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, err := readPresetCollection(path)
	if err != nil {
		return err
	}
	if _, exists := collection.Presets[name]; !exists && len(collection.Presets) >= MaxPresets {
		return fmt.Errorf("preset limit reached (%d); delete a preset first", MaxPresets)
	}
	collection.Presets[name] = snapshot
	if err := store.persist(path, collection); err != nil {
		return err
	}
	log.Printf("Saved named preset %s for %s (presets=%d/%d)", name, spot.NormalizeOwnCallsign(callsign), len(collection.Presets), MaxPresets)
	return nil
}

// ListPresets returns sorted canonical names; a missing collection is empty.
func ListPresets(callsign string) ([]string, error) {
	path, err := presetCollectionPath(callsign)
	if err != nil {
		return nil, err
	}
	mu := presetCollectionLock(path)
	mu.Lock()
	defer mu.Unlock()
	collection, err := readPresetCollection(path)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(collection.Presets))
	for name := range collection.Presets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// LoadPreset returns detached preferences, or ErrPresetNotFound. Legacy records
// migrate at the read boundary; marked records preserve exact selections.
func LoadPreset(callsign, name string) (*SavedPreset, error) {
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
	collection, err := readPresetCollection(path)
	if err != nil {
		return nil, err
	}
	set, exists := collection.Presets[name]
	if !exists {
		return nil, fmt.Errorf("preset %s: %w", name, ErrPresetNotFound)
	}
	// Legacy migration can grow preferences. Clone rechecks the independent
	// preset bound before LOAD can publish any state.
	return set.Clone()
}

// DeletePreset removes only the named snapshot, without modifying live sessions.
func DeletePreset(callsign, name string) error {
	return (presetStore{}).delete(callsign, name)
}

func (store presetStore) delete(callsign, name string) error {
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
	collection, err := readPresetCollection(path)
	if err != nil {
		return err
	}
	if _, exists := collection.Presets[name]; !exists {
		return fmt.Errorf("preset %s: %w", name, ErrPresetNotFound)
	}
	delete(collection.Presets, name)
	if err := store.persist(path, collection); err != nil {
		return err
	}
	log.Printf("Deleted named preset %s for %s (presets=%d/%d)", name, spot.NormalizeOwnCallsign(callsign), len(collection.Presets), MaxPresets)
	return nil
}

func checkPresetSize(set *SavedPreset) error {
	if set == nil {
		return errors.New("nil saved preset")
	}
	// Existing live callsign/DXCC lists can grow across commands. Reject an
	// impossible snapshot before yaml.Marshal allocates a node tree for them.
	if !presetFitsMinimumSize(set) {
		return fmt.Errorf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	bs, err := yaml.Marshal(set)
	if err != nil {
		return err
	}
	if len(bs) > MaxPresetBytes {
		return fmt.Errorf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	return nil
}

// This is a lower bound, never a replacement for the exact encoded-byte check.
// Maps need at least "key: true\n" and list entries at least "- value\n";
// quoting/indentation only increases the eventual size. Runtime caches are omitted.
func presetFitsMinimumSize(set *SavedPreset) bool {
	return set != nil && ConfigurationFromPreset(set).MinimumSizeFits(MaxPresetBytes)
}

func readPresetCollection(path string) (*presetCollection, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return &presetCollection{Presets: make(map[string]*SavedPreset)}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	bs, err := io.ReadAll(io.LimitReader(file, maxPresetCollectionBytes+1))
	if err != nil {
		return nil, err
	}
	if len(bs) > maxPresetCollectionBytes {
		return nil, errors.New("preset collection exceeds 8 MiB")
	}
	var collection presetCollection
	decoder := yaml.NewDecoder(bytes.NewReader(bs))
	decoder.KnownFields(true)
	if err := decoder.Decode(&collection); err != nil {
		return nil, fmt.Errorf("invalid preset collection: %w", err)
	}
	var extra any
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
	return &collection, nil
}

func (store presetStore) persist(path string, collection *presetCollection) error {
	bs, err := yaml.Marshal(collection)
	if err != nil {
		return err
	}
	if len(bs) > maxPresetCollectionBytes {
		return errors.New("preset collection exceeds 8 MiB")
	}
	write := store.write
	if write == nil {
		write = writeAtomicUserFile
	}
	return write(path, bs)
}
