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
	Filter                  `yaml:",inline"`
	Dialect                 string `yaml:"dialect,omitempty"`
	DedupePolicy            string `yaml:"dedupe_policy,omitempty"`
	Grid                    string `yaml:"grid,omitempty"`
	NoiseClass              string `yaml:"noise_class,omitempty"`
	PathMinObservationCount int    `yaml:"path_min_observation_count,omitempty"`
	SolarSummaryMinutes     int    `yaml:"solar_summary_minutes,omitempty"`
}

// Clone returns a detached, normalized preference snapshot. Callers must guard
// the source maps while cloning; runtime-only Filter fields are rebuilt on LOAD.
func (set *SavedPreset) Clone() (*SavedPreset, error) {
	bs, err := encodePreset(set)
	if err != nil {
		return nil, err
	}
	var result SavedPreset
	if err := yaml.Unmarshal(bs, &result); err != nil {
		return nil, err
	}
	result.normalize()
	if _, err := encodePreset(&result); err != nil {
		return nil, err
	}
	return &result, nil
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
		return "", errors.New("preset name must contain 1-32 letters, digits, underscores or hyphens")
	}
	for i := range len(name) {
		ch := name[i]
		alnum := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !alnum && (i == 0 || ch != '_' && ch != '-') {
			return "", errors.New("preset name must start with a letter or digit and contain only letters, digits, underscores or hyphens")
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

// LoadPreset returns detached, normalized preferences, or ErrPresetNotFound.
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
	// Normalization can grow serialized preferences (including Unicode case
	// conversion). Clone rechecks the bound before LOAD can publish any state.
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

func encodePreset(set *SavedPreset) ([]byte, error) {
	if set == nil {
		return nil, errors.New("nil saved preset")
	}
	// Existing live callsign/DXCC lists can grow across commands. Reject an
	// impossible snapshot before yaml.Marshal allocates a node tree for them.
	if !presetFitsMinimumSize(set) {
		return nil, fmt.Errorf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	bs, err := yaml.Marshal(set)
	if err != nil {
		return nil, err
	}
	if len(bs) > MaxPresetBytes {
		return nil, fmt.Errorf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	return bs, nil
}

// This is a lower bound, never a replacement for the exact encoded-byte check.
// Maps need at least "key: true\n" and list entries at least "- value\n";
// quoting/indentation only increases the eventual size. Runtime caches are omitted.
func presetFitsMinimumSize(set *SavedPreset) bool {
	remaining := MaxPresetBytes
	for _, token := range []string{set.Dialect, set.DedupePolicy, set.Grid, set.NoiseClass} {
		if len(token) > remaining {
			return false
		}
		remaining -= len(token)
	}
	f := &set.Filter
	for _, tokens := range [][]string{f.DXCallsigns, f.BlockDXCallsigns, f.DECallsigns, f.BlockDECallsigns} {
		if len(tokens) > remaining/3 {
			return false
		}
		remaining -= len(tokens) * 3
		for _, token := range tokens {
			if len(token) > remaining {
				return false
			}
			remaining -= len(token)
		}
	}
	for _, tokens := range []map[string]bool{
		f.Bands, f.BlockBands, f.Modes, f.BlockModes, f.Sources, f.BlockSources,
		f.Events, f.BlockEvents, f.Confidence, f.BlockConfidence, f.PathClasses, f.BlockPathClasses,
		f.DXContinents, f.BlockDXContinents, f.DEContinents, f.BlockDEContinents,
		f.DXGrid2Prefixes, f.BlockDXGrid2, f.DEGrid2Prefixes, f.BlockDEGrid2,
	} {
		if len(tokens) > remaining/7 {
			return false
		}
		remaining -= len(tokens) * 7
		for token := range tokens {
			if len(token) > remaining {
				return false
			}
			remaining -= len(token)
		}
	}
	for _, tokens := range []map[int]bool{f.DXZones, f.BlockDXZones, f.DEZones, f.BlockDEZones, f.DXDXCC, f.BlockDXDXCC, f.DEDXCC, f.BlockDEDXCC} {
		if len(tokens) > remaining/8 {
			return false
		}
		remaining -= len(tokens) * 8
	}
	return true
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
		if _, err := encodePreset(set); err != nil {
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
