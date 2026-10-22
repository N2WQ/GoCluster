// File role: Pure, bounded v5 snapshots backing native named-preset IO.
package filter

import (
	"errors"
	"fmt"

	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

// V5SavedPreset stores only definitions, exact controls and settings, never programs.
type V5SavedPreset struct {
	ConfigurationVersion int             `yaml:"configuration_version"`
	Configuration        V5Configuration `yaml:"configuration"`
}

// V5PresetReference owns the single applied baseline independently of its library.
type V5PresetReference struct {
	Name     string         `yaml:"name"`
	Baseline *V5SavedPreset `yaml:"baseline"`
}

// V5PresetCollection is a detached named library, not an active store.
type V5PresetCollection struct {
	Presets map[string]*V5SavedPreset `yaml:"presets"`
}

// Clone validates before copying, preserving attested immutable programs when
// definitions still match. Version zero is a newly constructed DTO, not disk input.
func (set *V5SavedPreset) Clone() (*V5SavedPreset, error) {
	if set == nil {
		return nil, errors.New("nil v5 saved preset")
	}
	if err := validateV5EncodingVersion(set.ConfigurationVersion); err != nil {
		return nil, err
	}
	cfg, err := prepareV5StoredConfiguration(set.Configuration)
	if err != nil {
		return nil, err
	}
	return &V5SavedPreset{ConfigurationVersion: MVPConfigurationVersion, Configuration: cfg}, nil
}

// Clone detaches the bounded applied snapshot without a YAML roundtrip.
func (ref *V5PresetReference) Clone() (*V5PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // Nil is the valid absence of an applied preset.
	}
	name, err := NormalizePresetName(ref.Name)
	if err != nil || name != ref.Name {
		return nil, errors.New("invalid applied preset name")
	}
	baseline, err := ref.Baseline.Clone()
	if err != nil {
		return nil, fmt.Errorf("invalid applied preset baseline: %w", err)
	}
	return &V5PresetReference{Name: name, Baseline: baseline}, nil
}

func validateV5EncodingVersion(version int) error {
	if version != 0 && version != MVPConfigurationVersion {
		return ErrUnsupportedConfigurationVersion
	}
	return nil
}

// Subtraction keeps the preflight linear and overflow-free before either model
// preparation or YAML printing allocates. Exact standalone bytes remain the oracle.
func v5ConfigurationMinimumRemaining(c V5Configuration, remaining int) (int, bool) {
	consume := func(size int) bool {
		if size < 0 || size > remaining {
			return false
		}
		remaining -= size
		return true
	}
	for _, value := range [...]string{c.Settings.Dialect, c.Settings.Grid, c.Settings.NoiseClass, c.Settings.DedupePolicy} {
		if !consume(len(value)) {
			return 0, false
		}
	}
	if len(c.Filters.Rules) > MaxRuleSlots {
		return 0, false
	}
	for _, rule := range c.Filters.Rules {
		if len(rule.Expr) > expr.MaxBytes || !consume(len(rule.Action)) {
			return 0, false
		}
		// Raw source can shrink during canonicalization. Bound its input before
		// preparation, but charge only a proven lower bound on the stored text.
		if rule.Expr != "" && !consume(1) {
			return 0, false
		}
	}
	return remaining, true
}

func prepareV5StoredConfiguration(c V5Configuration) (V5Configuration, error) {
	if _, fits := v5ConfigurationMinimumRemaining(c, MaxPresetBytes); !fits {
		return V5Configuration{}, storageAdmissionf("v5 configuration exceeds preset bounds")
	}
	canonical, _, err := PrepareV5Configuration(c)
	if err != nil {
		return V5Configuration{}, err
	}
	data, err := yaml.Marshal(&V5SavedPreset{ConfigurationVersion: MVPConfigurationVersion, Configuration: canonical})
	if err != nil {
		return V5Configuration{}, err
	}
	if len(data) > MaxPresetBytes {
		return V5Configuration{}, storageAdmissionf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	return canonical, nil
}

// EncodeV5PresetCollection prepares an owned snapshot and emits exact v5 YAML.
// It neither reads an authoritative library nor grants replacement authority.
func EncodeV5PresetCollection(collection *V5PresetCollection) ([]byte, error) {
	_, data, err := prepareV5PresetCollection(collection)
	return data, err
}

func prepareV5PresetCollection(collection *V5PresetCollection) (*V5PresetCollection, []byte, error) {
	if collection == nil {
		return nil, nil, errors.New("nil v5 preset collection")
	}
	if len(collection.Presets) > MaxPresets {
		return nil, nil, &storageAdmissionError{message: errPresetCollectionCount.Error(), cause: errPresetCollectionCount}
	}
	// Check every entry before allocating a map or copying definitions.
	for name, set := range collection.Presets {
		canonical, err := NormalizePresetName(name)
		if err != nil || name != canonical {
			return nil, nil, errors.New("invalid stored preset name")
		}
		if set == nil {
			return nil, nil, errors.New("nil v5 saved preset")
		}
		if _, fits := v5ConfigurationMinimumRemaining(set.Configuration, MaxPresetBytes); !fits {
			return nil, nil, storageAdmissionf("v5 configuration exceeds preset bounds")
		}
	}
	snapshot := &V5PresetCollection{Presets: make(map[string]*V5SavedPreset, len(collection.Presets))}
	for name, set := range collection.Presets {
		detached, err := set.Clone()
		if err != nil {
			return nil, nil, fmt.Errorf("invalid stored preset %s: %w", name, err)
		}
		snapshot.Presets[name] = detached
	}
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return nil, nil, err
	}
	if len(data) > maxPresetCollectionBytes {
		return nil, nil, storageAdmissionf("preset collection exceeds 8 MiB")
	}
	return snapshot, data, nil
}

// DecodeV5PresetCollection admits one strict document with a shared work budget.
// Missing, legacy and future nested versions fail; no migration or IO occurs.
func DecodeV5PresetCollection(data []byte) (*V5PresetCollection, error) {
	var collection V5PresetCollection
	if err := decodeV5StorageDocument(data, maxPresetCollectionBytes,
		storageDecodeLimits{work: maxPresetCollectionWork, presets: MaxPresets}, &collection); err != nil {
		return nil, err
	}
	snapshot, _, err := prepareV5PresetCollection(&collection)
	return snapshot, err
}
