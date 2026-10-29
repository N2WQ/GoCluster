// File role: Pure, bounded v6 snapshots backing native named-preset IO.
package filter

import (
	"errors"
	"fmt"

	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

// V6SavedPreset stores only definitions, exact controls and settings, never programs.
type V6SavedPreset struct {
	ConfigurationVersion int             `yaml:"configuration_version"`
	Configuration        V6Configuration `yaml:"configuration"`
}

// V6PresetReference owns the single applied baseline independently of its library.
type V6PresetReference struct {
	Name     string         `yaml:"name"`
	Baseline *V6SavedPreset `yaml:"baseline"`
}

// V6PresetCollection is a detached named library, not an active store.
type V6PresetCollection struct {
	Presets map[string]*V6SavedPreset `yaml:"presets"`
}

// Clone validates before copying, preserving attested immutable programs when
// definitions still match. Version zero is a newly constructed DTO, not disk input.
func (set *V6SavedPreset) Clone() (*V6SavedPreset, error) {
	if set == nil {
		return nil, errors.New("nil v6 saved preset")
	}
	if err := validateV6EncodingVersion(set.ConfigurationVersion); err != nil {
		return nil, err
	}
	cfg, err := prepareV6StoredConfiguration(set.Configuration)
	if err != nil {
		return nil, err
	}
	return &V6SavedPreset{ConfigurationVersion: CurrentConfigurationVersion, Configuration: cfg}, nil
}

// Clone detaches the bounded applied snapshot without a YAML roundtrip.
func (ref *V6PresetReference) Clone() (*V6PresetReference, error) {
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
	return &V6PresetReference{Name: name, Baseline: baseline}, nil
}

func validateV6EncodingVersion(version int) error {
	if version != 0 && version != CurrentConfigurationVersion {
		return ErrUnsupportedConfigurationVersion
	}
	return nil
}

// Subtraction keeps the preflight linear and overflow-free before either model
// preparation or YAML printing allocates. Exact standalone bytes remain the oracle.
func v6ConfigurationMinimumRemaining(c V6Configuration, remaining int) (int, bool) {
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

func prepareV6StoredConfiguration(c V6Configuration) (V6Configuration, error) {
	if _, fits := v6ConfigurationMinimumRemaining(c, MaxPresetBytes); !fits {
		return V6Configuration{}, storageAdmissionf("v6 configuration exceeds preset bounds")
	}
	canonical, _, err := PrepareV6Configuration(c)
	if err != nil {
		return V6Configuration{}, err
	}
	data, err := yaml.Marshal(&V6SavedPreset{ConfigurationVersion: CurrentConfigurationVersion, Configuration: canonical})
	if err != nil {
		return V6Configuration{}, err
	}
	if len(data) > MaxPresetBytes {
		return V6Configuration{}, storageAdmissionf("preset exceeds %d KiB", MaxPresetBytes/1024)
	}
	return canonical, nil
}

// EncodeV6PresetCollection prepares an owned snapshot and emits exact v6 YAML.
// It neither reads an authoritative library nor grants replacement authority.
func EncodeV6PresetCollection(collection *V6PresetCollection) ([]byte, error) {
	_, data, err := prepareV6PresetCollection(collection)
	return data, err
}

func prepareV6PresetCollection(collection *V6PresetCollection) (*V6PresetCollection, []byte, error) {
	if collection == nil {
		return nil, nil, errors.New("nil v6 preset collection")
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
			return nil, nil, errors.New("nil v6 saved preset")
		}
		if _, fits := v6ConfigurationMinimumRemaining(set.Configuration, MaxPresetBytes); !fits {
			return nil, nil, storageAdmissionf("v6 configuration exceeds preset bounds")
		}
	}
	snapshot := &V6PresetCollection{Presets: make(map[string]*V6SavedPreset, len(collection.Presets))}
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

// DecodeV6PresetCollection admits one strict document with a shared work budget.
// Missing, legacy and future nested versions fail; no migration or IO occurs.
func DecodeV6PresetCollection(data []byte) (*V6PresetCollection, error) {
	var collection V6PresetCollection
	if err := decodeV6StorageDocument(data, maxPresetCollectionBytes,
		storageDecodeLimits{work: maxPresetCollectionWork, presets: MaxPresets}, &collection); err != nil {
		return nil, err
	}
	snapshot, _, err := prepareV6PresetCollection(&collection)
	return snapshot, err
}
