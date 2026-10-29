// File role: Native runtime binding to the authoritative v6 DTO codecs.
package filter

import (
	"errors"

	"gopkg.in/yaml.v3"
)

// V6UserRecordFromRuntime captures preferences and metadata without granting
// authority to replace the full-SSID destination.
func V6UserRecordFromRuntime(record *UserRecord) (*V6UserRecord, error) {
	if record == nil {
		return nil, errors.New("nil user record")
	}
	if err := validateV6EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, err
	}
	ref, err := V6PresetReferenceFromRuntime(record.Preset)
	if err != nil {
		return nil, err
	}
	return &V6UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion,
		Configuration:        recordConfiguration(record).V6(), Preset: ref,
		RecentIPs: record.RecentIPs, LastLoginUTC: record.LastLoginUTC,
	}, nil
}

func RuntimeUserRecordFromV6(record *V6UserRecord) (*UserRecord, error) {
	if record == nil {
		return nil, errors.New("nil v6 user record")
	}
	if err := validateV6EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, err
	}
	cfg, value, err := PrepareV6Configuration(record.Configuration)
	if err != nil {
		return nil, err
	}
	ref, err := RuntimePresetReferenceFromV6(record.Preset)
	if err != nil {
		return nil, err
	}
	return &UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion, Filter: value, Preset: ref,
		Dialect: cfg.Settings.Dialect, Grid: cfg.Settings.Grid, NoiseClass: cfg.Settings.NoiseClass,
		DedupePolicy: cfg.Settings.DedupePolicy, PathMinObservationCount: cfg.Settings.PathMinObservationCount,
		SolarSummaryMinutes: cfg.Settings.SolarSummaryMinutes,
		RecentIPs:           trimRecentIPs(record.RecentIPs), LastLoginUTC: record.LastLoginUTC,
	}, nil
}

func (f Filter) MarshalYAML() (any, error) {
	return V6ConfigurationFromFilter(&f, SettingsConfiguration{}).Filters, nil
}

func (f *Filter) UnmarshalYAML(node *yaml.Node) error {
	var value V6FilterConfiguration
	if err := decodeV6StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	_, runtime, err := PrepareV6Configuration(V6Configuration{Filters: value})
	if err == nil {
		*f = runtime
	}
	return err
}

func (c Configuration) MarshalYAML() (any, error) { return c.V6(), nil }

func (f FilterConfiguration) MarshalYAML() (any, error) {
	return (Configuration{Filters: f}).V6().Filters, nil
}

func (set SavedPreset) MarshalYAML() (any, error) {
	return (&V6SavedPreset{ConfigurationVersion: set.ConfigurationVersion,
		Configuration: V6ConfigurationFromPreset(&set)}).Clone()
}

func (record UserRecord) MarshalYAML() (any, error) {
	return V6UserRecordFromRuntime(&record)
}

func (collection presetCollection) MarshalYAML() (any, error) {
	return v6PresetCollectionFromRuntime(&collection)
}

func v6PresetCollectionFromRuntime(collection *presetCollection) (*V6PresetCollection, error) {
	if collection == nil {
		return nil, errors.New("nil preset collection")
	}
	if len(collection.Presets) > MaxPresets {
		return nil, &storageAdmissionError{message: errPresetCollectionCount.Error(), cause: errPresetCollectionCount}
	}
	result := &V6PresetCollection{Presets: make(map[string]*V6SavedPreset, len(collection.Presets))}
	for name, set := range collection.Presets {
		value, err := V6SavedPresetFromRuntime(set)
		if err != nil {
			return nil, err
		}
		result.Presets[name] = value
	}
	return result, nil
}

func (c *Configuration) UnmarshalYAML(node *yaml.Node) error {
	var value V6Configuration
	if err := decodeV6StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	prepared, runtime, err := PrepareV6Configuration(value)
	if err != nil {
		return err
	}
	*c = ConfigurationFromFilter(&runtime, prepared.Settings)
	return nil
}

// V6ConfigurationFromPreset captures exact settings and controls. Only the
// immutable prepared payload supplies rules. Callers synchronize source access.
func V6ConfigurationFromPreset(set *SavedPreset) V6Configuration {
	if set == nil {
		return V6Configuration{}
	}
	return V6ConfigurationFromFilter(&set.Filter, SettingsConfiguration{
		Dialect: set.Dialect, Grid: set.Grid, NoiseClass: set.NoiseClass,
		DedupePolicy: set.DedupePolicy, PathMinObservationCount: set.PathMinObservationCount,
		SolarSummaryMinutes: set.SolarSummaryMinutes,
	})
}

// V6SavedPresetFromRuntime owns canonical definitions and stamps DTO version 6.
// This conversion never loads legacy files or grants replacement authority.
func V6SavedPresetFromRuntime(set *SavedPreset) (*V6SavedPreset, error) {
	if set == nil || set.ConfigurationVersion != CurrentConfigurationVersion {
		return nil, errors.New("invalid runtime v6 preset baseline")
	}
	return (&V6SavedPreset{
		ConfigurationVersion: CurrentConfigurationVersion,
		Configuration:        V6ConfigurationFromPreset(set),
	}).Clone()
}

// RuntimeSavedPresetFromV6 detaches controls/definitions and retains only the
// immutable prepared programs. Client location is not part of a saved Filter.
func RuntimeSavedPresetFromV6(set *V6SavedPreset) (*SavedPreset, error) {
	owned, err := set.Clone()
	if err != nil {
		return nil, err
	}
	cfg, value, err := PrepareV6Configuration(owned.Configuration)
	if err != nil {
		return nil, err
	}
	return &SavedPreset{
		ConfigurationVersion: CurrentConfigurationVersion, Filter: value,
		Dialect: cfg.Settings.Dialect, Grid: cfg.Settings.Grid, NoiseClass: cfg.Settings.NoiseClass,
		DedupePolicy: cfg.Settings.DedupePolicy, PathMinObservationCount: cfg.Settings.PathMinObservationCount,
		SolarSummaryMinutes: cfg.Settings.SolarSummaryMinutes,
	}, nil
}

// V6PresetReferenceFromRuntime validates and detaches one applied association.
func V6PresetReferenceFromRuntime(ref *PresetReference) (*V6PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // Absence is an explicit association.
	}
	baseline, err := V6SavedPresetFromRuntime(ref.Baseline)
	if err != nil {
		return nil, err
	}
	return (&V6PresetReference{Name: ref.Name, Baseline: baseline}).Clone()
}

// RuntimePresetReferenceFromV6 validates the shared v6 applied-baseline contract.
func RuntimePresetReferenceFromV6(ref *V6PresetReference) (*PresetReference, error) {
	owned, err := ref.Clone()
	if err != nil || owned == nil {
		return nil, err
	}
	baseline, err := RuntimeSavedPresetFromV6(owned.Baseline)
	if err != nil {
		return nil, err
	}
	return &PresetReference{Name: owned.Name, Baseline: baseline}, nil
}
