// File role: Native runtime binding to the authoritative v5 DTO codecs.
package filter

import (
	"errors"

	"gopkg.in/yaml.v3"
)

// V5UserRecordFromRuntime captures preferences and metadata without granting
// authority to replace the full-SSID destination.
func V5UserRecordFromRuntime(record *UserRecord) (*V5UserRecord, error) {
	if record == nil {
		return nil, errors.New("nil user record")
	}
	if err := validateV5EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, err
	}
	ref, err := V5PresetReferenceFromRuntime(record.Preset)
	if err != nil {
		return nil, err
	}
	return &V5UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion,
		Configuration:        recordConfiguration(record).V5(), Preset: ref,
		RecentIPs: record.RecentIPs, LastLoginUTC: record.LastLoginUTC,
	}, nil
}

func RuntimeUserRecordFromV5(record *V5UserRecord) (*UserRecord, error) {
	if record == nil {
		return nil, errors.New("nil v5 user record")
	}
	if err := validateV5EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, err
	}
	cfg, value, err := PrepareV5Configuration(record.Configuration)
	if err != nil {
		return nil, err
	}
	ref, err := RuntimePresetReferenceFromV5(record.Preset)
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
	return V5ConfigurationFromFilter(&f, SettingsConfiguration{}).Filters, nil
}

func (f *Filter) UnmarshalYAML(node *yaml.Node) error {
	var value V5FilterConfiguration
	if err := decodeV5StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	_, runtime, err := PrepareV5Configuration(V5Configuration{Filters: value})
	if err == nil {
		*f = runtime
	}
	return err
}

func (c Configuration) MarshalYAML() (any, error) { return c.V5(), nil }

func (f FilterConfiguration) MarshalYAML() (any, error) {
	return (Configuration{Filters: f}).V5().Filters, nil
}

func (set SavedPreset) MarshalYAML() (any, error) {
	return (&V5SavedPreset{ConfigurationVersion: set.ConfigurationVersion,
		Configuration: V5ConfigurationFromPreset(&set)}).Clone()
}

func (record UserRecord) MarshalYAML() (any, error) {
	return V5UserRecordFromRuntime(&record)
}

func (collection presetCollection) MarshalYAML() (any, error) {
	return v5PresetCollectionFromRuntime(&collection)
}

func v5PresetCollectionFromRuntime(collection *presetCollection) (*V5PresetCollection, error) {
	if collection == nil {
		return nil, errors.New("nil preset collection")
	}
	if len(collection.Presets) > MaxPresets {
		return nil, &storageAdmissionError{message: errPresetCollectionCount.Error(), cause: errPresetCollectionCount}
	}
	result := &V5PresetCollection{Presets: make(map[string]*V5SavedPreset, len(collection.Presets))}
	for name, set := range collection.Presets {
		value, err := V5SavedPresetFromRuntime(set)
		if err != nil {
			return nil, err
		}
		result.Presets[name] = value
	}
	return result, nil
}

func (c *Configuration) UnmarshalYAML(node *yaml.Node) error {
	var value V5Configuration
	if err := decodeV5StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	prepared, runtime, err := PrepareV5Configuration(value)
	if err != nil {
		return err
	}
	*c = ConfigurationFromFilter(&runtime, prepared.Settings)
	return nil
}

// V5ConfigurationFromPreset captures exact settings and controls. Only the
// immutable prepared payload supplies rules. Callers synchronize source access.
func V5ConfigurationFromPreset(set *SavedPreset) V5Configuration {
	if set == nil {
		return V5Configuration{}
	}
	return V5ConfigurationFromFilter(&set.Filter, SettingsConfiguration{
		Dialect: set.Dialect, Grid: set.Grid, NoiseClass: set.NoiseClass,
		DedupePolicy: set.DedupePolicy, PathMinObservationCount: set.PathMinObservationCount,
		SolarSummaryMinutes: set.SolarSummaryMinutes,
	})
}

// V5SavedPresetFromRuntime owns canonical definitions and stamps DTO version 5.
// This conversion never loads legacy files or grants replacement authority.
func V5SavedPresetFromRuntime(set *SavedPreset) (*V5SavedPreset, error) {
	if set == nil || set.ConfigurationVersion != CurrentConfigurationVersion {
		return nil, errors.New("invalid runtime v5 preset baseline")
	}
	return (&V5SavedPreset{
		ConfigurationVersion: MVPConfigurationVersion,
		Configuration:        V5ConfigurationFromPreset(set),
	}).Clone()
}

// RuntimeSavedPresetFromV5 detaches controls/definitions and retains only the
// immutable prepared programs. Runtime-only NEARBY cells are rebuilt at publish.
func RuntimeSavedPresetFromV5(set *V5SavedPreset) (*SavedPreset, error) {
	owned, err := set.Clone()
	if err != nil {
		return nil, err
	}
	cfg, value, err := PrepareV5Configuration(owned.Configuration)
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

// V5PresetReferenceFromRuntime validates and detaches one applied association.
func V5PresetReferenceFromRuntime(ref *PresetReference) (*V5PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // Absence is an explicit association.
	}
	baseline, err := V5SavedPresetFromRuntime(ref.Baseline)
	if err != nil {
		return nil, err
	}
	return (&V5PresetReference{Name: ref.Name, Baseline: baseline}).Clone()
}

// RuntimePresetReferenceFromV5 validates the shared v5 applied-baseline contract.
func RuntimePresetReferenceFromV5(ref *V5PresetReference) (*PresetReference, error) {
	owned, err := ref.Clone()
	if err != nil || owned == nil {
		return nil, err
	}
	baseline, err := RuntimeSavedPresetFromV5(owned.Baseline)
	if err != nil {
		return nil, err
	}
	return &PresetReference{Name: owned.Name, Baseline: baseline}, nil
}
