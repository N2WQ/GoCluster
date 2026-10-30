// File role: Tagged generation-seven snapshots and independent applied baselines.
package filter

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type V7SavedPreset struct {
	ConfigurationVersion int                    `yaml:"configuration_version"`
	Dialect              string                 `yaml:"dialect"`
	GO                   *V6FilterConfiguration `yaml:"go,omitempty"`
	CC                   *CCConfiguration       `yaml:"cc,omitempty"`
	Settings             SettingsConfiguration  `yaml:"settings"`
}

type V7PresetReference struct {
	Name     string         `yaml:"name"`
	Baseline *V7SavedPreset `yaml:"baseline"`
}

type V7PresetReferences struct {
	GO *V7PresetReference `yaml:"go,omitempty"`
	CC *V7PresetReference `yaml:"cc,omitempty"`
}

type V7PresetCollection struct {
	ConfigurationVersion int                       `yaml:"configuration_version"`
	ResetGeneration      int                       `yaml:"reset_generation"`
	Presets              map[string]*V7SavedPreset `yaml:"presets"`
}

// Clone validates the explicit owner and owns the selected bank's definitions.
func (set *V7SavedPreset) Clone() (*V7SavedPreset, error) {
	if set == nil {
		return nil, errors.New("nil v7 saved preset")
	}
	if err := validateV7EncodingVersion(set.ConfigurationVersion); err != nil {
		return nil, err
	}
	if !v7SettingsMinimumFits(set.Settings, MaxV7ConfigurationBytes) {
		return nil, storageAdmissionf("v7 preset exceeds byte limit")
	}
	result := &V7SavedPreset{ConfigurationVersion: V7ConfigurationVersion, Dialect: set.Dialect, Settings: cloneV7Settings(set.Settings)}
	switch set.Dialect {
	case "go":
		if set.GO == nil || set.CC != nil {
			return nil, errors.New("GO preset requires exactly its GO bank")
		}
		canonical, _, err := PrepareV6Configuration(V6Configuration{Filters: *set.GO})
		if err != nil {
			return nil, err
		}
		result.GO = &canonical.Filters
	case "cc":
		if set.CC == nil || set.GO != nil {
			return nil, errors.New("CC preset requires exactly its CC bank")
		}
		canonical, _, err := PrepareCCConfiguration(*set.CC)
		if err != nil {
			return nil, err
		}
		result.CC = &canonical
	default:
		return nil, errors.New("preset dialect must be explicit go or cc")
	}
	// The effective tag, not an inherited selector, owns later LOAD behavior.
	result.Settings.Dialect = result.Dialect
	data, err := yaml.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxV7ConfigurationBytes || len(data) > MaxPresetBytes {
		return nil, storageAdmissionf("v7 tagged preset exceeds %d bytes", MaxV7ConfigurationBytes)
	}
	return result, nil
}

func (ref *V7PresetReference) Clone() (*V7PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // Nil is absence of an applied snapshot.
	}
	name, err := NormalizePresetName(ref.Name)
	if err != nil || name != ref.Name {
		return nil, errors.New("invalid v7 applied preset name")
	}
	baseline, err := ref.Baseline.Clone()
	if err != nil {
		return nil, fmt.Errorf("invalid v7 baseline: %w", err)
	}
	return &V7PresetReference{Name: strings.Clone(name), Baseline: baseline}, nil
}

func (refs V7PresetReferences) Clone() (V7PresetReferences, error) {
	goRef, err := refs.GO.Clone()
	if err != nil {
		return V7PresetReferences{}, err
	}
	ccRef, err := refs.CC.Clone()
	if err != nil {
		return V7PresetReferences{}, err
	}
	if goRef != nil && goRef.Baseline.Dialect != "go" || ccRef != nil && ccRef.Baseline.Dialect != "cc" {
		return V7PresetReferences{}, errors.New("applied preset belongs to another bank")
	}
	return V7PresetReferences{GO: goRef, CC: ccRef}, nil
}

// V7PresetFromConfiguration captures only the selected bank and shared choices.
func V7PresetFromConfiguration(c V7Configuration, dialect string) (*V7SavedPreset, error) {
	set := &V7SavedPreset{Dialect: dialect, Settings: c.Settings}
	switch dialect {
	case "go":
		set.GO = &c.Filters.GO
	case "cc":
		set.CC = &c.Filters.CC
	default:
		return nil, errors.New("invalid preset dialect")
	}
	return set.Clone()
}

// MatchesConfiguration ignores active selection; shared preferences still count.
func (set *V7SavedPreset) MatchesConfiguration(c V7Configuration) bool {
	if set == nil {
		return false
	}
	settings, baseline := c.Settings, set.Settings
	settings.Dialect, baseline.Dialect = "", ""
	if settings != baseline {
		return false
	}
	switch set.Dialect {
	case "go":
		return set.GO != nil && (V6Configuration{Filters: *set.GO}).Equal(V6Configuration{Filters: c.Filters.GO})
	case "cc":
		return set.CC != nil && set.CC.Equal(c.Filters.CC)
	default:
		return false
	}
}

// ApplyV7Preset restores only the tagged bank, plus its shared preferences/tag.
func ApplyV7Preset(c V7Configuration, set *V7SavedPreset) (V7Configuration, error) {
	canonical, err := set.Clone()
	if err != nil {
		return V7Configuration{}, err
	}
	c = c.Clone()
	c.Settings = canonical.Settings
	if canonical.Dialect == "go" {
		c.Filters.GO = *canonical.GO
	} else {
		c.Filters.CC = *canonical.CC
	}
	c, _, _, err = PrepareV7Configuration(c)
	return c, err
}

func EncodeV7SavedPreset(set *V7SavedPreset) ([]byte, error) {
	canonical, err := set.Clone()
	if err != nil {
		return nil, err
	}
	return marshalV7Admitted(canonical, MaxV7ConfigurationBytes, maxUserRecordWork, v7ConfigurationNodes)
}

func DecodeV7SavedPreset(data []byte) (*V7SavedPreset, error) {
	var set V7SavedPreset
	if err := decodeV7StorageDocument(data, MaxV7ConfigurationBytes, maxUserRecordWork, v7ConfigurationNodes, &set); err != nil {
		return nil, err
	}
	return set.Clone()
}

func EncodeV7PresetCollection(collection *V7PresetCollection) ([]byte, error) {
	_, data, err := prepareV7PresetCollection(collection)
	return data, err
}

func prepareV7PresetCollection(collection *V7PresetCollection) (*V7PresetCollection, []byte, error) {
	if collection == nil {
		return nil, nil, errors.New("nil v7 preset collection")
	}
	if err := validateV7EncodingVersion(collection.ConfigurationVersion); err != nil {
		return nil, nil, err
	}
	if err := validateV7Generation(collection.ResetGeneration); err != nil {
		return nil, nil, err
	}
	if len(collection.Presets) > MaxPresets {
		return nil, nil, storageAdmissionf("preset count exceeds %d", MaxPresets)
	}
	result := &V7PresetCollection{ConfigurationVersion: V7ConfigurationVersion, ResetGeneration: collection.ResetGeneration, Presets: make(map[string]*V7SavedPreset, len(collection.Presets))}
	for name, set := range collection.Presets {
		canonical, err := NormalizePresetName(name)
		if err != nil || canonical != name {
			return nil, nil, errors.New("invalid v7 stored preset name")
		}
		detached, err := set.Clone()
		if err != nil {
			return nil, nil, fmt.Errorf("preset %s: %w", name, err)
		}
		result.Presets[strings.Clone(name)] = detached
	}
	data, err := marshalV7Admitted(result, maxPresetCollectionBytes, maxPresetCollectionWork, v7LibraryNodes)
	return result, data, err
}

func DecodeV7PresetCollection(data []byte) (*V7PresetCollection, error) {
	var collection V7PresetCollection
	if err := decodeV7StorageDocument(data, maxPresetCollectionBytes, maxPresetCollectionWork, v7LibraryNodes, &collection); err != nil {
		return nil, err
	}
	canonical, _, err := prepareV7PresetCollection(&collection)
	return canonical, err
}
