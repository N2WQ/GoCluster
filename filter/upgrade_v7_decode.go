// File role: Frozen pre-seven recognition for discard, never rule compilation.
package filter

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"gopkg.in/yaml.v3"
)

func v7UpgradeVersion(node *yaml.Node) (int, error) {
	marker := v6StoredField(node, "configuration_version")
	if marker == nil {
		return 0, nil
	}
	var version int
	if marker.Kind != yaml.ScalarNode || marker.Tag != "!!int" || marker.Decode(&version) != nil || version < 1 || version > V7ConfigurationVersion {
		return 0, ErrUnsupportedConfigurationVersion
	}
	return version, nil
}

// Bound physical and alias-expanded nodes before recursive schema recognition.
func admitV7UpgradeNodes(node *yaml.Node, maximum int) error {
	active := make(map[*yaml.Node]bool)
	visits := 0
	var walk func(*yaml.Node, int) error
	walk = func(current *yaml.Node, depth int) error {
		if current == nil {
			return errors.New("nil upgrade node")
		}
		visits++
		if visits > maximum || depth > v7MaxDepth {
			return storageAdmissionf("upgrade node/depth limit exceeded")
		}
		if active[current] {
			return errors.New("recursive upgrade alias")
		}
		active[current] = true
		defer delete(active, current)
		if current.Kind == yaml.AliasNode {
			return walk(current.Alias, depth+1)
		}
		for _, child := range current.Content {
			if err := walk(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(node, 0)
}

func decodeV7DiscardProfile(node *yaml.Node, version int, state *storageDecodeState) (SettingsConfiguration, []string, time.Time, error) {
	switch version {
	case V6ConfigurationVersion:
		var old V6UserRecord
		if err := decodeV7FrozenValue(node, state, &old, state.validateV6StoredFields); err != nil {
			return SettingsConfiguration{}, nil, time.Time{}, err
		}
		return old.Configuration.Settings, trimRecentIPs(old.RecentIPs), old.LastLoginUTC, nil
	case retiredV5Version:
		var old retiredV5Record
		if err := decodeRetiredV5Value(node, state, &old); err != nil {
			return SettingsConfiguration{}, nil, time.Time{}, err
		}
		return SettingsConfiguration(old.Configuration.Settings), trimRecentIPs(old.RecentIPs), old.LastLoginUTC, nil
	default:
		if version < 0 || version > 4 {
			return SettingsConfiguration{}, nil, time.Time{}, ErrUnsupportedConfigurationVersion
		}
		old, err := decodeLegacyLogin(node, version, state, nil)
		if err != nil {
			return SettingsConfiguration{}, nil, time.Time{}, err
		}
		return SettingsConfiguration{Dialect: old.Dialect, Grid: old.Grid, NoiseClass: old.NoiseClass, DedupePolicy: old.DedupePolicy, PathMinObservationCount: old.PathMinObservationCount, SolarSummaryMinutes: old.SolarSummaryMinutes}, old.RecentIPs, old.LastLoginUTC, nil
	}
}

func decodeV7FrozenValue(node *yaml.Node, state *storageDecodeState, output any, validate func(*yaml.Node, reflect.Type) error) error {
	typ := reflect.TypeOf(output).Elem()
	if err := validateStoredValueWithFields(node, typ, validate); err != nil {
		return err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(node, typ, reflect.Value{}, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) > 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	state.materialize = true
	defer func() { state.materialize = false }()
	guard = newStorageDecodeGuard(true)
	if _, _, err := state.decode(node, typ, reflect.ValueOf(output).Elem(), guard); err != nil {
		return err
	}
	if len(guard.typeErrors) > 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return nil
}

func validateV7DiscardPreset(node *yaml.Node, version int, state *storageDecodeState) error {
	switch version {
	case V6ConfigurationVersion:
		var old V6SavedPreset
		return decodeV7FrozenValue(node, state, &old, state.validateV6StoredFields)
	case retiredV5Version:
		var old retiredV5Preset
		return decodeRetiredV5Value(node, state, &old)
	default:
		if version < 0 || version > 4 {
			return ErrUnsupportedConfigurationVersion
		}
		return validateLegacyOwner(node, version, state, true)
	}
}

// Library preparation retains valid v7 entries and drops only recognized old
// entries. One state accounts for all entries and alias-expanded work.
func prepareV7LibraryReset(original []byte) (*V7PresetCollection, []byte, bool, error) {
	node, state, err := parseUpgradeDocument(original, maxPresetCollectionBytes, maxPresetCollectionWork)
	if err != nil {
		return nil, nil, false, err
	}
	if err := admitV7UpgradeNodes(node, v7LibraryNodes); err != nil {
		return nil, nil, false, err
	}
	if err := state.rawDuplicates(node); err != nil {
		return nil, nil, false, err
	}
	marked := false
	generation := 0
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return nil, nil, false, errors.New("invalid preset library key")
		}
		switch key.Value {
		case "presets":
		case "configuration_version":
			marked = true
			version, err := v7UpgradeVersion(node)
			if err != nil || version != V7ConfigurationVersion {
				return nil, nil, false, ErrUnsupportedConfigurationVersion
			}
		case "reset_generation":
			value := v6StoredNode(node.Content[i+1])
			if value.Kind != yaml.ScalarNode || value.Tag != "!!int" || value.Decode(&generation) != nil || validateV7Generation(generation) != nil {
				return nil, nil, false, errors.New("invalid library reset generation")
			}
		default:
			return nil, nil, false, fmt.Errorf("unknown preset library field %q", key.Value)
		}
	}
	if !marked && v6StoredField(node, "reset_generation") != nil {
		return nil, nil, false, errors.New("unversioned library reset marker")
	}
	if marked && v6StoredField(node, "reset_generation") == nil {
		return nil, nil, false, errors.New("missing library reset generation")
	}
	entries := v6StoredField(node, "presets")
	if entries == nil || entries.Kind != yaml.MappingNode || entries.Tag != "!!map" {
		return nil, nil, false, errors.New("presets must be a mapping")
	}
	if len(entries.Content)/2 > MaxPresets {
		return nil, nil, false, storageAdmissionf("preset count exceeds %d", MaxPresets)
	}
	if err := state.rawDuplicates(entries); err != nil {
		return nil, nil, false, err
	}
	collection := &V7PresetCollection{ConfigurationVersion: V7ConfigurationVersion, ResetGeneration: generation, Presets: make(map[string]*V7SavedPreset, len(entries.Content)/2)}
	removed := false
	seenNames := make(map[string]bool, len(entries.Content)/2)
	for i := 0; i < len(entries.Content); i += 2 {
		key, value := v6StoredNode(entries.Content[i]), v6StoredNode(entries.Content[i+1])
		name, err := NormalizePresetName(key.Value)
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || err != nil || name != key.Value {
			return nil, nil, false, errors.New("invalid stored preset name")
		}
		if seenNames[name] {
			return nil, nil, false, errors.New("duplicate interpreted preset name")
		}
		seenNames[name] = true
		version, err := v7UpgradeVersion(value)
		if err != nil {
			return nil, nil, false, err
		}
		if version != V7ConfigurationVersion {
			if err := validateV7DiscardPreset(value, version, state); err != nil {
				return nil, nil, false, fmt.Errorf("old preset %s: %w", name, err)
			}
			removed = true
			continue
		}
		var set V7SavedPreset
		if err := decodeV7FrozenValue(value, state, &set, func(n *yaml.Node, t reflect.Type) error {
			return (&v7ShapeGuard{state: state, maximum: v7LibraryNodes, active: make(map[*yaml.Node]bool)}).validate(n, t, 0)
		}); err != nil {
			return nil, nil, false, err
		}
		canonical, err := set.Clone()
		if err != nil {
			return nil, nil, false, err
		}
		collection.Presets[name] = canonical
	}
	if removed {
		collection.ResetGeneration = V7ConfigurationVersion
	}
	canonical, data, err := prepareV7PresetCollection(collection)
	if err != nil {
		return nil, nil, false, err
	}
	if marked && !removed {
		return canonical, nil, false, nil
	}
	return canonical, data, removed, nil
}
