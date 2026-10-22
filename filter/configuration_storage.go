// File role: Applied-preset references and strict v5 stored-value validation.
package filter

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// ErrUnsupportedConfigurationVersion protects unrecognized disk state.
var ErrUnsupportedConfigurationVersion = errors.New("unsupported configuration version")

// PresetReference retains one applied baseline independently of library edits.
type PresetReference struct {
	Name     string       `yaml:"name"`
	Baseline *SavedPreset `yaml:"baseline"`
}

// Clone validates and detaches the single bounded applied snapshot.
func (ref *PresetReference) Clone() (*PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // Absence is an explicit association.
	}
	name, err := NormalizePresetName(ref.Name)
	if err != nil || name != ref.Name {
		return nil, errors.New("invalid applied preset name")
	}
	baseline, err := ref.Baseline.Clone()
	if err != nil {
		return nil, fmt.Errorf("invalid applied preset baseline: %w", err)
	}
	return &PresetReference{Name: name, Baseline: baseline}, nil
}

// UnmarshalYAML validates the applied name and v5 baseline before admission.
func (ref *PresetReference) UnmarshalYAML(node *yaml.Node) error {
	var value V5PresetReference
	if err := decodeV5StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	runtime, err := RuntimePresetReferenceFromV5(&value)
	if err == nil {
		*ref = *runtime
	}
	return err
}

// Struct validation supplies field metadata only. Values retain their ordered
// checks, including aliased keys and interpreted duplicates.
func validateStoredValueWithFields(node *yaml.Node, typ reflect.Type, validateFields func(*yaml.Node, reflect.Type) error) error {
	if node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Tag == "!!null" || typ == nil {
		return errors.New("null is not a configured value")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeFor[DefaultBool]() {
		var value DefaultBool
		return value.UnmarshalYAML(node)
	}
	if typ == reflect.TypeFor[time.Time]() {
		if node.Kind != yaml.ScalarNode || node.Tag != "!!timestamp" && node.Tag != "!!str" {
			return errors.New("login timestamp must be a scalar timestamp")
		}
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode {
			return errors.New("expected a mapping")
		}
		return validateFields(node, typ)
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return errors.New("expected a rule map")
		}
		seen := make(map[string]bool, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			if err := validateStoredValueWithFields(node.Content[i], typ.Key(), validateFields); err != nil {
				return err
			}
			keyNode := node.Content[i]
			if keyNode.Kind == yaml.AliasNode && keyNode.Alias != nil {
				keyNode = keyNode.Alias
			}
			key := keyNode.Value
			if typ.Key().Kind() == reflect.Int {
				var value int
				if err := keyNode.Decode(&value); err != nil {
					return err
				}
				key = strconv.Itoa(value)
			}
			if seen[key] {
				return fmt.Errorf("duplicate interpreted rule key %q", key)
			}
			seen[key] = true
			if err := validateStoredValueWithFields(node.Content[i+1], typ.Elem(), validateFields); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode {
			return errors.New("expected a list")
		}
		for _, item := range node.Content {
			if err := validateStoredValueWithFields(item, typ.Elem(), validateFields); err != nil {
				return err
			}
		}
	case reflect.Bool:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
			return errors.New("expected true or false")
		}
	case reflect.Int:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
			return errors.New("expected an integer")
		}
	case reflect.String:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return errors.New("expected a string")
		}
	default:
		return errors.New("unsupported stored field type")
	}
	return nil
}
