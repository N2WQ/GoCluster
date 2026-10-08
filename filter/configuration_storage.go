// File role: Presence-aware disk versioning and bounded applied-preset references.
// Absent markers select legacy migration; later versions add exact state/SNR rules.
// Invalid/future versions fail before records or shared presets can be rewritten.
package filter

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// ErrUnsupportedConfigurationVersion prevents rewriting unrecognized disk state.
var ErrUnsupportedConfigurationVersion = errors.New("unsupported configuration version")

// PresetReference identifies the applied snapshot, independently of later
// changes or deletion in the shared named library. Only one baseline is retained.
type PresetReference struct {
	Name     string       `yaml:"name"`
	Baseline *SavedPreset `yaml:"baseline"`
}

// Clone validates and detaches the single bounded applied snapshot.
func (ref *PresetReference) Clone() (*PresetReference, error) {
	if ref == nil {
		return nil, nil //nolint:nilnil // A nil reference is the valid absence of an associated preset.
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

// UnmarshalYAML validates the applied name and baseline before admitting a record.
func (ref *PresetReference) UnmarshalYAML(node *yaml.Node) error {
	if err := validateStoredMapping(node, reflect.TypeFor[PresetReference]()); err != nil {
		return err
	}
	if err := validateCurrentStoredValues(node, reflect.TypeFor[PresetReference]()); err != nil {
		return err
	}
	type plain PresetReference
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	detached, err := (*PresetReference)(&decoded).Clone()
	if err != nil {
		return err
	}
	*ref = *detached
	return nil
}

func storedConfigurationVersion(node *yaml.Node) (int, error) {
	if node.Kind != yaml.MappingNode {
		return 0, errors.New("stored configuration must be a mapping")
	}
	seen := false
	version := 0
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value != "configuration_version" {
			continue
		}
		if seen {
			return 0, errors.New("duplicate configuration_version")
		}
		seen = true
		value := node.Content[i+1]
		if value.Kind != yaml.ScalarNode || value.Tag != "!!int" {
			return 0, fmt.Errorf("%w: marker must be an integer", ErrUnsupportedConfigurationVersion)
		}
		if err := value.Decode(&version); err != nil || (version < 1 || version > CurrentConfigurationVersion) {
			return 0, fmt.Errorf("%w: expected 1 through %d", ErrUnsupportedConfigurationVersion, CurrentConfigurationVersion)
		}
	}
	return version, nil
}

// validateStoredStateFields admits states only in version two. Validate the
// fixed vocabulary before Node.Decode allocates rule maps, including fields
// inherited through legacy YAML merges. Only merge sources are followed: an
// unrelated legacy field must not change the historical migration contract.
func validateStoredStateFields(node *yaml.Node, version int) error {
	pending := []*yaml.Node{node}
	seen := make(map[*yaml.Node]bool)
	for len(pending) != 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == nil || seen[current] {
			continue
		}
		seen[current] = true
		switch current.Kind {
		case yaml.AliasNode:
			pending = append(pending, current.Alias)
			continue
		case yaml.SequenceNode:
			pending = append(pending, current.Content...)
			continue
		case yaml.MappingNode:
		default:
			continue // Decode owns malformed merge shape errors.
		}
		if err := validateStoredStateMapping(current, version, &pending); err != nil {
			return err
		}
	}
	return nil
}

// Each YAML node is visited once even when merge aliases form a cycle. State
// presence in any merge source is rejected for legacy records before decoding.
func validateStoredStateMapping(node *yaml.Node, version int, pending *[]*yaml.Node) error {
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		// Match yaml.v3's merge recognition before aliases are resolved as
		// ordinary field names; an aliased << key is not a merge directive.
		if key.Kind == yaml.ScalarNode && key.Value == "<<" && key.Tag == "!!merge" {
			*pending = append(*pending, node.Content[i+1])
			continue
		}
		if key.Kind == yaml.AliasNode && key.Alias != nil {
			key = key.Alias
		}
		name := key.Value
		switch name {
		case "dxstates", "blockdxstates", "destates", "blockdestates",
			"alldxstates", "blockalldxstates", "alldestates", "blockalldestates":
			if version < stateConfigurationVersion {
				return fmt.Errorf("state field %s requires configuration version %d", name, stateConfigurationVersion)
			}
		default:
			continue
		}
		switch name {
		case "dxstates", "blockdxstates", "destates", "blockdestates":
			value := node.Content[i+1]
			if value.Kind == yaml.AliasNode && value.Alias != nil {
				value = value.Alias
			}
			if value.Kind != yaml.MappingNode || len(value.Content)/2 > maxStateRuleEntries {
				return fmt.Errorf("invalid bounded state map %s", name)
			}
			for j := 0; j < len(value.Content); j += 2 {
				key := value.Content[j]
				if key.Kind == yaml.AliasNode && key.Alias != nil {
					key = key.Alias
				}
				if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || !spot.IsState(key.Value) {
					return fmt.Errorf("state map %s contains an unsupported key", name)
				}
			}
		}
	}
	return nil
}

// Custom node decoding does not inherit Decoder.KnownFields. Enforce the same
// fixed struct schema here, including inline Filter fields, before decoding.
func validateStoredMapping(node *yaml.Node, typ reflect.Type) error {
	if node.Kind != yaml.MappingNode {
		return errors.New("stored configuration must be a mapping")
	}
	fields := make(map[string]reflect.Type)
	addStoredFields(fields, typ)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || fields[key.Value] == nil {
			return fmt.Errorf("unknown stored configuration field %q", key.Value)
		}
	}
	return nil
}

func addStoredFields(fields map[string]reflect.Type, typ reflect.Type) {
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		tag := strings.Split(field.Tag.Get("yaml"), ",")
		if tag[0] == "-" {
			continue
		}
		inline := false
		for _, flag := range tag[1:] {
			inline = inline || flag == "inline"
		}
		if inline {
			addStoredFields(fields, field.Type)
			continue
		}
		name := tag[0]
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		fields[name] = field.Type
	}
}

// Exact records cannot turn malformed nulls or string booleans into zero values.
// Missing pointer toggles still denote DEFAULT; legacy decoding remains separate.
func validateCurrentStoredValues(node *yaml.Node, typ reflect.Type) error {
	fields := make(map[string]reflect.Type)
	addStoredFields(fields, typ)
	for i := 0; i < len(node.Content); i += 2 {
		field := node.Content[i].Value
		if err := validateStoredValue(node.Content[i+1], fields[field]); err != nil {
			return fmt.Errorf("invalid stored field %s: %w", field, err)
		}
	}
	return nil
}

func validateStoredValue(node *yaml.Node, typ reflect.Type) error {
	if node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	if node.Tag == "!!null" || typ == nil {
		return errors.New("null is not a configured value")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
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
		// SavedPreset owns its presence/version checks and legacy migration.
		if typ == reflect.TypeFor[SavedPreset]() {
			return nil
		}
		return validateCurrentStoredValues(node, typ)
	case reflect.Map:
		if node.Kind != yaml.MappingNode {
			return errors.New("expected a rule map")
		}
		seen := make(map[string]bool, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			if err := validateStoredValue(node.Content[i], typ.Key()); err != nil {
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
			if err := validateStoredValue(node.Content[i+1], typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode {
			return errors.New("expected a list")
		}
		for _, item := range node.Content {
			if err := validateStoredValue(item, typ.Elem()); err != nil {
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
