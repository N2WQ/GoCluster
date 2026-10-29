// File role: Strict presence/type prechecks over plain v6 DTOs, followed by the
// existing operation-owned decoder. Baselines never open their own work budget.
package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"

	"dxcluster/filter/expr"
	"gopkg.in/yaml.v3"
)

func decodeV6StorageDocument(data []byte, byteLimit int, limits storageDecodeLimits, output any) error {
	if len(data) > byteLimit {
		return storageAdmissionf("stored v6 document exceeds %d bytes", byteLimit)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return errors.New("stored v6 document must contain one YAML mapping")
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("stored v6 document must contain one YAML document")
	}
	return decodeV6StorageNode(&document, limits, output)
}

func decodeV6StorageNode(document *yaml.Node, limits storageDecodeLimits, output any) error {
	node := document
	if node.Kind == yaml.DocumentNode && len(node.Content) == 1 {
		node = node.Content[0]
	}
	if node.Kind != yaml.MappingNode {
		return errors.New("stored v6 document must contain one YAML mapping")
	}
	state, err := newStorageDecodeStateWithLimits(document, limits)
	if err != nil {
		return err
	}
	out := reflect.ValueOf(output).Elem()
	// This schema is acyclic and fixed-depth. Maps/rule counts are checked before
	// recursive strict validation, avoiding allocation sized to rejected input.
	if err := validateStoredValueWithFields(node, out.Type(), state.validateV6StoredFields); err != nil {
		return err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, out.Type(), reflect.Value{}, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	if state.presetCountExceeded {
		return &storageAdmissionError{message: errPresetCollectionCount.Error(), cause: errPresetCollectionCount}
	}
	state.materialize = true
	guard = newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, out.Type(), out, guard); err != nil {
		return err
	}
	if len(guard.typeErrors) != 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return nil
}

func v6StoredNode(node *yaml.Node) *yaml.Node {
	if node.Kind == yaml.AliasNode && node.Alias != nil {
		return node.Alias
	}
	return node
}

func v6StoredField(node *yaml.Node, name string) *yaml.Node {
	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == name {
			return v6StoredNode(node.Content[i+1])
		}
	}
	return nil
}

func validateV6StoredVersion(node *yaml.Node) error {
	marker := v6StoredField(node, "configuration_version")
	if marker == nil || marker.Kind != yaml.ScalarNode || marker.Tag != "!!int" {
		return fmt.Errorf("%w: explicit v6 integer marker required", ErrUnsupportedConfigurationVersion)
	}
	var version int
	if err := marker.Decode(&version); err != nil || version != CurrentConfigurationVersion {
		return fmt.Errorf("%w: expected %d", ErrUnsupportedConfigurationVersion, CurrentConfigurationVersion)
	}
	return nil
}

func (state *storageDecodeState) validateV6StoredFields(node *yaml.Node, typ reflect.Type) error {
	if typ == reflect.TypeFor[V6UserRecord]() || typ == reflect.TypeFor[V6SavedPreset]() {
		if err := validateV6StoredVersion(node); err != nil {
			return err
		}
	}
	if err := state.validateStoredMapping(node, typ); err != nil {
		return err
	}
	// Even all-duplicate mappings must not size a duplicate map to arbitrary input.
	if len(node.Content)/2 > len(state.storedFields(typ)) {
		return errors.New("too many or duplicate stored v6 fields")
	}
	if err := state.rawDuplicates(node); err != nil {
		return err
	}
	var required []string
	switch typ {
	case reflect.TypeFor[V6UserRecord](), reflect.TypeFor[V6SavedPreset]():
		required = []string{"configuration"}
	case reflect.TypeFor[V6Configuration]():
		required = []string{"filters", "settings"}
	case reflect.TypeFor[V6FilterConfiguration]():
		required = []string{"rules"}
		if rules := v6StoredField(node, "rules"); rules != nil && rules.Kind == yaml.SequenceNode && len(rules.Content) > MaxRuleSlots {
			return storageAdmissionf("rules exceed slot limit %d", MaxRuleSlots)
		}
	case reflect.TypeFor[RuleDefinition]():
		required = []string{"slot", "action", "expr"}
		if text := v6StoredField(node, "expr"); text != nil && len(text.Value) > expr.MaxBytes {
			return errors.New("rule expression exceeds byte limit")
		}
	case reflect.TypeFor[V6PresetReference]():
		required = []string{"name", "baseline"}
	case reflect.TypeFor[V6PresetCollection]():
		required = []string{"presets"}
		if presets := v6StoredField(node, "presets"); presets != nil && presets.Kind == yaml.MappingNode && len(presets.Content)/2 > MaxPresets {
			return &storageAdmissionError{message: errPresetCollectionCount.Error(), cause: errPresetCollectionCount}
		}
	}
	for _, name := range required {
		if v6StoredField(node, name) == nil {
			return fmt.Errorf("missing required stored field %s", name)
		}
	}
	fields := state.storedFields(typ)
	for i := 0; i < len(node.Content); i += 2 {
		name := node.Content[i].Value
		if err := validateStoredValueWithFields(node.Content[i+1], fields[name].typ, state.validateV6StoredFields); err != nil {
			return fmt.Errorf("invalid stored field %s: %w", name, err)
		}
	}
	return nil
}
