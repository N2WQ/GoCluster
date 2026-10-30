// File role: Strict fixed-depth v7 decoding with one physical/logical work budget.
// Nested banks and both applied baselines never open independent decoders.
package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	v7ConfigurationNodes = 32768
	v7ProfileNodes       = 65536
	v7LibraryNodes       = 524288
	v7MaxDepth           = 16
)

func marshalV7Admitted(value any, byteLimit int, work uint64, nodes int) ([]byte, error) {
	data, err := yaml.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(data) > byteLimit {
		return nil, storageAdmissionf("v7 document exceeds %d bytes", byteLimit)
	}
	// A dry decode of the COMPLETE encoded owner is the shared-work oracle.
	if err := validateV7EncodedDocument(data, byteLimit, work, nodes, reflect.TypeOf(value)); err != nil {
		return nil, err
	}
	return data, nil
}

func parseV7Document(data []byte, byteLimit int) (*yaml.Node, error) {
	if len(data) > byteLimit {
		return nil, storageAdmissionf("v7 document exceeds %d bytes", byteLimit)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document, extra yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("v7 document must contain one mapping")
	}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("v7 document must contain exactly one YAML document")
	}
	return &document, nil
}

func validateV7EncodedDocument(data []byte, byteLimit int, work uint64, nodes int, typ reflect.Type) error {
	document, err := parseV7Document(data, byteLimit)
	if err != nil {
		return err
	}
	_, err = prepareV7Decode(document, work, nodes, typ)
	return err
}

func decodeV7StorageDocument(data []byte, byteLimit int, work uint64, nodes int, output any) error {
	document, err := parseV7Document(data, byteLimit)
	if err != nil {
		return err
	}
	out := reflect.ValueOf(output)
	if out.Kind() != reflect.Pointer || out.IsNil() {
		return errors.New("v7 decode output must be a pointer")
	}
	state, err := prepareV7Decode(document, work, nodes, out.Elem().Type())
	if err != nil {
		return err
	}
	state.materialize = true
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, out.Elem().Type(), out.Elem(), guard); err != nil {
		return err
	}
	if len(guard.typeErrors) > 0 {
		return &yaml.TypeError{Errors: guard.typeErrors}
	}
	return nil
}

func prepareV7Decode(document *yaml.Node, work uint64, nodes int, typ reflect.Type) (*storageDecodeState, error) {
	if err := admitV7UpgradeNodes(document, nodes); err != nil {
		return nil, err
	}
	state, err := newStorageDecodeStateWithLimits(document, storageDecodeLimits{work: work, presets: MaxPresets})
	if err != nil {
		return nil, err
	}
	shape := v7ShapeGuard{state: state, maximum: nodes, active: make(map[*yaml.Node]bool)}
	if err := shape.validate(document.Content[0], typ, 0); err != nil {
		return nil, err
	}
	guard := newStorageDecodeGuard(true)
	if _, _, err := state.decode(document, typ, reflect.Value{}, guard); err != nil {
		return nil, err
	}
	if len(guard.typeErrors) > 0 {
		return nil, &yaml.TypeError{Errors: guard.typeErrors}
	}
	return state, nil
}

type v7ShapeGuard struct {
	state           *storageDecodeState
	maximum, visits int
	active          map[*yaml.Node]bool
}

func (g *v7ShapeGuard) validate(node *yaml.Node, typ reflect.Type, depth int) error {
	if node == nil {
		return errors.New("nil v7 node")
	}
	g.visits++
	if g.visits > g.maximum || depth > v7MaxDepth {
		return storageAdmissionf("v7 document exceeds node/depth limit")
	}
	if g.active[node] {
		return errors.New("recursive v7 alias")
	}
	g.active[node] = true
	defer delete(g.active, node)
	if node.Kind == yaml.AliasNode {
		return g.validate(node.Alias, typ, depth+1)
	}
	if node.Tag == "!!null" {
		return errors.New("null is not a v7 configured value")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeFor[DefaultBool]() {
		var value DefaultBool
		return value.UnmarshalYAML(node)
	}
	if typ == reflect.TypeFor[CCAction]() {
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return errors.New("CC action must be a string")
		}
		return nil
	}
	if typ == reflect.TypeFor[time.Time]() {
		return validateStoredValueWithFields(node, typ, g.state.validateV6StoredFields)
	}
	switch typ.Kind() {
	case reflect.Struct:
		if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
			return errors.New("v7 object must be a mapping")
		}
		fields := g.state.storedFields(typ)
		if len(node.Content)/2 > len(fields) {
			return errors.New("too many or duplicate v7 fields")
		}
		if err := g.state.validateStoredMapping(node, typ); err != nil {
			return err
		}
		if err := g.state.rawDuplicates(node); err != nil {
			return err
		}
		for _, name := range requiredV7Fields(typ) {
			if v6StoredField(node, name) == nil {
				return fmt.Errorf("missing required v7 field %s", name)
			}
		}
		if typ == reflect.TypeFor[V7UserRecord]() || typ == reflect.TypeFor[V7SavedPreset]() || typ == reflect.TypeFor[V7PresetCollection]() {
			marker := v6StoredField(node, "configuration_version")
			var version int
			if marker == nil || marker.Kind != yaml.ScalarNode || marker.Tag != "!!int" || marker.Decode(&version) != nil || version != V7ConfigurationVersion {
				return ErrUnsupportedConfigurationVersion
			}
		}
		for i := 0; i < len(node.Content); i += 2 {
			if err := g.validate(node.Content[i+1], fields[node.Content[i].Value].typ, depth+1); err != nil {
				return fmt.Errorf("%s: %w", node.Content[i].Value, err)
			}
		}
	case reflect.Map:
		if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
			return errors.New("v7 map must be a mapping")
		}
		limit := MaxPresets
		switch typ.Elem() {
		case reflect.TypeFor[CCGeography]():
			limit = 4
		case reflect.TypeFor[CCCategory]():
			limit = len(ccNativeBands)
		case reflect.TypeFor[bool]():
			limit = len(ccNativeSlots)
		}
		if len(node.Content)/2 > limit {
			return storageAdmissionf("v7 map exceeds %d entries", limit)
		}
		if err := g.state.rawDuplicates(node); err != nil {
			return err
		}
		seen := make(map[string]bool, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			if err := g.validate(node.Content[i], typ.Key(), depth+1); err != nil {
				return err
			}
			key := v6StoredNode(node.Content[i]).Value
			if seen[key] {
				return fmt.Errorf("duplicate interpreted v7 key %q", key)
			}
			seen[key] = true
			if err := g.validate(node.Content[i+1], typ.Elem(), depth+1); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
			return errors.New("v7 list must be a sequence")
		}
		limit := MaxCCModes
		if typ.Elem() == reflect.TypeFor[int]() {
			limit = MaxCCCountries
		}
		if typ.Elem() == reflect.TypeFor[RuleDefinition]() {
			limit = MaxRuleSlots
		}
		if len(node.Content) > limit {
			return storageAdmissionf("v7 list exceeds %d items", limit)
		}
		for _, item := range node.Content {
			if err := g.validate(item, typ.Elem(), depth+1); err != nil {
				return err
			}
		}
	case reflect.String:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
			return errors.New("v7 value must be a string")
		}
	case reflect.Int:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!int" {
			return errors.New("v7 value must be an integer")
		}
	case reflect.Bool:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" {
			return errors.New("v7 value must be a boolean")
		}
	default:
		return fmt.Errorf("unsupported v7 value type %s", typ)
	}
	return nil
}

func requiredV7Fields(typ reflect.Type) []string {
	switch typ {
	case reflect.TypeFor[V7UserRecord]():
		return []string{"configuration_version", "configuration", "presets", "notice"}
	case reflect.TypeFor[V7Configuration]():
		return []string{"filters", "settings"}
	case reflect.TypeFor[V7FilterBanks]():
		return []string{"go", "cc"}
	case reflect.TypeFor[V6FilterConfiguration]():
		return []string{"rules"}
	case reflect.TypeFor[RuleDefinition]():
		return []string{"slot", "action", "expr"}
	case reflect.TypeFor[CCConfiguration]():
		return []string{"modes", "sources", "geography", "dxbm", "include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic"}
	case reflect.TypeFor[CCGeography]():
		return []string{"global", "bands"}
	case reflect.TypeFor[CCCategory]():
		return []string{"action"}
	case reflect.TypeFor[CCDXBM]():
		return []string{"active", "base_allow", "slots"}
	case reflect.TypeFor[V7SavedPreset]():
		return []string{"configuration_version", "dialect", "settings"}
	case reflect.TypeFor[V7PresetReference]():
		return []string{"name", "baseline"}
	case reflect.TypeFor[V7PresetCollection]():
		return []string{"configuration_version", "reset_generation", "presets"}
	case reflect.TypeFor[V7NoticeState]():
		return []string{"profile_reset_generation", "profile_acknowledged_generation", "library_acknowledged_generation"}
	default:
		return nil
	}
}
