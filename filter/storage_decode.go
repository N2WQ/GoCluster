// File role: Operation-owned compatible decoding of persisted user/preset YAML.
// The original typed traversal is accounted before bulk materialization. Its
// yaml.v3 alias guards reset at the old plain Node.Decode boundaries, while the
// document's physical/logical work remains shared and bounded before bulk decode.
package filter

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

type storageDecodeWork struct {
	P uint64
	E uint64
}

type storageDecodeField struct {
	typ   reflect.Type
	index []int
}

// Scratch state belongs to one document/callback. No node, key or type cache
// survives the operation. Field schemas are immutable and raw-duplicate results
// are safe to memoize by node; typed visits and merge skipping stay contextual.
type storageDecodeState struct {
	work                storageDecodeWork
	limits              storageDecodeLimits
	presetCountExceeded bool
	materialize         bool
	duplicates          map[*yaml.Node]error
	fields              map[reflect.Type]map[string]storageDecodeField
}

type storageDecodeGuard struct {
	visits        uint64
	aliases       uint64
	aliasDepth    uint64
	active        map[*yaml.Node]bool
	merged        map[any]bool
	knownFields   bool
	typeErrors    []string
	marker        int
	presetEntries *storagePresetEntries
}

type storageDecodeLimits struct {
	work    uint64
	presets int
}

// One logical collection map borrows this observer across merge frames. It
// never changes traversal or suppresses losing values. Once overflow is known,
// only the first limit+1 typed keys remain; later errors keep their precedence.
type storagePresetEntries struct {
	keys     map[string]bool
	depth    int
	overflow bool
}

func (state *storageDecodeState) observePresetEntry(entries *storagePresetEntries, key string) {
	if entries.overflow || entries.keys[key] {
		return
	}
	entries.keys[key] = true
	if len(entries.keys) > state.limits.presets {
		entries.overflow = true
		state.presetCountExceeded = true
	}
}

type storagePlainUserRecord UserRecord
type storagePlainSavedPreset SavedPreset
type storagePlainPresetReference PresetReference

func checkedStorageAdd(value *uint64, amount uint64) error {
	if amount > math.MaxUint64-*value {
		return errors.New("stored configuration decoding work overflow")
	}
	*value += amount
	return nil
}

func newStorageDecodeState(node *yaml.Node) (*storageDecodeState, error) {
	return newStorageDecodeStateWithLimits(node, storageDecodeLimits{work: maxUserRecordWork})
}

func newStorageDecodeStateWithLimits(node *yaml.Node, limits storageDecodeLimits) (*storageDecodeState, error) {
	state := &storageDecodeState{
		limits:     limits,
		duplicates: make(map[*yaml.Node]error),
		fields:     make(map[reflect.Type]map[string]storageDecodeField),
	}
	// Content is a physical tree. Alias pointers are intentionally not followed,
	// so ignored recursive aliases remain compatible and anchors count once.
	pending := []*yaml.Node{node}
	for len(pending) != 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == nil {
			return nil, errors.New("nil stored YAML node")
		}
		amount := uint64(1)
		if current.Kind == yaml.ScalarNode {
			amount += uint64(len(current.Value))
		}
		if err := checkedStorageAdd(&state.work.P, amount); err != nil {
			return nil, err
		}
		if err := state.admitWork(); err != nil {
			return nil, err
		}
		pending = append(pending, current.Content...)
	}
	return state, nil
}

func (state *storageDecodeState) admitWork() error {
	if state.work.P > math.MaxUint64-state.work.E {
		return errors.New("stored configuration decoding work overflow")
	}
	if state.limits.work != 0 && state.work.P+state.work.E > state.limits.work {
		return storageAdmissionf("stored configuration decoding work exceeds %d units", state.limits.work)
	}
	return nil
}

func newStorageDecodeGuard(knownFields bool) *storageDecodeGuard {
	return &storageDecodeGuard{active: make(map[*yaml.Node]bool), knownFields: knownFields}
}

func (state *storageDecodeState) visit(node *yaml.Node, guard *storageDecodeGuard) error {
	if state.materialize {
		return nil
	}
	amount := uint64(1)
	if node.Kind == yaml.ScalarNode {
		amount += uint64(len(node.Value))
	}
	if err := checkedStorageAdd(&state.work.E, amount); err != nil {
		return err
	}
	if err := state.admitWork(); err != nil {
		return err
	}
	if err := checkedStorageAdd(&guard.visits, 1); err != nil {
		return err
	}
	if guard.aliasDepth != 0 {
		if err := checkedStorageAdd(&guard.aliases, 1); err != nil {
			return err
		}
	}
	// Keep yaml.v3 v3.0.1's separate prefix guard exactly. Scalar byte charges
	// never participate in this ratio, and every old Node.Decode starts it anew.
	allowed := 0.99
	if guard.visits >= 4000000 {
		allowed = 0.10
	} else if guard.visits > 400000 {
		allowed -= 0.89 * (float64(guard.visits-400000) / 3600000)
	}
	if guard.aliases > 100 && guard.visits > 1000 && float64(guard.aliases)/float64(guard.visits) > allowed {
		return errors.New("yaml: document contains excessive aliasing")
	}
	return nil
}

func (state *storageDecodeState) rawDuplicates(node *yaml.Node) error {
	if err, seen := state.duplicates[node]; seen {
		return err
	}
	type identity struct {
		kind  yaml.Kind
		value string
	}
	seen := make(map[identity]*yaml.Node, len(node.Content)/2)
	var err error
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		id := identity{key.Kind, key.Value}
		if previous := seen[id]; previous != nil {
			err = &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: mapping key %#v already defined at line %d", key.Line, key.Value, previous.Line)}}
			break
		}
		seen[id] = key
	}
	state.duplicates[node] = err
	return err
}

func storedMergeKey(node *yaml.Node) bool {
	return node.Kind == yaml.ScalarNode && node.Value == "<<" && (node.Tag == "" || node.Tag == "!" || node.ShortTag() == "!!merge")
}

func (state *storageDecodeState) storedFields(typ reflect.Type) map[string]storageDecodeField {
	if fields := state.fields[typ]; fields != nil {
		return fields
	}
	fields := make(map[string]storageDecodeField)
	var add func(reflect.Type, []int)
	add = func(current reflect.Type, prefix []int) {
		for i := range current.NumField() {
			field := current.Field(i)
			if field.PkgPath != "" {
				continue
			}
			tag := strings.Split(field.Tag.Get("yaml"), ",")
			if tag[0] == "-" {
				continue
			}
			index := append(append([]int(nil), prefix...), i)
			inline := false
			for _, flag := range tag[1:] {
				inline = inline || flag == "inline"
			}
			if inline {
				add(field.Type, index)
				continue
			}
			name := tag[0]
			if name == "" {
				name = strings.ToLower(field.Name)
			}
			fields[name] = storageDecodeField{field.Type, index}
		}
	}
	add(typ, nil)
	state.fields[typ] = fields
	return fields
}

// Owner prechecks borrow the same operation-owned schema as typed decoding.
// Only names/types are reused; each owner and value is validated afresh.
func (state *storageDecodeState) validateStoredMapping(node *yaml.Node, typ reflect.Type) error {
	if node.Kind != yaml.MappingNode {
		return errors.New("stored configuration must be a mapping")
	}
	fields := state.storedFields(typ)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" || fields[key.Value].typ == nil {
			return fmt.Errorf("unknown stored configuration field %q", key.Value)
		}
	}
	return nil
}

func (state *storageDecodeState) validateCurrentStoredValues(node *yaml.Node, typ reflect.Type) error {
	fields := state.storedFields(typ)
	for i := 0; i < len(node.Content); i += 2 {
		field := node.Content[i].Value
		if err := validateStoredValueWithFields(node.Content[i+1], fields[field].typ, state.validateCurrentStoredValues); err != nil {
			return fmt.Errorf("invalid stored field %s: %w", field, err)
		}
	}
	return nil
}

func storedTypeError(guard *storageDecodeGuard, node *yaml.Node, typ reflect.Type) {
	guard.typeErrors = append(guard.typeErrors, fmt.Sprintf("line %d: cannot unmarshal %s into %s", node.Line, node.ShortTag(), typ))
}

func storedDecodeError(guard *storageDecodeGuard, err error) error {
	// Stock callbacks aggregate only a direct TypeError. A wrapped owner error
	// remains terminal, so later fields cannot replace its failure boundary.
	switch typed := err.(type) { //nolint:errorlint // Match yaml.v3's direct TypeError callback boundary; wrapped errors must stay terminal.
	case *yaml.TypeError:
		guard.typeErrors = append(guard.typeErrors, typed.Errors...)
		return nil
	default:
		return err
	}
}

func (state *storageDecodeState) decode(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) (reflect.Value, bool, error) {
	if node == nil {
		return reflect.Value{}, false, errors.New("nil stored YAML node")
	}
	if err := state.visit(node, guard); err != nil {
		return reflect.Value{}, false, err
	}
	if node.Kind == yaml.DocumentNode {
		if len(node.Content) != 1 {
			return reflect.Value{}, false, nil
		}
		return state.decode(node.Content[0], typ, out, guard)
	}
	if node.Kind == yaml.AliasNode {
		if guard.active[node] {
			return reflect.Value{}, false, fmt.Errorf("yaml: anchor '%s' value contains itself", node.Value)
		}
		guard.active[node] = true
		if err := checkedStorageAdd(&guard.aliasDepth, 1); err != nil {
			return reflect.Value{}, false, err
		}
		value, good, err := state.decode(node.Alias, typ, out, guard)
		guard.aliasDepth--
		delete(guard.active, node)
		return value, good, err
	}
	taggedNull := node.ShortTag() == "!!null"
	// Stock prepare skips pointer/custom preparation for every null-tagged
	// node, but Kind still selects scalar resolution, mapping or sequence.
	if !taggedNull {
		for typ.Kind() == reflect.Pointer {
			if out.IsValid() {
				if out.IsNil() {
					out.Set(reflect.New(typ.Elem()))
				}
				out = out.Elem()
			}
			typ = typ.Elem()
		}
		if handled, good, err := state.decodeOwner(node, typ, out, guard); handled {
			return reflect.Value{}, good, err
		}
	}
	switch node.Kind {
	case yaml.ScalarNode:
		value := reflect.New(typ).Elem()
		if err := node.Decode(value.Addr().Interface()); err != nil {
			return reflect.Value{}, false, storedDecodeError(guard, err)
		}
		if taggedNull {
			good := typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Map || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Interface
			if good && out.IsValid() {
				out.SetZero()
			}
			return value, good, nil
		}
		if out.IsValid() {
			out.Set(value)
		}
		return value, true, nil
	case yaml.MappingNode:
		return state.decodeMapping(node, typ, out, guard)
	case yaml.SequenceNode:
		return state.decodeSequence(node, typ, out, guard)
	default:
		return reflect.Value{}, false, fmt.Errorf("yaml: cannot decode node with unknown kind %d", node.Kind)
	}
}

func (state *storageDecodeState) decodeMapping(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) (reflect.Value, bool, error) {
	if err := state.rawDuplicates(node); err != nil {
		return reflect.Value{}, false, storedDecodeError(guard, err)
	}
	if typ.Kind() == reflect.Interface {
		mapType := reflect.TypeFor[map[string]any]()
		for i := 0; i < len(node.Content); i += 2 {
			if tag := node.Content[i].ShortTag(); tag != "!!str" && tag != "!!merge" {
				mapType = reflect.TypeFor[map[any]any]()
				break
			}
		}
		var target reflect.Value
		if state.materialize {
			target = reflect.New(mapType).Elem()
		}
		_, good, err := state.decodeMappingContents(node, mapType, target, guard)
		value := reflect.Zero(mapType)
		if target.IsValid() {
			value = target
		}
		if out.IsValid() && good {
			out.Set(value)
		}
		return value, good, err
	}
	if typ.Kind() != reflect.Map && typ.Kind() != reflect.Struct {
		storedTypeError(guard, node, typ)
		return reflect.Value{}, false, nil
	}
	return state.decodeMappingContents(node, typ, out, guard)
}

func (state *storageDecodeState) decodeMappingContents(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) (reflect.Value, bool, error) {
	if !state.materialize && state.limits.presets > 0 && typ == reflect.TypeFor[map[string]*SavedPreset]() {
		previous := guard.presetEntries
		if previous == nil {
			guard.presetEntries = &storagePresetEntries{keys: make(map[string]bool, state.limits.presets+1)}
		}
		guard.presetEntries.depth++
		defer func() {
			guard.presetEntries.depth--
			guard.presetEntries = previous
		}()
	}
	merged := guard.merged
	guard.merged = nil
	defer func() { guard.merged = merged }()
	var merge *yaml.Node
	if typ.Kind() == reflect.Struct {
		fields := state.storedFields(typ)
		done := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if storedMergeKey(key) {
				merge = node.Content[i+1]
				continue
			}
			name, good, err := state.decode(key, reflect.TypeFor[string](), reflect.Value{}, guard)
			if err != nil {
				return reflect.Value{}, false, err
			}
			if !good || merged != nil && merged[name.String()] {
				continue
			}
			if merged != nil {
				merged[name.String()] = true
			}
			field, known := fields[name.String()]
			if !known {
				if guard.knownFields {
					guard.typeErrors = append(guard.typeErrors, fmt.Sprintf("line %d: field %s not found in type %s", key.Line, name.String(), typ))
				}
				continue // Unknown values are not decoded.
			}
			if done[name.String()] {
				guard.typeErrors = append(guard.typeErrors, fmt.Sprintf("line %d: field %s already set in type %s", key.Line, name.String(), typ))
				continue
			}
			done[name.String()] = true
			if node.ShortTag() == "!!null" && len(field.index) > 1 {
				// yaml.v3 passes an invalid inline field to unmarshal here and
				// panics. Refuse this malformed input without a process panic.
				return reflect.Value{}, false, errors.New("yaml: null-tagged mapping cannot decode an inline stored field")
			}
			var target reflect.Value
			if out.IsValid() {
				target = out.FieldByIndex(field.index)
			}
			value, valid, err := state.decode(node.Content[i+1], field.typ, target, guard)
			if err != nil {
				return reflect.Value{}, false, err
			}
			if valid && name.String() == "configuration_version" {
				guard.marker = int(value.Int())
			}
		}
	} else {
		if err := state.decodeMapEntries(node, typ, out, guard, merged, &merge); err != nil {
			return reflect.Value{}, false, err
		}
	}
	guard.merged = merged
	if merge != nil {
		if err := state.decodeMerge(node, merge, typ, out, guard); err != nil {
			return reflect.Value{}, false, err
		}
	}
	return out, true, nil
}

func (state *storageDecodeState) decodeMapEntries(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard, merged map[any]bool, merge **yaml.Node) error {
	mapIsNew := out.IsValid() && out.IsNil()
	if mapIsNew {
		out.Set(reflect.MakeMap(typ))
	}
	for i := 0; i < len(node.Content); i += 2 {
		if storedMergeKey(node.Content[i]) {
			*merge = node.Content[i+1]
			continue
		}
		key, good, err := state.decode(node.Content[i], typ.Key(), reflect.Value{}, guard)
		if err != nil {
			return err
		}
		if !good {
			continue
		}
		if !key.Comparable() {
			return fmt.Errorf("yaml: invalid map key: %#v", key.Interface())
		}
		identity := key.Interface()
		if merged != nil {
			if merged[identity] {
				continue
			}
			merged[identity] = true
		}
		var value reflect.Value
		if out.IsValid() {
			value = reflect.New(typ.Elem()).Elem()
		}
		_, good, err = state.decode(node.Content[i+1], typ.Elem(), value, guard)
		if err != nil {
			return err
		}
		if entries := guard.presetEntries; entries != nil && typ == reflect.TypeFor[map[string]*SavedPreset]() {
			// The first frame creates a map even when empty; merge frames borrow
			// that existing map. Match insertion of null values without building it.
			if good || node.Content[i+1].ShortTag() == "!!null" && (entries.depth == 1 || !entries.keys[key.String()]) {
				state.observePresetEntry(entries, key.String())
			}
		}
		if out.IsValid() && (good || node.Content[i+1].ShortTag() == "!!null" && (mapIsNew || !out.MapIndex(key).IsValid())) {
			out.SetMapIndex(key, value)
		}
	}
	return nil
}

func (state *storageDecodeState) decodeMerge(parent, merge *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) error {
	previous := guard.merged
	if previous == nil {
		guard.merged = make(map[any]bool)
		for i := 0; i < len(parent.Content); i += 2 {
			key, good, err := state.decode(parent.Content[i], reflect.TypeFor[any](), reflect.Value{}, guard)
			if err != nil {
				return err
			}
			if good {
				if !key.Comparable() {
					return fmt.Errorf("yaml: invalid map key: %#v", key.Interface())
				}
				guard.merged[key.Interface()] = true
			}
		}
	}
	defer func() { guard.merged = previous }()
	sources := []*yaml.Node{merge}
	if merge.Kind == yaml.SequenceNode {
		sources = merge.Content // The merge-sequence container is not unmarshalled.
	}
	for _, source := range sources {
		if source.Kind != yaml.MappingNode && (source.Kind != yaml.AliasNode || source.Alias == nil || source.Alias.Kind != yaml.MappingNode) {
			return errors.New("yaml: map merge requires map or sequence of maps as the value")
		}
		if _, _, err := state.decode(source, typ, out, guard); err != nil {
			return err
		}
	}
	return nil
}

func (state *storageDecodeState) decodeSequence(node *yaml.Node, typ reflect.Type, out reflect.Value, guard *storageDecodeGuard) (reflect.Value, bool, error) {
	if typ.Kind() == reflect.Interface {
		typ = reflect.TypeFor[[]any]()
		var target reflect.Value
		if state.materialize {
			target = reflect.New(typ).Elem()
		}
		_, good, err := state.decodeSequence(node, typ, target, guard)
		value := reflect.Zero(typ)
		if target.IsValid() {
			value = target
		}
		if out.IsValid() && good {
			out.Set(value)
		}
		return value, good, err
	}
	if typ.Kind() != reflect.Slice {
		storedTypeError(guard, node, typ)
		return reflect.Value{}, false, nil
	}
	if out.IsValid() {
		out.Set(reflect.MakeSlice(typ, len(node.Content), len(node.Content)))
	}
	j := 0
	for _, item := range node.Content {
		var value reflect.Value
		if out.IsValid() {
			value = reflect.New(typ.Elem()).Elem()
		}
		_, good, err := state.decode(item, typ.Elem(), value, guard)
		if err != nil {
			return reflect.Value{}, false, err
		}
		if good {
			if out.IsValid() {
				out.Index(j).Set(value)
			}
			j++
		}
	}
	if out.IsValid() {
		out.Set(out.Slice(0, j))
	}
	return out, true, nil
}
