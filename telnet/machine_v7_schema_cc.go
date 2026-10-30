// File role: Typed native CC lists, switches and complete DXBM replacement.
package telnet

import (
	"strings"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

var machineV7CCFields = [...]string{
	"modes", "sources", "geography", "dxbm", "include_beacons", "allow_wwv",
	"allow_wcy", "allow_announce", "allow_self", "allow_toxic",
}

var machineV7GeographyNames = [...]string{"DOC", "DOS", "DXCTY", "DXSTATE"}

type v7MachineCCPatch struct {
	configuration filter.CCConfiguration
	fields        uint16
	geography     map[string]v7MachineGeographyPatch
}

type v7MachineGeographyPatch struct {
	global *filter.CCCategory
	bands  map[string]filter.CCCategory
}

func (p *v7MachineCCPatch) decode(node *yaml.Node, path string, complete bool) error {
	fields, err := machineObject(node, path, machineV7CCFields[:], complete)
	if err != nil {
		return err
	}
	c := &p.configuration
	toggles := [6]*filter.DefaultBool{&c.IncludeBeacons, &c.AllowWWV, &c.AllowWCY, &c.AllowAnnounce, &c.AllowSelf, &c.AllowToxic}
	for i, name := range machineV7CCFields {
		field := fields[name]
		if field == nil {
			continue
		}
		fieldPath := path + "." + name
		switch i {
		case 0:
			c.Modes, err = decodeV7StringList(field, fieldPath, filter.MaxCCModes)
		case 1:
			c.Sources, err = decodeV7StringList(field, fieldPath, 2)
		case 2:
			err = p.decodeGeography(field, fieldPath, complete)
		case 3:
			c.DXBM, err = decodeV7DXBM(field, fieldPath)
		default:
			*toggles[i-4], err = machineDefaultBoolean(field, fieldPath)
		}
		if err != nil {
			return err
		}
		p.fields |= 1 << uint(i)
	}
	return nil
}

func decodeV7StringList(node *yaml.Node, path string, limit int) ([]string, error) {
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return nil, schemaError(path, "expected a sequence")
	}
	if len(node.Content) > limit {
		return nil, schemaError(path, "exceeds native collection limit")
	}
	values := make([]string, len(node.Content))
	for i, item := range node.Content {
		value, err := machineString(item, path)
		if err != nil {
			return nil, err
		}
		values[i] = strings.Clone(value)
	}
	return values, nil
}

func decodeV7DXBM(node *yaml.Node, path string) (filter.CCDXBM, error) {
	fields, err := machineObject(node, path, []string{"active", "base_allow", "slots"}, true)
	if err != nil {
		return filter.CCDXBM{}, err
	}
	active, err := machineBoolean(fields["active"], path+".active")
	if err != nil {
		return filter.CCDXBM{}, err
	}
	base, err := machineBoolean(fields["base_allow"], path+".base_allow")
	if err != nil {
		return filter.CCDXBM{}, err
	}
	slots := filter.CCSupportedSlots()
	names := make([]string, len(slots))
	for i := range slots {
		names[i] = slots[i].Name
	}
	entries, err := v7NativeObject(fields["slots"], path+".slots", names, false)
	if err != nil {
		return filter.CCDXBM{}, err
	}
	result := filter.CCDXBM{Active: active, BaseAllow: base, Slots: make(map[string]bool, len(entries))}
	for name, entry := range entries {
		value, err := machineBoolean(entry, path+".slots."+name)
		if err != nil {
			return filter.CCDXBM{}, err
		}
		if !active {
			return filter.CCDXBM{}, schemaError(path+".slots", "inactive DXBM requires empty slots")
		}
		result.Slots[name] = value
	}
	if !active && !base {
		return filter.CCDXBM{}, schemaError(path, "inactive DXBM requires base_allow true and empty slots")
	}
	return result, nil
}

// Native identities are canonicalized before duplicate detection; e.g. DOC/doc
// or MW/mw cannot silently replace one another. Diagnostics use fixed names.
func v7NativeObject(node *yaml.Node, path string, allowed []string, complete bool) (map[string]*yaml.Node, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content)/2 > len(allowed) {
		return nil, schemaError(path, "expected a bounded native mapping")
	}
	result := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key, err := machineString(node.Content[i], path)
		if err != nil {
			return nil, err
		}
		name := strings.ToUpper(key)
		known := false
		for _, candidate := range allowed {
			known = known || name == candidate
		}
		if !known || result[name] != nil {
			return nil, schemaError(path, "unknown or duplicate native identity")
		}
		result[name] = node.Content[i+1]
	}
	if complete && len(result) != len(allowed) {
		return nil, schemaError(path, "all native categories are required")
	}
	return result, nil
}
