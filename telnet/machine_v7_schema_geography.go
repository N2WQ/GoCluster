// File role: Native geography PATCH semantics, including per-band INHERIT.
package telnet

import (
	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func (p *v7MachineCCPatch) decodeGeography(node *yaml.Node, path string, complete bool) error {
	entries, err := v7NativeObject(node, path, machineV7GeographyNames[:], complete)
	if err != nil {
		return err
	}
	p.geography = make(map[string]v7MachineGeographyPatch, len(entries))
	p.configuration.Geography = make(map[string]filter.CCGeography, len(entries))
	for name, node := range entries {
		categoryPath := path + "." + name
		fields, err := machineObject(node, categoryPath, []string{"global", "bands"}, complete)
		if err != nil {
			return err
		}
		if !complete && fields["global"] != nil && fields["bands"] != nil {
			return schemaError(categoryPath, "PATCH cannot combine global and bands for one category")
		}
		edit := v7MachineGeographyPatch{}
		canonical := filter.CCGeography{}
		if global := fields["global"]; global != nil {
			row, err := decodeV7Category(global, categoryPath+".global", name, false)
			if err != nil {
				return err
			}
			edit.global, canonical.Global = &row, row
		}
		if bands := fields["bands"]; bands != nil {
			rows, err := v7NativeObject(bands, categoryPath+".bands", filter.CCSupportedBands(), false)
			if err != nil {
				return err
			}
			edit.bands = make(map[string]filter.CCCategory, len(rows))
			canonical.Bands = make(map[string]filter.CCCategory, len(rows))
			for band, node := range rows {
				row, err := decodeV7Category(node, categoryPath+".bands."+band, name, !complete)
				if err != nil {
					return err
				}
				edit.bands[band], canonical.Bands[band] = row, row
			}
		}
		p.geography[name], p.configuration.Geography[name] = edit, canonical
	}
	return nil
}

func decodeV7Category(node *yaml.Node, path, name string, allowInherit bool) (filter.CCCategory, error) {
	valuesName := "states"
	limit := filter.MaxCCStates
	if name == "DOC" || name == "DXCTY" {
		valuesName, limit = "countries", filter.MaxCCCountries
	}
	fields, err := machineObject(node, path, []string{"action", valuesName}, false)
	if err != nil {
		return filter.CCCategory{}, err
	}
	action, err := requiredMachineString(fields, "action", path+".action")
	if err != nil {
		return filter.CCCategory{}, err
	}
	if action != "off" && action != "pass" && action != "reject" && (!allowInherit || action != "INHERIT") {
		return filter.CCCategory{}, schemaError(path+".action", "expected off, pass, reject; INHERIT is allowed only for a PATCH band row")
	}
	result := filter.CCCategory{Action: filter.CCAction(action)}
	values := fields[valuesName]
	if values == nil {
		return result, nil
	}
	if values.Kind != yaml.SequenceNode || values.Tag != "!!seq" || len(values.Content) > limit {
		return result, schemaError(path+"."+valuesName, "expected a bounded native sequence")
	}
	if (action == "off" || action == "INHERIT") && len(values.Content) != 0 {
		return result, schemaError(path, "off and INHERIT cannot carry nonempty values")
	}
	if valuesName == "states" {
		result.States, err = decodeV7StringList(values, path+".states", limit)
		return result, err
	}
	result.Countries = make([]int, len(values.Content))
	for i, node := range values.Content {
		country, err := machineInteger(node, path+".countries")
		if err != nil {
			return result, err
		}
		if country <= 0 {
			return result, schemaError(path+".countries", "expected positive ADIF integers")
		}
		result.Countries[i] = country
	}
	return result, nil
}
