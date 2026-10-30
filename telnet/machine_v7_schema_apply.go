// File role: Apply detached presence masks under the authoritative session lease.
package telnet

import "dxcluster/filter"

func (r v7MachineRequest) apply(before filter.V7Configuration, activeDialect string) (filter.V7Configuration, error) {
	full := r.resource == "CONFIG" && r.verb != "PATCH"
	if !full {
		if r.resource == "FILTER" && r.dialect != activeDialect {
			return before, schemaError("configuration.dialect", "must identify the currently active filter bank")
		}
		if r.bankFields&v7BankGO != 0 && activeDialect != "go" || r.bankFields&v7BankCC != 0 && activeDialect != "cc" {
			return before, schemaError("configuration.filters", "ordinary edits cannot supply an inactive bank")
		}
		if r.bankFields != 0 && r.goPatch.settingFields&1 != 0 {
			return before, schemaError("configuration", "ordinary requests cannot combine filters with settings.dialect")
		}
	}
	result := before.Clone()
	goPatch := r.goPatch
	goPatch.resource = "CONFIG"
	goResult, err := goPatch.apply(filter.V6Configuration{Filters: result.Filters.GO, Settings: result.Settings})
	if err != nil {
		return before, err
	}
	result.Filters.GO, result.Settings = goResult.Filters, goResult.Settings
	result.Filters.CC = r.ccPatch.apply(result.Filters.CC)
	return result, nil
}

func (p v7MachineCCPatch) apply(result filter.CCConfiguration) filter.CCConfiguration {
	wanted := p.configuration
	if p.fields&1 != 0 {
		result.Modes = wanted.Modes
	}
	if p.fields&2 != 0 {
		result.Sources = wanted.Sources
	}
	if p.fields&4 != 0 {
		for name, edit := range p.geography {
			category := result.Geography[name]
			if edit.global != nil {
				category.Global = *edit.global
				category.Bands = make(map[string]filter.CCCategory)
			}
			if category.Bands == nil {
				category.Bands = make(map[string]filter.CCCategory)
			}
			for band, row := range edit.bands {
				if row.Action == "INHERIT" {
					delete(category.Bands, band)
				} else {
					category.Bands[band] = row
				}
			}
			result.Geography[name] = category
		}
	}
	if p.fields&8 != 0 {
		result.DXBM = wanted.DXBM
	}
	targets := [6]*filter.DefaultBool{&result.IncludeBeacons, &result.AllowWWV, &result.AllowWCY, &result.AllowAnnounce, &result.AllowSelf, &result.AllowToxic}
	values := [6]filter.DefaultBool{wanted.IncludeBeacons, wanted.AllowWWV, wanted.AllowWCY, wanted.AllowAnnounce, wanted.AllowSelf, wanted.AllowToxic}
	for i, target := range targets {
		if p.fields&(1<<uint(i+4)) != 0 {
			*target = values[i]
		}
	}
	return result
}
