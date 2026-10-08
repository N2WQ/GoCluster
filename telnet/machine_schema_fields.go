// File role: Defines versioned YAML fields and fixed presence-mask positions.
// Schema 2 adds bounded state domains; schema 3 adds per-mode SNR minima.
// Older masks stay fixed and supplied collections replace their entire value.
package telnet

import (
	"strconv"
	"strings"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

// These fixed schema lists also define presence-mask positions. Their size is
// independent of clients and requests; no retained lookup registry is needed.
var machineFilterFields = [...]string{
	"bands", "modes", "sources", "events", "confidence", "path_classes", "dx_continents", "de_continents",
	"dx_zones", "de_zones", "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc",
	"dx_callsigns", "block_dx_callsigns", "de_callsigns", "block_de_callsigns",
	"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic", "nearby_enabled",
}

// Append new fields without changing v1 presence-mask positions.
var machineFilterFieldsV2 = [...]string{
	"bands", "modes", "sources", "events", "confidence", "path_classes", "dx_continents", "de_continents",
	"dx_zones", "de_zones", "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc",
	"dx_callsigns", "block_dx_callsigns", "de_callsigns", "block_de_callsigns",
	"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic", "nearby_enabled",
	"dx_states", "de_states",
}

var machineFilterFieldsV3 = [...]string{
	"bands", "modes", "sources", "events", "confidence", "path_classes", "dx_continents", "de_continents",
	"dx_zones", "de_zones", "dx_grid2", "de_grid2", "dx_dxcc", "de_dxcc",
	"dx_callsigns", "block_dx_callsigns", "de_callsigns", "block_de_callsigns",
	"include_beacons", "allow_wwv", "allow_wcy", "allow_announce", "allow_self", "allow_toxic", "nearby_enabled",
	"dx_states", "de_states", "min_snr",
}

var machineSettingFields = [...]string{
	"dialect", "grid", "noise_class", "dedupe_policy", "path_min_observation_count", "solar_summary_minutes",
}

const (
	machineRuleAllowAll uint8 = 1 << iota
	machineRuleBlockAll
	machineRuleAllow
	machineRuleBlock
)

func (r *machineRequest) decodeFilters(node *yaml.Node, path string, complete bool) error {
	names := machineFilterFields[:]
	switch r.SchemaVersion {
	case 2:
		names = machineFilterFieldsV2[:]
	case 3:
		names = machineFilterFieldsV3[:]
	}
	fields, err := machineObject(node, path, names, complete)
	if err != nil {
		return err
	}
	f := &r.Configuration.Filters
	stringsTargets := [14]*filter.StringRules{&f.Bands, &f.Modes, &f.Sources, &f.Events, &f.Confidence, &f.PathClasses, &f.DXContinents, &f.DEContinents, nil, nil, &f.DXGrid2, &f.DEGrid2, nil, nil}
	integerTargets := [14]*filter.IntRules{8: &f.DXZones, 9: &f.DEZones, 12: &f.DXDXCC, 13: &f.DEDXCC}
	patterns := [4]*[]string{&f.DXCallsigns, &f.BlockDXCallsigns, &f.DECallsigns, &f.BlockDECallsigns}
	toggles := [6]*filter.DefaultBool{&f.IncludeBeacons, &f.AllowWWV, &f.AllowWCY, &f.AllowAnnounce, &f.AllowSelf, &f.AllowToxic}
	for i, name := range names {
		field := fields[name]
		if field == nil {
			continue
		}
		fieldPath := path + "." + name
		switch {
		case i < 14:
			if stringsTargets[i] != nil {
				r.ruleFields[i], err = decodeMachineRules(stringsTargets[i], field, fieldPath, complete, machineStringRuleKey)
			} else {
				r.ruleFields[i], err = decodeMachineRules(integerTargets[i], field, fieldPath, complete, machineIntegerRuleKey)
			}
		case i < 18:
			*patterns[i-14], err = machinePatterns(field, fieldPath)
		case i < 24:
			*toggles[i-18], err = machineDefaultBoolean(field, fieldPath)
		case i == 24:
			f.NearbyEnabled, err = machineBoolean(field, fieldPath)
		case i == 25:
			r.ruleFields[14], err = decodeMachineRules(&f.DXStates, field, fieldPath, complete, machineStringRuleKey)
		case i == 26:
			r.ruleFields[15], err = decodeMachineRules(&f.DEStates, field, fieldPath, complete, machineStringRuleKey)
		case i == 27:
			f.MinSNR, err = machineMinSNRMap(field, fieldPath)
		}
		if err != nil {
			return err
		}
		r.filterFields |= 1 << uint(i)
	}
	return nil
}

func (r *machineRequest) decodeSettings(node *yaml.Node, path string, complete bool) error {
	fields, err := machineObject(node, path, machineSettingFields[:], complete)
	if err != nil {
		return err
	}
	s := &r.Configuration.Settings
	stringsTargets := [4]*string{&s.Dialect, &s.Grid, &s.NoiseClass, &s.DedupePolicy}
	integerTargets := [2]*int{&s.PathMinObservationCount, &s.SolarSummaryMinutes}
	for i, name := range machineSettingFields {
		field := fields[name]
		if field == nil {
			continue
		}
		fieldPath := path + "." + name
		if i < 4 {
			*stringsTargets[i], err = machineString(field, fieldPath)
		} else {
			*integerTargets[i-4], err = machineInteger(field, fieldPath, false)
		}
		if err != nil {
			return err
		}
		r.settingFields |= 1 << uint(i)
	}
	return nil
}

func decodeMachineRules[K string | int](rules *filter.RuleSet[K], node *yaml.Node, path string, complete bool, key func(*yaml.Node, string) (K, string, error)) (uint8, error) {
	fields, err := machineObject(node, path, []string{"allow_all", "block_all", "allow", "block"}, complete)
	if err != nil {
		return 0, err
	}
	var presence uint8
	if value := fields["allow_all"]; value != nil {
		rules.AllowAll, err = machineBoolean(value, path+".allow_all")
		presence |= machineRuleAllowAll
	}
	if err == nil {
		if value := fields["block_all"]; value != nil {
			rules.BlockAll, err = machineBoolean(value, path+".block_all")
			presence |= machineRuleBlockAll
		}
	}
	if err == nil {
		if value := fields["allow"]; value != nil {
			rules.Allow, err = machineRuleMap(value, path+".allow", key)
			presence |= machineRuleAllow
		}
	}
	if err == nil {
		if value := fields["block"]; value != nil {
			rules.Block, err = machineRuleMap(value, path+".block", key)
			presence |= machineRuleBlock
		}
	}
	return presence, err
}

func machineRuleMap[K string | int](node *yaml.Node, path string, key func(*yaml.Node, string) (K, string, error)) (map[K]bool, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return nil, schemaError(path, "expected a mapping")
	}
	values := make(map[K]bool, len(node.Content)/2)
	seen := make(map[string]struct{}, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		selected, interpreted, err := key(node.Content[i], path)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[interpreted]; duplicate {
			return nil, schemaError(path, "duplicate rule key after interpretation")
		}
		seen[interpreted] = struct{}{}
		enabled, err := machineBoolean(node.Content[i+1], path)
		if err != nil {
			return nil, err
		}
		values[selected] = enabled
	}
	return values, nil
}

// Check raw entry and key-byte limits before constructing the typed map. Keys
// stay exact; alias normalization could redirect a saved dormant threshold.
func machineMinSNRMap(node *yaml.Node, path string) (map[string]int, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || len(node.Content)%2 != 0 {
		return nil, schemaError(path, "expected a mapping")
	}
	count := len(node.Content) / 2
	if count > filter.MaxMinSNREntries {
		return nil, schemaError(path, "exceeds mode-entry limit")
	}
	remaining := filter.MaxMinSNRKeyBytes
	for i := 0; i < len(node.Content); i += 2 {
		key, err := machineString(node.Content[i], path)
		if err != nil {
			return nil, err
		}
		if len(key) > remaining {
			return nil, schemaError(path, "exceeds aggregate mode-key byte limit")
		}
		remaining -= len(key)
		if !filter.ValidMinSNRModeKey(key) {
			return nil, schemaError(path, "expected an exact uppercase mode key")
		}
	}
	values := make(map[string]int, count)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		if _, duplicate := values[key]; duplicate {
			return nil, schemaError(path, "duplicate mode key")
		}
		value, err := machineInteger(node.Content[i+1], path, false)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, nil
}

func machineStringRuleKey(node *yaml.Node, path string) (string, string, error) {
	value, err := machineString(node, path)
	return value, strings.ToUpper(value), err
}

func machineIntegerRuleKey(node *yaml.Node, path string) (int, string, error) {
	value, err := machineInteger(node, path, true)
	return value, strconv.Itoa(value), err
}

func machinePatterns(node *yaml.Node, path string) ([]string, error) {
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return nil, schemaError(path, "expected a list of strings")
	}
	patterns := make([]string, len(node.Content))
	for i, value := range node.Content {
		var err error
		patterns[i], err = machineString(value, path)
		if err != nil {
			return nil, err
		}
	}
	return patterns, nil
}

func machineDefaultBoolean(node *yaml.Node, path string) (filter.DefaultBool, error) {
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" && node.Value == "DEFAULT" {
		return filter.DefaultBoolDefault, nil
	}
	value, err := machineBoolean(node, path)
	if err != nil {
		return 0, schemaError(path, "expected true, false or DEFAULT")
	}
	if value {
		return filter.DefaultBoolTrue, nil
	}
	return filter.DefaultBoolFalse, nil
}

// apply borrows omitted fields from the caller's snapshot. The caller guards
// borrowed fields through resultant preflight and detachment; this allows an
// oversized existing configuration to be reduced without cloning it first.
// Supplied collections are independent; applying never mutates before or merges
// individual entries into a supplied map or list.
func (r machineRequest) apply(before filter.Configuration) (filter.Configuration, error) {
	if r.resource != "FILTER" && r.resource != "SETTINGS" && r.resource != "CONFIG" {
		return before, schemaError("configuration", "unsupported resource")
	}
	f, proposed := &before.Filters, r.Configuration.Filters
	f.Bands = applyMachineRules(f.Bands, proposed.Bands, r.ruleFields[0])
	f.Modes = applyMachineRules(f.Modes, proposed.Modes, r.ruleFields[1])
	f.Sources = applyMachineRules(f.Sources, proposed.Sources, r.ruleFields[2])
	f.Events = applyMachineRules(f.Events, proposed.Events, r.ruleFields[3])
	f.Confidence = applyMachineRules(f.Confidence, proposed.Confidence, r.ruleFields[4])
	f.PathClasses = applyMachineRules(f.PathClasses, proposed.PathClasses, r.ruleFields[5])
	f.DXContinents = applyMachineRules(f.DXContinents, proposed.DXContinents, r.ruleFields[6])
	f.DEContinents = applyMachineRules(f.DEContinents, proposed.DEContinents, r.ruleFields[7])
	f.DXZones = applyMachineRules(f.DXZones, proposed.DXZones, r.ruleFields[8])
	f.DEZones = applyMachineRules(f.DEZones, proposed.DEZones, r.ruleFields[9])
	f.DXGrid2 = applyMachineRules(f.DXGrid2, proposed.DXGrid2, r.ruleFields[10])
	f.DEGrid2 = applyMachineRules(f.DEGrid2, proposed.DEGrid2, r.ruleFields[11])
	f.DXDXCC = applyMachineRules(f.DXDXCC, proposed.DXDXCC, r.ruleFields[12])
	f.DEDXCC = applyMachineRules(f.DEDXCC, proposed.DEDXCC, r.ruleFields[13])
	f.DXStates = applyMachineRules(f.DXStates, proposed.DXStates, r.ruleFields[14])
	f.DEStates = applyMachineRules(f.DEStates, proposed.DEStates, r.ruleFields[15])
	patterns := [4]*[]string{&f.DXCallsigns, &f.BlockDXCallsigns, &f.DECallsigns, &f.BlockDECallsigns}
	proposedPatterns := [4][]string{proposed.DXCallsigns, proposed.BlockDXCallsigns, proposed.DECallsigns, proposed.BlockDECallsigns}
	for i := range patterns {
		if r.filterFields&(1<<uint(i+14)) != 0 {
			*patterns[i] = proposedPatterns[i]
		}
	}
	toggles := [6]*filter.DefaultBool{&f.IncludeBeacons, &f.AllowWWV, &f.AllowWCY, &f.AllowAnnounce, &f.AllowSelf, &f.AllowToxic}
	proposedToggles := [6]filter.DefaultBool{proposed.IncludeBeacons, proposed.AllowWWV, proposed.AllowWCY, proposed.AllowAnnounce, proposed.AllowSelf, proposed.AllowToxic}
	for i := range toggles {
		if r.filterFields&(1<<uint(i+18)) != 0 {
			*toggles[i] = proposedToggles[i]
		}
	}
	if r.filterFields&(1<<24) != 0 {
		f.NearbyEnabled = proposed.NearbyEnabled
	}
	if r.filterFields&(1<<27) != 0 {
		f.MinSNR = proposed.MinSNR
	}
	s, desired := &before.Settings, r.Configuration.Settings
	settings := [4]*string{&s.Dialect, &s.Grid, &s.NoiseClass, &s.DedupePolicy}
	proposedSettings := [4]string{desired.Dialect, desired.Grid, desired.NoiseClass, desired.DedupePolicy}
	for i := range settings {
		if r.settingFields&(1<<uint(i)) != 0 {
			*settings[i] = proposedSettings[i]
		}
	}
	if r.settingFields&(1<<4) != 0 {
		s.PathMinObservationCount = desired.PathMinObservationCount
	}
	if r.settingFields&(1<<5) != 0 {
		s.SolarSummaryMinutes = desired.SolarSummaryMinutes
	}
	return before, nil
}

func applyMachineRules[K string | int](before, proposed filter.RuleSet[K], presence uint8) filter.RuleSet[K] {
	if presence&machineRuleAllowAll != 0 {
		before.AllowAll = proposed.AllowAll
	}
	if presence&machineRuleBlockAll != 0 {
		before.BlockAll = proposed.BlockAll
	}
	if presence&machineRuleAllow != 0 {
		before.Allow = proposed.Allow
	}
	if presence&machineRuleBlock != 0 {
		before.Block = proposed.Block
	}
	return before
}
