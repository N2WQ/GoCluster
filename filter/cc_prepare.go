// File role: Raw-count and canonical-byte admission of native CC definitions;
// callers receive detached criteria and an immutable matching generation.
package filter

import (
	"fmt"
	"slices"
	"strings"

	"dxcluster/spot"
)

// PrepareCCConfiguration first bounds raw collections and scalar bytes, then
// canonicalizes detached definitions. The returned DTO and immutable plan have
// separate slice/map ownership. No caller tree or historical cache is retained.
func PrepareCCConfiguration(input CCConfiguration) (CCConfiguration, *PreparedCCFilter, error) {
	if err := preflightCCConfiguration(input); err != nil {
		return CCConfiguration{}, nil, err
	}
	canonical := input
	var err error
	canonical.Modes, err = prepareCCModes(input.Modes)
	if err != nil {
		return CCConfiguration{}, nil, err
	}
	canonical.Sources, err = prepareCCSources(input.Sources)
	if err != nil {
		return CCConfiguration{}, nil, err
	}
	canonical.Geography, err = prepareCCGeography(input.Geography)
	if err != nil {
		return CCConfiguration{}, nil, err
	}
	canonical.DXBM, err = prepareCCDXBM(input.DXBM)
	if err != nil {
		return CCConfiguration{}, nil, err
	}
	bytes, err := canonicalCCBytes(canonical)
	if err != nil {
		return CCConfiguration{}, nil, err
	}
	if bytes > MaxCCCanonicalBytes {
		return CCConfiguration{}, nil, storageAdmissionf("native CC configuration exceeds %d bytes", MaxCCCanonicalBytes)
	}
	return canonical, buildPreparedCCFilter(canonical.Clone(), bytes), nil
}

func preflightCCConfiguration(c CCConfiguration) error {
	if len(c.Modes) > MaxCCModes || len(c.Sources) > len(SupportedSources) || len(c.Geography) > len(ccGeographyNames) || len(c.DXBM.Slots) > len(ccNativeSlots) {
		return storageAdmissionf("native CC collection exceeds its cardinality limit")
	}
	for _, toggle := range c.toggles() {
		if toggle > DefaultBoolTrue {
			return fmt.Errorf("native CC delivery controls must be DEFAULT, false or true")
		}
	}
	remaining := MaxCCCanonicalBytes
	consume := func(text string) bool {
		if len(text) > remaining {
			return false
		}
		remaining -= len(text)
		return true
	}
	for _, values := range [][]string{c.Modes, c.Sources} {
		for _, value := range values {
			if !consume(value) {
				return storageAdmissionf("native CC scalar bytes exceed configuration limit")
			}
		}
	}
	for name, geography := range c.Geography {
		if len(geography.Bands) > len(ccNativeBands) || !consume(name) {
			return storageAdmissionf("native CC geography exceeds its limits")
		}
		if err := preflightCCCategory(geography.Global, consume); err != nil {
			return err
		}
		for band, category := range geography.Bands {
			if !consume(band) {
				return storageAdmissionf("native CC scalar bytes exceed configuration limit")
			}
			if err := preflightCCCategory(category, consume); err != nil {
				return err
			}
		}
	}
	for name := range c.DXBM.Slots {
		if !consume(name) {
			return storageAdmissionf("native CC scalar bytes exceed configuration limit")
		}
	}
	return nil
}

func preflightCCCategory(c CCCategory, consume func(string) bool) error {
	if len(c.Countries) > MaxCCCountries || len(c.States) > MaxCCStates {
		return storageAdmissionf("native CC category exceeds its value limit")
	}
	if !consume(string(c.Action)) {
		return storageAdmissionf("native CC scalar bytes exceed configuration limit")
	}
	for _, state := range c.States {
		if !consume(state) {
			return storageAdmissionf("native CC scalar bytes exceed configuration limit")
		}
	}
	return nil
}

func prepareCCModes(input []string) ([]string, error) {
	result := make([]string, 0, len(input))
	for _, value := range input {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("native CC mode must not be empty")
		}
		mode := canonicalModeToken(value)
		if !IsSupportedMode(mode) {
			return nil, fmt.Errorf("unsupported native CC mode %q", value)
		}
		result = append(result, strings.Clone(mode))
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func prepareCCSources(input []string) ([]string, error) {
	result := make([]string, 0, len(input))
	for _, value := range input {
		switch strings.ToUpper(strings.TrimSpace(value)) {
		case "HUMAN":
			result = append(result, "HUMAN")
		case "SKIMMER":
			result = append(result, "SKIMMER")
		default:
			return nil, fmt.Errorf("unsupported native CC source %q", value)
		}
	}
	slices.Sort(result)
	return slices.Compact(result), nil
}

func prepareCCGeography(input map[string]CCGeography) (map[string]CCGeography, error) {
	result := make(map[string]CCGeography, len(ccGeographyNames))
	for _, name := range ccGeographyNames {
		result[name] = CCGeography{Global: CCCategory{Action: CCOff}, Bands: make(map[string]CCCategory)}
	}
	seen := make(map[string]bool, len(input))
	for rawName, geography := range input {
		name := strings.ToUpper(rawName)
		if ccGeographyIndex(name) < 0 || seen[name] {
			return nil, fmt.Errorf("unknown or duplicate native CC geography %q", rawName)
		}
		seen[name] = true
		global, err := prepareCCCategory(name, geography.Global)
		if err != nil {
			return nil, err
		}
		entry := CCGeography{Global: global, Bands: make(map[string]CCCategory, len(geography.Bands))}
		for rawBand, category := range geography.Bands {
			band := strings.ToUpper(rawBand)
			if ccBandIndex(band) < 0 {
				return nil, fmt.Errorf("unsupported native CC band %q", rawBand)
			}
			if _, duplicate := entry.Bands[band]; duplicate {
				return nil, fmt.Errorf("duplicate native CC band %q", rawBand)
			}
			value, err := prepareCCCategory(name, category)
			if err != nil {
				return nil, err
			}
			entry.Bands[band] = value
		}
		result[name] = entry
	}
	return result, nil
}

func ccGeographyIndex(name string) int {
	for index, candidate := range ccGeographyNames {
		if name == candidate {
			return index
		}
	}
	return -1
}

func prepareCCCategory(name string, input CCCategory) (CCCategory, error) {
	action := CCAction(strings.ToLower(string(input.Action)))
	if action != CCOff && action != CCPass && action != CCReject {
		return CCCategory{}, fmt.Errorf("native CC %s action must be off, pass or reject", name)
	}
	country := name == "DOC" || name == "DXCTY"
	if country && len(input.States) != 0 || !country && len(input.Countries) != 0 || action == CCOff && (len(input.Countries) != 0 || len(input.States) != 0) {
		return CCCategory{}, fmt.Errorf("native CC %s has values incompatible with its category/action", name)
	}
	result := CCCategory{Action: action}
	if country {
		result.Countries = slices.Clone(input.Countries)
		for _, value := range result.Countries {
			if value <= 0 {
				return CCCategory{}, fmt.Errorf("native CC country must be a positive ADIF integer")
			}
		}
		slices.Sort(result.Countries)
		result.Countries = slices.Compact(result.Countries)
	} else {
		result.States = make([]string, 0, len(input.States))
		for _, value := range input.States {
			state := strings.ToUpper(value)
			if !spot.IsState(state) {
				return CCCategory{}, fmt.Errorf("unsupported native CC state %q", value)
			}
			result.States = append(result.States, strings.Clone(state))
		}
		slices.Sort(result.States)
		result.States = slices.Compact(result.States)
	}
	return result, nil
}

func prepareCCDXBM(input CCDXBM) (CCDXBM, error) {
	result := CCDXBM{Active: input.Active, BaseAllow: input.BaseAllow, Slots: make(map[string]bool, len(input.Slots))}
	seen := make(map[string]bool, len(input.Slots))
	for rawName, enabled := range input.Slots {
		name := strings.ToUpper(rawName)
		if ccSlotIndex(name) < 0 || seen[name] {
			return CCDXBM{}, fmt.Errorf("unknown or duplicate native CC slot %q", rawName)
		}
		seen[name] = true
		if input.Active && enabled != input.BaseAllow {
			result.Slots[name] = enabled
		}
	}
	if !input.Active {
		result.BaseAllow = true
	}
	return result, nil
}

// DefaultCCConfiguration uses only the trusted operator-default factory, never
// arbitrary GO rules. Explicit empty native selectors do not call this factory.
func DefaultCCConfiguration(modes, sources []string) (CCConfiguration, error) {
	defaults, err := PrepareDefaultRuleSet(modes, sources)
	if err != nil {
		return CCConfiguration{}, err
	}
	return ccConfigurationFromDefaultRules(defaults)
}

// CurrentDefaultCCConfiguration derives a detached native selection from the
// one configured default owner. It introduces no second mutable default store.
func CurrentDefaultCCConfiguration() (CCConfiguration, error) {
	return ccConfigurationFromDefaultRules(newFilterDefaultRules())
}

func ccConfigurationFromDefaultRules(defaults *PreparedRuleSet) (CCConfiguration, error) {
	var config CCConfiguration
	for index := range defaults.rules[0].program.nodes {
		node := &defaults.rules[0].program.nodes[index]
		switch node.predicate.field {
		case ruleFieldMode:
			config.Modes = append(config.Modes, node.predicate.text...)
		case ruleFieldSource:
			config.Sources = append(config.Sources, node.predicate.text...)
		}
	}
	canonical, _, err := PrepareCCConfiguration(config)
	return canonical, err
}
