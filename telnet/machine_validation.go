// File role: Validates exact client proposals against the active server contract.
// Validation never normalizes, prunes, substitutes an unavailable selection, or
// mutates either live preferences or the caller's detached configuration.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func validateNamedRules(name string, rules filter.StringRules, valid func(string) bool) error {
	for _, entries := range []map[string]bool{rules.Allow, rules.Block} {
		for key := range entries {
			if !valid(key) {
				return fmt.Errorf("filters.%s contains an unsupported key", name)
			}
		}
	}
	return nil
}

func validateNumberRules(name string, rules filter.IntRules, valid func(int) bool) error {
	for _, entries := range []map[int]bool{rules.Allow, rules.Block} {
		for key := range entries {
			if !valid(key) {
				return fmt.Errorf("filters.%s contains an unsupported key", name)
			}
		}
	}
	return nil
}

func canonicalUpperChoice(valid func(string) bool) func(string) bool {
	return func(value string) bool {
		return value == strings.ToUpper(strings.TrimSpace(value)) && valid(value)
	}
}

func (s *Server) validateMachineConfiguration(cfg filter.Configuration) error {
	if err := cfg.ValidateStateRules(); err != nil {
		return err
	}
	f := cfg.Filters
	checks := []struct {
		name  string
		rules filter.StringRules
		valid func(string) bool
	}{
		{"bands", f.Bands, func(v string) bool { return v == spot.NormalizeBand(v) && spot.IsValidBand(v) }},
		{"modes", f.Modes, func(v string) bool {
			return v != "" && v == spot.CanonicalModeForFilter(v) && filter.IsSupportedMode(v)
		}},
		{"sources", f.Sources, canonicalUpperChoice(filter.IsSupportedSource)},
		{"events", f.Events, canonicalUpperChoice(filter.IsSupportedEvent)},
		{"confidence", f.Confidence, canonicalUpperChoice(filter.IsSupportedConfidenceSymbol)},
		{"path_classes", f.PathClasses, canonicalUpperChoice(filter.IsSupportedPathClass)},
		{"dx_continents", f.DXContinents, canonicalUpperChoice(filter.IsSupportedContinent)},
		{"de_continents", f.DEContinents, canonicalUpperChoice(filter.IsSupportedContinent)},
		{"dx_grid2", f.DXGrid2, validMachineGrid2},
		{"de_grid2", f.DEGrid2, validMachineGrid2},
	}
	for _, check := range checks {
		if err := validateNamedRules(check.name, check.rules, check.valid); err != nil {
			return err
		}
	}
	for _, check := range []struct {
		name  string
		rules filter.IntRules
		valid func(int) bool
	}{
		{"dx_zones", f.DXZones, filter.IsSupportedZone},
		{"de_zones", f.DEZones, filter.IsSupportedZone},
		// Human DXCC commands accept positive entity codes independently of the
		// locally loaded CTY database. Client writes preserve that same domain.
		{"dx_dxcc", f.DXDXCC, func(v int) bool { return v > 0 }},
		{"de_dxcc", f.DEDXCC, func(v int) bool { return v > 0 }},
	} {
		if err := validateNumberRules(check.name, check.rules, check.valid); err != nil {
			return err
		}
	}
	for _, list := range [][]string{f.DXCallsigns, f.BlockDXCallsigns, f.DECallsigns, f.BlockDECallsigns} {
		for _, pattern := range list {
			if !validMachinePattern(pattern) {
				return fmt.Errorf("filters contains an invalid callsign pattern")
			}
		}
	}
	return s.validateMachineSettings(cfg.Settings)
}

func validMachineGrid2(value string) bool {
	return len(value) == 2 && value[0] >= 'A' && value[0] <= 'R' && value[1] >= 'A' && value[1] <= 'R'
}

func validMachinePattern(value string) bool {
	if value == "" || value != strings.TrimSpace(value) {
		return false
	}
	if strings.Count(value, "*") > 1 || (strings.Contains(value, "*") && !strings.HasPrefix(value, "*") && !strings.HasSuffix(value, "*")) {
		return false
	}
	for i := range len(value) {
		b := value[i]
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '/' || b == '-' || b == '*' {
			continue
		}
		return false
	}
	return true
}

func (s *Server) validateMachineSettings(settings filter.SettingsConfiguration) error {
	if settings.Dialect != "" && settings.Dialect != "go" && settings.Dialect != "cc" {
		return fmt.Errorf("settings.dialect must be go, cc or an empty default selection")
	}
	if settings.Grid != "" && (settings.Grid != strings.ToUpper(strings.TrimSpace(settings.Grid)) || (len(settings.Grid) != 4 && len(settings.Grid) != 6) || pathreliability.EncodeCell(settings.Grid) == pathreliability.InvalidCell) {
		return fmt.Errorf("settings.grid must be a valid uppercase Maidenhead locator or empty for lookup")
	}
	if settings.NoiseClass != "" && (settings.NoiseClass != strings.ToUpper(strings.TrimSpace(settings.NoiseClass)) || !s.noiseClassKnown(settings.NoiseClass)) {
		return fmt.Errorf("settings.noise_class is unavailable on this server")
	}
	if settings.DedupePolicy != "" {
		var available bool
		switch settings.DedupePolicy {
		case "FAST":
			available = s.dedupeFastEnabled
		case "MED":
			available = s.dedupeMedEnabled
		case "SLOW":
			available = s.dedupeSlowEnabled
		default:
			return fmt.Errorf("settings.dedupe_policy must be FAST, MED, SLOW or an empty default selection")
		}
		if !available {
			return fmt.Errorf("settings.dedupe_policy is unavailable on this server")
		}
	}
	if settings.PathMinObservationCount != 0 {
		minimum, enabled := s.pathMinObservationDefault()
		if !enabled || settings.PathMinObservationCount <= minimum || settings.PathMinObservationCount > maxUserPathMinObservationCount {
			return fmt.Errorf("settings.path_min_observation_count must be zero or an available value above the server minimum")
		}
	}
	switch settings.SolarSummaryMinutes {
	case 0, 15, 30, 60:
	default:
		return fmt.Errorf("settings.solar_summary_minutes must be 0, 15, 30 or 60")
	}
	return nil
}
