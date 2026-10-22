// File role: Validates exact client proposals against the active server contract.
// Validation never normalizes, prunes, substitutes an unavailable selection, or
// mutates either live preferences or the caller's detached configuration.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

// The general locator parser accepts a usable prefix. Command/configuration
// boundaries require the complete token to be an exact supported locator.
func validExactGrid(grid string) bool {
	if len(grid) != 4 && len(grid) != 6 {
		return false
	}
	_, _, valid := pathreliability.GridCenterLatLon(grid)
	return valid
}

func (s *Server) validateMachineSettings(settings filter.SettingsConfiguration) error {
	return s.validateMachineSettingsWithGridRecovery(settings, false)
}

// Recovery waives only mapping availability for an unchanged saved GRID.
// Exact syntax and every other setting's availability remain mandatory.
func (s *Server) validateMachineSettingsWithGridRecovery(settings filter.SettingsConfiguration, allowUnavailableGrid bool) error {
	if settings.Dialect != "" && settings.Dialect != "go" && settings.Dialect != "cc" {
		return fmt.Errorf("settings.dialect must be go, cc or an empty default selection")
	}
	if settings.Grid != "" {
		if settings.Grid != strings.ToUpper(strings.TrimSpace(settings.Grid)) || !validExactGrid(settings.Grid) {
			return fmt.Errorf("settings.grid must be a valid uppercase 4 or 6 character Maidenhead locator or empty for lookup")
		}
		if !allowUnavailableGrid && pathreliability.EncodeCell(settings.Grid) == pathreliability.InvalidCell {
			return fmt.Errorf("settings.grid mapping is unavailable on this server")
		}
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
