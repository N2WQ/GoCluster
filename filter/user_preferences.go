// File role: Commits loaded preferences while retaining this SSID's login metadata.
// Named LOAD stages all runtime changes before calling this atomic disk commit.
package filter

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"dxcluster/spot"
)

// SaveUserPreferences atomically replaces only a login callsign's preferences.
// Existing login timestamp and IP history belong to this SSID, never to the preset.
// Its applied preset association remains unchanged.
func SaveUserPreferences(callsign string, set *SavedPreset, recentIPs []string) error {
	return saveUserPreferences(callsign, set, recentIPs, writeAtomicUserFile)
}

func saveUserPreferences(callsign string, set *SavedPreset, recentIPs []string, write func(string, []byte) error) error {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if !spot.IsValidNormalizedCallsign(callsign) {
		return errors.New("invalid login callsign")
	}
	if set == nil {
		return errors.New("nil saved preset")
	}
	record, err := LoadUserRecord(callsign)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load current user record: %w", err)
	}
	if record == nil {
		record = &UserRecord{}
	}
	applyRecordConfiguration(record, ConfigurationFromPreset(set))
	record.RecentIPs = MergeRecentIPs(recentIPs, record.RecentIPs)
	return saveUserRecord(callsign, record, write)
}

// SaveConfiguration atomically commits exact preferences and the explicit
// applied-preset reference. A nil reference clears the association. Transaction
// ownership, schema/availability validation and detached input lifetime are the
// telnet caller's responsibility.
func SaveConfiguration(callsign string, cfg Configuration, ref *PresetReference, recentIPs []string) error {
	return saveConfiguration(callsign, cfg, ref, recentIPs, writeAtomicUserFile)
}

func saveConfiguration(callsign string, cfg Configuration, ref *PresetReference, recentIPs []string, write func(string, []byte) error) error {
	callsign = strings.ToUpper(strings.TrimSpace(callsign))
	if !spot.IsValidNormalizedCallsign(callsign) {
		return errors.New("invalid login callsign")
	}
	if err := cfg.ValidateStateRules(); err != nil {
		return err
	}
	for _, toggle := range cfg.Filters.toggles() {
		if toggle > DefaultBoolTrue {
			return errors.New("invalid default boolean selection")
		}
	}
	record, err := LoadUserRecord(callsign)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("load current user record: %w", err)
	}
	if record == nil {
		record = &UserRecord{}
	}
	record.Preset, err = ref.Clone()
	if err != nil {
		return err
	}
	applyRecordConfiguration(record, cfg)
	record.RecentIPs = MergeRecentIPs(recentIPs, record.RecentIPs)
	return saveUserRecord(callsign, record, write)
}

func applyRecordConfiguration(record *UserRecord, cfg Configuration) {
	record.ConfigurationVersion = CurrentConfigurationVersion
	record.Filter = cfg.FilterValue()
	record.Dialect = cfg.Settings.Dialect
	record.Grid = cfg.Settings.Grid
	record.NoiseClass = cfg.Settings.NoiseClass
	record.DedupePolicy = cfg.Settings.DedupePolicy
	record.PathMinObservationCount = cfg.Settings.PathMinObservationCount
	record.SolarSummaryMinutes = cfg.Settings.SolarSummaryMinutes
}
