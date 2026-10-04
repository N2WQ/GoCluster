// File role: Commits loaded preferences while retaining this SSID's login metadata.
// Named LOAD stages all runtime changes before calling this atomic disk commit.
package filter

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// SaveUserPreferences atomically replaces only a login callsign's preferences.
// Existing login timestamp and IP history belong to this SSID, never to the preset.
// Ordinary per-SSID autosave behavior remains owned by SaveUserRecord.
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
	record.Filter = set.Filter
	record.Dialect = set.Dialect
	record.DedupePolicy = NormalizeDedupePolicy(set.DedupePolicy)
	record.Grid = set.Grid
	record.NoiseClass = set.NoiseClass
	record.PathMinObservationCount = normalizePathMinObservationCount(set.PathMinObservationCount)
	record.SolarSummaryMinutes = normalizeSolarSummaryMinutes(set.SolarSummaryMinutes)
	record.RecentIPs = MergeRecentIPs(recentIPs, record.RecentIPs)
	bs, err := yaml.Marshal(record)
	if err != nil {
		return err
	}
	return write(userRecordPath(callsign), bs)
}
