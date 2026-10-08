// File role: Bounded exact per-mode minimum report thresholds shared by live
// delivery, history, machine configuration and persisted snapshots.
package filter

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"dxcluster/spot"
)

const (
	// MaxMinSNREntries caps active and dormant rules in each owned snapshot.
	MaxMinSNREntries = 128
	// MaxMinSNRKeyBytes caps aggregate stored mode-key bytes before copying.
	MaxMinSNRKeyBytes = 65536
)

// ValidMinSNRModeKey accepts the exact uppercase taxonomy name grammar without
// rebinding stored keys through aliases. Unknown names remain valid dormant keys.
func ValidMinSNRModeKey(raw string) bool {
	if raw == "" {
		return false
	}
	for i := range len(raw) {
		c := raw[i]
		if c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// IsActiveMinSNRMode excludes removed modes and keys now interpreted as aliases.
// An exact canonical mode reactivates its saved threshold when it returns.
func IsActiveMinSNRMode(raw string) bool {
	return ValidMinSNRModeKey(raw) && raw == spot.CanonicalModeForFilter(raw) && IsSupportedMode(raw)
}

// ValidateMinSNRRules must precede detached copying or typed-map construction.
// Both limits include dormant entries; no map proportional to rejected input is
// constructed here. Threshold presence distinguishes disabled from real zero.
func (c Configuration) ValidateMinSNRRules() error {
	entries := c.Filters.MinSNR
	if len(entries) > MaxMinSNREntries {
		return fmt.Errorf("filters.min_snr exceeds %d entries", MaxMinSNREntries)
	}
	remaining := MaxMinSNRKeyBytes
	for key := range entries {
		if len(key) > remaining {
			return fmt.Errorf("filters.min_snr exceeds %d mode-key bytes", MaxMinSNRKeyBytes)
		}
		remaining -= len(key)
		if !ValidMinSNRModeKey(key) {
			return fmt.Errorf("filters.min_snr contains an invalid mode key")
		}
	}
	return nil
}

// ResetMinSNR releases all active and dormant thresholds.
func (f *Filter) ResetMinSNR() {
	f.MinSNR = nil
}

// MinSNRSummary exposes every retained rule and its current activity for support
// readbacks. Invalid or oversized live maps are rejected before allocating lists.
func (f *Filter) MinSNRSummary() string {
	if f == nil || len(f.MinSNR) == 0 {
		return "MINSNR: OFF"
	}
	if err := ConfigurationFromFilter(f, SettingsConfiguration{}).ValidateMinSNRRules(); err != nil {
		return "MINSNR: invalid configuration"
	}
	keys := make([]string, 0, len(f.MinSNR))
	for key := range f.MinSNR {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for i, key := range keys {
		label := key + ">=" + strconv.Itoa(f.MinSNR[key]) + " dB"
		if !IsActiveMinSNRMode(key) {
			label += " (inactive)"
		}
		keys[i] = label
	}
	return "MINSNR: " + strings.Join(keys, ", ")
}
