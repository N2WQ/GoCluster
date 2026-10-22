package filter

import (
	"crypto/sha256"
	"slices"
)

// Equal compares canonical, slot-sorted configurations returned by preparation.
// Nil and empty rule lists are equivalent; cached program identities are ignored.
// Callers must prepare edited or untrusted definitions before comparison.
func (c V5Configuration) Equal(other V5Configuration) bool {
	return c.Settings == other.Settings &&
		c.Filters.NearbyEnabled == other.Filters.NearbyEnabled &&
		c.Filters.toggles() == other.Filters.toggles() &&
		slices.Equal(c.Filters.Rules, other.Filters.Rules)
}

// Fingerprint covers exact canonical values, not program addresses or YAML.
// It has the same prepared-input contract as Equal and a separate v5 domain.
func (c V5Configuration) Fingerprint() [32]byte {
	h := sha256.New()
	writeFingerprintString(h, "configuration-v5")
	for _, value := range []string{c.Settings.Dialect, c.Settings.Grid, c.Settings.NoiseClass, c.Settings.DedupePolicy} {
		writeFingerprintString(h, value)
	}
	writeFingerprintInt(h, c.Settings.PathMinObservationCount)
	writeFingerprintInt(h, c.Settings.SolarSummaryMinutes)
	writeFingerprintInt(h, len(c.Filters.Rules))
	for _, definition := range c.Filters.Rules {
		writeFingerprintInt(h, definition.Slot)
		writeFingerprintString(h, string(definition.Action))
		writeFingerprintString(h, definition.Expr)
	}
	for _, toggle := range c.Filters.toggles() {
		writeFingerprintInt(h, int(toggle))
	}
	writeFingerprintBool(h, c.Filters.NearbyEnabled)
	var result [32]byte
	h.Sum(result[:0])
	return result
}
