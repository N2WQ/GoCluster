// File role: Configuration equality and revision fingerprints without serialization.
package filter

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// Equal compares immutable rules, exact controls and settings.
func (c Configuration) Equal(other Configuration) bool {
	if c.Settings != other.Settings || c.Filters.toggles() != other.Filters.toggles() ||
		(c.Filters.preparedCC == nil) != (other.Filters.preparedCC == nil) {
		return false
	}
	if c.Filters.preparedCC != nil {
		return c.Filters.nativeCC().Equal(other.Filters.nativeCC())
	}
	return c.Filters.preparedRules.Equal(other.Filters.preparedRules)
}

// Fingerprint uses the v6 domain and canonical slot-sorted definitions.
func (c Configuration) Fingerprint() [32]byte {
	if c.Filters.preparedCC != nil {
		h := sha256.New()
		writeFingerprintString(h, "active-cc-configuration-v7")
		for _, text := range []string{c.Settings.Dialect, c.Settings.Grid, c.Settings.NoiseClass, c.Settings.DedupePolicy} {
			writeFingerprintString(h, text)
		}
		writeFingerprintInt(h, c.Settings.PathMinObservationCount)
		writeFingerprintInt(h, c.Settings.SolarSummaryMinutes)
		native := c.Filters.nativeCC().Fingerprint()
		_, _ = h.Write(native[:])
		var result [32]byte
		h.Sum(result[:0])
		return result
	}
	return c.V6().Fingerprint()
}

func writeFingerprintString(h hash.Hash, value string) {
	writeFingerprintInt(h, len(value))
	var chunk [256]byte
	for len(value) > 0 {
		n := copy(chunk[:], value)
		_, _ = h.Write(chunk[:n])
		value = value[n:]
	}
}

func writeFingerprintInt(h hash.Hash, value int) {
	var bytes [8]byte
	binary.LittleEndian.PutUint64(bytes[:], uint64(value))
	_, _ = h.Write(bytes[:])
}
