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
		return fingerprintCWSpeed(result, c.Settings.CWSpeed)
	}
	return fingerprintCWSpeed(c.V6().Fingerprint(), c.Settings.CWSpeed)
}

func fingerprintCWSpeed(base [32]byte, enabled bool) [32]byte {
	if !enabled {
		return base
	}
	var payload [33]byte
	copy(payload[:32], base[:])
	payload[32] = 1
	return sha256.Sum256(payload[:])
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
