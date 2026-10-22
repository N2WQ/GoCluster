// File role: Configuration equality and revision fingerprints without serialization.
package filter

import (
	"encoding/binary"
	"hash"
)

// Equal compares immutable rules, exact controls and settings.
func (c Configuration) Equal(other Configuration) bool {
	return c.Settings == other.Settings &&
		c.Filters.NearbyEnabled == other.Filters.NearbyEnabled &&
		c.Filters.toggles() == other.Filters.toggles() &&
		c.Filters.preparedRules.Equal(other.Filters.preparedRules)
}

// Fingerprint uses the v5 domain and canonical slot-sorted definitions.
func (c Configuration) Fingerprint() [32]byte {
	return c.V5().Fingerprint()
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

func writeFingerprintBool(h hash.Hash, value bool) {
	var bytes [1]byte
	if value {
		bytes[0] = 1
	}
	_, _ = h.Write(bytes[:])
}
