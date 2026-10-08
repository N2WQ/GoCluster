// File role: Configuration equality and revision fingerprints without serialization.
package filter

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"maps"
)

func equalRules[K string | int](a, b RuleSet[K]) bool {
	return a.AllowAll == b.AllowAll && a.BlockAll == b.BlockAll && maps.Equal(a.Allow, b.Allow) && maps.Equal(a.Block, b.Block)
}

// Equal compares configured values. Pattern order is immaterial, but duplicates
// remain significant; nil and empty collections carry the same configuration.
func (c Configuration) Equal(other Configuration) bool {
	if c.Settings != other.Settings || c.Filters.NearbyEnabled != other.Filters.NearbyEnabled || c.Filters.toggles() != other.Filters.toggles() {
		return false
	}
	if !maps.Equal(c.Filters.MinSNR, other.Filters.MinSNR) {
		return false
	}
	aStrings, bStrings := c.Filters.stringRules(), other.Filters.stringRules()
	for i := range aStrings {
		if !equalRules(aStrings[i], bStrings[i]) {
			return false
		}
	}
	aInts, bInts := c.Filters.intRules(), other.Filters.intRules()
	for i := range aInts {
		if !equalRules(aInts[i], bInts[i]) {
			return false
		}
	}
	aPatterns, bPatterns := c.Filters.patterns(), other.Filters.patterns()
	for i := range aPatterns {
		if !equalPatterns(aPatterns[i], bPatterns[i]) {
			return false
		}
	}
	return true
}

func equalPatterns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		if counts[value] == 0 {
			return false
		}
		counts[value]--
	}
	return true
}

// Fingerprint is deterministic across map/list order and includes multiplicity.
// Unordered components add per-entry SHA-256 digests into fixed-width lanes,
// then include the count and component boundaries in the final SHA-256 digest.
// It is a configuration-change detector, not a client authentication token.
func (c Configuration) Fingerprint() [32]byte {
	h := sha256.New()
	writeFingerprintString(h, "configuration-v3")
	for _, value := range []string{c.Settings.Dialect, c.Settings.Grid, c.Settings.NoiseClass, c.Settings.DedupePolicy} {
		writeFingerprintString(h, value)
	}
	writeFingerprintInt(h, c.Settings.PathMinObservationCount)
	writeFingerprintInt(h, c.Settings.SolarSummaryMinutes)
	for _, rules := range c.Filters.stringRules() {
		writeFingerprintBool(h, rules.AllowAll)
		writeFingerprintBool(h, rules.BlockAll)
		writeStringRuleFingerprint(h, rules.Allow)
		writeStringRuleFingerprint(h, rules.Block)
	}
	for _, rules := range c.Filters.intRules() {
		writeFingerprintBool(h, rules.AllowAll)
		writeFingerprintBool(h, rules.BlockAll)
		writeIntRuleFingerprint(h, rules.Allow)
		writeIntRuleFingerprint(h, rules.Block)
	}
	writeMinSNRFingerprint(h, c.Filters.MinSNR)
	for _, patterns := range c.Filters.patterns() {
		var sum fingerprintSum
		for _, pattern := range patterns {
			entry := sha256.New()
			writeFingerprintString(entry, pattern)
			sum.add(entry)
		}
		sum.write(h, len(patterns))
	}
	for _, toggle := range c.Filters.toggles() {
		writeFingerprintInt(h, int(toggle))
	}
	writeFingerprintBool(h, c.Filters.NearbyEnabled)
	var result [32]byte
	h.Sum(result[:0])
	return result
}

type fingerprintSum [4]uint64

func (sum *fingerprintSum) add(entry hash.Hash) {
	var digest [32]byte
	entry.Sum(digest[:0])
	for i := range sum {
		sum[i] += binary.LittleEndian.Uint64(digest[i*8:])
	}
}

func (sum fingerprintSum) write(h hash.Hash, count int) {
	writeFingerprintInt(h, count)
	var bytes [32]byte
	for i, value := range sum {
		binary.LittleEndian.PutUint64(bytes[i*8:], value)
	}
	_, _ = h.Write(bytes[:])
}

func writeStringRuleFingerprint(h hash.Hash, entries map[string]bool) {
	var sum fingerprintSum
	for key, value := range entries {
		entry := sha256.New()
		writeFingerprintString(entry, key)
		writeFingerprintBool(entry, value)
		sum.add(entry)
	}
	sum.write(h, len(entries))
}

func writeIntRuleFingerprint(h hash.Hash, entries map[int]bool) {
	var sum fingerprintSum
	for key, value := range entries {
		entry := sha256.New()
		writeFingerprintInt(entry, key)
		writeFingerprintBool(entry, value)
		sum.add(entry)
	}
	sum.write(h, len(entries))
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

func writeMinSNRFingerprint(h hash.Hash, entries map[string]int) {
	var sum fingerprintSum
	for key, value := range entries {
		entry := sha256.New()
		writeFingerprintString(entry, key)
		writeFingerprintInt(entry, value)
		sum.add(entry)
	}
	sum.write(h, len(entries))
}
