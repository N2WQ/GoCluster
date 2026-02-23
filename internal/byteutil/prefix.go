// Package byteutil provides byte-slice helpers shared across storage packages.
package byteutil

// PrefixUpperBound returns the smallest byte prefix that is strictly greater
// than all keys with the given prefix. Nil means no upper bound exists.
func PrefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xFF {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
