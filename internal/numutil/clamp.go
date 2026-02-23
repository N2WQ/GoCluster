// Package numutil provides shared numeric conversion helpers.
package numutil

import "math"

// ClampUint16 converts an int to uint16 with saturation at [0, MaxUint16].
func ClampUint16(value int) uint16 {
	if value <= 0 {
		return 0
	}
	if value > math.MaxUint16 {
		return math.MaxUint16
	}
	return uint16(value)
}
