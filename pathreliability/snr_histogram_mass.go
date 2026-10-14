// File role: Owns bounded exact scratch arithmetic for the fixed SNR median.
// Crawler notes: Mass is measured in binary32's smallest positive quantum;
// these stack-local words never become retained bucket state or shared scratch.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0131-path-p50-midpoint-and-even-split-semantics.md.
// Related tests: snr_histogram_exact_test.go, snr_histogram_mass_test.go.
package pathreliability

import (
	"math"
	"math/bits"
)

// Every finite nonnegative binary32 mass is an integer multiple of 2^-149.
// A bin needs at most 277 bits, a 50-bin total 283, and twice a prefix 284.
// Five little-endian words provide 320 bits without heap or retained storage.
// The bound depends on the fixed histogram geometry and validated inputs.
type snrHistogramMass [5]uint64

// add requires a validated finite nonnegative binary32 value. At most 50
// values enter either accumulator, so carry cannot escape the fifth word.
func (mass *snrHistogramMass) add(weight float32) {
	encoding := math.Float32bits(weight)
	fraction := uint64(encoding & 0x7fffff)
	exponent := (encoding >> 23) & 0xff
	var shift uint32
	if exponent != 0 {
		fraction |= 1 << 23
		shift = exponent - 1
	}
	word, offset := shift/64, shift%64
	low, high := fraction<<offset, fraction>>(64-offset)
	var carry uint64
	mass[word], carry = bits.Add64(mass[word], low, 0)
	mass[word+1], carry = bits.Add64(mass[word+1], high, carry)
	for i := word + 2; carry != 0 && i < uint32(len(mass)); i++ {
		mass[i], carry = bits.Add64(mass[i], 0, carry)
	}
}

// compareDouble compares twice the exact cumulative mass with the exact total.
// Shift carries come from adjacent lower words. Never halve an odd total or
// convert these sums to floating point: either would discard decisive quanta.
func (mass *snrHistogramMass) compareDouble(total *snrHistogramMass) int {
	for i := len(mass) - 1; i >= 0; i-- {
		doubled := mass[i] << 1
		if i > 0 {
			doubled |= mass[i-1] >> 63
		}
		if doubled < total[i] {
			return -1
		}
		if doubled > total[i] {
			return 1
		}
	}
	return 0
}
