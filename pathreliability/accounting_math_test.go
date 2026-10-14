package pathreliability

import (
	"math"
	"testing"
)

func TestAccountingSafeClockBounds(t *testing.T) {
	for _, c := range []struct{ now, then, want int64 }{
		{70, 0, 70}, {10, 20, 0}, {0, -200, 200},
		{math.MaxInt64, math.MinInt64, math.MaxInt64},
		{0, math.MinInt64, math.MaxInt64}, {-1, math.MinInt64, math.MaxInt64},
	} {
		if got := nonnegativeSecondsSince(c.now, c.then); got != c.want {
			t.Fatalf("difference(%d,%d)=%d, want %d", c.now, c.then, got, c.want)
		}
	}
	for _, c := range []struct {
		value float64
		want  int64
	}{
		{1.1, 2}, {0, 0}, {-1, 0}, {math.NaN(), 0},
		{float64(math.MaxInt64), math.MaxInt64}, {math.Inf(1), math.MaxInt64},
	} {
		if got := ceilNonnegativeInt64(c.value); got != c.want {
			t.Fatalf("ceil(%g)=%d, want %d", c.value, got, c.want)
		}
	}
}

// These bit patterns were derived separately from the implementation using
// binary powers, nearest-even rounding, and gradual underflow.
func TestAgedContributionIndependentPrecisionVectors(t *testing.T) {
	for _, c := range []struct {
		name      string
		age       int64
		weight    float64
		mass      float64
		histogram uint32
		count     uint64
	}{
		{"minimum", 149, 1, 0x1p-149, 0x00000001, 0x36a0000000000000},
		{"below_tie", 150, 1 - 0x1p-53, (1 - 0x1p-53) * 0x1p-150, 0, 0x3690000000000000},
		{"tie", 150, 1, 0x1p-150, 0, 0x3690000000000000},
		{"above_tie", 150, 1 + 0x1p-52, (1 + 0x1p-52) * 0x1p-150, 1, 0x3690000000000000},
		{"weighted_factor_float32_zero", 160, 0x1p100, 0x1p-60, 0x21800000, 0x35f0000000000000},
		{"minimum_count", 1074, 0x1p1000, 0x1p-74, 0x1a800000, 1},
		{"count_tie_zero_mass_lives", 1075, 0x1p1000, 0x1p-75, 0x1a000000, 0},
		{"count_zero_mass_lives", 1076, 0x1p1000, 0x1p-76, 0x19800000, 0},
		{"huge_weight_tie", 1173, 0x1p1023, 0x1p-150, 0, 0},
		{"largest_weight_lives", 1173, math.MaxFloat64, (1 - 0x1p-53) * 0x1p-149, 1, 0},
		{"largest_weight_dies", 1174, math.MaxFloat64, (1 - 0x1p-53) * 0x1p-150, 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			mass, count := agedContribution(c.weight, c.age, 1)
			if mass != c.mass || math.Float32bits(float32(mass)) != c.histogram || math.Float64bits(count) != c.count {
				t.Fatalf("mass=%g bits=%08x countbits=%016x; want mass=%g bits=%08x countbits=%016x", mass, math.Float32bits(float32(mass)), math.Float64bits(count), c.mass, c.histogram, c.count)
			}
		})
	}
}
