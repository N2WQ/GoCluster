package main

import (
	"math"
	"slices"
	"testing"
)

func TestConfidenceSummaryAnalyticalOddEvenAndUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name         string
		scores       []int
		mean, median float64
		available    bool
	}{
		{"nil", nil, 0, 0, false},
		{"empty", []int{}, 0, 0, false},
		{"observed-zero", []int{0}, 0, 0, true},
		{"odd-unordered", []int{90, 10, 20}, 40, 20, true},
		{"even-half", []int{1, 0}, 0.5, 0.5, true},
		{"even-distinct-middle", []int{9, 1, 4, 2}, 4, 3, true},
		{"fractional-odd", []int{4, 1, 2}, 7.0 / 3.0, 2, true},
		{"negative-half", []int{-1, 0}, -0.5, -0.5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mean, median, available := confidenceSummary(append([]int(nil), tc.scores...))
			if available != tc.available || (available && (mean != tc.mean || median != tc.median)) {
				t.Fatalf("scores%v: mean%v median%v available%v want%v/%v/%v", tc.scores, mean, median, available, tc.mean, tc.median, tc.available)
			}
		})
	}
}

// Exact arithmetic precedes final float64 presentation. Same-sign native
// endpoints intentionally expect that presentation, not exact decimal integers.
// Opposite endpoints sum to -1 and must preserve the representable -0.5 result.
func TestConfidenceSummaryNativeExtremaAndPermutation(t *testing.T) {
	m := int(^uint(0) >> 1)
	minimum := -m - 1
	for _, tc := range []struct {
		name         string
		scores       []int
		mean, median float64
	}{
		{"positive-pair", []int{m, m}, float64(m), float64(m)},
		{"negative-pair", []int{minimum, minimum}, float64(minimum), float64(minimum)},
		{"opposite-pair", []int{m, minimum}, -0.5, -0.5},
		{"opposite-and-small", []int{m, 2, minimum}, 1.0 / 3.0, 2},
		{"opposite-even-middle", []int{m, 1, 0, minimum}, 0, 0.5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for range 2 {
				mean, median, available := confidenceSummary(append([]int(nil), tc.scores...))
				if !available || math.IsNaN(mean) || math.IsInf(mean, 0) || math.IsNaN(median) || math.IsInf(median, 0) || mean != tc.mean || median != tc.median {
					t.Fatalf("native M=%d scores%v mean%v median%v available%v want%v/%v", m, tc.scores, mean, median, available, tc.mean, tc.median)
				}
				slices.Reverse(tc.scores)
			}
		})
	}
}
