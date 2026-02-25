package main

import "testing"

func TestFormatConfidenceSingleReporterOnlyUnknown(t *testing.T) {
	tests := []struct {
		name           string
		percent        int
		totalReporters int
		want           string
	}{
		{name: "single reporter unknown", percent: 100, totalReporters: 1, want: "?"},
		{name: "multiple reporters map to probable", percent: 10, totalReporters: 2, want: "P"},
		{name: "multiple reporters map to very likely at majority", percent: 51, totalReporters: 2, want: "V"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := formatConfidence(tc.percent, tc.totalReporters); got != tc.want {
				t.Fatalf("formatConfidence(%d, %d) = %q, want %q", tc.percent, tc.totalReporters, got, tc.want)
			}
		})
	}
}
