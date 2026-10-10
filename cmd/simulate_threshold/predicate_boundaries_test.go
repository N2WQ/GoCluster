package main

import "testing"

func TestThresholdInclusiveCutsAndDistance3FixedExtras(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		distance, support, subject, score int
		cuts                              thresholdSettings
		want                              bool
	}{
		{"reports-below", 1, 2, 1, 55, thresholdSettings{3, 1, 55}, false},
		{"all-exact", 1, 3, 2, 55, thresholdSettings{3, 1, 55}, true},
		{"advantage-below", 1, 3, 3, 55, thresholdSettings{3, 1, 55}, false},
		{"score-below", 1, 3, 2, 54, thresholdSettings{3, 1, 55}, false},
		{"all-above", 1, 4, 2, 56, thresholdSettings{3, 1, 55}, true},
		{"distance2-no-extra", 2, 3, 2, 55, thresholdSettings{3, 1, 55}, true},
		{"distance3-exact-no-extra-reports", 3, 3, 1, 60, thresholdSettings{3, 1, 55}, true},
		{"distance3-extra-advantage-below", 3, 3, 2, 60, thresholdSettings{3, 1, 55}, false},
		{"distance3-extra-score-below", 3, 3, 1, 59, thresholdSettings{3, 1, 55}, false},
		{"distance3-report-cut-still-applies", 3, 2, 0, 60, thresholdSettings{3, 1, 55}, false},
		{"negative-ordinary-cuts", 1, 0, 0, 0, thresholdSettings{-1, -2, -5}, true},
		{"negative-distance3-cuts-exact", 3, 0, 0, 0, thresholdSettings{-1, -1, -5}, true},
		{"negative-distance3-advantage-neighbor", 3, 0, 0, 0, thresholdSettings{-1, 0, -5}, false},
		{"negative-distance3-score-neighbor", 3, 0, 0, 0, thresholdSettings{-1, -1, -4}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := decisionRecord{distance: tc.distance, winnerSupport: tc.support,
				subjectSupport: tc.subject, totalReporters: tc.support + tc.subject, winnerConfidence: tc.score}
			if got := checkThresholds(&d, tc.cuts.reports, tc.cuts.advantage, tc.cuts.confidence); got != tc.want {
				t.Fatalf("literal cut boundary input=%+v cuts=%+v got=%v want=%v", d, tc.cuts, got, tc.want)
			}
		})
	}
	if checkThresholds(nil, 3, 1, 55) {
		t.Fatal("nil decision passed")
	}
}

// M is the executing architecture's native maximum. M+1/M+5 are impossible
// mathematical cuts, not wrapped negative values or a new CLI flag domain.
func TestThresholdNativeIntegerExtraBoundaries(t *testing.T) {
	m := int(^uint(0) >> 1)
	minimum := -m - 1
	for _, tc := range []struct {
		name     string
		distance int
		cuts     thresholdSettings
		want     bool
	}{
		{"distance1-all-M-equality", 1, thresholdSettings{m, m, m}, true},
		{"distance2-all-M-equality", 2, thresholdSettings{m, m, m}, true},
		{"distance3-both-extra-exact-M", 3, thresholdSettings{m, m - 1, m - 5}, true},
		{"distance3-advantage-lower-neighbor", 3, thresholdSettings{m, m - 2, m - 5}, true},
		{"distance3-advantage-M-plus1-impossible", 3, thresholdSettings{m, m, m - 5}, false},
		{"distance3-score-lower-neighbor", 3, thresholdSettings{m, m - 1, m - 6}, true},
		{"distance3-score-M-plus1-impossible", 3, thresholdSettings{m, m - 1, m - 4}, false},
		{"distance3-score-M-plus5-impossible", 3, thresholdSettings{m, m - 1, m}, false},
		{"distance3-minimum-negative-cuts", 3, thresholdSettings{minimum, minimum, minimum}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := decisionRecord{distance: tc.distance, winnerSupport: m, subjectSupport: 0,
				totalReporters: m, winnerConfidence: m}
			if got := checkThresholds(&d, tc.cuts.reports, tc.cuts.advantage, tc.cuts.confidence); got != tc.want {
				t.Fatalf("native M=%d input=%+v cuts=%+v got=%v want=%v", m, d, tc.cuts, got, tc.want)
			}
		})
	}
}
