package main

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func TestReappearanceFollowupSelectedFieldTiesAndCombinedCap(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		observations            []reappearanceFixtureRow
		wantSubject, wantWinner float64
	}{
		{"binary-subject", []reappearanceFixtureRow{reappearanceObservation(2, 1001, "s", 6999, "pending"), reappearanceObservation(2, 1001, "S", 7001, "pending"), reappearanceObservation(2, 1001, "W", 7000, "rejected")}, 7001, 7000},
		{"numeric-frequency", []reappearanceFixtureRow{reappearanceObservation(2, 1001, "S", 7001, "pending"), reappearanceObservation(2, 1001, "S", 7000, "pending"), reappearanceObservation(2, 1001, "W", 7000.5, "rejected")}, 7000, 7000.5},
		{"row100-frequency-tie", reappearanceCappedTieObservations(), 7000, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := append([]reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied")}, tc.observations...)
			forward := reappearanceFixtureDB(t, values)
			slices.Reverse(values)
			reverse := reappearanceFixtureDB(t, values)
			var gotCases [2][]reappearanceCase
			var reports [2]string
			for i, path := range []string{forward, reverse} {
				gotCases[i] = reappearanceLoadFixture(t, path, 24)
				if len(gotCases[i]) != 1 || gotCases[i][0].subjectFreqKHz != tc.wantSubject || gotCases[i][0].winnerFreqKHz != tc.wantWinner {
					t.Fatalf("literal duplicate ts/id firstfreq/cap: %+v", gotCases[i])
				}
				var output bytes.Buffer
				if err := reportReappearances(&output, "fixed history", 24, gotCases[i]); err != nil {
					t.Fatal(err)
				}
				reports[i] = output.String()
			}
			if !reflect.DeepEqual(gotCases[0], gotCases[1]) || reports[0] != reports[1] {
				t.Fatalf("reverse insertion changed selected firstfrequencies/report:\n%s\n%s", reports[0], reports[1])
			}
		})
	}
}

func reappearanceCappedTieObservations() []reappearanceFixtureRow {
	var values []reappearanceFixtureRow
	for range 99 {
		values = append(values, reappearanceObservation(2, 1001, "S", 7000, "pending"))
	}
	return append(values, reappearanceObservation(2, 1001, "W", 999, "pending"), reappearanceObservation(2, 1001, "W", 0, "rejected"))
}

func TestReappearanceOuterAllSelectedTiesAreReproducible(t *testing.T) {
	var values []reappearanceFixtureRow
	for _, spec := range []struct {
		subject, winner      string
		freq                 float64
		distance, confidence int
	}{
		{"s", "W", 1, 1, 1}, {"S", "w", 1, 1, 1}, {"S", "W", 20, 1, 60},
		{"S", "W", 10, 2, 60}, {"S", "W", 10, 1, 61}, {"S", "W", 10, 1, 60},
	} {
		value := reappearanceSeed(1, 1000, spec.subject, spec.winner, "applied")
		value.freq, value.distance, value.confidence = spec.freq, spec.distance, spec.confidence
		values = append(values, value)
	}
	values = append(values, reappearanceObservation(2, 1001, "S", 7000, "pending"), reappearanceObservation(3, 1002, "W", 7000.5, "pending"))
	forward := reappearanceFixtureDB(t, values)
	slices.Reverse(values)
	reverse := reappearanceFixtureDB(t, values)
	var reports [2]string
	want := [][3]float64{{10, 1, 60}, {10, 1, 61}, {10, 2, 60}, {20, 1, 60}, {1, 1, 1}, {1, 1, 1}}
	for i, path := range []string{forward, reverse} {
		cases := reappearanceLoadFixture(t, path, 24)
		if len(cases) != 6 {
			t.Fatalf("nonunique ts/id seeds discarded: %+v", cases)
		}
		var selected [][3]float64
		for _, c := range cases {
			selected = append(selected, [3]float64{c.freqKHz, float64(c.distance), float64(c.confidence)})
		}
		if !reflect.DeepEqual(selected, want) {
			t.Fatalf("literal outer selected-field tie order: %v want%v", selected, want)
		}
		var output bytes.Buffer
		if err := reportReappearances(&output, "fixed history", 24, cases); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
	}
	if reports[0] != reports[1] {
		t.Fatalf("outer raw text/numeric ties change report underreverse:\n%s\n%s", reports[0], reports[1])
	}
}

func reappearanceManyCasesFixture() []reappearanceFixtureRow {
	var values []reappearanceFixtureRow
	for i := range 21 {
		subject, winner := fmt.Sprintf("S%02d", i), fmt.Sprintf("W%02d", i)
		values = append(values, reappearanceSeed(1, 1000, subject, winner, "applied"),
			reappearanceObservation(int64(100+i*2), 1001, subject, 7000, "pending"),
			reappearanceObservation(int64(101+i*2), 1002, winner, 7000.5, "pending"))
	}
	return values
}

func TestReappearanceReportSample20CapWithDuplicateSeedKeys(t *testing.T) {
	values := reappearanceManyCasesFixture()
	forward := reappearanceFixtureDB(t, values)
	slices.Reverse(values)
	reverse := reappearanceFixtureDB(t, values)
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		cases := reappearanceLoadFixture(t, path, 24)
		if len(cases) != 21 {
			t.Fatalf("samplecap incorrectly caps analysis: %+v", cases)
		}
		for j, c := range cases {
			if c.subject != fmt.Sprintf("S%02d", j) || c.winner != fmt.Sprintf("W%02d", j) || c.freqSeparationKHz != 0.5 {
				t.Errorf("literal full seed ordering/projection at%d: %+v", j, c)
			}
		}
		var output bytes.Buffer
		if err := reportReappearances(&output, "fixed history", 24, cases); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
		reappearanceRequireText(t, reports[i], "Dual-reappearance cases: 21\n", "First-frequency separation >=0.5 kHz: 21 (100.0% of dual cases)")
		reappearanceRequireSampleCap(t, reports[i])
	}
	if reports[0] != reports[1] {
		t.Fatalf("duplicate-key capped report order changes:\n%s\n%s", reports[0], reports[1])
	}
}
