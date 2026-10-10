package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func TestInspectionUnknownRowsExcludedBeforeSampleLimits(t *testing.T) {
	var values []inspectionFixtureRow
	for i := range 11 {
		values = append(values, inspectionFixtureValue(fmt.Sprintf("A%02d", i), 2, 55, "Applied"),
			inspectionFixtureValue(fmt.Sprintf("D1_R%02d", i), 1, 70, "Rejected"),
			inspectionFixtureValue(fmt.Sprintf("HIGH_R%02d", i), 2, 71, "rejected"))
	}
	for i := range 12 {
		value := inspectionFixtureValue(fmt.Sprintf("UNKNOWN%02d", i), 1, 100, "pending")
		value.support, value.reporters = 100, 101
		values = append(values, value)
	}
	data := inspectionLoadFixture(t, inspectionFixtureDB(t, values))
	if data.total != 45 || data.applied != 11 || data.rejected != 22 || data.unknown != 12 || len(data.appliedSamples) != 10 || len(data.distance1Samples) != 10 || len(data.highScoreSamples) != 10 {
		t.Fatalf("unknown-before-limit populations/caps: %+v", data)
	}
	wantOutcomes := []outcomeCount{
		{value: sql.NullString{String: "pending", Valid: true}, count: 12},
		{value: sql.NullString{String: "Applied", Valid: true}, count: 11},
		{value: sql.NullString{String: "Rejected", Valid: true}, count: 11},
		{value: sql.NullString{String: "rejected", Valid: true}, count: 11},
	}
	if !reflect.DeepEqual(data.outcomes, wantOutcomes) {
		t.Fatalf("breakdown count-descending/raw-binary order: %+v", data.outcomes)
	}
	for _, tc := range []struct {
		prefix  string
		samples []decisionSample
	}{{"A", data.appliedSamples}, {"D1_R", data.distance1Samples}, {"HIGH_R", data.highScoreSamples}} {
		for i, sample := range tc.samples {
			if want := fmt.Sprintf("%s%02d", tc.prefix, i); sample.subject != want {
				t.Errorf("%s cap member%d=%+v want literal%s", tc.prefix, i, sample, want)
			}
		}
	}
}

// Selected leading fields tie across the cap. Raw outcome/reason case must
// break each tie even when the legacy columns inherit NOCASE collation.
func TestInspectionBinarySampleTiesAcrossCapsAreReproducible(t *testing.T) {
	appliedLabels := []string{"APPLied", "APPlied", "APpLied", "APplied", "ApPLied", "ApPlied", "AppLied", "Applied", "aPPLied", "aPPlied", "aPpLied"}
	reasons := []string{"AAAA", "AAAa", "AAaA", "AAaa", "AaAA", "AaAa", "AaaA", "Aaaa", "aAAA", "aAAa", "aAaA"}
	var values []inspectionFixtureRow
	for i := range appliedLabels {
		a := inspectionFixtureValue("APPLIED_TIE", 2, 55, appliedLabels[i])
		d := inspectionFixtureValue("DISTANCE1_TIE", 1, 70, "rejected")
		d.reason = reasons[i]
		h := inspectionFixtureValue("HIGH_TIE", 2, 71, "rejected")
		h.reason = reasons[i]
		values = append(values, a, d, h)
	}
	forward := inspectionFixtureDB(t, values)
	slices.Reverse(values)
	reverse := inspectionFixtureDB(t, values)
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		data := inspectionLoadFixture(t, path)
		if data.total != 33 || data.applied != 11 || data.rejected != 22 || data.unknown != 0 || len(data.outcomes) != 12 || len(data.appliedSamples) != 10 || len(data.distance1Samples) != 10 || len(data.highScoreSamples) != 10 {
			t.Fatalf("binary tie populations/caps: %+v", data)
		}
		var labels, distanceReasons, highReasons []string
		for j := range 10 {
			labels = append(labels, data.appliedSamples[j].decision.String)
			distanceReasons = append(distanceReasons, data.distance1Samples[j].reason.String)
			highReasons = append(highReasons, data.highScoreSamples[j].reason.String)
		}
		if !reflect.DeepEqual(labels, appliedLabels[:10]) || !reflect.DeepEqual(distanceReasons, reasons[:10]) || !reflect.DeepEqual(highReasons, reasons[:10]) {
			t.Fatalf("literal binary capped selection changed: applied%q distance%q high%q", labels, distanceReasons, highReasons)
		}
		var output bytes.Buffer
		if err := reportInspection(&output, "fixed historical source", data); err != nil {
			t.Fatal(err)
		}
		reports[i] = output.String()
	}
	if reports[0] != reports[1] {
		t.Fatalf("NOCASE reverse insertion changed raw breakdown or sample report:\n%s\n%s", reports[0], reports[1])
	}
}
