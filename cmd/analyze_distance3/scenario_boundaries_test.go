package main

import (
	"bytes"
	"testing"
)

func TestDistance3ExtraReportBoundary(t *testing.T) {
	scenario := distance3Scenario{name: "four-report fixture", extraReports: 1, extraAdvantage: 1, extraConf: 5}
	for _, tc := range []struct {
		support int
		added   int
	}{
		{3, 0}, {4, 1}, {5, 1},
	} {
		cases := []distance3Case{
			distance3CaseValue("R", tc.support, 1, 65, "rejected"),
			distance3CaseValue("U", 4, 1, 65, "pending"),
		}
		got := compareDistance3(cases, scenario)
		if got.retained != 0 || got.removed != 0 || got.added != tc.added || got.unknownPassed != 1 {
			t.Errorf("four-report boundary support%d: %+v, want added%d and separate unknown1", tc.support, got, tc.added)
		}
	}
}

func TestDistance3ReportAsymmetricNetSignsExcludeUnknowns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cases []distance3Case
		want  string
	}{
		{"net-plus-one", []distance3Case{
			distance3CaseValue("A_keep", 3, 1, 65, "applied"),
			distance3CaseValue("A_remove", 3, 1, 64, "applied"),
			distance3CaseValue("R_add1", 3, 1, 65, "rejected"),
			distance3CaseValue("R_add2", 3, 1, 65, "rejected"),
			distance3CaseValue("U", 3, 1, 65, "pending"),
		}, "Assumed baseline: extras reports=0, advantage=1, score=5\n  Retained applied: 1; removed applied: 1; added rejected: 2\n  Recognized rows passing: 3/4 (75.0%); net change from applied: +1\n  Unknown rows passing (separate): 1/1\n"},
		{"net-minus-one", []distance3Case{
			distance3CaseValue("A_keep", 3, 1, 65, "applied"),
			distance3CaseValue("A_remove1", 3, 1, 64, "applied"),
			distance3CaseValue("A_remove2", 3, 1, 64, "applied"),
			distance3CaseValue("R_add", 3, 1, 65, "rejected"),
			distance3CaseValue("U", 3, 1, 65, "pending"),
		}, "Assumed baseline: extras reports=0, advantage=1, score=5\n  Retained applied: 1; removed applied: 2; added rejected: 1\n  Recognized rows passing: 2/4 (50.0%); net change from applied: -1\n  Unknown rows passing (separate): 1/1\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := reportDistance3(&out, "fixed fixture", tc.cases); err != nil {
				t.Fatal(err)
			}
			distance3RequireText(t, out.String(), tc.want)
		})
	}
}
