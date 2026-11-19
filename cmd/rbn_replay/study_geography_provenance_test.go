package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCCV2StudyGeographyReportRefusals(t *testing.T) {
	for _, drift := range []string{"old_schema", "missing_scenario", "different_cty", "missing_coverage", "negative_coverage"} {
		t.Run(drift, func(t *testing.T) {
			p := studyTestPlan(t)
			for i := range p.Inputs {
				p.Inputs[i].MaxRows = 100
			}
			paths := studyTestComparisonReports(t, p, "geography-report", false, false)
			data, err := os.ReadFile(paths[2])
			if err != nil {
				t.Fatal(err)
			}
			var report studyReport
			if err := json.Unmarshal(data, &report); err != nil {
				t.Fatal(err)
			}
			switch drift {
			case "old_schema":
				report.SchemaVersion = 1
			case "missing_scenario":
				report.InputScenario = studyInputScenario{}
			case "different_cty":
				report.InputScenario.CTYSHA256 = strings.Repeat("f", 64)
			case "missing_coverage":
				report.InputCoverage = studyInputCoverage{}
			case "negative_coverage":
				report.InputCoverage.Known, report.InputCoverage.UnknownNoMatch = -1, report.FocalRows+1
			}
			if err := writeJSONAtomic(paths[2], report); err != nil {
				t.Fatal(err)
			}
			if _, err := studyReadReports(p, "geography-report", studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout"}, p.Candidates[0]); err == nil {
				t.Fatalf("study accepted %s input scenario/coverage drift", drift)
			}
		})
	}
}

func TestCCV2StudyGeographyPairedCovariates(t *testing.T) {
	for _, drift := range []string{"grid", "prefix", "derived_flag", "lookup_call", "coverage_tallies"} {
		t.Run(drift, func(t *testing.T) {
			p := studyTestPlan(t)
			p.Inputs = p.Inputs[:1]
			paths := studyTestWriteReports(t, p, "geography-pair", func(in studyInput, arm studyArm) []studyDecision {
				d := studyTestDecision(in.Archive.SHA256+":1", "frozen-episode", false, "K1ABC")
				d.Ordinal, d.ReceiverFamily, d.RawSHA256 = 1, "N0BBB", hashAnyStable(d.Raw)
				d.ReceiverLocation = studyReceiverLocation{LookupCall: "N0BBB", CTYPrefix: "N0B", Grid: "FN30", Known: true, Derived: true, Reason: "cty_representative"}
				if arm.Name == "repaired_on" {
					switch drift {
					case "grid":
						d.ReceiverLocation.Grid = "JO50"
					case "prefix":
						d.ReceiverLocation.CTYPrefix = "DL"
					case "derived_flag":
						d.ReceiverLocation.Derived = false
					case "lookup_call":
						d.ReceiverLocation.LookupCall = "N0BBB/DL"
					}
				}
				return []studyDecision{*d}
			})
			reports, err := studyReadReports(p, "geography-pair", studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout"}, p.Candidates[0])
			if err != nil {
				t.Fatal(err)
			}
			group := reports[p.Inputs[0].DateUTC]
			if drift == "coverage_tallies" {
				// The header still reconciles arithmetically, but its known/unknown
				// split disagrees with the actual complete decision stream.
				group[2].InputCoverage = studyInputCoverage{FocalRows: 1, UnknownNoMatch: 1}
			}
			if _, err := studyReadDayPairs(p.Inputs[0], group); err == nil {
				t.Fatalf("paired comparison accepted %s drift", drift)
			}
		})
	}
}

func TestCCV2StudyGeographyBindingValidation(t *testing.T) {
	for _, reason := range []string{"", "asset_load_failed", "unknown_site"} {
		d := studyTestDecision("one", "episode", false, "K1ABC")
		d.ReceiverLocation.Reason = reason
		if studyReceiverLocationBinding(d.ReceiverLocation, d.Raw.Spotter) {
			t.Fatalf("unrecognized or setup failure reason was accepted as row uncertainty: %s", reason)
		}
	}
	p := studyTestPlan(t)
	for i, field := range []string{"cty", "res1", "res2"} {
		t.Run(fmt.Sprintf("missing_%s_manifest_role", field), func(t *testing.T) {
			arm := p.Arms[0]
			arm.Assets = append(append([]studyFile{}, arm.Assets[:i]...), arm.Assets[i+1:]...)
			if err := studyValidateInputScenario(p.InputScenario, []studyArm{arm}); err == nil {
				t.Fatal("missing frozen geography role accepted")
			}
		})
	}
	// Old artifacts remain on disk; validation rejects their schema rather than
	// mutating or silently upgrading them into the new conditional population.
	path := filepath.Join(t.TempDir(), "old-complete.json")
	data := []byte(`{"schema_version":1,"complete":true}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := studyReadReports(p, "old", studyCLIRequest{Compare: path, Phase: "holdout"}, p.Candidates[0]); err == nil {
		t.Fatal("old artifact admitted")
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(data) {
		t.Fatalf("old artifact was rewritten: %v", err)
	}
}
