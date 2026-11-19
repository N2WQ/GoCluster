package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func studyAuditTestJSON(t *testing.T, path string, value any) studyFile {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	sum, err := studyHashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return studyFile{Path: path, SHA256: sum}
}

type studyAuditTestFixture struct {
	Plan  studyAuditPlan
	Audit studyFile
	Study studyPlan
	Day   *studyRawDay
}

// Expected populations and fractions are hand-counted. Beacon sightings supply
// support even though they have no terminal decisions; the B receiver cannot.
// Moving ON's first receipt one second removes the sighting at exactly t_start.
func studyAuditTestInputs(t *testing.T) studyAuditTestFixture {
	t.Helper()
	archive := studyTestZIP(t, "N0BBB,14020,20m,K1ABD,10,2026-11-15 00:10:00,CQ,CW\n"+
		"N0BBB,14020,20m,K1ABC,10,2026-11-15 00:10:00,BEACON,CW\n"+
		"N0BBB,14020,20m,K1ABC,10,2026-11-15 00:10:01,NCDXF B,CW\n"+
		"N0BBB,14020,20m,K1ABC/B,10,2026-11-15 00:10:02,CQ,CW\n"+
		"N0AAA,14020,20m,K1ABC,10,2026-11-15 00:10:03,CQ,CW\n"+
		"N0BBB,14020,20m,K2ABD,10,2026-11-15 00:10:04,CQ,CW\n"+
		"N0BBB,14020,20m,K3ABD,10,2026-11-15 00:10:05,CQ,CW\n"+
		"N0BBB,14020,20m,K4ABD,10,2026-11-15 00:10:06,CQ,CW\n"+
		"N0BBB,14020,20m,K1ABC,10,2026-11-15 00:25:00,BEACON,RTTY\n")
	p := studyTestPlan(t)
	p.Inputs = []studyInput{{DateUTC: "2026-11-15", Phase: "holdout", Archive: archive, MaxRows: 16, MaxCSVBytes: 16384}}
	p.ReceiverMetadata = studyAuditTestJSON(t, filepath.Join(t.TempDir(), "receivers.json"), []studyReceiverGroup{})
	planFile := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "study.json"), p)
	day, err := loadStudyRawDay(p.Inputs[0], nil, p.WarmupSeconds)
	if err != nil {
		t.Fatal(err)
	}
	if !day.Rows[0].A || day.Rows[4].A {
		t.Fatal("literal cohort fixture changed")
	}
	paths := studyTestWriteReports(t, p, planFile.SHA256, func(in studyInput, arm studyArm) []studyDecision {
		var decisions []studyDecision
		for _, ordinal := range []int{1, 6, 7, 8} {
			raw := day.Rows[ordinal-1]
			d := studyTestDecision(fmt.Sprintf("%s:%d", archive.SHA256, ordinal), raw.Episode, false, "K9REF")
			d.Ordinal, d.Raw, d.RawSHA256, d.ReceiverFamily = int64(ordinal), raw.Row, hashAnyStable(raw.Row), raw.Family
			d.SubjectIdentity, d.FinalIdentity, d.FinalObserved = raw.Row.DXCall, raw.Row.DXCall, raw.Row.DXCall
			d.DecisionAtUTC = raw.Row.Time
			apply := ordinal == 1 || ordinal == 6 && arm.Name == "repaired_on" || ordinal == 7 && arm.Name == "repaired_off"
			if apply {
				d.Applied, d.IdentityCorrection = true, true
				d.FinalIdentity, d.FinalObserved = strings.Replace(raw.Row.DXCall, "ABD", "ABC", 1), strings.Replace(raw.Row.DXCall, "ABD", "ABC", 1)
				d.PlainDistance, d.ConfiguredDistance = 1, 1
			}
			if ordinal == 1 && arm.Name == "repaired_on" {
				d.DecisionAtUTC = d.DecisionAtUTC.Add(time.Second)
			}
			if ordinal == 8 && arm.Name == "repaired_on" {
				d.Status, d.Applied, d.IdentityCorrection = "suppressed", true, true
				d.FinalIdentity, d.FinalObserved = "K4ABC", "K4ABC"
			}
			decisions = append(decisions, *d)
		}
		// Terminal stream order is not archive order, including a delayed first
		// decision. Every arm deliberately uses a different order.
		if arm.Name == "repaired_on" {
			decisions[0], decisions[3] = decisions[3], decisions[0]
		}
		if arm.Name == "repaired_off" {
			decisions[1], decisions[2] = decisions[2], decisions[1]
		}
		return decisions
	})
	audit := studyAuditPlan{SchemaVersion: 1, Frozen: true, Exposed: true, Stability: replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500},
		Groups: []studyAuditGroup{{Plan: planFile, Phase: "holdout", Candidate: "one"}}}
	for arm, path := range paths {
		var report studyReport
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &report); err != nil {
			t.Fatal(err)
		}
		report.RawRows, report.PendingBound, report.CallCorrectionSHA256 = 9, 1, strings.Repeat("f", 64)
		reportFile := studyAuditTestJSON(t, path, report)
		manifest := replayManifest{DateUTC: "2026-11-15", InputZipPath: archive.Path, LicenseInputs: report.LicenseInputs}
		manifest.CSV.RecordsTotal, manifest.CSV.RecordsProcessed = 9, 8
		manifest.Outputs.ManifestJSON = filepath.Join(filepath.Dir(path), "manifest.json")
		manifest.Results.Stability = replayStabilitySummary{WindowMinutes: 60, MinFollowOn: 2, FreqToleranceHz: 1000, TotalApplied: 2, StableApplied: 1}
		if arm == 0 {
			manifest.Results.Stability.TotalApplied = 1
		}
		manifestFile := studyAuditTestJSON(t, manifest.Outputs.ManifestJSON, manifest)
		audit.Groups[0].Reports = append(audit.Groups[0].Reports, studyAuditReportInput{Report: reportFile, Manifest: manifestFile})
	}
	auditFile := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "audit.json"), audit)
	return studyAuditTestFixture{Plan: audit, Audit: auditFile, Study: p, Day: day}
}

func studyAuditTestReadJSON(t *testing.T, path string, dst any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatal(err)
	}
}

func TestStudyAuditFrozenToyFullUnion(t *testing.T) {
	f := studyAuditTestInputs(t)
	before := make(map[string]string)
	for _, file := range []studyFile{f.Audit, f.Plan.Groups[0].Plan, f.Study.ReceiverMetadata, f.Study.Inputs[0].Archive} {
		before[file.Path] = file.SHA256
	}
	for _, binding := range f.Plan.Groups[0].Reports {
		before[binding.Report.Path], before[binding.Manifest.Path] = binding.Report.SHA256, binding.Manifest.SHA256
		var r studyReport
		studyAuditTestReadJSON(t, binding.Report.Path, &r)
		before[r.Decisions.Path] = r.Decisions.SHA256
	}
	out := filepath.Join(t.TempDir(), "audit")
	if err := runCCV2Audit(f.Audit.Path, f.Audit.SHA256, out); err != nil {
		t.Fatal(err)
	}
	var got studyAuditResult
	studyAuditTestReadJSON(t, filepath.Join(out, "aggregates.json"), &got)
	if !got.Complete || !got.Exposed || len(got.Days) != 1 {
		t.Fatalf("completion: %+v", got)
	}
	d := got.Days[0]
	c := got.Counts
	if d.RawRows != 9 || d.AcceptedARows != 8 || d.FocalRows != 4 || c.RawUnion != 4 || c.Common != 1 || c.CommonRetimed != 1 ||
		c.CommonSame != 0 || c.ONOnly != 1 || c.OFFOnly != 1 || c.Neither != 1 || c.Stable10 != 1 || c.ExactRateComparison != "off_higher" {
		t.Fatalf("hand-counted union changed: day=%+v counts=%+v", d, c)
	}
	if c.Arms != ([3]studyAuditFraction{{Stable: 1, Applied: 1, Defined: true}, {Stable: 1, Applied: 2, Defined: true}, {Stable: 0, Applied: 2, Defined: true}}) {
		t.Fatalf("fractions=%+v", c.Arms)
	}
	if c.ONOnlyFraction != (studyAuditFraction{Stable: 0, Applied: 1, Defined: true}) || c.OFFOnlyFraction != (studyAuditFraction{Stable: 0, Applied: 1, Defined: true}) {
		t.Fatalf("literal ON/OFF-only fractions=%+v/%+v", c.ONOnlyFraction, c.OFFOnlyFraction)
	}
	var complete struct {
		Complete                     bool `json:"complete"`
		Aggregates, Receipts, Paired studyFile
	}
	studyAuditTestReadJSON(t, filepath.Join(out, "COMPLETE.json"), &complete)
	if !complete.Complete {
		t.Fatal("missing final complete authority")
	}
	for _, file := range []studyFile{got.Receipts, got.Paired} {
		if err := studyCheckFile(file); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(got.Paired.Path)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	var ids []string
	for scanner.Scan() {
		var row studyAuditPairRecord
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, row.RawID)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, ordinal := range []int{1, 6, 7, 8} {
		want = append(want, fmt.Sprintf("%s:%d", f.Study.Inputs[0].Archive.SHA256, ordinal))
	}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("position-zipped or omitted IDs: %v", ids)
	}
	for path, sum := range before {
		if err := studyCheckFile(studyFile{Path: path, SHA256: sum}); err != nil {
			t.Fatal("changed frozen input:", err)
		}
	}
	if _, err := os.Stat(filepath.Join(out, "FAILED.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failure marker on success: %v", err)
	}
}
