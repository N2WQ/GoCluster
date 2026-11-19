package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/spot"
)

func studyAuditTestRefreeze(t *testing.T, f *studyAuditTestFixture) {
	t.Helper()
	f.Audit = studyAuditTestJSON(t, f.Audit.Path, f.Plan)
}

func studyAuditTestChangeReport(t *testing.T, f *studyAuditTestFixture, arm int, change func(*studyReport)) {
	t.Helper()
	binding := &f.Plan.Groups[0].Reports[arm]
	var report studyReport
	studyAuditTestReadJSON(t, binding.Report.Path, &report)
	change(&report)
	binding.Report = studyAuditTestJSON(t, binding.Report.Path, report)
}

func studyAuditTestChangeDecisions(t *testing.T, f *studyAuditTestFixture, arm int, change func([]studyDecision) []studyDecision) {
	t.Helper()
	studyAuditTestChangeReport(t, f, arm, func(report *studyReport) {
		data, err := os.ReadFile(report.Decisions.Path)
		if err != nil {
			t.Fatal(err)
		}
		var rows []studyDecision
		for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
			var row studyDecision
			if err := json.Unmarshal(line, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
		rows = change(rows)
		var b bytes.Buffer
		encoder := json.NewEncoder(&b)
		for _, row := range rows {
			if err := encoder.Encode(row); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(report.Decisions.Path, b.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
		sum, err := studyHashFile(report.Decisions.Path)
		if err != nil {
			t.Fatal(err)
		}
		report.Decisions.SHA256 = sum
	})
}

func studyAuditTestRefused(t *testing.T, f studyAuditTestFixture) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "refused")
	if err := runCCV2Audit(f.Audit.Path, f.Audit.SHA256, out); err == nil {
		t.Fatal("invalid frozen inputs accepted")
	}
	if _, err := os.Stat(filepath.Join(out, "COMPLETE.json")); !os.IsNotExist(err) {
		t.Fatalf("failure published COMPLETE: %v", err)
	}
}

func TestStudyAuditProvenanceRefusals(t *testing.T) {
	for _, variant := range []string{"plan_sha", "report_sha", "manifest_sha", "decision_sha", "archive_sha", "receiver_sha", "mask", "candidate", "source", "executable", "asset_roles", "config", "pipeline", "raw_count", "manifest_count", "manifest_path", "legacy_count"} {
		t.Run(variant, func(t *testing.T) {
			f := studyAuditTestInputs(t)
			group := &f.Plan.Groups[0]
			switch variant {
			case "plan_sha":
				group.Plan.SHA256 = strings.Repeat("0", 64)
			case "report_sha":
				group.Reports[0].Report.SHA256 = strings.Repeat("0", 64)
			case "manifest_sha":
				group.Reports[0].Manifest.SHA256 = strings.Repeat("0", 64)
			case "decision_sha":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.Decisions.SHA256 = strings.Repeat("0", 64) })
			case "archive_sha":
				if err := os.WriteFile(f.Study.Inputs[0].Archive.Path, []byte("changed"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "receiver_sha":
				if err := os.WriteFile(f.Study.ReceiverMetadata.Path, []byte("changed"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "mask":
				studyAuditTestChangeReport(t, &f, 2, func(r *studyReport) { r.MaskSchemaPresent = false })
			case "candidate":
				studyAuditTestChangeReport(t, &f, 2, func(r *studyReport) { r.CandidateSHA256 = strings.Repeat("0", 64) })
			case "source":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.SourceRevision = strings.Repeat("0", 40) })
			case "executable":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.ExecutableSHA256 = strings.Repeat("0", 64) })
			case "asset_roles":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.AssetsSHA256 = strings.Repeat("0", 64) })
			case "config":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.ConfigFilesSHA256 = strings.Repeat("0", 64) })
			case "pipeline":
				studyAuditTestChangeReport(t, &f, 2, func(r *studyReport) { r.PipelineWithoutBayesSHA256 = strings.Repeat("0", 64) })
			case "raw_count":
				studyAuditTestChangeReport(t, &f, 0, func(r *studyReport) { r.RawRows = 8 })
			default:
				binding := &group.Reports[0]
				var manifest replayManifest
				studyAuditTestReadJSON(t, binding.Manifest.Path, &manifest)
				switch variant {
				case "manifest_count":
					manifest.CSV.RecordsProcessed = 7
				case "manifest_path":
					manifest.InputZipPath = "other.zip"
				case "legacy_count":
					manifest.Results.Stability.StableApplied = 0
				}
				binding.Manifest = studyAuditTestJSON(t, binding.Manifest.Path, manifest)
			}
			studyAuditTestRefreeze(t, &f)
			studyAuditTestRefused(t, f)
		})
	}
}

func TestStudyAuditArchiveBindingRejectsSelfConsistentForgery(t *testing.T) {
	if !studyCohortA("N0CCC") {
		t.Fatal("independent SHA256 family fixture must remain cohort A")
	}
	for _, variant := range []string{"raw", "family", "episode", "ordinal", "nonfocal", "cutoff_clock"} {
		t.Run(variant, func(t *testing.T) {
			f := studyAuditTestInputs(t)
			for arm := 0; arm < 3; arm++ {
				studyAuditTestChangeDecisions(t, &f, arm, func(rows []studyDecision) []studyDecision {
					for i := range rows {
						d := &rows[i]
						if d.Ordinal != 1 {
							continue
						}
						switch variant {
						case "raw":
							d.Raw.DXCall = "K8ZZZ"
						case "family":
							d.ReceiverFamily = "N0CCC"
						case "episode":
							d.EpisodeID = "forged-episode"
						case "ordinal":
							d.Ordinal = 2
						case "nonfocal":
							d.Ordinal = 2
							d.Raw = f.Day.Rows[1].Row
							d.EpisodeID = "forged-beacon"
						case "cutoff_clock":
							d.DecisionAfterCutoff = true
						}
						d.RawID = fmt.Sprintf("%s:%d", f.Study.Inputs[0].Archive.SHA256, d.Ordinal)
						d.RawSHA256 = hashAnyStable(d.Raw)
						d.SubjectIdentity = spot.CorrectionVoteKey(d.Raw.DXCall)
						d.IdentityCorrection = d.Applied && d.SubjectIdentity != d.FinalIdentity
					}
					return rows
				})
			}
			studyAuditTestRefreeze(t, &f)
			studyAuditTestRefused(t, f)
		})
	}
}

func TestStudyAuditTerminalUnionRejectsDuplicateMissingAndExtra(t *testing.T) {
	for _, variant := range []string{"duplicate", "missing", "extra", "missing_arm", "duplicate_arm"} {
		t.Run(variant, func(t *testing.T) {
			f := studyAuditTestInputs(t)
			switch variant {
			case "missing_arm":
				f.Plan.Groups[0].Reports = f.Plan.Groups[0].Reports[:2]
			case "duplicate_arm":
				f.Plan.Groups[0].Reports[2] = f.Plan.Groups[0].Reports[1]
			default:
				studyAuditTestChangeDecisions(t, &f, 2, func(rows []studyDecision) []studyDecision {
					if variant == "duplicate" {
						rows[1] = rows[0]
						return rows
					}
					if variant == "missing" {
						return rows[:len(rows)-1]
					}
					return append(rows, rows[0])
				})
			}
			studyAuditTestRefreeze(t, &f)
			studyAuditTestRefused(t, f)
		})
	}
}

func TestStudyAuditPlanStrictAndNewOutput(t *testing.T) {
	f := studyAuditTestInputs(t)
	data, err := os.ReadFile(f.Audit.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{[]byte(`null`), []byte(`{"schema_version":1,"schema_version":1}`), []byte(`{"schema_version":1,"unknown":true}`),
		bytes.Replace(data, []byte(`"groups":[`), []byte(`"groups":null,"unused":[`), 1), append(bytes.Repeat([]byte(" "), studyMaxJSONBytes), data...)} {
		path := filepath.Join(t.TempDir(), "invalid.json")
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		sum, err := studyHashFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadStudyAuditPlan(path, sum); err == nil {
			t.Fatal("malformed/duplicate/null/unknown/oversize plan accepted")
		}
	}
	for _, change := range []func(*studyAuditPlan){func(p *studyAuditPlan) { p.Exposed = false }, func(p *studyAuditPlan) { p.Frozen = false },
		func(p *studyAuditPlan) { p.Stability = replayStabilityConfig{} }, func(p *studyAuditPlan) { p.Groups = nil },
		func(p *studyAuditPlan) { p.Groups = append(p.Groups, p.Groups[0]) }, func(p *studyAuditPlan) { p.Groups[0].Plan.Path = "https://example.test/plan" }} {
		fresh := studyAuditTestInputs(t)
		change(&fresh.Plan)
		studyAuditTestRefreeze(t, &fresh)
		studyAuditTestRefused(t, fresh)
	}
	if err := runCCV2Audit(f.Audit.Path, f.Audit.SHA256, t.TempDir()); err == nil {
		t.Fatal("existing output directory accepted")
	}
}

func FuzzStudyAuditPlan(f *testing.F) {
	for _, seed := range []string{`null`, `{}`, `{"schema_version":1,"frozen":true,"exposed":true,"stability":{"window_minutes":15,"min_follow_on":3,"freq_tolerance_hz":500},"groups":[]}`, `{"groups":null,"groups":[]}`} {
		f.Add([]byte(seed))
	}
	valid := studyAuditPlan{SchemaVersion: 1, Frozen: true, Exposed: true, Stability: replayStabilityConfig{WindowMinutes: 15, MinFollowOn: 3, FreqToleranceHz: 500},
		Groups: []studyAuditGroup{{Plan: studyFile{Path: "original.json", SHA256: strings.Repeat("a", 64)}, Phase: "holdout", Candidate: "frozen"}}}
	for _, arm := range []string{"shipped", "repaired_off", "repaired_on"} {
		valid.Groups[0].Reports = append(valid.Groups[0].Reports, studyAuditReportInput{Report: studyFile{Path: filepath.Join(arm, "complete.json"), SHA256: strings.Repeat("b", 64)},
			Manifest: studyFile{Path: filepath.Join(arm, "manifest.json"), SHA256: strings.Repeat("c", 64)}})
	}
	seed, err := json.Marshal(valid)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			t.Skip()
		}
		var plan studyAuditPlan
		if err := studyDecodeJSON(data, &plan); err != nil {
			return
		}
		if err := validateStudyAuditPlan(plan); err != nil {
			return
		}
		// Acceptance must preserve all frozen semantics through strict encoding.
		roundtrip, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		var decoded studyAuditPlan
		if err := studyDecodeJSON(roundtrip, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(plan, decoded) || validateStudyAuditPlan(decoded) != nil {
			t.Fatal("accepted plan did not strictly round-trip")
		}
		if plan.SchemaVersion != 1 || !plan.Frozen || !plan.Exposed || plan.Stability.WindowMinutes != 15 || plan.Stability.MinFollowOn != 3 || plan.Stability.FreqToleranceHz != 500 || len(plan.Groups) == 0 || len(plan.Groups) > 32 {
			t.Fatal("accepted plan violates literal frozen contract")
		}
		reports := 0
		for _, group := range plan.Groups {
			reports += len(group.Reports)
			if len(group.Candidate) == 0 || len(group.Candidate) > 64 || len(group.Reports) < 3 || len(group.Reports)%3 != 0 {
				t.Fatal("accepted group cardinality violated")
			}
			for _, binding := range group.Reports {
				for _, file := range []studyFile{group.Plan, binding.Report, binding.Manifest} {
					if len(file.Path) == 0 || len(file.Path) > 1024 || len(file.SHA256) != 64 {
						t.Fatal("accepted file bound violated")
					}
				}
			}
		}
		if reports > 96 {
			t.Fatal("accepted report population exceeded 32 days")
		}
	})
}

func TestStudyAuditDistinctPlansCannotRepeatPopulation(t *testing.T) {
	first, second := studyAuditTestInputs(t), studyAuditTestInputs(t)
	if first.Plan.Groups[0].Plan.SHA256 == second.Plan.Groups[0].Plan.SHA256 {
		t.Fatal("fixture plans must be differently hash-bound")
	}
	if first.Study.Inputs[0].Archive.SHA256 != second.Study.Inputs[0].Archive.SHA256 {
		t.Fatal("fixture archives must share immutable content")
	}
	first.Plan.Groups = append(first.Plan.Groups, second.Plan.Groups[0])
	studyAuditTestRefreeze(t, &first)
	out := filepath.Join(t.TempDir(), "overlap")
	err := runCCV2Audit(first.Audit.Path, first.Audit.SHA256, out)
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("distinct plan hashes double-counted population: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "paired_union.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("overlap reached scoring output: %v", err)
	}
}

func TestStudyAuditBoundedReadAtLimitAndOneOver(t *testing.T) {
	for _, fixture := range []struct {
		name, data string
		refuse     bool
	}{
		{"at_limit", "abc", false}, {"one_over", "abcd", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "bounded.json")
			if err := os.WriteFile(path, []byte(fixture.data), 0o600); err != nil {
				t.Fatal(err)
			}
			data, err := studyReadBounded(path, 3)
			if fixture.refuse {
				if err == nil {
					t.Fatal("one byte beyond caller's explicit limit was accepted")
				}
				return
			}
			if err != nil || string(data) != fixture.data {
				t.Fatalf("exact caller limit was refused: data=%q err=%v", data, err)
			}
		})
	}
}
