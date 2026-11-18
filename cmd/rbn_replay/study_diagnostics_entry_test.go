package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiagnosticRequestIsolation(t *testing.T) {
	valid := studyCLIRequest{DiagnosticSelection: "unopened-selection.json", DiagnosticFreeze: strings.Repeat("a", 64), Date: "2026-11-15"}
	for _, tc := range []struct {
		name string
		req  studyCLIRequest
		want bool
	}{
		{"ordinary", studyCLIRequest{}, false},
		{"ordinary_comparison", studyCLIRequest{Compare: "ordinary.json", Date: "2026-11-17"}, false},
		{"exposed_pair", valid, false},
		{"selection_only", studyCLIRequest{DiagnosticSelection: valid.DiagnosticSelection, Date: valid.Date}, true},
		{"freeze_only", studyCLIRequest{DiagnosticFreeze: valid.DiagnosticFreeze, Date: valid.Date}, true},
		{"comparison", studyCLIRequest{DiagnosticSelection: valid.DiagnosticSelection, DiagnosticFreeze: valid.DiagnosticFreeze, Compare: "report.json", Date: valid.Date}, true},
		{"compact_date", studyCLIRequest{DiagnosticSelection: valid.DiagnosticSelection, DiagnosticFreeze: valid.DiagnosticFreeze, Date: "20261115"}, true},
		{"unexposed_date", studyCLIRequest{DiagnosticSelection: valid.DiagnosticSelection, DiagnosticFreeze: valid.DiagnosticFreeze, Date: "2026-11-17"}, true},
		{"bad_sha", studyCLIRequest{DiagnosticSelection: valid.DiagnosticSelection, DiagnosticFreeze: "bad", Date: valid.Date}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := studyDiagnosticValidateRequest(tc.req); (err != nil) != tc.want {
				t.Fatalf("request validation=%v want error=%v", err, tc.want)
			}
		})
	}
}

func TestDiagnosticUnparsedFlagNames(t *testing.T) {
	for _, argument := range []string{"-study-diagnostic-selection", "--study-diagnostic-freeze", "-study-diagnostic-selection=", "--study-diagnostic-freeze=abc"} {
		if !studyDiagnosticCLIFlag(argument) {
			t.Errorf("diagnostic flag not recognized: %q", argument)
		}
	}
	for _, argument := range []string{"positional", "study-diagnostic-selection", "-study-plan", "-study-audit-plan", "-study-diagnostic-selection-extra", "---study-diagnostic-selection", "value-study-diagnostic-freeze"} {
		if studyDiagnosticCLIFlag(argument) {
			t.Errorf("ordinary argument classified as diagnostic flag: %q", argument)
		}
	}
}

// Reuse the existing actual-main subprocess fixture, with an empty working
// directory so a broken ordinary fallback cannot find a replay configuration.
// Specific refusal messages distinguish correct routing from that fallback.
func TestDiagnosticActualMainIsolation(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name  string
		build func([]string) []string
		want  string
	}{
		{"selection_only", func(a []string) []string { return append(a, "-study-diagnostic-selection", "unused-selection.json") }, "supplied together"},
		{"freeze_only", func(a []string) []string { return append(a, "-study-diagnostic-freeze", sha) }, "supplied together"},
		{"both_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection=", "-study-diagnostic-freeze=")
		}, "supplied together"},
		{"selection_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection=", "-study-diagnostic-freeze", sha)
		}, "supplied together"},
		{"freeze_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze=")
		}, "supplied together"},
		{"date_missing", func(a []string) []string {
			return append(a, "-date=", "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha)
		}, "YYYY-MM-DD"},
		{"date_compact", func(a []string) []string {
			return append(a, "-date", "20261115", "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha)
		}, "YYYY-MM-DD"},
		{"date_unexposed", func(a []string) []string {
			return append(a, "-date", "2026-11-17", "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha)
		}, "exposed November"},
		{"comparison", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-study-compare", "unused-report.json")
		}, "cannot be used for study comparison"},
		{"comparison_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-study-compare=")
		}, "cannot be used for study comparison"},
		{"audit", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-study-audit-plan", "unused-audit.json")
		}, "audit uses only its frozen plan inputs"},
		{"audit_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-study-audit-plan=")
		}, "audit uses only its frozen plan inputs"},
		{"download", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-force-download")
		}, "ordinary replay overrides"},
		{"download_false", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-force-download=false")
		}, "ordinary replay overrides"},
		{"replay_config_empty", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-replay-config=")
		}, "ordinary replay overrides"},
		{"config", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-config", "unused-config")
		}, "ordinary replay overrides"},
		{"archive_dir", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "-archive-dir", "unused-archives")
		}, "ordinary replay overrides"},
		{"positional", func(a []string) []string {
			return append(a, "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha, "positional")
		}, "positional arguments are not allowed"},
		{"diagnostics_after_positional", func([]string) []string {
			return []string{"-date", "2026-11-15", "positional", "-study-diagnostic-selection", "unused-selection.json", "-study-diagnostic-freeze", sha}
		}, "positional arguments are not allowed"},
		{"diagnostics_after_terminator", func([]string) []string {
			return []string{"-date", "2026-11-15", "--", "--study-diagnostic-selection=unused-selection.json", "--study-diagnostic-freeze=" + sha}
		}, "positional arguments are not allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			working := t.TempDir()
			outputDir := filepath.Join(working, "capture")
			args := tc.build([]string{"-study-plan", "unused-plan.json", "-study-freeze", sha,
				"-study-arm", "repaired_off", "-date", "2026-11-15", "-study-out", outputDir})
			encoded, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCCV2StudyActualMainRefusals$", "-test.timeout=15s")
			cmd.Dir = working
			cmd.Env = append(os.Environ(), "GOCLUSTER_CCV2_STUDY_MAIN="+string(encoded))
			output, err := cmd.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("diagnostic refusal did not terminate promptly: %v output=%s", ctx.Err(), output)
			}
			if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("actual main refusal=%v output=%s want=%s", err, output, tc.want)
			}
			entries, err := os.ReadDir(working)
			if err != nil || len(entries) != 0 {
				t.Fatalf("refused diagnostic invocation created working-directory artifacts: entries=%v err=%v", entries, err)
			}
		})
	}
}

func TestDiagnosticReportMarkerPresence(t *testing.T) {
	for _, key := range []string{"diagnostics", "Diagnostics", "DIAGNOSTICS", "dIaGnOsTiCs", `\u0064iagnostics`} {
		for _, value := range []string{"null", "false", `""`, "{}", "[]", "0", `{"unfinished":`, ""} {
			data := []byte(`{"schema_version":2,"` + key + `":` + value + `}`)
			if err := studyDiagnosticRejectReportMarker(data); !errors.Is(err, errStudyDiagnosticReportMarker) {
				t.Fatalf("top-level marker key=%q value=%q: %v", key, value, err)
			}
		}
	}
	for _, data := range []string{`{}`, `{"nested":{"diagnostics":null}}`, `{"nested":[{"Diagnostics":false}]}`, `{"diagnostics_extra":null}`, `{"text":"diagnostics"}`, `{"number":1e1000}`} {
		if err := studyDiagnosticRejectReportMarker([]byte(data)); err != nil {
			t.Fatalf("marker-absent report %s: %v", data, err)
		}
	}
}

func TestDiagnosticReportMarkerBounds(t *testing.T) {
	exact := append([]byte("{}"), bytes.Repeat([]byte(" "), studyMaxJSONBytes-2)...)
	if err := studyDiagnosticRejectReportMarker(exact); err != nil {
		t.Fatal(err)
	}
	if err := studyDiagnosticRejectReportMarker(append(exact, ' ')); err == nil {
		t.Fatal("accepted report beyond byte limit")
	}
	for _, depth := range []int{32, 33} {
		data := []byte(`{"nested":` + strings.Repeat("[", depth) + strings.Repeat("]", depth) + `}`)
		if err := studyDiagnosticRejectReportMarker(data); (err != nil) != (depth == 33) {
			t.Fatalf("depth=%d err=%v", depth, err)
		}
	}
	for _, data := range []string{"", "null", "[]", `{"nested":`, `{} {}`, `{"nested":[1,]}`} {
		if err := studyDiagnosticRejectReportMarker([]byte(data)); err == nil {
			t.Fatalf("accepted invalid report shape %q", data)
		}
	}
}

func TestDiagnosticComparisonMarkerRefusal(t *testing.T) {
	p := studyTestPlan(t)
	for i := range p.Inputs {
		p.Inputs[i].MaxRows = 100
	}
	const digest = "diagnostic-report-marker"
	paths := studyTestComparisonReports(t, p, digest, false, false)
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.TrimSpace(data)
	request := studyCLIRequest{Compare: strings.Join(paths, ","), Phase: "holdout"}
	for _, value := range []string{"null", "false", "{}", `{"unfinished":`} {
		marked := append(append([]byte(nil), original[:len(original)-1]...), []byte(`,"DiAgNoStIcS":`+value+`}`)...)
		if err := os.WriteFile(paths[0], marked, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := studyReadReports(p, digest, request, p.Candidates[0]); !errors.Is(err, errStudyDiagnosticReportMarker) {
			t.Fatalf("qualification reader did not reject marker before typed decode, value=%q: %v", value, err)
		}
	}
	var report studyReport
	if err := json.Unmarshal(original, &report); err != nil {
		t.Fatal(err)
	}
	report.References.R1Unknown = map[string]int{"diagnostics": 0}
	nested, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths[0], nested, 0o600); err != nil {
		t.Fatal(err)
	}
	if reports, err := studyReadReports(p, digest, request, p.Candidates[0]); err != nil || len(reports) != len(p.Inputs) {
		t.Fatalf("marker-absent ordinary report checks changed: reports=%d err=%v", len(reports), err)
	}
}

func FuzzStudyDiagnosticReportMarker(f *testing.F) {
	for _, seed := range []string{`{}`, `{"diagnostics":null}`, `{"nested":{"diagnostics":false}}`, `{"diagnostics":`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > studyMaxJSONBytes {
			return
		}
		_ = studyDiagnosticRejectReportMarker(data)
		prefix := []byte(`{"DiAgNoStIcS":`)
		if len(data)+len(prefix) > studyMaxJSONBytes {
			return
		}
		marked := append(prefix, data...)
		if err := studyDiagnosticRejectReportMarker(marked); !errors.Is(err, errStudyDiagnosticReportMarker) {
			t.Fatalf("valid marker prefix did not refuse arbitrary/malformed value: %v", err)
		}
	})
}
