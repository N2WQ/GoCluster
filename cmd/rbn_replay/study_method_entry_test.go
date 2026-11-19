package main

import (
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

func TestCCV2F2LegacyRefusal(t *testing.T) {
	for _, key := range []string{"experimental", "EXPERIMENTAL", `\u0065xperimental`} {
		for _, value := range []string{"false", "null", "{}", "[]", `{"unfinished":`, ""} {
			data := []byte(`{"schema_version":2,"` + key + `":` + value + `}`)
			if err := studyDiagnosticRejectReportMarker(data); !errors.Is(err, errStudyExperimentalReportMarker) {
				t.Fatalf("marker value interpreted before presence refusal: %s: %v", data, err)
			}
			file := methodTestWrite(t, filepath.Join(t.TempDir(), "report.json"), data)
			var report studyReport
			if err := studyAuditReadJSON(file, &report); !errors.Is(err, errStudyExperimentalReportMarker) {
				t.Fatalf("typed audit reread admitted marker: %v", err)
			}
		}
	}
}

func TestCCV2F2ActualMainIsolation(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-study-method-plan="}, "supplied together"},
		{[]string{"-study-method-plan=unused", "-study-method-freeze=", "-study-method-candidate=x"}, "supplied together"},
		{[]string{"-study-method-plan=unused", "-study-method-freeze=unused", "-study-method-candidate=x", "-study-compare="}, "cannot be combined"},
		{[]string{"positional", "--study-method-plan=unused"}, "positional arguments"},
		{[]string{"--", "--study-method-plan=unused"}, "positional arguments"},
	} {
		working := t.TempDir()
		encoded, err := json.Marshal(tc.args)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCCV2StudyActualMainRefusals$", "-test.timeout=15s")
		cmd.Dir = working
		cmd.Env = append(os.Environ(), "GOCLUSTER_CCV2_STUDY_MAIN="+string(encoded))
		output, runErr := cmd.CombinedOutput()
		cancel()
		if runErr == nil || !strings.Contains(string(output), tc.want) {
			t.Fatalf("CLI did not refuse on correct boundary: %v %s", runErr, output)
		}
		entries, err := os.ReadDir(working)
		if err != nil || len(entries) != 0 {
			t.Fatal("refused method invocation created artifacts")
		}
	}
	for _, arg := range []string{"-study-method-plan", "--study-method-freeze=abc", "-study-method-candidate="} {
		if !studyMethodCLIFlag(arg) {
			t.Fatalf("method flag unrecognized: %s", arg)
		}
	}
	for _, arg := range []string{"---study-method-plan", "value-study-method-plan", "-study-method-plan-extra"} {
		if studyMethodCLIFlag(arg) {
			t.Fatalf("nonflag recognized: %s", arg)
		}
	}
}
