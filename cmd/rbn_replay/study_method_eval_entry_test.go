package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestCCV2F3EvaluationActualMainRefusal(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"-study-method-eval-plan="}, "required together"},
		{[]string{"-study-method-eval-plan=unused", "-study-method-eval-freeze=unused", "-study-method-eval-out=unused", "-date=2026-11-15"}, "only frozen plan inputs"},
		{[]string{"positional", "--study-method-eval-plan=unused"}, "positional arguments"},
		{[]string{"--", "--study-method-eval-out=unused"}, "positional arguments"},
	} {
		working := t.TempDir()
		args, err := json.Marshal(tc.args)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCCV2StudyActualMainRefusals$", "-test.timeout=15s")
		cmd.Dir = working
		cmd.Env = append(os.Environ(), "GOCLUSTER_CCV2_STUDY_MAIN="+string(args))
		output, runErr := cmd.CombinedOutput()
		cancel()
		if runErr == nil || !strings.Contains(string(output), tc.want) {
			t.Fatalf("wrong evaluation refusal: %v %s", runErr, output)
		}
		entries, err := os.ReadDir(working)
		if err != nil || len(entries) != 0 {
			t.Fatal("refused evaluation created artifacts")
		}
	}
}
