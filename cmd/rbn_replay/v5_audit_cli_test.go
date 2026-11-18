package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Exercise actual flag routing in the existing native test image. Rejected
// audit invocations must never enter ordinary replay or discover downloads.
func TestV5AuditActualMainIsolation(t *testing.T) {
	const helper = "GOCLUSTER_V5_AUDIT_MAIN"
	if encoded := os.Getenv(helper); encoded != "" {
		var args []string
		if err := json.Unmarshal([]byte(encoded), &args); err != nil {
			t.Fatal(err)
		}
		flag.CommandLine = flag.NewFlagSet("rbn_replay", flag.ExitOnError)
		os.Args = append([]string{os.Args[0]}, args...)
		main()
		os.Exit(0)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		extra []string
	}{
		{"download", []string{"-force-download"}},
		{"replay_config", []string{"-replay-config", "unused.yaml"}},
		{"date", []string{"-date", "2050-01-01"}},
		{"study_plan", []string{"-study-plan", "unused.json"}},
		{"positional", []string{"unused"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outputDir := filepath.Join(t.TempDir(), "audit")
			args := append([]string{"-study-audit-plan", "unused.json", "-study-audit-out", outputDir}, tc.extra...)
			encoded, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestV5AuditActualMainIsolation$", "-test.timeout=15s")
			cmd.Env = append(os.Environ(), helper+"="+string(encoded))
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "audit uses only its frozen plan inputs") {
				t.Fatalf("audit routing refusal=%v output=%s", err, output)
			}
			if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
				t.Fatalf("refused audit created output directory: %v", err)
			}
		})
	}
}
