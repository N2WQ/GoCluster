package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestInspectionMainChild(t *testing.T) {
	mode := os.Getenv("DXC_INSPECTION_MAIN_CHILD")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child argument separator")
	}
	os.Args = append([]string{"inspect_decisions"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			t.Fatalf("close fixture stdout: %v", err)
		}
	}
	main()
	os.Exit(0)
}

func inspectionMainResult(t *testing.T, path, mode string) (int, string, string) {
	t.Helper()
	return inspectionMainAt(t, t.TempDir(), mode, "-db", path)
}

// Use the exact saved executable already executing in the selected stable
// lane. Timeout or launch/join errors must not satisfy a nonzero-main oracle.
// The deliberate main exit is not launched with -test.paniconexit0.
func inspectionMainAt(t *testing.T, directory, mode string, commandArgs ...string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	args := append([]string{"-test.run=^TestInspectionMainChild$", "--"}, commandArgs...)
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), "DXC_INSPECTION_MAIN_CHILD="+mode)
	cmd.WaitDelay = 2 * time.Second
	var output, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("actual main missed execution/join bound: %v", ctx.Err())
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("child launch/join failure: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return code, output.String(), stderr.String()
}

func TestInspectionActualMainMixedEmptyAndUnknownOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []inspectionFixtureRow
		wants  []string
	}{
		{"mixed", inspectionMixedFixture(), inspectionMixedReportLiterals()},
		{"empty", nil, []string{"All rows: 0\nApplied: 0\nRejected: 0\nUnknown: 0\n", "Raw outcome values (case-only known classifications):\n  (none)\n"}},
		{"unknown-only", inspectionMixedFixture()[7:], []string{"All rows: 7\nApplied: 0\nRejected: 0\nUnknown: 7\n", "  <NULL>: 1 (unknown)\n", "  \"<NULL>\": 1 (unknown)\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := inspectionFixtureDB(t, tc.values)
			code, output, stderr := inspectionMainResult(t, path, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("valid actual main status%d stdout=%q stderr=%q", code, output, stderr)
			}
			inspectionRequireText(t, output, append(tc.wants, "DESCRIPTIVE DECISION INSPECTION\n", fmt.Sprintf("Database: %q\n", path))...)
			inspectionRequireDescriptive(t, output)
			if tc.name != "mixed" {
				inspectionRequireText(t, output,
					"Applied samples (at most 10; lexical selected-field order):\n  (none)\n",
					"Distance-1 rejection samples (at most 10; support descending, selected-field ties):\n  (none)\n",
					"High-score rejection samples (score >70; at most 10; score descending, selected-field ties):\n  (none)\n")
			}
		})
	}
}

func TestInspectionActualMainIgnoredMalformedFieldsRemainUnused(t *testing.T) {
	code, output, stderr := inspectionMainResult(t, inspectionIgnoredFieldFixture(t), "run")
	if code != 0 || stderr != "" {
		t.Fatalf("unchanged projection actual main status%d stdout=%q stderr=%q", code, output, stderr)
	}
	inspectionRequireText(t, output, "All rows: 3\nApplied: 1\nRejected: 1\nUnknown: 1\n",
		"\"A_UNUSED\" -> \"W1XYZ\" distance=1 logged score=50 reporters=4 outcome=\"applied\"\n",
		"\"HIGH_UNUSED\" -> \"W1XYZ\" distance=2 logged score=71 support=3/4 outcome=\"rejected\" reason=\"GATE\"\n")
	inspectionRequireDescriptive(t, output)
}

func TestInspectionActualMainSQLStageFailuresPublishNoPartialReport(t *testing.T) {
	valid := []inspectionFixtureRow{inspectionFixtureValue("APPLIED_GOOD", 2, 55, "applied"),
		inspectionFixtureValue("DISTANCE_GOOD", 1, 70, "rejected"),
		inspectionFixtureValue("HIGH_GOOD", 2, 95, "Rejected"),
		inspectionFixtureValue("HIGH_LATE", 2, 71, "rejected")}
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) string
		wants []string
	}{
		{"stage1-missing-table", func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-schema.db") }, []string{"query outcome breakdown", "no such table", "decisions"}},
		{"stage2-missing-column", func(t *testing.T) string {
			return inspectionFixtureDB(t, valid, "ALTER TABLE decisions RENAME COLUMN total_reporters TO other_total")
		}, []string{"query applied samples", "no such column", "total_reporters"}},
		{"stage2-selected-bad-cell", func(t *testing.T) string {
			return inspectionFixtureDB(t, valid, "UPDATE decisions SET total_reporters='bad-reporters' WHERE subject='APPLIED_GOOD'")
		}, []string{"scan applied samples", "total_reporters", "bad-reporters"}},
		{"stage3-selected-bad-cell", func(t *testing.T) string {
			return inspectionFixtureDB(t, valid, "UPDATE decisions SET min_reports='bad-min-reports' WHERE subject='DISTANCE_GOOD'")
		}, []string{"scan distance-1 rejection samples", "min_reports", "bad-min-reports"}},
		{"stage4-selected-bad-cell-after-good-row", func(t *testing.T) string {
			return inspectionFixtureDB(t, valid, "UPDATE decisions SET winner_support='bad-support' WHERE subject='HIGH_LATE'")
		}, []string{"scan high-score rejection samples", "winner_support", "bad-support"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output, stderr := inspectionMainResult(t, tc.setup(t), "run")
			if code != 1 || output != "" {
				t.Fatalf("failed actual main status%d stdout=%q stderr=%q", code, output, stderr)
			}
			inspectionRequireText(t, stderr, tc.wants...)
		})
	}
}

func TestInspectionActualMainClosedOutputAfterValidLoad(t *testing.T) {
	path := inspectionFixtureDB(t, []inspectionFixtureRow{inspectionFixtureValue("WITNESS", 1, 71, "Rejected")})
	code, output, stderr := inspectionMainResult(t, path, "run")
	if code != 0 || stderr != "" || !strings.Contains(output, "\"WITNESS\" -> \"W1XYZ\" distance=1 logged score=71 support=3/4 outcome=\"Rejected\" reason=\"GATE\"") {
		t.Fatalf("paired valid report witness: status%d stdout=%q stderr=%q", code, output, stderr)
	}
	code, output, stderr = inspectionMainResult(t, path, "closed-output")
	if code != 1 || output != "" || !strings.Contains(stderr, "write decision inspection") || !strings.Contains(stderr, "file already closed") {
		t.Fatalf("output failure lost/failed before reporting: status%d stdout=%q stderr=%q", code, output, stderr)
	}
}

func TestInspectionActualMainPreservesDefaultDatabaseFlag(t *testing.T) {
	path := inspectionFixtureDB(t, []inspectionFixtureRow{inspectionFixtureValue("DEFAULT_WITNESS", 1, 71, "Rejected")})
	directory := t.TempDir()
	defaultName := "data/logs/callcorr_debug_modified_2025-12-04.db"
	target := filepath.Join(directory, filepath.FromSlash(defaultName))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, stderr := inspectionMainAt(t, directory, "run")
	if code != 0 || stderr != "" {
		t.Fatalf("default flag actual main status%d stdout=%q stderr=%q", code, output, stderr)
	}
	inspectionRequireText(t, output, `Database: "data/logs/callcorr_debug_modified_2025-12-04.db"`,
		"All rows: 1\nApplied: 0\nRejected: 1\nUnknown: 0\n", `"DEFAULT_WITNESS" -> "W1XYZ"`)
}
