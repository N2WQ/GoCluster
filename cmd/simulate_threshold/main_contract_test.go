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

func TestThresholdMainChild(t *testing.T) {
	mode := os.Getenv("DXC_THRESHOLD_MAIN_CHILD")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child argument separator")
	}
	os.Args = append([]string{"simulate_threshold"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			t.Fatalf("close fixture stdout: %v", err)
		}
	}
	main()
	os.Exit(0)
}

// Execute the exact saved test executable already running in the stable lane.
// Its intentional main exit is not launched with -test.paniconexit0.
func thresholdMainResult(t *testing.T, path, mode string, cuts ...string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	args := append([]string{"-test.run=^TestThresholdMainChild$", "--", "-db", path}, cuts...)
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "DXC_THRESHOLD_MAIN_CHILD="+mode)
	cmd.WaitDelay = 2 * time.Second
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
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
	return code, out.String(), stderr.String()
}

func TestThresholdActualMainDefaultsMixedAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []thresholdFixtureRow
		wants  []string
	}{
		{"mixed", thresholdMixedFixture(), []string{"Selected distance-1..3 rows: 5\n", "Retained applied: 1\nRemoved applied: 1\nAdded rejected: 1\n", "Recognized rows passing: 2/4 (50.0%)\nNet change from applied: +0\n", "Unknown rows passing (separate): 1/1;"}},
		{"empty", nil, []string{"Selected distance-1..3 rows: 0\n", "Applied: 0 (N/A)\nRejected: 0 (N/A)\nUnknown: 0 (N/A)\n", "  (none; no baseline inferred)\n", "Recognized rows passing: 0/0 (N/A)\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := thresholdFixtureDB(t, tc.values)
			code, out, stderr := thresholdMainResult(t, path, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("valid actual main status=%d stdout=%q stderr=%q", code, out, stderr)
			}
			thresholdRequireText(t, out, append(tc.wants, "HYPOTHETICAL THRESHOLD COMPARISON\n", fmt.Sprintf("Database: %q", path),
				"Hypothetical cuts: winner reports >= 3; advantage >= 1; logged score >= 55\n")...)
			thresholdRequireDescriptive(t, out)
		})
	}
}

func TestThresholdActualMainAsymmetricAndNegativeFlagWiring(t *testing.T) {
	values := []thresholdFixtureRow{
		thresholdFixtureValue(1, "EXACT", 76, "rejected"),
		thresholdFixtureValue(2, "REPORTS_BELOW", 90, "applied"),
		thresholdFixtureValue(3, "ADVANTAGE_BELOW", 90, "applied"),
		thresholdFixtureValue(4, "SCORE_BELOW", 75, "applied"),
	}
	for i := range values {
		values[i].support, values[i].subjectSupport, values[i].reporters = 15, 11, 26
	}
	values[1].support, values[1].subjectSupport, values[1].reporters = 14, 0, 14
	values[2].subjectSupport, values[2].reporters = 12, 27
	path := thresholdFixtureDB(t, values)
	code, out, stderr := thresholdMainResult(t, path, "run", "-min-reports", "15", "-min-advantage", "4", "-min-confidence", "76")
	if code != 0 || stderr != "" {
		t.Fatalf("custom cut actual main: status%d stdout=%q stderr=%q", code, out, stderr)
	}
	thresholdRequireText(t, out, "Hypothetical cuts: winner reports >= 15; advantage >= 4; logged score >= 76\n",
		"Retained applied: 0\nRemoved applied: 3\nAdded rejected: 1\n", "Recognized rows passing: 1/4 (25.0%)\nNet change from applied: -2\n", `id=1 "EXACT" -> "W1XYZ"`)
	code, out, stderr = thresholdMainResult(t, path, "run", "-min-reports", "-1", "-min-advantage", "-2", "-min-confidence", "-5")
	if code != 0 || stderr != "" {
		t.Fatalf("preserved negative cut flags: status%d stdout=%q stderr=%q", code, out, stderr)
	}
	thresholdRequireText(t, out, "Hypothetical cuts: winner reports >= -1; advantage >= -2; logged score >= -5\n",
		"Retained applied: 3\nRemoved applied: 0\nAdded rejected: 1\n", "Recognized rows passing: 4/4 (100.0%)\nNet change from applied: +1\n")
}

func TestThresholdActualMainSQLFailuresPublishNoPartialReport(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) string
		wants []string
	}{
		{"missing-table", func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-schema.db") }, []string{"query threshold decisions", "no such table", "decisions"}},
		{"missing-column", func(t *testing.T) string {
			return thresholdFixtureDB(t, nil, "ALTER TABLE decisions RENAME COLUMN winner_confidence TO other_score")
		}, []string{"query threshold decisions", "no such column", "winner_confidence"}},
		{"bad-cell-after-valid-row", func(t *testing.T) string {
			return thresholdFixtureDB(t, []thresholdFixtureRow{thresholdFixtureValue(1, "GOOD", 90, "applied"), thresholdFixtureValue(2, "BAD", 80, "applied")},
				"UPDATE decisions SET winner_support='not-an-integer' WHERE subject='BAD'")
		}, []string{"scan threshold decision", "winner_support", "not-an-integer"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.setup(t)
			code, out, stderr := thresholdMainResult(t, path, "run")
			if code != 1 || out != "" {
				t.Fatalf("failed actual main status=%d stdout=%q stderr=%q", code, out, stderr)
			}
			thresholdRequireText(t, stderr, tc.wants...)
		})
	}
}

func TestThresholdActualMainClosedOutputFailsAfterValidLoad(t *testing.T) {
	path := thresholdFixtureDB(t, []thresholdFixtureRow{thresholdFixtureValue(99, "WITNESS", 55, "rejected")})
	code, out, stderr := thresholdMainResult(t, path, "run")
	if code != 0 || stderr != "" || !strings.Contains(out, `id=99 "WITNESS" -> "W1XYZ"`) {
		t.Fatalf("paired valid fixture did not report witness: %d stdout=%q stderr=%q", code, out, stderr)
	}
	code, out, stderr = thresholdMainResult(t, path, "closed-output")
	if code != 1 || out != "" || !strings.Contains(stderr, "write threshold comparison") || !strings.Contains(stderr, "file already closed") {
		t.Fatalf("output failure lost/failed before reporting: %d stdout=%q stderr=%q", code, out, stderr)
	}
}
