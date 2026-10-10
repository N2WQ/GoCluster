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

func TestReappearanceMainChild(t *testing.T) {
	mode := os.Getenv("DXC_REAPPEARANCE_MAIN_CHILD")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child argument separator")
	}
	os.Args = append([]string{"investigate_reappearances"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			t.Fatalf("close fixture stdout: %v", err)
		}
	}
	main()
	os.Exit(0)
}

// Reuse the exact stable saved test executable. Child main exits deliberately,
// without -test.paniconexit0. A timeout or launch/join error cannot count as the
// command's unsuccessful status, and Run must return before fixture deletion.
func reappearanceMainAt(t *testing.T, directory, mode string, args ...string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	commandArgs := append([]string{"-test.run=^TestReappearanceMainChild$", "--"}, args...)
	cmd := exec.CommandContext(ctx, exe, commandArgs...)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), "DXC_REAPPEARANCE_MAIN_CHILD="+mode)
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

func reappearanceMainResult(t *testing.T, path, mode string, extraArgs ...string) (int, string, string) {
	t.Helper()
	args := append([]string{"-db", path}, extraArgs...)
	return reappearanceMainAt(t, t.TempDir(), mode, args...)
}

func TestReappearanceActualMainBoundaryEmptyNoDualAndUnknownSeeds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []reappearanceFixtureRow
		wants  []string
	}{
		{"boundaries", reappearanceBoundaryFixture(), reappearanceBoundaryReportLiterals()},
		{"empty", nil, []string{"Dual-reappearance cases: 0\n"}},
		{"no-dual", []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"), reappearanceObservation(2, 1001, "S", 7000, "pending")}, []string{"Dual-reappearance cases: 0\n"}},
		{"unknown-seeds", reappearanceUnknownSeedsFixture(), []string{"Dual-reappearance cases: 0\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := reappearanceFixtureDB(t, tc.values)
			code, output, stderr := reappearanceMainResult(t, path, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("valid actualmain status%d stdout=%q stderr=%q", code, output, stderr)
			}
			reappearanceRequireText(t, output, append(tc.wants, "DESCRIPTIVE REAPPEARANCE OBSERVATIONS\n", fmt.Sprintf("Database: %q\n", path), "Lookahead hours: 24\n")...)
			reappearanceRequireDescriptive(t, output)
			if tc.name != "boundaries" {
				reappearanceRequireText(t, output, "First-frequency separation <0.5 kHz: 0 (N/A)\nFirst-frequency separation >=0.5 kHz: 0 (N/A)\n", "Samples (at most 20; timestamp/id then selected-field ties):\n  (none)\n")
			}
		})
	}
}

func reappearanceUnknownSeedsFixture() []reappearanceFixtureRow {
	var values []reappearanceFixtureRow
	for i, outcome := range []any{nil, "", "pending", "PENDING", " applied ", "applied\t", "rejected"} {
		id := int64(i + 1)
		subject, winner := fmt.Sprintf("S%d", id), fmt.Sprintf("W%d", id)
		values = append(values, reappearanceSeed(id, 1000, subject, winner, outcome),
			reappearanceObservation(100+id*2, 1001, subject, 7000, "pending"), reappearanceObservation(101+id*2, 1002, winner, 7001, "pending"))
	}
	return values
}

func TestReappearanceActualMainExplicitLookaheadControlsMembership(t *testing.T) {
	path := reappearanceFixtureDB(t, []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"), reappearanceObservation(2, 1001, "S", 7000, "pending"), reappearanceObservation(3, 8200, "W", 7000.5, "pending")})
	for _, tc := range []struct {
		hours string
		wants []string
	}{
		{"1", []string{"Lookahead hours: 1\n", "Dual-reappearance cases: 0\n", "First-frequency separation >=0.5 kHz: 0 (N/A)\n"}},
		{"2", []string{"Lookahead hours: 2\n", "Dual-reappearance cases: 1\n", "First-frequency separation >=0.5 kHz: 1 (100.0% of dual cases)\n", `"S" -> "W" distance=3 logged score=65`}},
	} {
		t.Run("hours"+tc.hours, func(t *testing.T) {
			code, output, stderr := reappearanceMainResult(t, path, "run", "-lookahead", tc.hours)
			if code != 0 || stderr != "" {
				t.Fatalf("explicitlookahead actualmain status%d stdout=%q stderr=%q", code, output, stderr)
			}
			reappearanceRequireText(t, output, tc.wants...)
			reappearanceRequireDescriptive(t, output)
		})
	}
}

func TestReappearanceActualMainCausalSQLFailuresPublishNoPartialReport(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) string
		wants []string
	}{
		{"missing-table", func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-schema.db") }, []string{"query applied corrections", "no such table", "decisions"}},
		{"missing-column", func(t *testing.T) string {
			return reappearanceFixtureDB(t, reappearanceBoundaryFixture(), "ALTER TABLE decisions RENAME COLUMN freq_khz TO other_frequency")
		}, []string{"query applied corrections", "no such column", "freq_khz"}},
		{"late-outer-scan", func(t *testing.T) string {
			return reappearanceFixtureDB(t, reappearanceBoundaryFixture(), "UPDATE decisions SET freq_khz='bad-seed-frequency' WHERE id=2 AND ts=1000")
		}, []string{"scan applied corrections", "freq_khz", "bad-seed-frequency"}},
		{"late-inner-scan-after-both-found", func(t *testing.T) string {
			return reappearanceFixtureDB(t, reappearanceBoundaryFixture(), "UPDATE decisions SET freq_khz='bad-followup-frequency' WHERE id=202 AND ts=1003")
		}, []string{"correction 1 follow-up", "scan follow-up observations", "freq_khz", "bad-followup-frequency"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output, stderr := reappearanceMainResult(t, tc.setup(t), "run")
			if code != 1 || output != "" {
				t.Fatalf("SQLfailure actualmain status%d stdout=%q stderr=%q", code, output, stderr)
			}
			reappearanceRequireText(t, stderr, tc.wants...)
		})
	}
}

func TestReappearanceActualMainClosedOutputAfterValidLoad(t *testing.T) {
	path := reappearanceFixtureDB(t, reappearanceBoundaryFixture())
	code, output, stderr := reappearanceMainResult(t, path, "run")
	if code != 0 || stderr != "" {
		t.Fatalf("paired successful main status%d stdout=%q stderr=%q", code, output, stderr)
	}
	reappearanceRequireText(t, output, reappearanceBoundaryReportLiterals()...)
	code, output, stderr = reappearanceMainResult(t, path, "closed-output")
	if code != 1 || output != "" || !strings.Contains(stderr, "write reappearance observations") || !strings.Contains(stderr, "file already closed") {
		t.Fatalf("closedoutput failure lost: status%d stdout=%q stderr=%q", code, output, stderr)
	}
}

func TestReappearanceActualMainPreservesDefaultDBAnd24Hours(t *testing.T) {
	path := reappearanceFixtureDB(t, []reappearanceFixtureRow{reappearanceSeed(1, 1000, "S", "W", "applied"), reappearanceObservation(2, 1001, "S", 7000, "pending"), reappearanceObservation(3, 87400, "W", 7000.5, "pending")})
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
	code, output, stderr := reappearanceMainAt(t, directory, "run")
	if code != 0 || stderr != "" {
		t.Fatalf("defaultactualmain status%d stdout=%q stderr=%q", code, output, stderr)
	}
	reappearanceRequireText(t, output, `Database: "data/logs/callcorr_debug_modified_2025-12-04.db"`, "Lookahead hours: 24\n", "Dual-reappearance cases: 1\n", `"S" -> "W" distance=3 logged score=65`, "first subject=7000.000 kHz; first winner=7000.500 kHz; separation=0.500 kHz")
	reappearanceRequireDescriptive(t, output)
}
