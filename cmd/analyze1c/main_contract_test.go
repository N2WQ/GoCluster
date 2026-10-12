package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"dxcluster/internal/testsupport"
)

func TestDistanceAnalysisMainChild(t *testing.T) {
	mode := os.Getenv("DXC_ONE_C_MAIN_CHILD")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child argument separator")
	}
	os.Args = append([]string{"analyze1c"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			t.Fatalf("close fixture stdout: %v", err)
		}
	}
	main()
	os.Exit(0)
}

func oneCMainResult(t *testing.T, path, mode string) (int, string, string) {
	t.Helper()
	return oneCMainAt(t, t.TempDir(), mode, "-db", path)
}

// Execute the saved test binary already running in the selected stable lane.
// The deliberate main exit is not launched with -test.paniconexit0; context
// failure must not masquerade as the command's own unsuccessful exit status.
func oneCMainAt(t *testing.T, directory, mode string, commandArgs ...string) (int, string, string) {
	t.Helper()
	return testsupport.RunMain(t, "TestDistanceAnalysisMainChild", directory, []string{"DXC_ONE_C_MAIN_CHILD=" + mode}, commandArgs...)
}

func TestDistanceAnalysisActualMainMixedEmptyAndAbsentStatistics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []oneCFixtureRow
		wants  []string
	}{
		{"mixed", oneCMixedFixture(), oneCMixedReportLiterals()},
		{"empty", nil, []string{"All selected rows (every distance): 0\n", "Applied: 0 (N/A)\nRejected: 0 (N/A)\nUnknown: 0 (N/A)\n", "  (none; applied mean/median N/A)\n"}},
		{"no-applied", []oneCFixtureRow{{1, 80, "rejected", nil}, {3, 100, "pending", "UNKNOWN_REASON"}}, []string{
			"All selected rows (every distance): 2\nOverall outcome percentages use all selected rows.\nApplied: 0 (0.0%)\nRejected: 1 (50.0%)\nUnknown: 1 (50.0%)\n",
			"Distance 1: rows=1 applied=0 (0.0%) rejected=1 (100.0%) unknown=0 (0.0%)\n  Applied logged scores: n=0 mean=N/A median=N/A\n",
			"Distance 3: rows=1 applied=0 (0.0%) rejected=0 (0.0%) unknown=1 (100.0%)\n  Applied logged scores: n=0 mean=N/A median=N/A\n",
		}},
		{"all-distances-zero-half-and-fraction", []oneCFixtureRow{
			{-1, 80, "rejected", nil}, {0, 100, nil, nil}, {1, 0, "applied", nil},
			{3, 0, "applied", nil}, {3, 1, "APPLIED", nil}, {4, 1, "applied", nil}, {4, 2, "Applied", nil}, {4, 4, "APPLIED", nil},
		}, []string{
			"All selected rows (every distance): 8\nOverall outcome percentages use all selected rows.\nApplied: 6 (75.0%)\nRejected: 1 (12.5%)\nUnknown: 1 (12.5%)\n",
			"Distance -1: rows=1 applied=0 (0.0%) rejected=1 (100.0%) unknown=0 (0.0%)\n  Applied logged scores: n=0 mean=N/A median=N/A\n",
			"Distance 0: rows=1 applied=0 (0.0%) rejected=0 (0.0%) unknown=1 (100.0%)\n  Applied logged scores: n=0 mean=N/A median=N/A\n",
			"Distance 1: rows=1 applied=1 (100.0%) rejected=0 (0.0%) unknown=0 (0.0%)\n  Applied logged scores: n=1 mean=0.0 median=0.0\n",
			"Distance 3: rows=2 applied=2 (100.0%) rejected=0 (0.0%) unknown=0 (0.0%)\n  Applied logged scores: n=2 mean=0.5 median=0.5\n",
			"Distance 4: rows=3 applied=3 (100.0%) rejected=0 (0.0%) unknown=0 (0.0%)\n  Applied logged scores: n=3 mean=2.3 median=2.0\n",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := oneCFixtureDB(t, tc.values)
			code, output, stderr := oneCMainResult(t, path, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("valid actual main: status%d stdout=%q stderr=%q", code, output, stderr)
			}
			oneCRequireText(t, output, append(tc.wants, "DESCRIPTIVE DISTANCE SUMMARIES\n", fmt.Sprintf("Database: %q\n", path))...)
			oneCRequireDescriptive(t, output)
		})
	}
}

func TestDistanceAnalysisActualMainPreservesDefaultDatabaseFlag(t *testing.T) {
	path := oneCFixtureDB(t, []oneCFixtureRow{{1, 0, "applied", nil}, {1, 1, "Applied", nil}})
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
	code, output, stderr := oneCMainAt(t, directory, "run")
	if code != 0 || stderr != "" {
		t.Fatalf("default flag actual main: status%d stdout=%q stderr=%q", code, output, stderr)
	}
	oneCRequireText(t, output, `Database: "data/logs/callcorr_debug_modified_2025-12-04.db"`,
		"All selected rows (every distance): 2\nOverall outcome percentages use all selected rows.\nApplied: 2 (100.0%)\n", "Applied logged scores: n=2 mean=0.5 median=0.5\n")
}

func TestDistanceAnalysisActualMainSQLFailuresPublishNoPartialReport(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) string
		wants []string
	}{
		{"missing-table", func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-schema.db") }, []string{"query distance summaries", "no such table", "decisions"}},
		{"missing-column", func(t *testing.T) string {
			return oneCFixtureDB(t, nil, "ALTER TABLE decisions RENAME COLUMN winner_confidence TO other_score")
		}, []string{"query distance summaries", "no such column", "winner_confidence"}},
		{"bad-unknown-score-after-valid-row", func(t *testing.T) string {
			return oneCFixtureDB(t, []oneCFixtureRow{{1, 40, "applied", nil}, {2, 80, "pending", nil}},
				"UPDATE decisions SET winner_confidence='not-an-integer' WHERE distance=2")
		}, []string{"scan distance-summary row", "winner_confidence", "not-an-integer"}},
		{"null-unknown-score-after-valid-row", func(t *testing.T) string {
			return oneCFixtureDB(t, []oneCFixtureRow{{1, 40, "applied", nil}, {2, 80, "pending", nil}},
				"UPDATE decisions SET winner_confidence=NULL WHERE distance=2")
		}, []string{"scan distance-summary row", "winner_confidence", "converting NULL"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output, stderr := oneCMainResult(t, tc.setup(t), "run")
			if code != 1 || output != "" {
				t.Fatalf("failed actual main: status%d stdout=%q stderr=%q", code, output, stderr)
			}
			oneCRequireText(t, stderr, tc.wants...)
		})
	}
}

func TestDistanceAnalysisActualMainClosedOutputFollowsValidLoad(t *testing.T) {
	path := oneCFixtureDB(t, []oneCFixtureRow{{4, 1, "applied", nil}, {4, 2, "APPLIED", nil}, {4, 4, "Applied", nil}})
	code, output, stderr := oneCMainResult(t, path, "run")
	if code != 0 || stderr != "" || !strings.Contains(output, "Applied logged scores: n=3 mean=2.3 median=2.0\n") {
		t.Fatalf("paired valid output witness: status%d stdout=%q stderr=%q", code, output, stderr)
	}
	code, output, stderr = oneCMainResult(t, path, "closed-output")
	if code != 1 || output != "" || !strings.Contains(stderr, "write distance summary") || !strings.Contains(stderr, "file already closed") {
		t.Fatalf("output failure lost/failed before reporting: status%d stdout=%q stderr=%q", code, output, stderr)
	}
}
