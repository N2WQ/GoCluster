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

func TestDistance3MainChild(t *testing.T) {
	mode := os.Getenv("DXC_DISTANCE3_MAIN_CHILD")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing child argument separator")
	}
	os.Args = append([]string{"analyze_distance3"}, os.Args[separator+1:]...)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			t.Fatalf("close fixture stdout: %v", err)
		}
	}
	main()
	os.Exit(0)
}

// Reuse the exact currently executing stable test binary. The child calls main,
// and its deliberate exit is not launched with -test.paniconexit0.
func distance3MainResult(t *testing.T, path, mode string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestDistance3MainChild$", "--", "-db", path)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "DXC_DISTANCE3_MAIN_CHILD="+mode)
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

func TestDistance3ActualMainMixedAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []distance3FixtureRow
		wants  []string
	}{
		{"mixed", distance3MixedFixture(), []string{"Selected rows: 9\n", "Applied: 3 (33.3%)\nRejected: 2 (22.2%)\nUnknown: 4 (44.4%)\n", "Mean logged score: 40.0\nMinimum logged score: 10\n"}},
		{"empty", nil, []string{"Selected rows: 0\n", "Applied: 0 (N/A)\nRejected: 0 (N/A)\nUnknown: 0 (N/A)\n", "Mean logged score: N/A\n", "Recognized rows passing: 0/0 (N/A)"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := distance3FixtureDB(t, tc.values)
			code, out, stderr := distance3MainResult(t, path, "run")
			if code != 0 || stderr != "" {
				t.Fatalf("valid actual main status=%d stderr=%q stdout=%q", code, stderr, out)
			}
			distance3RequireText(t, out, append(tc.wants, "DISTANCE-3 HISTORICAL OBSERVATIONS\n", fmt.Sprintf("Database: %q", path))...)
			distance3RequireDescriptive(t, out)
		})
	}
}

func TestDistance3ActualMainSQLFailuresPublishNoPartialReport(t *testing.T) {
	for _, tc := range []struct {
		name        string
		setup       func(*testing.T) string
		want        []string
		missingFile bool
	}{
		{"missing-table", func(t *testing.T) string { return filepath.Join(t.TempDir(), "no-schema.db") }, []string{"query distance-3 decisions", "no such table", "decisions"}, true},
		{"missing-column", func(t *testing.T) string {
			return distance3FixtureDB(t, nil, "ALTER TABLE decisions RENAME COLUMN winner_confidence TO other_score")
		}, []string{"query distance-3 decisions", "no such column", "winner_confidence"}, false},
		{"bad-cell-after-valid-row", func(t *testing.T) string {
			return distance3FixtureDB(t, []distance3FixtureRow{
				distance3FixtureValue("GOOD", 90, "applied", "", 3),
				distance3FixtureValue("BAD", 80, "applied", "", 3),
			}, "UPDATE decisions SET winner_support='not-an-integer' WHERE subject='BAD'")
		}, []string{"scan distance-3 decision", "winner_support", "not-an-integer"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.setup(t)
			if tc.missingFile {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("missing-database precondition: %v", err)
				}
			}
			code, out, stderr := distance3MainResult(t, path, "run")
			if code != 1 || out != "" {
				t.Fatalf("failed actual main status=%d stdout=%q stderr=%q", code, out, stderr)
			}
			for _, want := range tc.want {
				if !strings.Contains(stderr, want) {
					t.Errorf("target SQL failure %q absent from stderr %q", want, stderr)
				}
			}
			if tc.missingFile {
				if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
					t.Fatalf("SQLite open effect lost: info=%v err=%v", info, err)
				}
			}
		})
	}
}

func TestDistance3ActualMainClosedOutputFailsAfterValidLoad(t *testing.T) {
	path := distance3FixtureDB(t, []distance3FixtureRow{distance3FixtureValue("WITNESS", 65, "applied", "", 3)})
	code, out, stderr := distance3MainResult(t, path, "run")
	if code != 0 || stderr != "" || !strings.Contains(out, "\"WITNESS\" -> \"W1XYZ\"") {
		t.Fatalf("paired valid input did not report witness: %d %q %q", code, out, stderr)
	}
	code, out, stderr = distance3MainResult(t, path, "closed-output")
	if code != 1 || out != "" || !strings.Contains(stderr, "write distance-3 analysis") || !strings.Contains(stderr, "file already closed") {
		t.Fatalf("output failure lost or failed before reporting: %d stdout=%q stderr=%q", code, out, stderr)
	}
}
