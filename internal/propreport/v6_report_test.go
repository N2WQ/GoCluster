package propreport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReportReadsArchiveWithoutWritingInputDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "archive")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "01-Nov-2026.log")
	const line = "2026/11/01 12:00:00 Path buckets (5m): 20m f=2 c=1"
	if err := os.WriteFile(path, []byte(line+"\n"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(dir, 0o755)
		_ = os.Chmod(path, 0o644)
	})

	entries, err := ParseLog(path)
	if err != nil {
		t.Fatalf("read archive: %v", err)
	}
	if len(entries) != 1 || entries[0] != line {
		t.Fatalf("entries = %q", entries)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Name() != filepath.Base(path) {
		t.Fatalf("report modified input directory: %v", files)
	}
}

func TestReportOmitsLossAndCompletenessWarnings(t *testing.T) {
	result, _, output := generateRecoveryReport(t, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC), []string{
		"2026/11/01 12:00:00 GOCLUSTER_PROPAGATION_LOSS_V1 dropped=4 unconfirmed=0",
	})
	if strings.Contains(output, "Input warning:") || strings.Contains(output, "log completeness is unknown") {
		t.Fatalf("unexpected completeness warning: %s", output)
	}
	encoded, err := os.ReadFile(result.JSONPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"input_completeness"`) {
		t.Fatalf("unexpected completeness fields: %s", encoded)
	}
}
