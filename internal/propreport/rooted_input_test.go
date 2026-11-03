package propreport

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReportInputUsesProvidedSnapshotWithoutPathFallback(t *testing.T) {
	input := "2026/11/02 01:02:03 source=verified\n"
	entries, err := parseLogFileWithOpener(context.Background(), "missing-log", func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(input)), nil
	})
	if err != nil || len(entries) != 1 || !strings.Contains(entries[0], "source=verified") {
		t.Fatalf("entries = %v, error = %v", entries, err)
	}
	want := errors.New("snapshot unavailable")
	if _, err := parseLogFileWithOpener(context.Background(), "missing-log", func() (io.ReadCloser, error) {
		return nil, want
	}); !errors.Is(err, want) {
		t.Fatalf("opener error was not returned: %v", err)
	}
}
