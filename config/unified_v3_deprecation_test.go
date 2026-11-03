package config

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestLegacySecondaryDedupWarningUsesDiagnosticsNotStdout(t *testing.T) {
	for _, tc := range []struct{ name, overlay string }{
		{"zero window", "dedup:\n  secondary_window_seconds: 0\n"},
		{"false preference", "dedup:\n  secondary_prefer_stronger_snr: false\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			baseline, baselineDiag, err := LoadWithDiagnostics(dir)
			if err != nil {
				t.Fatal(err)
			}
			if containsDiagnostic(baselineDiag.Warnings, "dedup.secondary_window_seconds and dedup.secondary_prefer_stronger_snr") {
				t.Fatalf("legacy warning without old key: %v", baselineDiag.Warnings)
			}
			writeTestConfigOverlay(t, dir, "dedupe.yaml", tc.overlay)
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			old := os.Stdout
			os.Stdout = writer
			cfg, diag, loadErr := LoadWithDiagnostics(dir)
			os.Stdout = old
			_ = writer.Close()
			output, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if loadErr != nil || readErr != nil {
				t.Fatalf("load=%v read=%v", loadErr, readErr)
			}
			if strings.Contains(string(output), "deprecated") {
				t.Fatalf("loader printed directly to stdout: %q", output)
			}
			if !containsDiagnostic(diag.Warnings, "dedup.secondary_window_seconds and dedup.secondary_prefer_stronger_snr are deprecated and ignored") {
				t.Fatalf("legacy warning missing: %v", diag.Warnings)
			}
			if cfg.Dedup.SecondaryFastWindowSeconds != baseline.Dedup.SecondaryFastWindowSeconds || cfg.Dedup.SecondaryMedWindowSeconds != baseline.Dedup.SecondaryMedWindowSeconds || cfg.Dedup.SecondarySlowWindowSeconds != baseline.Dedup.SecondarySlowWindowSeconds || cfg.Dedup.SecondaryFastPreferStrong != baseline.Dedup.SecondaryFastPreferStrong || cfg.Dedup.SecondaryMedPreferStrong != baseline.Dedup.SecondaryMedPreferStrong || cfg.Dedup.SecondarySlowPreferStrong != baseline.Dedup.SecondarySlowPreferStrong {
				t.Fatal("legacy key changed effective dedup settings")
			}
		})
	}
}
