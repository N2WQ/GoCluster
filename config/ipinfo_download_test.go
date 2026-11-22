package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIPInfoDownloadCredential(t *testing.T) {
	const sampleToken = "sample-token"
	for _, tc := range []struct {
		name, override, file, wantToken, warning string
		enabled                                  bool
	}{
		{name: "public config without file"},
		{
			name: "public config with file", file: "\ufeff# local credentials\r\nIPINFO_DOWNLOAD_TOKEN=" + sampleToken + "\r\n",
			wantToken: sampleToken, enabled: true,
		},
		{
			name: "invalid value", file: "IPINFO_DOWNLOAD_TOKEN=" + sampleToken + "&unexpected\n",
			warning: "expected one unquoted IPINFO_DOWNLOAD_TOKEN assignment",
		},
		{
			name: "duplicate value", file: "IPINFO_DOWNLOAD_TOKEN=" + sampleToken + "\nIPINFO_DOWNLOAD_TOKEN=second\n",
			warning: "expected one unquoted IPINFO_DOWNLOAD_TOKEN assignment",
		},
		{
			name: "oversize", file: strings.Repeat("#", maxIPInfoTokenFile+1),
			warning: "expected a regular file no larger than 4 KiB",
		},
		{
			name: "explicit enable requires file", override: "  ipinfo_download_enabled: true\n",
			warning: "IPinfo download disabled for credential file",
		},
		{
			name: "auto requires a file reference", override: "  ipinfo_download_token: " + sampleToken + "\n",
			warning: "IPinfo download requires a nonempty file: reference",
		},
		{
			name: "explicit disable ignores file", override: "  ipinfo_download_enabled: false\n",
			file: "IPINFO_DOWNLOAD_TOKEN=" + sampleToken + "\n",
		},
		{
			name:      "explicit inline token",
			override:  "  ipinfo_download_enabled: true\n  ipinfo_download_token: " + sampleToken + "\n",
			wantToken: sampleToken, enabled: true,
		},
		{
			name: "disabled reputation ignores file", override: "  enabled: false\n",
			file: "invalid content",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			if tc.override != "" {
				writeTestConfigOverlay(t, dir, "reputation.yaml", "reputation:\n"+tc.override)
			}
			if tc.file != "" {
				secretsDir := filepath.Join(filepath.Dir(dir), "secrets")
				if err := os.Mkdir(secretsDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(secretsDir, ".env"), []byte(tc.file), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, diagnostics, err := LoadWithDiagnostics(dir)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Reputation.IPInfoDownloadEnabled != tc.enabled || cfg.Reputation.IPInfoDownloadToken != tc.wantToken {
				t.Fatalf("download enabled = %t or token mismatch, want enabled = %t", cfg.Reputation.IPInfoDownloadEnabled, tc.enabled)
			}
			ipinfoWarning := false
			for _, warning := range diagnostics.Warnings {
				if strings.Contains(warning, sampleToken) {
					t.Fatal("credential included in a diagnostic")
				}
				if strings.HasPrefix(warning, "IPinfo download ") {
					ipinfoWarning = true
					if tc.warning == "" || !strings.Contains(warning, tc.warning) {
						t.Fatalf("unexpected IPinfo warning: %s", warning)
					}
				}
			}
			if ipinfoWarning != (tc.warning != "") {
				t.Fatalf("IPinfo warning present = %t, want %q", ipinfoWarning, tc.warning)
			}
		})
	}
}
