package config

import (
	"fmt"
	"strings"
	"testing"
)

// Preserve the public loader's existing presence/value contract while the
// consumer begins enforcing the loaded value across both lookup namespaces.
func TestReputationLookupCacheLimitContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    int
		wantErr string
	}{
		{name: "shipped", want: 50000},
		{name: "missing", wantErr: `required YAML setting "reputation.lookup_cache_max_entries" is missing`},
		{name: "null", value: "null", wantErr: `required YAML setting "reputation.lookup_cache_max_entries" must not be null`},
		{name: "zero", value: "0", wantErr: `invalid YAML setting "reputation.lookup_cache_max_entries": must be > 0`},
		{name: "negative", value: "-1", wantErr: `invalid YAML setting "reputation.lookup_cache_max_entries": must be >= 0`},
		{name: "tiny", value: "1", want: 1},
		{name: "uneven", value: "33", want: 33},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			if tc.name == "missing" {
				removeTestConfigKey(t, dir, "reputation.yaml", "reputation", "lookup_cache_max_entries")
			} else if tc.value != "" {
				text := fmt.Sprintf("reputation:\n  lookup_cache_max_entries: %s\n", tc.value)
				writeTestConfigOverlay(t, dir, "reputation.yaml", text)
			}
			cfg, err := Load(dir)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load: %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := cfg.Reputation.LookupCacheMaxEntries; got != tc.want {
				t.Fatalf("lookup cache limit = %d, want existing normalized value %d", got, tc.want)
			}
		})
	}
}

func TestReputationIPInfoBooleansDefaultTrueWhenOmitted(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `reputation:
  enabled: true
`
	writeTestConfigOverlay(t, dir, "reputation.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Reputation.IPInfoPebbleLoadIPv4 {
		t.Fatalf("expected ipinfo_pebble_load_ipv4 to default true when omitted")
	}
	if !cfg.Reputation.IPInfoDeleteCSVAfterImport {
		t.Fatalf("expected ipinfo_delete_csv_after_import to default true when omitted")
	}
	if !cfg.Reputation.IPInfoKeepGzip {
		t.Fatalf("expected ipinfo_keep_gzip to default true when omitted")
	}
	if !cfg.Reputation.IPInfoPebbleCleanup {
		t.Fatalf("expected ipinfo_pebble_cleanup to default true when omitted")
	}
	if !cfg.Reputation.IPInfoPebbleCompact {
		t.Fatalf("expected ipinfo_pebble_compact to default true when omitted")
	}
}

func TestReputationIPInfoBooleansHonorExplicitFalse(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `reputation:
  enabled: true
  ipinfo_pebble_load_ipv4: false
  ipinfo_delete_csv_after_import: false
  ipinfo_keep_gzip: false
  ipinfo_pebble_cleanup: false
  ipinfo_pebble_compact: false
`
	writeTestConfigOverlay(t, dir, "reputation.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Reputation.IPInfoPebbleLoadIPv4 {
		t.Fatalf("expected explicit ipinfo_pebble_load_ipv4=false to remain false")
	}
	if cfg.Reputation.IPInfoDeleteCSVAfterImport {
		t.Fatalf("expected explicit ipinfo_delete_csv_after_import=false to remain false")
	}
	if cfg.Reputation.IPInfoKeepGzip {
		t.Fatalf("expected explicit ipinfo_keep_gzip=false to remain false")
	}
	if cfg.Reputation.IPInfoPebbleCleanup {
		t.Fatalf("expected explicit ipinfo_pebble_cleanup=false to remain false")
	}
	if cfg.Reputation.IPInfoPebbleCompact {
		t.Fatalf("expected explicit ipinfo_pebble_compact=false to remain false")
	}
}
