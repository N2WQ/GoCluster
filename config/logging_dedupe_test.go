package config

import (
	"path/filepath"
	"testing"
)

func TestLoggingDropDedupeWindowDefault(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  enabled: true
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Logging.DropDedupeWindowSeconds != 120 {
		t.Fatalf("expected default drop_dedupe_window_seconds=120, got %d", cfg.Logging.DropDedupeWindowSeconds)
	}
}

func TestLoggingDropDedupeWindowAllowsZero(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  drop_dedupe_window_seconds: 0
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Logging.DropDedupeWindowSeconds != 0 {
		t.Fatalf("expected drop_dedupe_window_seconds=0, got %d", cfg.Logging.DropDedupeWindowSeconds)
	}
}

func TestLoggingDropDedupeWindowRejectsNegative(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  drop_dedupe_window_seconds: -1
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	if _, err := Load(dir); err == nil {
		t.Fatalf("expected Load() to fail for negative logging.drop_dedupe_window_seconds")
	}
}

func TestDroppedCallLoggingUsesShippedYAMLWithPerFileLogsEnabled(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  enabled: true
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Logging.DroppedCalls.Enabled {
		t.Fatalf("expected dropped-call logging enabled from shipped YAML")
	}
	if filepath.Clean(cfg.Logging.DroppedCalls.Dir) != filepath.Join("data", "logs", "dropped_calls") {
		t.Fatalf("unexpected dropped-call dir %q", cfg.Logging.DroppedCalls.Dir)
	}
	if cfg.Logging.DroppedCalls.RetentionDays != 7 {
		t.Fatalf("expected retention_days=7, got %d", cfg.Logging.DroppedCalls.RetentionDays)
	}
	if cfg.Logging.DroppedCalls.DedupeWindowSeconds != 120 {
		t.Fatalf("expected dedupe_window_seconds=120, got %d", cfg.Logging.DroppedCalls.DedupeWindowSeconds)
	}
	if !cfg.Logging.DroppedCalls.BadDEDX || !cfg.Logging.DroppedCalls.NoLicense || !cfg.Logging.DroppedCalls.Harmonics {
		t.Fatalf("expected all dropped-call file toggles enabled by default: %+v", cfg.Logging.DroppedCalls)
	}
}

func TestDroppedCallLoggingHonorsExplicitToggles(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  dropped_calls:
    enabled: true
    dir: "custom/drop"
    retention_days: 3
    dedupe_window_seconds: 0
    bad_de_dx: false
    no_license: false
    harmonics: false
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Logging.DroppedCalls.Enabled {
		t.Fatalf("expected dropped-call logging enabled")
	}
	if cfg.Logging.DroppedCalls.Dir != "custom/drop" {
		t.Fatalf("unexpected dropped-call dir %q", cfg.Logging.DroppedCalls.Dir)
	}
	if cfg.Logging.DroppedCalls.RetentionDays != 3 {
		t.Fatalf("expected retention_days=3, got %d", cfg.Logging.DroppedCalls.RetentionDays)
	}
	if cfg.Logging.DroppedCalls.DedupeWindowSeconds != 0 {
		t.Fatalf("expected dedupe_window_seconds=0, got %d", cfg.Logging.DroppedCalls.DedupeWindowSeconds)
	}
	if cfg.Logging.DroppedCalls.BadDEDX || cfg.Logging.DroppedCalls.NoLicense || cfg.Logging.DroppedCalls.Harmonics {
		t.Fatalf("expected all dropped-call file toggles disabled: %+v", cfg.Logging.DroppedCalls)
	}
}

func TestDroppedCallLoggingRejectsNegativeBounds(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "retention",
			body: `logging:
  dropped_calls:
    retention_days: -1
`,
		},
		{
			name: "dedupe",
			body: `logging:
  dropped_calls:
    dedupe_window_seconds: -1
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "app.yaml", tc.body)
			if _, err := Load(dir); err == nil {
				t.Fatalf("expected Load() to reject %s", tc.name)
			}
		})
	}
}

func TestFileOnlyEventLogsUseShippedYAMLDefaults(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	cases := []struct {
		name string
		got  EventFileLoggingConfig
		dir  string
	}{
		{name: "login_attempts", got: cfg.Logging.LoginAttempts, dir: filepath.Join("data", "logs", "login_attempts")},
		{name: "reputation_drops", got: cfg.Logging.ReputationDrops, dir: filepath.Join("data", "logs", "reputation_drops")},
		{name: "telnet_connections", got: cfg.Logging.TelnetConnections, dir: filepath.Join("data", "logs", "telnet_connections")},
		{name: "ingest_connections", got: cfg.Logging.IngestConnections, dir: filepath.Join("data", "logs", "ingest_connections")},
		{name: "peer_connections", got: cfg.Logging.PeerConnections, dir: filepath.Join("data", "logs", "peer_connections")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.got.Enabled {
				t.Fatalf("expected %s enabled", tc.name)
			}
			if filepath.Clean(tc.got.Dir) != tc.dir {
				t.Fatalf("unexpected dir %q", tc.got.Dir)
			}
			if tc.got.RetentionDays != 7 {
				t.Fatalf("expected retention_days=7, got %d", tc.got.RetentionDays)
			}
			if tc.got.DedupeWindowSeconds != 120 {
				t.Fatalf("expected dedupe_window_seconds=120, got %d", tc.got.DedupeWindowSeconds)
			}
		})
	}
}

func TestFileOnlyEventLogsHonorExplicitZeroDedupe(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  login_attempts:
    enabled: true
    dir: "custom/login"
    retention_days: 3
    dedupe_window_seconds: 0
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Logging.LoginAttempts.Enabled {
		t.Fatalf("expected login attempt logging enabled")
	}
	if cfg.Logging.LoginAttempts.Dir != "custom/login" {
		t.Fatalf("unexpected dir %q", cfg.Logging.LoginAttempts.Dir)
	}
	if cfg.Logging.LoginAttempts.RetentionDays != 3 {
		t.Fatalf("expected retention_days=3, got %d", cfg.Logging.LoginAttempts.RetentionDays)
	}
	if cfg.Logging.LoginAttempts.DedupeWindowSeconds != 0 {
		t.Fatalf("expected dedupe_window_seconds=0, got %d", cfg.Logging.LoginAttempts.DedupeWindowSeconds)
	}
}

func TestFileOnlyEventLogsRejectNegativeBounds(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "retention",
			body: `logging:
  peer_connections:
    retention_days: -1
`,
		},
		{
			name: "dedupe",
			body: `logging:
  ingest_connections:
    dedupe_window_seconds: -1
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			writeTestConfigOverlay(t, dir, "app.yaml", tc.body)
			if _, err := Load(dir); err == nil {
				t.Fatalf("expected Load() to reject %s", tc.name)
			}
		})
	}
}

func TestPropagationLoggingUsesShippedYAMLDefaults(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if !cfg.Logging.Propagation.Enabled {
		t.Fatalf("expected propagation logging enabled")
	}
	if filepath.Clean(cfg.Logging.Propagation.Dir) != filepath.Join("data", "logs", "propagation") {
		t.Fatalf("unexpected propagation dir %q", cfg.Logging.Propagation.Dir)
	}
	if cfg.Logging.Propagation.RetentionDays != 7 {
		t.Fatalf("expected retention_days=7, got %d", cfg.Logging.Propagation.RetentionDays)
	}
}

func TestPropagationLoggingHonorsExplicitDisableAndRejectsNegativeRetention(t *testing.T) {
	dir := testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	cfgText := `logging:
  propagation:
    enabled: false
    dir: "custom/prop"
    retention_days: 3
`
	writeTestConfigOverlay(t, dir, "app.yaml", cfgText)

	cfg, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.Logging.Propagation.Enabled {
		t.Fatalf("expected propagation logging disabled")
	}
	if cfg.Logging.Propagation.Dir != "custom/prop" || cfg.Logging.Propagation.RetentionDays != 3 {
		t.Fatalf("unexpected propagation config: %+v", cfg.Logging.Propagation)
	}

	dir = testConfigDir(t)
	writeRequiredFloodControlFile(t, dir)
	writeTestConfigOverlay(t, dir, "app.yaml", `logging:
  propagation:
    retention_days: -1
`)
	if _, err := Load(dir); err == nil {
		t.Fatalf("expected Load() to reject negative propagation retention")
	}
}
