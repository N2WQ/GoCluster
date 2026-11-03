package config

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"dxcluster/internal/logutil"
)

func TestLogFolderNameConflicts(t *testing.T) {
	shared := filepath.Join("logs", "shared")
	for _, tc := range []struct {
		name     string
		config   Config
		conflict bool
	}{
		{"same folder", Config{Logging: LoggingConfig{Enabled: true, Dir: shared, Propagation: PropagationLoggingConfig{Enabled: true, Dir: filepath.Join("logs", "x", "..", "shared")}}}, true},
		{"different folders", Config{Logging: LoggingConfig{Enabled: true, Dir: shared, Propagation: PropagationLoggingConfig{Enabled: true, Dir: filepath.Join(shared, "propagation")}}}, false},
		{"disabled category", Config{Logging: LoggingConfig{Enabled: false, Dir: shared, Propagation: PropagationLoggingConfig{Enabled: true, Dir: shared}}}, false},
		{"dropped-call subfolder", Config{Logging: LoggingConfig{LoginAttempts: EventFileLoggingConfig{Enabled: true, Dir: filepath.Join(shared, "bad_de_dx")}, DroppedCalls: DroppedCallLoggingConfig{Enabled: true, Dir: shared, BadDEDX: true}}}, true},
		{"disabled dropped-call file", Config{Logging: LoggingConfig{LoginAttempts: EventFileLoggingConfig{Enabled: true, Dir: filepath.Join(shared, "bad_de_dx")}, DroppedCalls: DroppedCallLoggingConfig{Enabled: true, Dir: shared}}}, false},
		{"peer overlong folder", Config{Logging: LoggingConfig{Enabled: true, Dir: filepath.Dir(logutil.PeerOverlongPath)}, Peering: PeeringConfig{Enabled: true}}, true},
		{"disabled peering", Config{Logging: LoggingConfig{Enabled: true, Dir: filepath.Dir(logutil.PeerOverlongPath)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLogFolderNames(&tc.config)
			if (err != nil) != tc.conflict {
				t.Fatalf("conflict=%t error=%v", tc.conflict, err)
			}
		})
	}
	if runtime.GOOS == "windows" {
		cfg := Config{Logging: LoggingConfig{Enabled: true, Dir: `logs\PEER`, Propagation: PropagationLoggingConfig{Enabled: true, Dir: `logs\peer`}}}
		if err := validateLogFolderNames(&cfg); err == nil {
			t.Fatal("Windows case-insensitive folder collision accepted")
		}
	}
}

func TestLoadRejectsDuplicateLogFolders(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "app.yaml", `logging:
  enabled: true
  dir: "logs/shared"
  propagation:
    enabled: true
    dir: "logs/other/../shared"
`)
	_, err := Load(dir)
	if err == nil || !strings.Contains(err.Error(), "must use separate log folders") {
		t.Fatalf("expected log folder collision error, got %v", err)
	}
}
