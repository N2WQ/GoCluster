package cluster

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestVOACAPFallbackSupportedOnGOOS(t *testing.T) {
	tests := []struct {
		goos string
		want bool
	}{
		{goos: "windows", want: true},
		{goos: "linux", want: false},
		{goos: "darwin", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.goos, func(t *testing.T) {
			if got := voacapFallbackSupportedOnGOOS(tt.goos); got != tt.want {
				t.Fatalf("voacapFallbackSupportedOnGOOS(%q) = %v, want %v", tt.goos, got, tt.want)
			}
		})
	}
}

func TestInitializeGridStoreLogsStartupErrorOnOpenFailure(t *testing.T) {
	var logs bytes.Buffer
	oldOutput := log.Writer()
	oldFlags := log.Flags()
	oldPrefix := log.Prefix()
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	defer func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	}()

	dbPath := filepath.Join(t.TempDir(), "grid.db")
	if err := os.WriteFile(dbPath, []byte("not a pebble directory"), 0o600); err != nil {
		t.Fatalf("write grid db placeholder: %v", err)
	}
	r := newClusterRuntime(BuildInfo{}, &config.Config{GridDBPath: dbPath}, "", config.LoadDiagnostics{})

	if r.initializeGridStore() {
		t.Fatalf("expected initializeGridStore to fail")
	}
	if r.startupErr == nil || !strings.Contains(r.startupErr.Error(), "Gridstore: failed to open grid database") {
		t.Fatalf("expected gridstore startup error, got %v", r.startupErr)
	}
	if !strings.Contains(logs.String(), "Gridstore: failed to open grid database") {
		t.Fatalf("expected gridstore failure in startup log, got %q", logs.String())
	}
}
