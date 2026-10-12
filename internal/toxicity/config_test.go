package toxicity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigValidatesEnabledSecretEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "toxicity.yaml")
	if err := os.WriteFile(path, []byte(`enabled: true
endpoint: "https://worker.example.test/classify"
bearer_token_env: "DXC_TEST_TOXICITY_TOKEN"
timeout_ms: 100
workers: 1
queue_size: 2
cache_max_entries: 8
cache_ttl_seconds: 60
max_comment_bytes: 512
safe_gate_file: "toxicity_safe_gate.yaml"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DXC_TEST_TOXICITY_TOKEN", "")
	_, _, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "DXC_TEST_TOXICITY_TOKEN") {
		t.Fatalf("expected missing env error, got %v", err)
	}
	t.Setenv("DXC_TEST_TOXICITY_TOKEN", "secret")
	cfg, present, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig with env error: %v", err)
	}
	if !present || !cfg.Enabled {
		t.Fatalf("expected enabled config")
	}
}
