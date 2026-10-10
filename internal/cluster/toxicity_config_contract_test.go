package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestRuntimeToxicityConfigContract(t *testing.T) {
	t.Setenv("DXC_TEST_RUNTIME_CONFIG_TOKEN", "fixture-only")
	for _, tc := range []struct {
		name    string
		enabled bool
		timeout string
		write   bool
		wantOK  bool
		active  bool
		gate    string
	}{
		{"absent", false, "0", false, true, false, "absent"},
		{"disabled-zero", false, "0", true, true, false, "valid"},
		{"enabled-zero", true, "0", true, true, true, "valid"},
		{"disabled-missing-gate", false, "0", true, true, false, "absent"},
		{"disabled-malformed-gate", false, "0", true, true, false, "malformed"},
		{"enabled-missing-gate", true, "0", true, false, false, "absent"},
		{"enabled-malformed-gate", true, "0", true, false, false, "malformed"},
		{"disabled-too-large", false, "2147483648", true, false, false, "valid"},
		{"enabled-too-large", true, "2147483648", true, false, false, "valid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.write {
				yaml := fmt.Sprintf("enabled: %t\nendpoint: http://127.0.0.1:1/classify\nbearer_token_env: DXC_TEST_RUNTIME_CONFIG_TOKEN\ntimeout_ms: %s\nworkers: 0\nqueue_size: 0\ncache_max_entries: 0\ncache_ttl_seconds: 0\nmax_comment_bytes: 0\nsafe_gate_file: gate.yaml\n", tc.enabled, tc.timeout)
				if err := os.WriteFile(filepath.Join(dir, "toxicity.yaml"), []byte(yaml), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.gate != "absent" {
				gate := "max_tokens: 8\nsafe_tokens: [CQ, POTA, '73']\nevent_prefixes: [POTA]\n"
				if tc.gate == "malformed" {
					gate = "max_tokens: null\n"
				}
				if err := os.WriteFile(filepath.Join(dir, "gate.yaml"), []byte(gate), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			r := &clusterRuntime{configSource: dir}
			t.Cleanup(r.retireOutputPipeline)
			if ok := r.initializeToxicityClassifier(); ok != tc.wantOK || (r.toxicityClassifier != nil) != tc.active {
				t.Fatalf("initialization=%t active=%t, want %t/%t", ok, r.toxicityClassifier != nil, tc.wantOK, tc.active)
			}
			if tc.active {
				// This input is wholly local. Startup/zero handling must not turn it
				// into HTTP work; port 1 is never contacted by a passing fixture.
				s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
				s.Comment = strings.Join([]string{"POTA", "73"}, " ")
				if !r.toxicityClassifier.ClassifyOrEnqueue(s, time.Now()) || s.ToxicityStatus != spot.ToxicitySafeLocal {
					t.Fatal("loaded gate did not produce synchronous local decision")
				}
				stats := r.toxicityClassifier.Snapshot()
				if stats.AICalls != 0 || stats.SafeBypass != 1 || r.toxicityClassifier.DrainTimeout() != 2650*time.Millisecond {
					t.Fatalf("unexpected production config consumer: %+v", stats)
				}
			}
			r.retireOutputPipeline()
			if r.toxicityClassifier != nil && (r.toxicityClassifier.Pending() != 0 || r.toxicityClassifier.ResultBacklog() != 0) {
				t.Fatal("runtime classifier retirement retained work")
			}
		})
	}
}
