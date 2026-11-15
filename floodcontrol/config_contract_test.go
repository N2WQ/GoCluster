package floodcontrol

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"

	"gopkg.in/yaml.v3"
)

func TestLoadedNoncanonicalFloodKeyEnforces(t *testing.T) {
	for _, key := range []string{"peer", " PEER "} {
		t.Run(key, func(t *testing.T) {
			cfg := loadFloodContractFixture(t, key, 1)
			downstream := make(chan *spot.Spot, 4)
			controller := New(cfg, downstream, nil, nil)
			now := time.Date(2026, time.November, 14, 12, 0, 0, 0, time.UTC)
			first := testSpot("W1AAA", spot.SourcePeer)
			second := testSpot("W1AAA", spot.SourcePeer)

			controller.process(now, first)
			if len(downstream) != 1 || <-downstream != first {
				t.Fatal("first PEER report was not forwarded")
			}
			controller.process(now, second)
			if len(downstream) != 0 {
				t.Fatal("excess PEER report bypassed the loaded threshold")
			}
			processed, observed, suppressed, dropped, overflow, actors := controller.GetStats()
			if processed != 2 || observed != 0 || suppressed != 1 || dropped != 0 || overflow != 0 || actors != 1 {
				t.Fatalf("unexpected decisions: processed=%d observed=%d suppressed=%d dropped=%d overflow=%d actors=%d",
					processed, observed, suppressed, dropped, overflow, actors)
			}
		})
	}
}

func TestLoadedZeroFloodThresholdPreservesOtherSource(t *testing.T) {
	cfg := loadFloodContractFixture(t, " PEER ", 0)
	downstream := make(chan *spot.Spot, 4)
	controller := New(cfg, downstream, nil, nil)
	now := time.Date(2026, time.November, 14, 12, 0, 0, 0, time.UTC)
	for range 2 {
		incoming := testSpot("W1AAA", spot.SourcePeer)
		controller.process(now, incoming)
		if len(downstream) != 1 || <-downstream != incoming {
			t.Fatal("explicit zero did not disable the PEER threshold")
		}
	}
	manual := testSpot("W1AAA", spot.SourceManual)
	controller.process(now, manual)
	if len(downstream) != 1 || <-downstream != manual {
		t.Fatal("first MANUAL report was not forwarded")
	}
	controller.process(now, testSpot("W1AAA", spot.SourceManual))
	if len(downstream) != 0 {
		t.Fatal("zero PEER threshold disabled MANUAL enforcement")
	}
	processed, observed, suppressed, dropped, overflow, actors := controller.GetStats()
	if processed != 4 || observed != 0 || suppressed != 1 || dropped != 0 || overflow != 0 || actors != 1 {
		t.Fatalf("unexpected source decisions: processed=%d observed=%d suppressed=%d dropped=%d overflow=%d actors=%d",
			processed, observed, suppressed, dropped, overflow, actors)
	}
}

// Load the real directory contract before exercising the consumer. Only the
// temporary flood policy changes; unrelated rails cannot supply the suppression.
func loadFloodContractFixture(t *testing.T, peerKey string, peerThreshold int) config.FloodControlConfig {
	t.Helper()
	source := filepath.Join("..", "data", "config")
	directory := t.TempDir()
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.EqualFold(filepath.Ext(entry.Name()), ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Name() == "floodcontrol.yaml" {
			var document map[string]any
			if err := yaml.Unmarshal(data, &document); err != nil {
				t.Fatal(err)
			}
			policy := floodContractMap(t, document, "flood_control")
			policy["enabled"] = true
			rails := floodContractMap(t, policy, "rails")
			for _, name := range []string{"source_node", "spotter_ip", "dxcall"} {
				floodContractMap(t, rails, name)["enabled"] = false
			}
			decall := floodContractMap(t, rails, "decall")
			decall["enabled"] = true
			decall["action"] = config.FloodActionSuppress
			thresholds := floodContractMap(t, decall, "thresholds_per_source_type")
			if _, exists := thresholds["PEER"]; !exists {
				t.Fatal("shipped fixture is missing the canonical PEER threshold")
			}
			delete(thresholds, "PEER")
			thresholds[peerKey] = peerThreshold
			thresholds["MANUAL"] = 1
			data, err = yaml.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(directory, entry.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := config.Load(directory)
	if err != nil {
		t.Fatalf("load flood consumer fixture: %v", err)
	}
	return loaded.FloodControl
}

func floodContractMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()
	child, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("shipped fixture %q is not a mapping", key)
	}
	return child
}
