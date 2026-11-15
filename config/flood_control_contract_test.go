package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

var floodThresholdPaths = [][]string{
	{"flood_control", "rails", "decall", "thresholds_per_source_type"},
	{"flood_control", "rails", "source_node", "thresholds_per_source_type"},
	{"flood_control", "rails", "spotter_ip", "thresholds_per_source_type"},
	{"flood_control", "rails", "dxcall", "thresholds_by_mode", "conservative"},
	{"flood_control", "rails", "dxcall", "thresholds_by_mode", "moderate"},
	{"flood_control", "rails", "dxcall", "thresholds_by_mode", "aggressive"},
}

func floodConfigNode(t *testing.T, root *yaml.Node, path []string) *yaml.Node {
	t.Helper()
	current := root.Content[0]
	for _, key := range path {
		var next *yaml.Node
		for i := 0; i+1 < len(current.Content); i += 2 {
			if current.Content[i].Value == key {
				next = current.Content[i+1]
				break
			}
		}
		if next == nil {
			t.Fatalf("missing fixture path %s", strings.Join(path, "."))
		}
		current = next
	}
	return current
}

// Nodes preserve float/null tags; generic map marshaling can turn 8.0 into 8
// before the loader sees the invalid scalar that this regression must exercise.
func editFloodConfig(t *testing.T, dir string, edit func(*yaml.Node)) {
	t.Helper()
	file := filepath.Join(dir, "floodcontrol.yaml")
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	edit(&root)
	data, err = yaml.Marshal(&root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func setFloodScalar(t *testing.T, root *yaml.Node, path []string, scalar string) {
	t.Helper()
	var replacement yaml.Node
	if err := yaml.Unmarshal([]byte(scalar), &replacement); err != nil {
		t.Fatal(err)
	}
	*floodConfigNode(t, root, path) = *replacement.Content[0]
}

func loadedFloodMaps(cfg *Config) []map[string]int {
	return []map[string]int{
		cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType,
		cfg.FloodControl.Rails.SourceNode.ThresholdsPerSourceType,
		cfg.FloodControl.Rails.SpotterIP.ThresholdsPerSourceType,
		cfg.FloodControl.Rails.DXCall.ThresholdsByMode.Conservative,
		cfg.FloodControl.Rails.DXCall.ThresholdsByMode.Moderate,
		cfg.FloodControl.Rails.DXCall.ThresholdsByMode.Aggressive,
	}
}

func TestLoadFloodCanonicalSourceKeys(t *testing.T) {
	baseline, err := Load(testConfigDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for index, path := range floodThresholdPaths {
		for _, spelling := range []string{"peer", "PeEr", " PEER "} {
			t.Run(strings.Join(path[2:], "/")+"/"+spelling, func(t *testing.T) {
				dir := testConfigDir(t)
				editFloodConfig(t, dir, func(root *yaml.Node) {
					node := floodConfigNode(t, root, path)
					for i := 0; i < len(node.Content); i += 2 {
						if node.Content[i].Value == "PEER" {
							node.Content[i].Value = spelling
						}
					}
				})
				before, err := os.ReadFile(filepath.Join(dir, "floodcontrol.yaml"))
				if err != nil {
					t.Fatal(err)
				}
				cfg, err := Load(dir)
				if err != nil {
					t.Fatal(err)
				}
				if got, want := loadedFloodMaps(cfg)[index], loadedFloodMaps(baseline)[index]; !reflect.DeepEqual(got, want) {
					t.Fatalf("effective map = %v, want %v", got, want)
				}
				after, err := os.ReadFile(filepath.Join(dir, "floodcontrol.yaml"))
				if err != nil || string(before) != string(after) {
					t.Fatalf("Load rewrote configuration: %v", err)
				}
			})
		}
	}
}

func TestLoadFloodRejectsSourceKeys(t *testing.T) {
	for _, path := range floodThresholdPaths {
		for _, variant := range []string{"missing", "unknown", "blank", "collision"} {
			t.Run(strings.Join(path[2:], "/")+"/"+variant, func(t *testing.T) {
				dir := testConfigDir(t)
				editFloodConfig(t, dir, func(root *yaml.Node) {
					node := floodConfigNode(t, root, path)
					for i := 0; i < len(node.Content); i += 2 {
						if node.Content[i].Value != "PEER" {
							continue
						}
						switch variant {
						case "missing":
							node.Content = append(node.Content[:i], node.Content[i+2:]...)
						case "unknown":
							node.Content[i].Value = "UNKNOWN"
						case "blank":
							node.Content[i].Value = " "
						case "collision":
							key := *node.Content[i]
							key.Value = " peer "
							node.Content = append(node.Content, &key, node.Content[i+1])
						}
						break
					}
				})
				var previous string
				for range 2 {
					cfg, err := Load(dir)
					if cfg != nil || err == nil || !strings.Contains(err.Error(), strings.Join(path, ".")) {
						t.Fatalf("%s source key accepted or wrong error: %v", variant, err)
					}
					if previous != "" && err.Error() != previous {
						t.Fatalf("nondeterministic error: %q then %q", previous, err)
					}
					previous = err.Error()
				}
			})
		}
	}
}

func floodIntegerPaths() [][]string {
	paths := [][]string{{"flood_control", "log_interval_seconds"}}
	for _, rail := range []string{"decall", "source_node", "spotter_ip", "dxcall"} {
		for _, key := range []string{"window_seconds", "max_entries_per_partition"} {
			paths = append(paths, []string{"flood_control", "rails", rail, key})
		}
	}
	for _, path := range floodThresholdPaths {
		paths = append(paths, append(append([]string(nil), path...), "PEER"))
	}
	return paths
}

func TestLoadFloodRejectsInvalidIntegers(t *testing.T) {
	for _, path := range floodIntegerPaths() {
		for _, scalar := range []string{"null", "1.5", "1.0", `"1"`, "true", "-1", "18446744073709551616"} {
			t.Run(strings.Join(path[1:], "/")+"="+scalar, func(t *testing.T) {
				dir := testConfigDir(t)
				editFloodConfig(t, dir, func(root *yaml.Node) {
					setFloodScalar(t, root, path, scalar)
				})
				if cfg, err := Load(dir); cfg != nil || err == nil || !strings.Contains(err.Error(), strings.Join(path, ".")) {
					t.Fatalf("invalid integer %s=%s: cfg=%v err=%v", strings.Join(path, "."), scalar, cfg != nil, err)
				}
			})
		}
	}
}

func TestLoadFloodIntegerBoundaries(t *testing.T) {
	seconds := min(int64(math.MaxInt), int64(math.MaxInt64)/int64(time.Second))
	for _, path := range floodIntegerPaths() {
		key := path[len(path)-1]
		if key == "max_entries_per_partition" {
			continue // Aggregate cardinality is exercised separately, without allocations.
		}
		limit := int64(math.MaxInt - 2)
		threshold := key == "PEER"
		if !threshold {
			limit = seconds
		}
		values := []int64{0, limit, limit + 1}
		if threshold {
			values = append(values, limit+2)
		}
		for _, n := range values {
			t.Run(fmt.Sprintf("%s=%d", strings.Join(path[1:], "/"), n), func(t *testing.T) {
				dir := testConfigDir(t)
				editFloodConfig(t, dir, func(root *yaml.Node) {
					setFloodScalar(t, root, path, fmt.Sprint(n))
				})
				cfg, err := Load(dir)
				valid := n == limit || (n == 0 && threshold)
				if valid && (err != nil || cfg == nil) {
					t.Fatalf("valid boundary rejected: %v", err)
				}
				if !valid && (err == nil || cfg != nil || !strings.Contains(err.Error(), strings.Join(path, "."))) {
					t.Fatalf("invalid boundary accepted or wrong error: %v", err)
				}
			})
		}
	}
}

func TestLoadFloodRequiredShapesEvenDisabled(t *testing.T) {
	paths := [][]string{{"flood_control"}, {"flood_control", "rails"}, {"flood_control", "rails", "decall"}, {"flood_control", "rails", "dxcall", "thresholds_by_mode"}}
	paths = append(paths, floodThresholdPaths...)
	for _, path := range paths {
		for _, shape := range []string{"null", "1", "[]"} {
			t.Run(strings.Join(path, "/")+"="+shape, func(t *testing.T) {
				dir := testConfigDir(t)
				editFloodConfig(t, dir, func(root *yaml.Node) {
					setFloodScalar(t, root, []string{"flood_control", "enabled"}, "false")
					setFloodScalar(t, root, path, shape)
				})
				if cfg, err := Load(dir); cfg != nil || err == nil || !strings.Contains(err.Error(), strings.Join(path, ".")) {
					t.Fatalf("disabled malformed block accepted or wrong error: %v", err)
				}
			})
		}
	}
}

func TestLoadFloodOverlayAndSentinels(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "runtime.yaml", "flood_control:\n  enabled: false\n  rails:\n    decall:\n      enabled: false\n      thresholds_per_source_type:\n        PEER: 0\n        MANUAL: 3\n")
	cfg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FloodControl.Enabled || cfg.FloodControl.Rails.DECall.Enabled || cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType["PEER"] != 0 || cfg.FloodControl.Rails.DECall.ThresholdsPerSourceType["MANUAL"] != 3 {
		t.Fatal("exact overlay precedence or zero/false semantics changed")
	}
}

func TestLoadFloodShippedPolicyUnchanged(t *testing.T) {
	dir := testConfigDir(t)
	data, err := os.ReadFile(filepath.Join(dir, "floodcontrol.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.FloodControl, decoded.FloodControl) {
		t.Fatalf("startup changed shipped flood policy: got %+v, want %+v", loaded.FloodControl, decoded.FloodControl)
	}
}

func TestLoadFloodIntegerChecksEvenDisabled(t *testing.T) {
	for _, flag := range [][]string{{"flood_control", "enabled"}, {"flood_control", "rails", "decall", "enabled"}} {
		t.Run(strings.Join(flag, "/"), func(t *testing.T) {
			dir := testConfigDir(t)
			editFloodConfig(t, dir, func(root *yaml.Node) {
				setFloodScalar(t, root, flag, "false")
				setFloodScalar(t, root, []string{"flood_control", "rails", "decall", "thresholds_per_source_type", "PEER"}, "1.0")
			})
			if cfg, err := Load(dir); cfg != nil || err == nil || !strings.Contains(err.Error(), "flood_control.rails.decall.thresholds_per_source_type.PEER") {
				t.Fatalf("disabled float threshold accepted or wrong error: %v", err)
			}
		})
	}
}

func TestLoadFloodMergedCanonicalCollision(t *testing.T) {
	dir := testConfigDir(t)
	writeTestConfigOverlay(t, dir, "runtime.yaml", "flood_control:\n  rails:\n    decall:\n      thresholds_per_source_type:\n        peer: 1\n")
	if cfg, err := Load(dir); cfg != nil || err == nil || !strings.Contains(err.Error(), "flood_control.rails.decall.thresholds_per_source_type") {
		t.Fatalf("merged differently spelled keys were not rejected: %v", err)
	}
}

func TestLoadFloodActorCardinality(t *testing.T) {
	for _, tc := range []struct {
		name       string
		deCap      int
		deSources  int
		nodeCap    int
		dxCap      int
		dxSelected int
		deDisabled bool
		globalOff  bool
		valid      bool
	}{
		{name: "one_partition_maximum", deCap: math.MaxInt, deSources: 1, valid: true},
		{name: "multiplication_overflow", deCap: math.MaxInt, deSources: 2},
		{name: "addition_boundary", deCap: math.MaxInt - 1, deSources: 1, nodeCap: 1, valid: true},
		{name: "addition_overflow", deCap: math.MaxInt, deSources: 1, nodeCap: 1},
		{name: "disabled_rail", deCap: math.MaxInt, deSources: 2, deDisabled: true, nodeCap: 1, valid: true},
		{name: "zero_thresholds", deCap: math.MaxInt, nodeCap: 1, valid: true},
		{name: "selected_dx_mode", dxCap: math.MaxInt, dxSelected: 1, valid: true},
		{name: "selected_dx_mode_overflow", dxCap: math.MaxInt, dxSelected: 2},
		{name: "global_disabled", deCap: math.MaxInt, deSources: 2, globalOff: true, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			editFloodConfig(t, dir, func(root *yaml.Node) {
				for _, rail := range []string{"decall", "source_node", "spotter_ip", "dxcall"} {
					setFloodScalar(t, root, []string{"flood_control", "rails", rail, "enabled"}, "false")
				}
				for _, path := range floodThresholdPaths {
					node := floodConfigNode(t, root, path)
					for i := 1; i < len(node.Content); i += 2 {
						node.Content[i].Tag, node.Content[i].Value = "!!int", "0"
					}
				}
				if tc.globalOff {
					setFloodScalar(t, root, []string{"flood_control", "enabled"}, "false")
				}
				for _, setting := range []struct {
					rail string
					cap  int
				}{{"decall", tc.deCap}, {"source_node", tc.nodeCap}, {"dxcall", tc.dxCap}} {
					if setting.cap == 0 {
						continue
					}
					base := []string{"flood_control", "rails", setting.rail}
					setFloodScalar(t, root, append(base, "enabled"), "true")
					setFloodScalar(t, root, append(base, "max_entries_per_partition"), fmt.Sprint(setting.cap))
				}
				if tc.deDisabled {
					setFloodScalar(t, root, []string{"flood_control", "rails", "decall", "enabled"}, "false")
				}
				for index := 0; index < tc.deSources; index++ {
					setFloodScalar(t, root, append(append([]string(nil), floodThresholdPaths[0]...), floodSupportedSourceTypes[index]), "1")
				}
				if tc.nodeCap > 0 {
					setFloodScalar(t, root, append(append([]string(nil), floodThresholdPaths[1]...), "PEER"), "1")
				}
				if tc.dxCap > 0 {
					setFloodScalar(t, root, []string{"flood_control", "rails", "dxcall", "active_mode"}, "moderate")
					// An inactive mode can have all seven sources enabled without
					// contributing to this process's actor cardinality.
					for _, source := range floodSupportedSourceTypes {
						setFloodScalar(t, root, append(append([]string(nil), floodThresholdPaths[3]...), source), "1")
					}
					for index := 0; index < tc.dxSelected; index++ {
						setFloodScalar(t, root, append(append([]string(nil), floodThresholdPaths[4]...), floodSupportedSourceTypes[index]), "1")
					}
				}
			})
			cfg, err := Load(dir)
			if tc.valid && (cfg == nil || err != nil) {
				t.Fatalf("valid aggregate rejected: %v", err)
			}
			if !tc.valid && (cfg != nil || err == nil || !strings.Contains(err.Error(), "combined active actor capacity")) {
				t.Fatalf("aggregate overflow accepted or wrong error: %v", err)
			}
		})
	}
}

func TestLoadFloodActorCapsRemainPositiveIntegers(t *testing.T) {
	for _, rail := range []string{"decall", "source_node", "spotter_ip", "dxcall"} {
		for _, scalar := range []string{"0", fmt.Sprint(uint64(math.MaxInt) + 1)} {
			t.Run(rail+"="+scalar, func(t *testing.T) {
				dir := testConfigDir(t)
				path := []string{"flood_control", "rails", rail, "max_entries_per_partition"}
				editFloodConfig(t, dir, func(root *yaml.Node) { setFloodScalar(t, root, path, scalar) })
				if cfg, err := Load(dir); cfg != nil || err == nil || !strings.Contains(err.Error(), strings.Join(path, ".")) {
					t.Fatalf("invalid cap accepted or wrong error: %v", err)
				}
			})
		}
	}
}

func FuzzFloodRawContract(f *testing.F) {
	for _, scalar := range []string{"0", "1", "null", "1.0", `"1"`, "true", "[]", "{}", "9223372036854775807"} {
		f.Add(uint8(0), scalar)
		f.Add(uint8(9), scalar)
		f.Add(uint8(255), scalar)
	}
	f.Fuzz(func(t *testing.T, selector uint8, scalar string) {
		if len(scalar) > 2048 {
			t.Skip()
		}
		var value any
		if err := yaml.Unmarshal([]byte(scalar), &value); err != nil {
			return
		}
		var raw map[string]any
		if err := yaml.Unmarshal([]byte(requiredFloodControlConfig), &raw); err != nil {
			t.Fatal(err)
		}
		paths := floodIntegerPaths()
		path := paths[int(selector)%len(paths)]
		if selector == 255 {
			path = []string{"flood_control", "rails"}
		}
		parent := raw
		for _, key := range path[:len(path)-1] {
			parent = parent[key].(map[string]any)
		}
		parent[path[len(path)-1]] = value
		err := validateRawFloodControl(raw)
		if selector != 255 {
			if _, integer := value.(int); !integer && err == nil {
				t.Fatalf("accepted noninteger %T at %s", value, strings.Join(path, "."))
			}
		}
	})
}
