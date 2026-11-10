package config

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecondaryFrequencyChangesShippedPolicy(t *testing.T) {
	cfg, err := Load(testConfigDir(t))
	if err != nil {
		t.Fatal(err)
	}
	want := SecondaryFrequencyChangesConfig{
		State: "off", CWRTTYToleranceHz: 500, FTToleranceHz: 3000,
		OtherSkimmerToleranceHz: 1000, HumanToleranceHz: 2000, MaxChanges: 2,
	}
	if got := cfg.Dedup.SecondaryFrequencyChanges; got != want {
		t.Fatalf("shipped frequency-change policy = %+v, want %+v", got, want)
	}
}

func TestSecondaryFrequencyChangesRequiredPresence(t *testing.T) {
	for _, key := range []string{"", "state", "cw_rtty_tolerance_hz", "ft_tolerance_hz", "other_skimmer_tolerance_hz", "human_tolerance_hz", "max_changes"} {
		for _, null := range []bool{false, true} {
			name := key
			if name == "" {
				name = "section"
			}
			t.Run(fmt.Sprintf("%s/null=%t", name, null), func(t *testing.T) {
				dir := testConfigDir(t)
				path := []string{"dedup", "secondary_frequency_changes"}
				if key != "" {
					path = append(path, key)
				}
				if null {
					body := "dedup:\n  secondary_frequency_changes: null\n"
					if key != "" {
						body = "dedup:\n  secondary_frequency_changes:\n    " + key + ": null\n"
					}
					writeTestConfigOverlay(t, dir, "dedupe.yaml", body)
				} else {
					removeTestConfigKey(t, dir, "dedupe.yaml", path...)
				}
				_, err := Load(dir)
				if err == nil || !strings.Contains(err.Error(), "required YAML setting \""+strings.Join(path, ".")+"\"") {
					t.Fatalf("missing/null %s was not rejected: %v", strings.Join(path, "."), err)
				}
			})
		}
	}
}

func TestSecondaryFrequencyChangesRejectsInvalidValues(t *testing.T) {
	for _, key := range []string{"cw_rtty_tolerance_hz", "ft_tolerance_hz", "other_skimmer_tolerance_hz", "human_tolerance_hz", "max_changes"} {
		for _, value := range []string{"0", "-1", "3000.5", "3000.0", "\"3000\"", "9223372036854775808"} {
			t.Run(key+"="+value, func(t *testing.T) {
				dir := testConfigDir(t)
				writeFrequencyChangeScalar(t, dir, key, value)
				_, err := Load(dir)
				if err == nil || !strings.Contains(err.Error(), "dedup.secondary_frequency_changes."+key) {
					t.Fatalf("%s=%s was not rejected: %v", key, value, err)
				}
			})
		}
	}
	for _, state := range []string{"", "SHADOW", "observe"} {
		t.Run("state="+state, func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "dedupe.yaml", fmt.Sprintf("dedup:\n  secondary_frequency_changes:\n    state: %q\n", state))
			_, err := Load(dir)
			if err == nil || !strings.Contains(err.Error(), "dedup.secondary_frequency_changes.state") {
				t.Fatalf("state %q was not rejected: %v", state, err)
			}
		})
	}
}

// Preserve the scalar's YAML spelling; a parse/marshal helper can turn 3000.0
// into an integer before the production raw-value validator sees the file.
func writeFrequencyChangeScalar(t *testing.T, dir, key, value string) {
	t.Helper()
	path := filepath.Join(dir, "dedupe.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	found := false
	for i, line := range lines {
		if strings.HasPrefix(line, "    "+key+":") {
			lines[i], found = "    "+key+": "+value, true
		}
	}
	if !found {
		t.Fatalf("frequency-change scalar %s missing", key)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSecondaryFrequencyChangesChecksLoadedAveragingToleranceEvenOff(t *testing.T) {
	for _, tc := range []struct {
		averaging string
		cw        int64
		valid     bool
	}{
		{"499.5", 500, true},
		{"500", 500, true},
		{"500.25", 500, false},
		{"500.25", 501, true},
		{"9223372036854775808", math.MaxInt64, false},
		{"9223372036854774784", 9223372036854774783, false},
		{"9223372036854774784", 9223372036854774784, true},
	} {
		t.Run(fmt.Sprintf("average=%s/cw=%d", tc.averaging, tc.cw), func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "pipeline.yaml", "spot_policy:\n  frequency_averaging_tolerance_hz: "+tc.averaging+"\n")
			writeTestConfigOverlay(t, dir, "dedupe.yaml", fmt.Sprintf("dedup:\n  secondary_frequency_changes:\n    state: \"off\"\n    cw_rtty_tolerance_hz: %d\n", tc.cw))
			_, err := Load(dir)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "must be >= spot_policy.frequency_averaging_tolerance_hz") {
				t.Fatalf("expected averaging-floor rejection, got %v", err)
			}
		})
	}
}

func TestSecondaryFrequencyChangesConfiguredValuesReachReadback(t *testing.T) {
	for _, state := range []string{"off", "shadow", "on"} {
		t.Run(state, func(t *testing.T) {
			dir := testConfigDir(t)
			writeTestConfigOverlay(t, dir, "dedupe.yaml", fmt.Sprintf(`dedup:
  secondary_frequency_changes:
    state: %q
    cw_rtty_tolerance_hz: 701
    ft_tolerance_hz: 3100
    other_skimmer_tolerance_hz: 1100
    human_tolerance_hz: 2100
    max_changes: 3
`, state))
			cfg, err := Load(dir)
			if err != nil {
				t.Fatal(err)
			}
			want := SecondaryFrequencyChangesConfig{
				State: state, CWRTTYToleranceHz: 701, FTToleranceHz: 3100,
				OtherSkimmerToleranceHz: 1100, HumanToleranceHz: 2100, MaxChanges: 3,
			}
			if cfg.Dedup.SecondaryFrequencyChanges != want {
				t.Fatalf("configured policy was re-defaulted: %+v", cfg.Dedup.SecondaryFrequencyChanges)
			}
			var output strings.Builder
			cfg.PrintTo(&output)
			line := "Secondary frequency changes: state=" + state + " cw_rtty=701Hz ft=3100Hz other_skimmer=1100Hz human=2100Hz max_changes=3"
			if !strings.Contains(output.String(), line) {
				t.Fatalf("readback missing configured values: %s", line)
			}
		})
	}
}
