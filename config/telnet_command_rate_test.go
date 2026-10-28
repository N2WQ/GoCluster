package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadTelnetCommandRate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rate  int
		burst int
	}{
		{name: "shipped", rate: 5, burst: 30},
		{name: "disabled", rate: 0, burst: 1},
		{name: "minimum enabled", rate: 1, burst: 1},
		{name: "maximum", rate: 1_000_000_000, burst: 1_000_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			if tc.name != "shipped" {
				writeTestConfigOverlay(t, dir, "runtime.yaml", fmt.Sprintf("telnet:\n  command_rate_per_second: %d\n  command_burst: %d\n", tc.rate, tc.burst))
			}
			cfg, err := Load(dir)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if cfg.Telnet.CommandRatePerSecond != tc.rate || cfg.Telnet.CommandBurst != tc.burst {
				t.Fatalf("command rate/burst = %d/%d, want %d/%d", cfg.Telnet.CommandRatePerSecond, cfg.Telnet.CommandBurst, tc.rate, tc.burst)
			}
		})
	}
}

func TestLoadRejectsInvalidTelnetCommandRate(t *testing.T) {
	for _, key := range []string{"command_rate_per_second", "command_burst"} {
		for _, tc := range []struct {
			name  string
			value string
			want  string
		}{
			{name: "missing", want: "is missing"},
			{name: "null", value: "null", want: "must not be null"},
			{name: "negative", value: "-1", want: "requires an integer"},
			{name: "above maximum", value: "1000000001", want: "requires an integer"},
			{name: "unsigned overflow", value: "18446744073709551616", want: "requires an integer"},
			{name: "decimal", value: "1.5", want: "requires an integer"},
			{name: "integral float", value: "1.0", want: "requires an integer"},
			{name: "infinity", value: ".inf", want: "requires an integer"},
			{name: "quoted integer", value: `"5"`, want: "requires an integer"},
			{name: "boolean", value: "true", want: "requires an integer"},
			{name: "string", value: "not-a-number", want: "requires an integer"},
		} {
			t.Run(key+"/"+tc.name, func(t *testing.T) {
				dir := testConfigDir(t)
				if tc.name == "missing" {
					removeTestConfigKey(t, dir, "runtime.yaml", "telnet", key)
				} else {
					// Preserve scalar bytes: re-marshaling can turn 1.0 into 1.
					path := filepath.Join(dir, "runtime.yaml")
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					lines := strings.Split(string(data), "\n")
					found := false
					for i, line := range lines {
						if strings.HasPrefix(line, "  "+key+":") {
							lines[i] = "  " + key + ": " + tc.value
							found = true
						}
					}
					if !found {
						t.Fatalf("shipped runtime config lacks %s", key)
					}
					if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				_, err := Load(dir)
				if err == nil || !strings.Contains(err.Error(), "telnet."+key) || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Load() error = %v, want %s and %s", err, key, tc.want)
				}
			})
		}
	}
	t.Run("zero burst while enabled", func(t *testing.T) {
		dir := testConfigDir(t)
		writeTestConfigOverlay(t, dir, "runtime.yaml", "telnet:\n  command_rate_per_second: 5\n  command_burst: 0\n")
		_, err := Load(dir)
		if err == nil || !strings.Contains(err.Error(), "telnet.command_burst") {
			t.Fatalf("Load() error = %v, want invalid command burst", err)
		}
	})
	t.Run("zero burst while disabled", func(t *testing.T) {
		dir := testConfigDir(t)
		writeTestConfigOverlay(t, dir, "runtime.yaml", "telnet:\n  command_rate_per_second: 0\n  command_burst: 0\n")
		_, err := Load(dir)
		if err == nil || !strings.Contains(err.Error(), "telnet.command_burst") {
			t.Fatalf("Load() error = %v, want invalid command burst", err)
		}
	})
}
