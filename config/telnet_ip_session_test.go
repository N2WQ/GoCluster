package config

import (
	"strings"
	"testing"
)

func TestLoadTelnetIPSessionLimit(t *testing.T) {
	tests := []struct {
		name    string
		overlay string
		want    int
	}{
		{name: "shipped", want: 10},
		{name: "disabled", overlay: "0", want: 0},
		{name: "custom", overlay: "3", want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			if tc.overlay != "" {
				writeTestConfigOverlay(t, dir, "runtime.yaml", "telnet:\n  max_sessions_per_ip: "+tc.overlay+"\n")
			}
			cfg, err := Load(dir)
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got := cfg.Telnet.MaxSessionsPerIP; got != tc.want {
				t.Fatalf("telnet.max_sessions_per_ip = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLoadRejectsInvalidTelnetIPSessionLimit(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "missing", want: `required YAML setting "telnet.max_sessions_per_ip" is missing`},
		{name: "null", value: "null", want: `required YAML setting "telnet.max_sessions_per_ip" must not be null`},
		{name: "negative", value: "-1", want: `invalid YAML setting "telnet.max_sessions_per_ip"`},
		{name: "invalid type", value: "not-a-number", want: "cannot unmarshal"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			if tc.name == "missing" {
				removeTestConfigKey(t, dir, "runtime.yaml", "telnet", "max_sessions_per_ip")
			} else {
				writeTestConfigOverlay(t, dir, "runtime.yaml", "telnet:\n  max_sessions_per_ip: "+tc.value+"\n")
			}
			_, err := Load(dir)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load() error = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}
