package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSelectiveRescueYAMLOptionalBoolean(t *testing.T) {
	for _, tc := range []struct {
		name, value   string
		want, invalid bool
	}{
		{name: "omitted"},
		{name: "false", value: "false"},
		{name: "true", value: "true", want: true},
		{name: "null", value: "null", invalid: true},
		{name: "zero", value: "0", invalid: true},
		{name: "string_false", value: "'false'", invalid: true},
		{name: "sequence", value: "[]", invalid: true},
		{name: "mapping", value: "{}", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testConfigDir(t)
			writeRequiredFloodControlFile(t, dir)
			removeTestConfigKey(t, dir, "pipeline.yaml", "call_correction", "selective_rescue_enabled")
			if tc.value != "" {
				writeTestConfigOverlay(t, dir, "pipeline.yaml", "call_correction:\n  selective_rescue_enabled: "+tc.value+"\n")
			}
			cfg, diagnostics, err := LoadWithDiagnostics(dir)
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "call_correction.selective_rescue_enabled") || !strings.Contains(err.Error(), "boolean") {
					t.Fatalf("optional boolean admitted invalid value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.CallCorrection.SelectiveRescueEnabled != tc.want {
				t.Fatalf("loaded enabled=%t, want %t", cfg.CallCorrection.SelectiveRescueEnabled, tc.want)
			}
			for _, warning := range diagnostics.Warnings {
				if strings.Contains(warning, "selective_rescue_enabled") {
					t.Fatalf("known optional boolean warned: %s", warning)
				}
			}
			data, err := json.Marshal(cfg.CallCorrection)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			value, present := fields["SelectiveRescueEnabled"]
			if present != tc.want || present && string(value) != "true" {
				t.Fatalf("false must preserve historical JSON shape, true must bind: %s", data)
			}
		})
	}
}
