package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSelectiveRescueYAMLOptionalBoolean(t *testing.T) {
	testOptionalCorrectionBoolean(t, "selective_rescue_enabled", "SelectiveRescueEnabled")
}

func TestCWSafeguardsYAMLOptionalBooleans(t *testing.T) {
	t.Run("shipped_disabled", func(t *testing.T) {
		dir := testConfigDir(t)
		writeRequiredFloodControlFile(t, dir)
		cfg, err := Load(dir)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.CallCorrection.CWFrequencySeparationEnabled || cfg.CallCorrection.CWBurstWPMEnabled {
			t.Fatal("CW call retention safeguards must ship disabled")
		}
	})
	t.Run("frequency", func(t *testing.T) {
		testOptionalCorrectionBoolean(t, "cw_frequency_separation_enabled", "CWFrequencySeparationEnabled")
	})
	t.Run("burst", func(t *testing.T) {
		testOptionalCorrectionBoolean(t, "cw_burst_wpm_enabled", "CWBurstWPMEnabled")
	})
}

func testOptionalCorrectionBoolean(t *testing.T, key, field string) {
	t.Helper()
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
			removeTestConfigKey(t, dir, "pipeline.yaml", "call_correction", key)
			if tc.value != "" {
				writeTestConfigOverlay(t, dir, "pipeline.yaml", "call_correction:\n  "+key+": "+tc.value+"\n")
			}
			cfg, diagnostics, err := LoadWithDiagnostics(dir)
			if tc.invalid {
				if err == nil || !strings.Contains(err.Error(), "call_correction."+key) || !strings.Contains(err.Error(), "boolean") {
					t.Fatalf("optional boolean admitted invalid value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, warning := range diagnostics.Warnings {
				if strings.Contains(warning, key) {
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
			value, present := fields[field]
			if present != tc.want || present && string(value) != "true" {
				t.Fatalf("false must preserve historical JSON shape, true must bind: %s", data)
			}
		})
	}
}
