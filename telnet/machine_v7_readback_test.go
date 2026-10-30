package telnet

import (
	"errors"
	"math"
	"strings"
	"testing"

	"dxcluster/filter"
	"gopkg.in/yaml.v3"
)

func TestV7MachineReadbackActiveTagAndCompleteBackup(t *testing.T) {
	cfg, rules := v7PreparedFixture(t)
	for _, active := range []string{"go", "cc"} {
		status := configurationReadbackStatus{Effective: readbackEffectiveStatus{Dialect: active}}
		for _, resource := range []string{"FILTER", "SETTINGS", "CONFIG", "CAPABILITIES"} {
			t.Run(active+"/"+resource, func(t *testing.T) {
				response, err := renderV7YAMLReadback(cfg, rules, status, map[string]readbackPresetStatus{"go": {}, "cc": {}}, resource, "MiXeD-01", "revision-1")
				if err != nil {
					t.Fatal(err)
				}
				var document struct {
					SchemaVersion int       `yaml:"schema_version"`
					RequestID     string    `yaml:"request_id"`
					Configuration yaml.Node `yaml:"configuration"`
					Status        struct {
						Active string `yaml:"active_dialect"`
					} `yaml:"status"`
				}
				if err := yaml.Unmarshal([]byte(response), &document); err != nil {
					t.Fatal(err)
				}
				if document.SchemaVersion != 7 || document.RequestID != "MiXeD-01" || document.Status.Active != active || !strings.HasPrefix(response, "---\r\n") || !strings.HasSuffix(response, "...\r\n") {
					t.Fatalf("wrong literal envelope: %q", response)
				}
				if resource == "FILTER" {
					var selected struct {
						Dialect string         `yaml:"dialect"`
						Filters map[string]any `yaml:"filters"`
					}
					if err := document.Configuration.Decode(&selected); err != nil {
						t.Fatal(err)
					}
					_, hasRules := selected.Filters["rules"]
					_, hasModes := selected.Filters["modes"]
					if selected.Dialect != active || hasRules != (active == "go") || hasModes != (active == "cc") {
						t.Fatal("FILTER exposed the wrong bank")
					}
				}
				if resource == "CONFIG" {
					configuration, err := yaml.Marshal(&document.Configuration)
					if err != nil {
						t.Fatal(err)
					}
					request, err := decodeV7MachineRequest(v7MachineRequestFixture(string(configuration), true), machineCommand{Verb: "PUT", Resource: "CONFIG", SchemaVersion: 7}, nil)
					if err != nil {
						t.Fatalf("complete export could not restore: %v", err)
					}
					got, err := request.apply(cfg, active)
					if err != nil || !got.Equal(cfg) {
						t.Fatalf("backup lost either bank or shared preferences: %v", err)
					}
				}
				if strings.Contains(response, "baseline:") || strings.Contains(response, "upgrade_notice:") {
					t.Fatal("readback exposed server-owned persistence metadata")
				}
			})
		}
	}
}

func TestV7MachineReadbackLargerThanLegacyLimit(t *testing.T) {
	cfg, rules := v7PreparedFixture(t)
	values := make([]int, 128)
	for i := range values {
		values[i] = math.MaxInt - i
	}
	for _, name := range []string{"DOC", "DXCTY"} {
		row := filter.CCCategory{Action: filter.CCPass, Countries: values}
		geography := filter.CCGeography{Global: row, Bands: make(map[string]filter.CCCategory)}
		for _, band := range filter.CCSupportedBands() {
			geography.Bands[band] = row
		}
		cfg.Filters.CC.Geography[name] = geography
	}
	cfg, _, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	status := configurationReadbackStatus{Effective: readbackEffectiveStatus{Dialect: "cc"}}
	response, err := renderV7YAMLReadback(cfg, rules, status, map[string]readbackPresetStatus{"go": {}, "cc": {}}, "CONFIG", "large-1", "revision-1")
	if err != nil || len(response) <= 65_536 || len(response) > 262_144 {
		t.Fatalf("large admitted export length=%d err=%v", len(response), err)
	}
	if _, err := encodeBoundedYAML(cfg); !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("fixture failed to cross the legacy bound")
	}
}

func TestV7MachineReadbackErrorsRemainVersionSeven(t *testing.T) {
	for _, response := range []string{
		renderV7YAMLCommandError("CONFIG", "error-1", "revision-1", "invalid_document", "Invalid native geography."),
		renderV7YAMLCommandError("CONFIG", "error-1", "revision-1", "invalid_document", strings.Repeat("x", 20_000)),
		renderV7YAMLCommandError("CONFIG", "error-1", "revision-1", "invalid_document", strings.Repeat("x", maxV7YAMLBytes)),
	} {
		var document struct {
			Version int               `yaml:"schema_version"`
			Error   map[string]string `yaml:"error"`
		}
		if err := yaml.Unmarshal([]byte(response), &document); err != nil {
			t.Fatal(err)
		}
		if document.Version != 7 || len(document.Error) == 0 || len(response) > 4_096 {
			t.Fatal("error/fallback lost its explicit version or small-frame bound")
		}
	}
	response, err := renderV7YAMLCommandSuccess("CONFIG", "valid-1", "revision-1", "VALIDATE", false, false)
	if err != nil || !strings.Contains(response, "schema_version: 7\r\n") || !strings.Contains(response, "applied: false\r\n") || !strings.Contains(response, "persisted: false\r\n") {
		t.Fatalf("validation result=%q err=%v", response, err)
	}
}

func TestV7MachineReadbackPresetStatusKeepsIndependentReferences(t *testing.T) {
	cfg, _ := v7PreparedFixture(t)
	goPreset, err := filter.V7PresetFromConfiguration(cfg, "go")
	if err != nil {
		t.Fatal(err)
	}
	ccPreset, err := filter.V7PresetFromConfiguration(cfg, "cc")
	if err != nil {
		t.Fatal(err)
	}
	refs := filter.V7PresetReferences{GO: &filter.V7PresetReference{Name: "GO", Baseline: goPreset}, CC: &filter.V7PresetReference{Name: "CC", Baseline: ccPreset}}
	cfg.Settings.Dialect = "go"
	status := v7ReadbackPresetStatuses(cfg, refs)
	if status["go"].Modified || status["cc"].Modified {
		t.Fatal("selection alone marked a preset modified")
	}
	cfg.Filters.CC.Modes = []string{}
	status = v7ReadbackPresetStatuses(cfg, refs)
	if status["go"].Modified || !status["cc"].Modified {
		t.Fatal("CC edit changed GO association or failed to mark CC")
	}
	cfg.Settings.Grid = "FN31"
	status = v7ReadbackPresetStatuses(cfg, refs)
	if !status["go"].Modified || !status["cc"].Modified {
		t.Fatal("shared edit did not mark both independent references")
	}
}
