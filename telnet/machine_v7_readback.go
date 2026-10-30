// File role: Bounded schema-seven views with explicit active-bank ownership.
// Full CONFIG is the complete backup; ordinary FILTER never exports another
// dialect as active. Preset baselines and server receipt metadata stay read-only.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/filter"
)

// Errors use the ordinary small-control queue without a large-output lease.
// Preparation can quote an invalid uploaded scalar, so diagnostics have their
// own bound, independent of the admitted configuration/export envelope.
const maxV7ErrorBytes = 4 << 10

type v7ReadbackStatus struct {
	configurationReadbackStatus `yaml:",inline"`
	ActiveDialect               string                          `yaml:"active_dialect"`
	Presets                     map[string]readbackPresetStatus `yaml:"presets"`
}

type v7DocumentError struct {
	Code    string `yaml:"code"`
	Message string `yaml:"message"`
}

type v7CommandResult struct {
	Operation string `yaml:"operation"`
	Valid     bool   `yaml:"valid"`
	Applied   bool   `yaml:"applied"`
	Persisted bool   `yaml:"persisted"`
}

func v7ReadbackPresetStatuses(cfg filter.V7Configuration, refs filter.V7PresetReferences) map[string]readbackPresetStatus {
	result := map[string]readbackPresetStatus{"go": {}, "cc": {}}
	for dialect, ref := range map[string]*filter.V7PresetReference{"go": refs.GO, "cc": refs.CC} {
		if ref != nil {
			result[dialect] = readbackPresetStatus{Associated: true, Name: ref.Name, Modified: !ref.Baseline.MatchesConfiguration(cfg)}
		}
	}
	return result
}

func renderV7YAMLReadback(cfg filter.V7Configuration, goSet *filter.PreparedRuleSet, status configurationReadbackStatus, presets map[string]readbackPresetStatus, resource, requestID, revision string) (string, error) {
	resource = strings.ToUpper(resource)
	active := status.Effective.Dialect
	if active != "go" && active != "cc" {
		return "", fmt.Errorf("active dialect must be go or cc")
	}
	status.Configured = cfg.Settings
	status.Preset = presets[active]
	if len(presets) > 2 || !readbackMetadataFits(status, requestID, revision) || len(cfg.Filters.GO.Rules) > filter.MaxRuleSlots || goSet.Counts().Rules > filter.MaxRuleSlots {
		return "", errReadbackTooLarge
	}
	for dialect, preset := range presets {
		if (dialect != "go" && dialect != "cc") || len(preset.Name) > 32 {
			return "", errReadbackTooLarge
		}
	}
	var configuration any
	switch resource {
	case "FILTER":
		var bank any = cfg.Filters.GO
		if active == "cc" {
			bank = cfg.Filters.CC
		}
		configuration = struct {
			Dialect string `yaml:"dialect"`
			Filters any    `yaml:"filters"`
		}{active, bank}
	case "SETTINGS":
		configuration = cfg.Settings
	case "CONFIG":
		configuration = cfg
	case "CAPABILITIES":
		configuration = v7MachineCapabilities()
	default:
		return "", fmt.Errorf("unknown YAML resource")
	}
	return encodeBoundedYAMLLimit(struct {
		SchemaVersion int              `yaml:"schema_version"`
		RequestID     string           `yaml:"request_id"`
		Resource      string           `yaml:"resource"`
		Revision      string           `yaml:"revision"`
		Configuration any              `yaml:"configuration"`
		Status        v7ReadbackStatus `yaml:"status"`
	}{7, requestID, resource, revision, configuration, v7ReadbackStatus{status, active, presets}}, maxV7YAMLBytes)
}

func renderV7YAMLCommandError(resource, requestID, revision, code, message string) string {
	if len(resource)+len(requestID)+len(revision)+len(code)+len(message) <= maxV7ErrorBytes {
		response, err := encodeBoundedYAMLLimit(struct {
			SchemaVersion int             `yaml:"schema_version"`
			RequestID     string          `yaml:"request_id"`
			Resource      string          `yaml:"resource"`
			Revision      string          `yaml:"revision,omitempty"`
			Error         v7DocumentError `yaml:"error"`
		}{7, requestID, resource, revision, v7DocumentError{code, message}}, maxV7ErrorBytes)
		if err == nil {
			return response
		}
	}
	return "---\r\nschema_version: 7\r\nrequest_id: \"\"\r\nresource: \"\"\r\nerror:\r\n  code: response_too_large\r\n  message: Error details exceed the response limit.\r\n...\r\n"
}

func renderV7YAMLCommandSuccess(resource, requestID, revision, verb string, applied, persisted bool) (string, error) {
	if (verb != "PUT" && verb != "PATCH" && verb != "VALIDATE") || !machineResourceAllowed(verb, resource) {
		return "", schemaError("configuration", "unsupported command or resource")
	}
	if len(resource)+len(requestID)+len(revision)+len(verb) > maxYAMLBytes {
		return "", errReadbackTooLarge
	}
	return encodeBoundedYAMLLimit(struct {
		SchemaVersion int             `yaml:"schema_version"`
		RequestID     string          `yaml:"request_id"`
		Resource      string          `yaml:"resource"`
		Revision      string          `yaml:"revision"`
		Result        v7CommandResult `yaml:"result"`
	}{7, requestID, resource, revision, v7CommandResult{verb, true, applied, persisted}}, maxYAMLBytes)
}
